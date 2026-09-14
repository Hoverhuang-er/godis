package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Hoverhuang-er/godis/internal/ai"
	"github.com/Hoverhuang-er/godis/internal/cluster"
	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/database"
	idatabase "github.com/Hoverhuang-er/godis/internal/interface/database"
	_ "github.com/Hoverhuang-er/godis/internal/lib/greenteagc"
	"github.com/Hoverhuang-er/godis/internal/lib/logger"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	rclient "github.com/Hoverhuang-er/godis/internal/redis/client"
	"github.com/Hoverhuang-er/godis/internal/redis/server/gnet"
	stdserver "github.com/Hoverhuang-er/godis/internal/redis/server/std"
	"github.com/Hoverhuang-er/godis/internal/sentinel"
	"github.com/Hoverhuang-er/godis/internal/web"
	"log/slog"
)

// setupAI installs the embedding backend from the [ai] config section. A bad
// HTTP provider setting is fatal rather than silently downgraded to the local
// embedder, because the two produce incomparable vectors and mixing them would
// corrupt an existing index.
func setupAI() {
	opts := ai.OptionsFromConfig(config.Properties)
	embedder, err := ai.NewEmbedder(opts)
	if err != nil {
		slog.Error("invalid [ai] embedding configuration: " + err.Error())
		os.Exit(1)
	}
	ai.SetEmbedder(embedder)
	ai.SetChunkOptions(ai.ChunkOptions{Size: opts.Size, Overlap: opts.Overlap})
	slog.Info("AI embedder ready", "provider", embedder.Name(), "dim", embedder.Dim())
}

var banner = `
   ______          ___
  / ____/___  ____/ (_)____
 / / __/ __ \/ __  / / ___/
/ /_/ / /_/ / /_/ / (__  )
\____/\____/\__,_/_/____/
`

var defaultProperties = &config.ServerProperties{
	Bind:           "0.0.0.0",
	Port:           6379,
	AppendOnly:     false,
	AppendFilename: "",
	MaxClients:     1000,
	RunID:          utils.RandString(40),

	// HTTP API defaults
	HttpApiEnabled: true,
	HttpApiPort:    63790,
	HttpApiHost:    "127.0.0.1",
	// Web Dashboard defaults
	WebPort: 63800,
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

// bootstrapConfNamespace writes the shared local config into the conf
// namespace of whatever engine is active (standalone server or cluster).
func bootstrapConfNamespace(db idatabase.DB) {
	switch d := db.(type) {
	case *database.Server:
		database.BootstrapConfNamespace(d)
	case *cluster.Cluster:
		d.BootstrapConfNamespace()
	}
}

// parseJoinFlag extracts the --join <addr> / --join=<addr> target from the
// command line. Returns "" when absent.
func parseJoinFlag(args []string) string {
	for i := 0; i < len(args); i++ {
		if args[i] == "--join" && i+1 < len(args) {
			return args[i+1]
		}
		if len(args[i]) > len("--join=") && args[i][:len("--join=")] == "--join=" {
			return args[i][len("--join="):]
		}
	}
	return ""
}
func main() {
	// Check for hidden flags before any other setup
	joinTarget := parseJoinFlag(os.Args[1:])
	for _, arg := range os.Args[1:] {
		if arg == "--cli" {
			runCLI()
			return
		}
		if arg == "--web" {
			startWebDashboard()
			return
		}
		if arg == "--migrate" {
			runMigrate()
			return
		}
	}
	print(banner)
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	})

	// Determine config source
	nacosAddr := os.Getenv("NACOS_ADDR")
	if nacosAddr != "" {
		// Load config from Nacos config center
		namespaceId := os.Getenv("NACOS_NAMESPACE")
		group := os.Getenv("NACOS_GROUP")
		if group == "" {
			group = "DEFAULT_GROUP"
		}
		dataId := os.Getenv("NACOS_DATA_ID")
		if dataId == "" {
			dataId = "godis"
		}
		config.SetupConfigFromNacos(nacosAddr, namespaceId, group, dataId)
	} else {
		// Detect local config file
		configFilename, found := config.DetectConfigFile()
		if found {
			config.SetupConfig(configFilename)
		} else {
			config.Properties = defaultProperties
		}
	}
	// --join: switch this node into cluster mode and join the given seed.
	// host:port uses the TCP join channel; http(s)://host:port uses the
	// seed's HTTP API. The seed auto-promotes on receiving the join.
	if joinTarget != "" {
		config.Properties.ClusterEnable = true
		switch {
		case strings.HasPrefix(joinTarget, "http://"):
			config.Properties.ClusterJoinHTTP = strings.TrimPrefix(joinTarget, "http://")
		case strings.HasPrefix(joinTarget, "https://"):
			// ponytail: httpJoinCluster hardcodes http://; https joins
			// work only if the seed terminates TLS in front of the API.
			config.Properties.ClusterJoinHTTP = strings.TrimPrefix(joinTarget, "https://")
		default:
			config.Properties.ClusterSeed = joinTarget
		}
	}
	listenAddr := net.JoinHostPort(config.Properties.Bind, strconv.Itoa(config.Properties.Port))

	// The embedding backend serves the knowledge-base commands and must be in
	// place before the first client can issue one.
	setupAI()

	var shutdownDB idatabase.DB
	stopCh := make(chan struct{})

	web.RegisterClusterStatusFn(func() (map[string]any, bool) {
		cl := cluster.Current()
		if cl == nil {
			return nil, false
		}
		stats := cl.ClusterStats()
		return map[string]any{
			"enabled":           stats.Enabled,
			"cluster_id":        config.Properties.ClusterID,
			"mode":              stats.Mode,
			"role":              stats.Role,
			"master_id":         stats.MasterID,
			"master_addr":       stats.MasterAddr,
			"node_count":        stats.NodeCount,
			"sync_lag_seconds":  stats.SyncLagSeconds,
			"wal_seq_applied":   stats.WalSeqApplied,
			"wal_seq_published": stats.WalSeqPublished,
		}, true
	})

	if config.Properties.UseGnet {
		var db idatabase.DB
		if config.Properties.ClusterEnable {
			db = cluster.MakeCluster(nil)
		} else {
			db = database.NewStandaloneServer()
		}
		bootstrapConfNamespace(db)
		shutdownDB = db
		srv := gnet.NewGnetServer(db)

		cluster.RegisterHotSwitch(srv)
		cluster.RegisterJoinIntercept(srv)

		if config.Properties.HttpApiEnabled {
			apiAddr := net.JoinHostPort(config.Properties.HttpApiHost, strconv.Itoa(config.Properties.HttpApiPort))
			redisAddr := net.JoinHostPort(config.Properties.Bind, strconv.Itoa(config.Properties.Port))
			apiSrv := web.NewApiServer(apiAddr, redisAddr, config.Properties.RequirePass)
			apiSrv.Start()
		}

		go func() { _ = srv.Run(listenAddr) }()
	} else {
		handler := stdserver.MakeHandler()
		bootstrapConfNamespace(handler.CurrentDB())

		cluster.RegisterHotSwitch(handler)
		cluster.RegisterJoinIntercept(handler)

		if config.Properties.HttpApiEnabled {
			apiAddr := net.JoinHostPort(config.Properties.HttpApiHost, strconv.Itoa(config.Properties.HttpApiPort))
			redisAddr := net.JoinHostPort(config.Properties.Bind, strconv.Itoa(config.Properties.Port))
			apiSrv := web.NewApiServer(apiAddr, redisAddr, config.Properties.RequirePass)
			apiSrv.Start()
		}

		go func() { _ = stdserver.Serve(listenAddr, handler, stopCh) }()
	}

	// Sentinel: optional RESP server exposing the SENTINEL command surface
	// for failover of externally-monitored masters.
	if config.Properties.SentinelEnabled {
		sentCfg := sentinel.Config{
			Enabled:  true,
			Port:     config.Properties.SentinelPort,
			SelfAddr: config.Properties.AnnounceAddress(),
		}
		for _, mc := range config.Properties.SentinelMonitors {
			sentCfg.Monitors = append(sentCfg.Monitors, sentinel.MonitorConfig{
				Name:        mc.Name,
				Host:        mc.Host,
				Port:        mc.Port,
				Quorum:      mc.Quorum,
				DownAfterMs: mc.DownAfterMs,
			})
		}
		sen := sentinel.New(sentCfg)
		sen.Start()
		sentinelAddr := net.JoinHostPort(config.Properties.Bind, strconv.Itoa(config.Properties.SentinelPort))
		go func() { _ = sentinel.Serve(sentinelAddr, sentinel.NewHandler(sen), stopCh) }()
		slog.Info("sentinel started", "addr", sentinelAddr, "monitors", len(sentCfg.Monitors))
	}

	// Wait for SIGINT/SIGTERM for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("received shutdown signal, gracefully shutting down...", "signal", sig)

	// Step 1: stop accepting new connections, drain in-flight requests
	slog.Info("step 1/3: stopping tcp listener and draining connections...")
	close(stopCh)
	time.Sleep(500 * time.Millisecond) // brief pause for listener to close

	// Step 2: flush data to bolt
	if shutdownDB != nil {
		slog.Info("step 2/3: flushing data to ~/.godis ...")
		start := time.Now()
		if closer, ok := shutdownDB.(interface{ Close() }); ok {
			closer.Close()
		}
		slog.Info("step 2/3: data flush complete",
			"elapsed", time.Since(start).Round(time.Millisecond).String())
	}
	slog.Info("step 3/3: godis shutdown complete")
}
func startWebDashboard() {
	flags := parseCLIFlags()
	addr := net.JoinHostPort(flags.host, strconv.Itoa(flags.port))

	fmt.Fprintf(os.Stderr, "Connecting to godis at %s\n", addr)

	c, err := rclient.MakeClient(addr)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to connect to godis: %v", err))
		os.Exit(1)
	}
	c.Start()
	defer c.Close()

	if flags.auth != "" {
		reply := c.Send(utils.ToCmdLine("AUTH", flags.auth))
		if isError(reply) {
			slog.Error(fmt.Sprintf("AUTH failed: %s", formatReply(reply)))
			os.Exit(1)
		}
	}

	dashAddr := net.JoinHostPort("", strconv.Itoa(config.Properties.WebPort))
	dash := web.NewDashboard(dashAddr, c)
	dash.Start()

	slog.Info("godis web dashboard at http://localhost:" + strconv.Itoa(config.Properties.WebPort))
	select {}
}
