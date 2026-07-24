
package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"strconv"
	"time"

	"github.com/Hoverhuang-er/godis/internal/cluster"
	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/database"
	idatabase "github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	_ "github.com/Hoverhuang-er/godis/internal/lib/greenteagc"
	rclient "github.com/Hoverhuang-er/godis/internal/redis/client"
	"github.com/Hoverhuang-er/godis/internal/redis/server/gnet"
	stdserver "github.com/Hoverhuang-er/godis/internal/redis/server/std"
	"github.com/Hoverhuang-er/godis/internal/lib/logger"
	"github.com/Hoverhuang-er/godis/internal/web"
	"log/slog"
)

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
	WebPort:        63800,
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}
func main() {
	// Check for hidden flags before any other setup
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
	listenAddr := net.JoinHostPort(config.Properties.Bind, strconv.Itoa(config.Properties.Port))

	var shutdownDB idatabase.DB

	if config.Properties.UseGnet {
		var db idatabase.DB
		if config.Properties.ClusterEnable {
			db = cluster.MakeCluster()
		} else {
			db = database.NewStandaloneServer()
		}
		shutdownDB = db
		srv := gnet.NewGnetServer(db)

		if config.Properties.HttpApiEnabled {
			apiAddr := net.JoinHostPort(config.Properties.HttpApiHost, strconv.Itoa(config.Properties.HttpApiPort))
			redisAddr := net.JoinHostPort(config.Properties.Bind, strconv.Itoa(config.Properties.Port))
			apiSrv := web.NewApiServer(apiAddr, redisAddr, config.Properties.RequirePass)
			apiSrv.Start()
		}

		go func() { _ = srv.Run(listenAddr) }()
	} else {
		handler := stdserver.MakeHandler()

		if config.Properties.HttpApiEnabled {
			apiAddr := net.JoinHostPort(config.Properties.HttpApiHost, strconv.Itoa(config.Properties.HttpApiPort))
			redisAddr := net.JoinHostPort(config.Properties.Bind, strconv.Itoa(config.Properties.Port))
			apiSrv := web.NewApiServer(apiAddr, redisAddr, config.Properties.RequirePass)
			apiSrv.Start()
		}

		go func() { _ = stdserver.Serve(listenAddr, handler) }()
	}

	// Wait for SIGINT/SIGTERM for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("received shutdown signal, gracefully shutting down...", "signal", sig)

	if shutdownDB != nil {
		slog.Info("step 1/2: closing server and flushing data to ~/.godis ...")
		start := time.Now()
		if closer, ok := shutdownDB.(interface{ Close() }); ok {
			closer.Close()
		}
		slog.Info("step 1/2: data flush complete",
			"elapsed", time.Since(start).Round(time.Millisecond).String())
	}
	slog.Info("step 2/2: godis shutdown complete")
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
