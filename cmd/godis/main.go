
package main

import (
	"fmt"
	"net"
	"os"
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
	Port:           6399,
	AppendOnly:     false,
	AppendFilename: "",
	MaxClients:     1000,
	RunID:          utils.RandString(40),
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

	var err error
	if config.Properties.UseGnet {
		var db idatabase.DB
		if config.Properties.ClusterEnable {
			db = cluster.MakeCluster()
		} else {
			db = database.NewStandaloneServer()
		}
		server := gnet.NewGnetServer(db)
		err = server.Run(listenAddr)
	} else {
		handler := stdserver.MakeHandler()
		err = stdserver.Serve(listenAddr, handler)
	}
	if err != nil {
		slog.Error(fmt.Sprintf("start server failed: %v", err))
	}
}

func startWebDashboard() {
	slog.Info("starting web dashboard")

	// Parse --web-port flag
	webPort := 8080
	for i, arg := range os.Args[1:] {
		if arg == "--web-port" && i+1 < len(os.Args[1:]) {
			if p, err := strconv.Atoi(os.Args[1:][i+1]); err == nil {
				webPort = p
			}
		}
	}

	addr := fmt.Sprintf(":%d", webPort)

	// Determine godis server address
	serverHost := "127.0.0.1"
	serverPort := 6399
	password := ""
	for i, arg := range os.Args[1:] {
		if arg == "--server-host" && i+1 < len(os.Args[1:]) {
			serverHost = os.Args[1:][i+1]
		}
		if arg == "--server-port" && i+1 < len(os.Args[1:]) {
			if p, err := strconv.Atoi(os.Args[1:][i+1]); err == nil {
				serverPort = p
			}
		}
		if arg == "-a" && i+1 < len(os.Args[1:]) {
			password = os.Args[1:][i+1]
		}
	}

	serverAddr := net.JoinHostPort(serverHost, strconv.Itoa(serverPort))
	slog.Info("connecting to godis server", "addr", serverAddr)

	c, err := rclient.MakeClient(serverAddr)
	if err != nil {
		slog.Error(fmt.Sprintf("failed to connect to godis: %v", err))
		os.Exit(1)
	}
	c.Start()
	defer c.Close()

	if password != "" {
		reply := c.Send(utils.ToCmdLine("AUTH", password))
		if isError(reply) {
			slog.Error(fmt.Sprintf("AUTH failed: %s", formatReply(reply)))
			os.Exit(1)
		}
	}

	// Start hot key reset loop
	go func() {
		for {
			time.Sleep(60 * time.Second)
			web.ResetHotKeys()
		}
	}()

	dash := web.NewDashboard(addr, c)
	dash.Start()

	slog.Info("dashboard available at http://localhost" + addr)
	select {}
}

// Dependencies used by startWebDashboard
var _ = web.RecordKeyAccess
