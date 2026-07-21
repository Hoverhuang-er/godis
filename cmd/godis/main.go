package main

import (
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/Hoverhuang-er/godis/internal/cluster"
	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/database"
	idatabase "github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	_ "github.com/Hoverhuang-er/godis/internal/lib/greenteagc"
	"github.com/Hoverhuang-er/godis/internal/redis/server/gnet"
	stdserver "github.com/Hoverhuang-er/godis/internal/redis/server/std"
	"github.com/Hoverhuang-er/godis/internal/lib/logger"
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
