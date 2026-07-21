package config

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/hdt3213/godis/internal/lib/utils"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"log/slog"
)

var (
	ClusterMode    = "cluster"
	StandaloneMode = "standalone"
)

// ServerProperties defines global config properties
type ServerProperties struct {
	RunID             string `cfg:"runid" toml:"runid"`
	Bind              string `cfg:"bind" toml:"bind"`
	Port              int    `cfg:"port" toml:"port"`
	Dir               string `cfg:"dir" toml:"dir"`
	AnnounceHost      string `cfg:"announce-host" toml:"announce_host"`
	AppendOnly        bool   `cfg:"appendonly" toml:"appendonly"`
	AppendFilename    string `cfg:"appendfilename" toml:"appendfilename"`
	AppendFsync       string `cfg:"appendfsync" toml:"appendfsync"`
	AofUseRdbPreamble bool   `cfg:"aof-use-rdb-preamble" toml:"aof_use_rdb_preamble"`
	MaxClients        int    `cfg:"maxclients" toml:"maxclients"`
	RequirePass       string `cfg:"requirepass" toml:"requirepass"`
	Databases         int    `cfg:"databases" toml:"databases"`
	RDBFilename       string `cfg:"dbfilename" toml:"dbfilename"`
	MasterAuth        string `cfg:"masterauth" toml:"masterauth"`
	SlaveAnnouncePort int    `cfg:"slave-announce-port" toml:"slave_announce_port"`
	SlaveAnnounceIP   string `cfg:"slave-announce-ip" toml:"slave_announce_ip"`
	ReplTimeout       int    `cfg:"repl-timeout" toml:"repl_timeout"`
	UseGnet           bool   `cfg:"use-gnet" toml:"use_gnet"`

	ConfigMode string `cfg:"-" toml:"config_mode"` // "standalone" or "cluster", auto-detected

	SlowLogSlowerThan int64 `cfg:"slowlog-log-slower-than" toml:"slowlog_log_slower_than"`
	SlowLogMaxLen     int   `cfg:"slowlog-max-len" toml:"slowlog_max_len"`

	ClusterEnable      bool   `cfg:"cluster-enable" toml:"cluster_enable"`
	ClusterAsSeed      bool   `cfg:"cluster-as-seed" toml:"cluster_as_seed"`
	ClusterSeed        string `cfg:"cluster-seed" toml:"cluster_seed"`
	RaftListenAddr     string `cfg:"raft-listen-address" toml:"raft_listen_address"`
	RaftAdvertiseAddr  string `cfg:"raft-advertise-address" toml:"raft_advertise_address"`
	MasterInCluster    string `cfg:"master-in-cluster" toml:"master_in_cluster"`
}

var configFilePath string

func GetConfigFilePath() string {
	return configFilePath
}

type ServerInfo struct {
	StartUpTime time.Time
}

func (p *ServerProperties) AnnounceAddress() string {
	if p.AnnounceHost != "" {
		return net.JoinHostPort(p.AnnounceHost, strconv.Itoa(p.Port))
	}
	return ""
}

func (p *ServerProperties) RaftAnnounceAddress() string {
	if p.RaftAdvertiseAddr != "" {
		return p.RaftAdvertiseAddr
	}
	return ""
}

var Properties *ServerProperties
var EachTimeServerInfo *ServerInfo

func init() {
	Properties = &ServerProperties{
		Bind:              "127.0.0.1",
		Port:              6379,
		AppendOnly:        false,
		AppendFilename:    "",
		MaxClients:        1000,
		Databases:         16,
		RunID:             utils.RandString(40),
		SlowLogSlowerThan: 10000,
		SlowLogMaxLen:     128,
	}
}

// SetupConfig loads configuration from the given TOML-format file.
func SetupConfig(configFilename string) {
	Properties.RunID = utils.RandString(40)

	absPath, err := filepath.Abs(configFilename)
	if err != nil {
		slog.Warn("failed to resolve config path", "path", configFilename, "error", err)
		absPath = configFilename
	}
	configFilePath = absPath

	loadTomlConfig(configFilename)

	if Properties.Dir == "" {
		Properties.Dir = "."
	}
}

// SetupConfigFromNacos loads configuration from Nacos config center.
func SetupConfigFromNacos(addr, namespaceId, group, dataId string) {
	Properties.RunID = utils.RandString(40)

	sc := []constant.ServerConfig{
		*constant.NewServerConfig(addr, 8848),
	}
	cc := *constant.NewClientConfig(
		constant.WithNamespaceId(namespaceId),
		constant.WithTimeoutMs(5000),
		constant.WithNotLoadCacheAtStart(true),
	)

	configClient, err := clients.NewConfigClient(
		vo.NacosClientParam{
			ClientConfig:  &cc,
			ServerConfigs: sc,
		},
	)
	if err != nil {
		slog.Error("failed to create nacos config client", "error", err)
		return
	}

	content, err := configClient.GetConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
	if err != nil {
		slog.Error("failed to get config from nacos", "error", err)
		return
	}

	parseTomlContent(content)
	slog.Info("loaded config from nacos", "addr", addr, "dataId", dataId, "group", group)
}

func loadTomlConfig(filename string) {
	f, err := os.Open(filename)
	if err != nil {
		slog.Warn("config file not found, using defaults", "path", filename)
		return
	}
	defer f.Close()

	var buf strings.Builder
	_, err = io.Copy(&buf, f)
	if err != nil {
		slog.Error("failed to read config file", "error", err)
		return
	}

	parseTomlContent(buf.String())
}

// parseTomlContent parses TOML content into Properties.
func parseTomlContent(content string) {
	var flatMap map[string]interface{}
	_, err := toml.Decode(content, &flatMap)
	if err == nil {
		applyTomlFlat(flatMap)
	}

	type tomlServer struct {
		Bind       string `toml:"bind"`
		Dir        string `toml:"dir"`
		Port       int    `toml:"port"`
		MaxClients int    `toml:"maxclients"`
		Databases  int    `toml:"databases"`
		UseGnet    bool   `toml:"use_gnet"`
	}
	type tomlAOF struct {
		AppendOnly        bool   `toml:"appendonly"`
		AppendFilename    string `toml:"appendfilename"`
		AppendFsync       string `toml:"appendfsync"`
		AofUseRdbPreamble bool   `toml:"aof_use_rdb_preamble"`
		RDBFilename       string `toml:"dbfilename"`
	}
	type tomlSecurity struct {
		RequirePass string `toml:"requirepass"`
		MasterAuth  string `toml:"masterauth"`
	}
	type tomlReplication struct {
		SlaveAnnounceIP   string `toml:"slave_announce_ip"`
		SlaveAnnouncePort int    `toml:"slave_announce_port"`
		ReplTimeout       int    `toml:"repl_timeout"`
		AnnounceHost      string `toml:"announce_host"`
	}
	type tomlSlowLog struct {
		SlowerThan int64 `toml:"log_slower_than"`
		MaxLen     int   `toml:"max_len"`
	}
	type tomlCluster struct {
		Enable            bool   `toml:"enable"`
		AsSeed            bool   `toml:"as_seed"`
		Seed              string `toml:"seed"`
		RaftListenAddr    string `toml:"raft_listen_address"`
		RaftAdvertiseAddr string `toml:"raft_advertise_address"`
		MasterInCluster   string `toml:"master_in_cluster"`
	}
	type tomlConfig struct {
		ConfigMode  string           `toml:"config_mode"`
		Server      tomlServer       `toml:"server"`
		AOF         tomlAOF          `toml:"aof"`
		Security    tomlSecurity     `toml:"security"`
		Replication tomlReplication  `toml:"replication"`
		SlowLog     tomlSlowLog      `toml:"slowlog"`
		Cluster     tomlCluster      `toml:"cluster"`
	}

	var cfg tomlConfig
	if _, err := toml.Decode(content, &cfg); err == nil {
		if cfg.Server.Bind != "" {
			Properties.Bind = cfg.Server.Bind
		}
		if cfg.Server.Port != 0 {
			Properties.Port = cfg.Server.Port
		}
		if cfg.Server.Dir != "" {
			Properties.Dir = cfg.Server.Dir
		}
		if cfg.Server.MaxClients != 0 {
			Properties.MaxClients = cfg.Server.MaxClients
		}
		if cfg.Server.Databases != 0 {
			Properties.Databases = cfg.Server.Databases
		}
		Properties.UseGnet = cfg.Server.UseGnet

		Properties.AppendOnly = cfg.AOF.AppendOnly
		if cfg.AOF.AppendFilename != "" {
			Properties.AppendFilename = cfg.AOF.AppendFilename
		}
		if cfg.AOF.AppendFsync != "" {
			Properties.AppendFsync = cfg.AOF.AppendFsync
		}
		Properties.AofUseRdbPreamble = cfg.AOF.AofUseRdbPreamble
		if cfg.AOF.RDBFilename != "" {
			Properties.RDBFilename = cfg.AOF.RDBFilename
		}

		if cfg.Security.RequirePass != "" {
			Properties.RequirePass = cfg.Security.RequirePass
		}
		if cfg.Security.MasterAuth != "" {
			Properties.MasterAuth = cfg.Security.MasterAuth
		}

		if cfg.Replication.SlaveAnnounceIP != "" {
			Properties.SlaveAnnounceIP = cfg.Replication.SlaveAnnounceIP
		}
		if cfg.Replication.SlaveAnnouncePort != 0 {
			Properties.SlaveAnnouncePort = cfg.Replication.SlaveAnnouncePort
		}
		if cfg.Replication.ReplTimeout != 0 {
			Properties.ReplTimeout = cfg.Replication.ReplTimeout
		}
		if cfg.Replication.AnnounceHost != "" {
			Properties.AnnounceHost = cfg.Replication.AnnounceHost
		}

		if cfg.SlowLog.SlowerThan != 0 {
			Properties.SlowLogSlowerThan = cfg.SlowLog.SlowerThan
		}
		if cfg.SlowLog.MaxLen != 0 {
			Properties.SlowLogMaxLen = cfg.SlowLog.MaxLen
		}

		Properties.ClusterEnable = cfg.Cluster.Enable
		Properties.ClusterAsSeed = cfg.Cluster.AsSeed
		if cfg.Cluster.Seed != "" {
			Properties.ClusterSeed = cfg.Cluster.Seed
		}
		if cfg.Cluster.RaftListenAddr != "" {
			Properties.RaftListenAddr = cfg.Cluster.RaftListenAddr
		}
		if cfg.Cluster.RaftAdvertiseAddr != "" {
			Properties.RaftAdvertiseAddr = cfg.Cluster.RaftAdvertiseAddr
		}
		if cfg.Cluster.MasterInCluster != "" {
			Properties.MasterInCluster = cfg.Cluster.MasterInCluster
		}

		if cfg.ConfigMode != "" {
			Properties.ConfigMode = cfg.ConfigMode
		}
	}

	if Properties.ConfigMode == "" {
		if Properties.ClusterEnable {
			Properties.ConfigMode = ClusterMode
		} else {
			Properties.ConfigMode = StandaloneMode
		}
	}
}

func applyTomlFlat(flatMap map[string]interface{}) {
	t := reflect.TypeOf(Properties).Elem()
	v := reflect.ValueOf(Properties).Elem()
	n := t.NumField()
	for i := range n {
		field := t.Field(i)
		fieldVal := v.Field(i)
		key, ok := field.Tag.Lookup("toml")
		if !ok || key == "" || key == "-" {
			continue
		}
		raw, ok := flatMap[key]
		if !ok {
			continue
		}
		switch field.Type.Kind() {
		case reflect.String:
			if s, ok := raw.(string); ok {
				fieldVal.SetString(s)
			}
		case reflect.Int, reflect.Int64:
			switch r := raw.(type) {
			case int64:
				fieldVal.SetInt(r)
			case float64:
				fieldVal.SetInt(int64(r))
			}
		case reflect.Bool:
			if b, ok := raw.(bool); ok {
				fieldVal.SetBool(b)
			}
		}
	}
}


func GetTmpDir() string {
	return Properties.Dir + "/tmp"
}

// DetectConfigFile finds the appropriate config file.
// Priority:
//  1. CONFIG env var (explicit path)
//  2. standalone.toml (standalone mode)
//  3. cluster.toml (cluster mode)
func DetectConfigFile() (string, bool) {
	configFile := os.Getenv("CONFIG")
	if configFile != "" {
		return configFile, true
	}

	for _, candidate := range []string{"standalone.toml", "cluster.toml"} {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, true
		}
	}
	return "", false
}
