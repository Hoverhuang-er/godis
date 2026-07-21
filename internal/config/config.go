package config

import (
	_ "embed"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/fsnotify/fsnotify"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/spf13/viper"
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

	PrometheusEnabled bool `cfg:"prometheus-enabled" toml:"prometheus_enabled"`
	PrometheusPort    int  `cfg:"prometheus-port" toml:"prometheus_port"`

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
	return p.RaftListenAddr
}

var Properties *ServerProperties
var EachTimeServerInfo *ServerInfo

func init() {
	Properties = &ServerProperties{
		Bind:              "0.0.0.0",
		Port:              6399,
		MaxClients:        128,
		Databases:         16,
		AppendFsync:       "everysec",
		AofUseRdbPreamble: true,
		SlowLogSlowerThan: 10000,
		SlowLogMaxLen:     128,
		PrometheusEnabled: true,
		PrometheusPort:    9121,
	}
	EachTimeServerInfo = &ServerInfo{}
}

//go:embed default.toml
var defaultConfigContent string

// SetupConfig loads configuration from the given TOML file using viper.
// Supports hot-reload: changes to the config file are picked up automatically.
func SetupConfig(configFilename string) {
	Properties.RunID = utils.RandString(40)

	absPath, err := filepath.Abs(configFilename)
	if err != nil {
		slog.Warn("failed to resolve config path", "path", configFilename, "error", err)
		absPath = configFilename
	}
	configFilePath = absPath

	v := viper.New()
	v.SetConfigFile(configFilename)

	if err := v.ReadInConfig(); err != nil {
		slog.Warn("failed to read config file, using defaults", "path", configFilename, "error", err)
		return
	}

	populateFromViper(v)


	// Hot-reload: watch config file for changes
	v.WatchConfig()
	v.OnConfigChange(func(in fsnotify.Event) {
		slog.Info("config file changed, reloading", "path", in.Name)
		populateFromViper(v)
	})
}

func populateFromViper(v *viper.Viper) {
	Properties.ConfigMode = v.GetString("config_mode")
	Properties.Bind = getStr(v, "server.bind", Properties.Bind)
	Properties.Port = getInt(v, "server.port", Properties.Port)
	Properties.Dir = getStr(v, "server.dir", Properties.Dir)
	if Properties.Dir == "" {
		Properties.Dir = "."
	}
	Properties.AnnounceHost = getStr(v, "server.announce_host", Properties.AnnounceHost)
	Properties.MaxClients = getInt(v, "server.maxclients", Properties.MaxClients)
	Properties.Databases = getInt(v, "server.databases", Properties.Databases)
	Properties.UseGnet = getBool(v, "server.use_gnet", Properties.UseGnet)

	Properties.AppendOnly = getBool(v, "aof.appendonly", Properties.AppendOnly)
	Properties.AppendFilename = getStr(v, "aof.appendfilename", Properties.AppendFilename)
	Properties.AppendFsync = getStr(v, "aof.appendfsync", Properties.AppendFsync)
	Properties.AofUseRdbPreamble = getBool(v, "aof.aof_use_rdb_preamble", Properties.AofUseRdbPreamble)
	Properties.RDBFilename = getStr(v, "aof.dbfilename", Properties.RDBFilename)

	Properties.RequirePass = getStr(v, "security.requirepass", Properties.RequirePass)
	Properties.MasterAuth = getStr(v, "security.masterauth", Properties.MasterAuth)

	Properties.AnnounceHost = getStr(v, "replication.announce_host", Properties.AnnounceHost)
	Properties.SlaveAnnouncePort = getInt(v, "replication.slave_announce_port", Properties.SlaveAnnouncePort)
	Properties.ReplTimeout = getInt(v, "replication.repl_timeout", Properties.ReplTimeout)

	Properties.SlowLogSlowerThan = int64(getInt(v, "slowlog.log_slower_than", int(Properties.SlowLogSlowerThan)))
	Properties.SlowLogMaxLen = getInt(v, "slowlog.max_len", Properties.SlowLogMaxLen)

	Properties.ClusterEnable = getBool(v, "cluster.enable", Properties.ClusterEnable)
	Properties.ClusterAsSeed = getBool(v, "cluster.as_seed", Properties.ClusterAsSeed)
	Properties.ClusterSeed = getStr(v, "cluster.seed", Properties.ClusterSeed)
	Properties.RaftListenAddr = getStr(v, "cluster.raft_listen_address", Properties.RaftListenAddr)
	Properties.RaftAdvertiseAddr = getStr(v, "cluster.raft_advertise_address", Properties.RaftAdvertiseAddr)
	Properties.MasterInCluster = getStr(v, "cluster.master_in_cluster", Properties.MasterInCluster)

	Properties.PrometheusEnabled = getBool(v, "monitoring.prometheus_enabled", Properties.PrometheusEnabled)
	Properties.PrometheusPort = getInt(v, "monitoring.prometheus_port", Properties.PrometheusPort)
}

func getStr(v *viper.Viper, key, def string) string {
	if v.IsSet(key) {
		return v.GetString(key)
	}
	return def
}

func getInt(v *viper.Viper, key string, def int) int {
	if v.IsSet(key) {
		return v.GetInt(key)
	}
	return def
}

func getBool(v *viper.Viper, key string, def bool) bool {
	if v.IsSet(key) {
		return v.GetBool(key)
	}
	return def
}

// SetupConfigFromNacos loads configuration from Nacos config center.
func SetupConfigFromNacos(addr, namespaceId, group, dataId string) {
	Properties.RunID = utils.RandString(40)

	clientConfig := constant.ClientConfig{
		NamespaceId:         namespaceId,
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		LogDir:              "/tmp/nacos/log",
		CacheDir:            "/tmp/nacos/cache",
		LogLevel:            "warn",
	}
	serverConfigs := []constant.ServerConfig{
		{IpAddr: addr, Port: 8848},
	}

	configClient, err := clients.CreateConfigClient(map[string]interface{}{
		"serverConfigs": serverConfigs,
		"clientConfig":  clientConfig,
	})
	if err != nil {
		slog.Error("failed to create nacos config client", "error", err)
		return
	}

	content, err := configClient.GetConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
	})
	if err != nil {
		slog.Error("failed to get nacos config", "error", err)
		return
	}
	parseTomlContent(content)
	loadNacosDynamicConfig(configClient, dataId, group)
}
func loadNacosDynamicConfig(client config_client.IConfigClient, dataId, group string) {
	// Watch for config changes from Nacos
	err := client.ListenConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
		OnChange: func(namespace, group, dataId, data string) {
			slog.Info("nacos config changed, reloading")
			parseTomlContent(data)
		},
	})
	if err != nil {
		slog.Error("failed to listen nacos config", "error", err)
	}
}

func parseTomlContent(content string) {
	flatMap := make(map[string]interface{})
	err := toml.Unmarshal([]byte(content), &flatMap)
	if err != nil {
		slog.Error("failed to parse toml content", "error", err)
		return
	}
	applyTomlFlat(flatMap)
}

func applyTomlFlat(flatMap map[string]interface{}) {
	v := viper.New()
	for k, val := range flatMap {
		v.Set(k, val)
	}
	populateFromViper(v)
}

// GetTmpDir returns the temporary directory for godis.
func GetTmpDir() string {
	if Properties.Dir == "" {
		return "/tmp"
	}
	return Properties.Dir + "/tmp"
}

// DetectConfigFile finds the appropriate config file.
// Priority:
//  1. CONFIG env var (explicit path)
//  2. standalone.toml (standalone mode)
//  3. cluster.toml (cluster mode)
//
// If none found, writes a default standalone.toml to the working directory.
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

	// No config file found: write default standalone.toml
	if err := os.WriteFile("standalone.toml", []byte(defaultConfigContent), 0644); err != nil {
		slog.Warn("failed to write default config", "error", err)
		return "", false
	}
	slog.Info("wrote default standalone.toml, loading it")
	return "standalone.toml", true
}
