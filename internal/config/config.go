package config

import (
	_ "embed"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/fsnotify/fsnotify"
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

	ClusterEnable bool   `cfg:"cluster-enable" toml:"cluster_enable"`
	ClusterAsSeed bool   `cfg:"cluster-as-seed" toml:"cluster_as_seed"`
	ClusterSeed   string `cfg:"cluster-seed" toml:"cluster_seed"`
	// ClusterJoinHTTP optionally points at the HTTP API (port 63790) of the
	// seed node. When set, a joining node sends its cluster-join handshake
	// over HTTP (POST /api/cluster/join) instead of the TCP join channel on
	// the seed's redis port. Either transport may be used; leave empty to
	// join via the classic TCP channel (ClusterSeed).
	ClusterJoinHTTP string `cfg:"cluster-join-http" toml:"cluster_join_http"`
	// ClusterJoinHTTPToken is the auth token used for HTTP cluster joins
	// when the seed has a password configured.
	ClusterJoinHTTPToken string `cfg:"cluster-join-http-token" toml:"cluster_join_http_token"`
	// ClusterID uniquely identifies the cluster. It is auto-generated and
	// written back to the config source when the first node switches to
	// cluster mode; joining nodes must present the same id.
	ClusterID            string `cfg:"cluster-id" toml:"cluster_id"`
	RaftListenAddr       string `cfg:"raft-listen-address" toml:"raft_listen_address"`
	RaftAdvertiseAddr    string `cfg:"raft-advertise-address" toml:"raft_advertise_address"`
	MasterInCluster      string `cfg:"master-in-cluster" toml:"master_in_cluster"`
	ClusterWorkerPool    bool   `cfg:"cluster-worker-pool" toml:"cluster_worker_pool"`
	ClusterRelayParallel bool   `cfg:"cluster-relay-parallel" toml:"cluster_relay_parallel"`
	AzureEntraTenantID   string `cfg:"azure-entra-tenant-id" toml:"azure_entra_tenant_id"`
	AzureEntraAppID      string `cfg:"azure-entra-app-id" toml:"azure_entra_app_id"`

	// Cluster tuning: heartbeat probe interval (seconds), failover threshold
	// (consecutive missed probes before a master is considered dead), the maximum
	// allowed major-version gap between nodes, and whether node-to-node data
	// replication goes through OSS instead of direct TCP connections.
	ClusterHeartbeatInterval   int  `cfg:"cluster-heartbeat-interval" toml:"cluster_heartbeat_interval"`
	ClusterFailoverMaxFailures int  `cfg:"cluster-failover-max-failures" toml:"cluster_failover_max_failures"`
	ClusterMaxVersionGap       int  `cfg:"cluster-max-version-gap" toml:"cluster_max_version_gap"`
	ClusterSyncViaOss          bool `cfg:"cluster-sync-via-oss" toml:"cluster_sync_via_oss"`
	ClusterSnapshotInterval    int  `cfg:"cluster-snapshot-interval" toml:"cluster_snapshot_interval"`

	// OSS (S3-compatible object storage) settings. Used for node-to-node
	// data replication in cluster mode: masters upload compressed RDB
	// snapshots, replicas download them. Snapshots are compressed with gzip
	// and intentionally NOT encrypted.
	OssEnabled         bool   `cfg:"oss-enabled" toml:"oss_enabled"`
	OssEndpoint        string `cfg:"oss-endpoint" toml:"oss_endpoint"`
	OssBucket          string `cfg:"oss-bucket" toml:"oss_bucket"`
	OssAccessKeyID     string `cfg:"oss-access-key-id" toml:"oss_access_key_id"`
	OssSecretAccessKey string `cfg:"oss-secret-access-key" toml:"oss_secret_access_key"`
	OssRegion          string `cfg:"oss-region" toml:"oss_region"`
	OssPrefix          string `cfg:"oss-prefix" toml:"oss_prefix"`
	OssUseSSL          bool   `cfg:"oss-use-ssl" toml:"oss_use_ssl"`
	OssForcePathStyle  bool   `cfg:"oss-force-path-style" toml:"oss_force_path_style"`

	// HTTP API settings
	HttpApiEnabled bool   `cfg:"http-api-enabled" toml:"http_api_enabled"`
	HttpApiPort    int    `cfg:"http-api-port" toml:"http_api_port"`
	HttpApiHost    string `cfg:"http-api-host" toml:"http_api_host"`
	// Data directory for bolt persistence (~/.godis), survives upgrades
	DataDir string `cfg:"data-dir" toml:"data_dir"`
	// Web Dashboard settings
	WebPort int `cfg:"web-port" toml:"web_port"`
	// Sentinel settings
	SentinelEnabled  bool                    `cfg:"sentinel-enabled" toml:"sentinel_enabled"`
	SentinelPort     int                     `cfg:"sentinel-port" toml:"sentinel_port"`
	SentinelMonitors []SentinelMonitorConfig `cfg:"sentinel-monitors" toml:"sentinel_monitors"`
}

// SentinelMonitorConfig describes one master monitored by the sentinel.
type SentinelMonitorConfig struct {
	Name        string `toml:"name" mapstructure:"name"`
	Host        string `toml:"host" mapstructure:"host"`
	Port        int    `toml:"port" mapstructure:"port"`
	Quorum      int    `toml:"quorum" mapstructure:"quorum"`
	DownAfterMs int    `toml:"down_after_ms" mapstructure:"down_after_ms"`
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

func (p *ServerProperties) HeartbeatInterval() time.Duration {
	interval := p.ClusterHeartbeatInterval
	if interval <= 0 {
		interval = 15
	}
	return time.Duration(interval) * time.Second
}

func (p *ServerProperties) FailoverTimeout() time.Duration {
	interval := p.ClusterHeartbeatInterval
	if interval <= 0 {
		interval = 15
	}
	failures := p.ClusterFailoverMaxFailures
	if failures <= 0 {
		failures = 10
	}
	return time.Duration(interval*failures) * time.Second
}

var Properties *ServerProperties
var EachTimeServerInfo *ServerInfo

func init() {
	Properties = &ServerProperties{
		Bind:                       "0.0.0.0",
		Port:                       6379,
		MaxClients:                 128,
		Databases:                  16,
		AppendFsync:                "everysec",
		AofUseRdbPreamble:          true,
		SlowLogSlowerThan:          10000,
		SlowLogMaxLen:              128,
		PrometheusEnabled:          true,
		PrometheusPort:             9121,
		ClusterWorkerPool:          true,
		ClusterRelayParallel:       true,
		AzureEntraTenantID:         "",
		AzureEntraAppID:            "",
		ClusterHeartbeatInterval:   15,
		ClusterFailoverMaxFailures: 10,
		ClusterMaxVersionGap:       3,
		ClusterSyncViaOss:          false,
		ClusterSnapshotInterval:    60,
		OssUseSSL:                  false,
		OssForcePathStyle:          false,
		HttpApiEnabled:             true,
		HttpApiHost:                "127.0.0.1",
		HttpApiPort:                63790,
		WebPort:                    63800,
		SentinelPort:               26379,
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

	EachTimeServerInfo.StartUpTime = time.Now()
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
	Properties.ClusterJoinHTTP = getStr(v, "cluster.join_http", Properties.ClusterJoinHTTP)
	Properties.ClusterJoinHTTPToken = getStr(v, "cluster.join_http_token", Properties.ClusterJoinHTTPToken)
	Properties.ClusterID = getStr(v, "cluster.cluster_id", Properties.ClusterID)
	Properties.RaftListenAddr = getStr(v, "cluster.raft_listen_address", Properties.RaftListenAddr)
	Properties.RaftAdvertiseAddr = getStr(v, "cluster.raft_advertise_address", Properties.RaftAdvertiseAddr)
	Properties.MasterInCluster = getStr(v, "cluster.master_in_cluster", Properties.MasterInCluster)

	Properties.ClusterWorkerPool = getBool(v, "cluster.worker_pool", Properties.ClusterWorkerPool)

	Properties.ClusterHeartbeatInterval = getInt(v, "cluster.heartbeat_interval", Properties.ClusterHeartbeatInterval)
	Properties.ClusterFailoverMaxFailures = getInt(v, "cluster.failover_max_failures", Properties.ClusterFailoverMaxFailures)
	Properties.ClusterMaxVersionGap = getInt(v, "cluster.max_version_gap", Properties.ClusterMaxVersionGap)
	Properties.ClusterSyncViaOss = getBool(v, "cluster.sync_via_oss", Properties.ClusterSyncViaOss)
	Properties.ClusterSnapshotInterval = getInt(v, "cluster.snapshot_interval", Properties.ClusterSnapshotInterval)

	Properties.OssEnabled = getBool(v, "oss.enabled", Properties.OssEnabled)
	Properties.OssEndpoint = getStr(v, "oss.endpoint", Properties.OssEndpoint)
	Properties.OssBucket = getStr(v, "oss.bucket", Properties.OssBucket)
	Properties.OssAccessKeyID = getStr(v, "oss.access_key_id", Properties.OssAccessKeyID)
	Properties.OssSecretAccessKey = getStr(v, "oss.secret_access_key", Properties.OssSecretAccessKey)
	Properties.OssRegion = getStr(v, "oss.region", Properties.OssRegion)
	Properties.OssPrefix = getStr(v, "oss.prefix", Properties.OssPrefix)
	Properties.OssUseSSL = getBool(v, "oss.use_ssl", Properties.OssUseSSL)
	Properties.OssForcePathStyle = getBool(v, "oss.force_path_style", Properties.OssForcePathStyle)

	Properties.AzureEntraTenantID = getStr(v, "azure.entra_tenant_id", Properties.AzureEntraTenantID)
	Properties.AzureEntraAppID = getStr(v, "azure.entra_app_id", Properties.AzureEntraAppID)
	Properties.ClusterRelayParallel = getBool(v, "cluster.relay_parallel", Properties.ClusterRelayParallel)

	Properties.PrometheusEnabled = getBool(v, "monitoring.prometheus_enabled", Properties.PrometheusEnabled)
	Properties.PrometheusPort = getInt(v, "monitoring.prometheus_port", Properties.PrometheusPort)

	Properties.WebPort = getInt(v, "web.port", Properties.WebPort)
	Properties.HttpApiEnabled = getBool(v, "http_api.enabled", Properties.HttpApiEnabled)
	Properties.HttpApiPort = getInt(v, "http_api.port", Properties.HttpApiPort)
	Properties.HttpApiHost = getStr(v, "http_api.host", Properties.HttpApiHost)
	Properties.DataDir = getStr(v, "data.dir", Properties.DataDir)
	if Properties.DataDir == "" {
		if usr, err := user.Current(); err == nil {
			Properties.DataDir = filepath.Join(usr.HomeDir, ".godis")
		}
	}
	_ = os.MkdirAll(Properties.DataDir, 0o755)
	Properties.SentinelEnabled = getBool(v, "sentinel.enable", Properties.SentinelEnabled)
	Properties.SentinelPort = getInt(v, "sentinel.port", Properties.SentinelPort)
	var monitors []SentinelMonitorConfig
	if err := v.UnmarshalKey("sentinel.monitors", &monitors); err == nil {
		Properties.SentinelMonitors = monitors
	}
	notifyReload()
}

var reloadHooks []func()

// RegisterReloadHook registers a callback invoked after every config reload
// (config-file change or nacos change). Used to react to runtime switches
// such as enabling cluster mode on a running standalone node.
func RegisterReloadHook(hook func()) {
	reloadHooks = append(reloadHooks, hook)
}

func notifyReload() {
	for _, hook := range reloadHooks {
		hook()
	}
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
	nacosClient = configClient
	nacosDataId = dataId
	nacosGroup = group
	nacosLastContent = content
	loadNacosDynamicConfig(configClient, dataId, group)
}

var (
	nacosClient      config_client.IConfigClient
	nacosDataId      string
	nacosGroup       string
	nacosLastContent string
)

func loadNacosDynamicConfig(client config_client.IConfigClient, dataId, group string) {
	// Watch for config changes from Nacos
	err := client.ListenConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
		OnChange: func(namespace, group, dataId, data string) {
			slog.Info("nacos config changed, reloading")
			nacosLastContent = data
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

// WriteBackClusterID persists the auto-generated cluster id to the active
// config source (config file when running file-based, Nacos otherwise) so
// that later nodes can discover the cluster id. The id is written into the
// [cluster] section under cluster_id.
func WriteBackClusterID(id string) {
	Properties.ClusterID = id
	if nacosClient != nil {
		content := nacosLastContent
		if content == "" {
			if got, err := nacosClient.GetConfig(vo.ConfigParam{DataId: nacosDataId, Group: nacosGroup}); err == nil {
				content = got
			}
		}
		patched := setTomlKey(content, "cluster", "cluster_id", id)
		if patched != content {
			if _, err := nacosClient.PublishConfig(vo.ConfigParam{
				DataId:  nacosDataId,
				Group:   nacosGroup,
				Content: patched,
			}); err != nil {
				slog.Error("failed to publish cluster_id to nacos", "error", err)
				return
			}
			nacosLastContent = patched
			slog.Info("cluster_id written back to nacos", "cluster_id", id)
		}
		return
	}
	if configFilePath == "" {
		return
	}
	bin, err := os.ReadFile(configFilePath)
	if err != nil {
		slog.Error("failed to read config file for cluster_id write-back", "error", err)
		return
	}
	patched := setTomlKey(string(bin), "cluster", "cluster_id", id)
	if err := os.WriteFile(configFilePath, []byte(patched), 0644); err != nil {
		slog.Error("failed to write cluster_id to config file", "error", err)
		return
	}
	slog.Info("cluster_id written back to config file", "path", configFilePath, "cluster_id", id)
}

// setTomlKey returns content with the key under section set to value. When
// the section or key is absent it is appended, preserving all other content.
func setTomlKey(content, section, key, value string) string {
	sectionHeader := "[" + section + "]"
	lines := strings.Split(content, "\n")
	out := make([]string, 0, len(lines)+2)
	inSection := false
	found := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			if inSection && !found {
				out = append(out, key+" = "+strconv.Quote(value))
				found = true
			}
			inSection = trimmed == sectionHeader
		} else if inSection {
			kv := strings.SplitN(line, "=", 2)
			if len(kv) == 2 && strings.TrimSpace(kv[0]) == key {
				out = append(out, key+" = "+strconv.Quote(value))
				found = true
				continue
			}
		}
		out = append(out, line)
	}
	if inSection && !found {
		out = append(out, key+" = "+strconv.Quote(value))
		found = true
	}
	if !found {
		out = append(out, "", sectionHeader, key+" = "+strconv.Quote(value))
	}
	return strings.Join(out, "\n")
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
