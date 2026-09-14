// Package monitoring provides Prometheus-compatible metrics for godis,
// compatible with the redis_exporter metric naming conventions.
package monitoring

import (
	"fmt"
	"math"
	"net/http"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/Hoverhuang-er/godis/internal/config"
)

const (
	hotKeyTopN         = 20
	bigKeyTopN         = 20
	bigKeyStringBytes  = 1024 * 1024 // 1 MB string threshold
	bigKeyListLength   = 10000       // 10k elements
	bigKeySetLength    = 5000        // 5k elements
	bigKeyZSetLength   = 5000
	bigKeyHashLength   = 5000
	hotKeyResetSeconds = 300 // reset hot key counters every 5 min
)

// Metrics tracks godis server metrics for Prometheus scraping.
type Metrics struct {
	// Cumulative counters (atomic for lock-free concurrent access)
	totalCommands    atomic.Int64
	totalConnections atomic.Int64
	keyspaceHits     atomic.Int64
	keyspaceMisses   atomic.Int64
	currConnections  atomic.Int64

	// Production-grade counters (godis_* namespace, dashboard 15286 aligned)
	netInputBytes       atomic.Int64
	netOutputBytes      atomic.Int64
	rejectedConns       atomic.Int64
	partialResync       atomic.Int64
	partialResyncDenied atomic.Int64
	fullResyncs         atomic.Int64
	slowLogCount        atomic.Int64
	expiredKeys         atomic.Int64
	evictedKeys         atomic.Int64

	// Command latency histogram (godis_commands_duration_seconds)
	cmdDurationMu sync.Mutex
	cmdDurations  map[string]*durationBucket

	// Ops per second tracking
	opsLastSec int64
	opsCount   int64

	startTime time.Time

	// DB stats callback
	dbStatsFn func() []DBStat

	// Cluster stats callback, registered when this node runs cluster mode
	clusterStatsFn func() ClusterStats

	// Replication stats callback, registered by the database layer
	replStatsFn func() ReplStats

	// Big key tracking
	bigKeysMu sync.RWMutex
	bigKeys   []BigKeyInfo

	// Hot key tracking (key -> access count)
	hotKeysMu   sync.RWMutex
	hotKeyCount map[string]int64
}

// durationBucket accumulates command latency observations.
type durationBucket struct {
	count   int64
	sumNsec int64
}

// ReplStats is a snapshot of replication state scraped for /metrics.
type ReplStats struct {
	Role            string // master, slave
	ConnectedSlaves int
	MasterLinkUp    bool
	ReplOffset      int64
	BacklogLen      int64
	SlaveLags       []SlaveLag // per-connected-slave lag, master only
}

// SlaveLag describes the replication lag of one connected slave.
type SlaveLag struct {
	IP         string
	Port       int
	LagSeconds float64
}

// DBStat holds keys/expires count for one database.
type DBStat struct {
	Index   int
	Keys    int64
	Expires int64
}

// ClusterStats is a snapshot of cluster-mode state scraped for /metrics.
type ClusterStats struct {
	Enabled         bool
	Mode            string // raft, master-slave, standalone
	Role            string // master, slave, none
	MasterID        string
	MasterAddr      string
	NodeCount       int
	SyncLastSuccess int64 // unix ts of last successful WAL push/pull
	SyncLagSeconds  float64
	WalSeqApplied   uint64 // highest WAL seq applied locally
	WalSeqPublished uint64 // highest WAL seq published by the master
	LastUploadMs    float64
	LastDownloadMs  float64
}

// BigKeyInfo describes a key that exceeds size thresholds.
type BigKeyInfo struct {
	Key   string
	DB    int
	Type  string // string, list, set, zset, hash
	Size  int64  // bytes for string, element count for others
	Bytes int64  // approximate memory usage
}

// HotKeyInfo describes a frequently accessed key.
type HotKeyInfo struct {
	Key   string
	DB    int
	Count int64
	Type  string // string, list, set, zset, hash
}

// New creates a Metrics instance.
func New(dbStatsFn func() []DBStat) *Metrics {
	m := &Metrics{
		startTime:    time.Now(),
		dbStatsFn:    dbStatsFn,
		hotKeyCount:  make(map[string]int64),
		cmdDurations: make(map[string]*durationBucket),
	}
	go m.tickLoop()
	go m.hotKeyResetLoop()
	return m
}

// SetClusterStatsFn registers the callback providing cluster-mode stats.
// Standalone nodes leave it nil and emit no cluster metrics.
func (m *Metrics) SetClusterStatsFn(fn func() ClusterStats) {
	m.clusterStatsFn = fn
}

// SetReplStatsFn registers the callback providing replication stats.
func (m *Metrics) SetReplStatsFn(fn func() ReplStats) {
	m.replStatsFn = fn
}

// RecordCommandDuration observes the execution latency of a command.
func (m *Metrics) RecordCommandDuration(cmdName string, dur time.Duration) {
	m.cmdDurationMu.Lock()
	b := m.cmdDurations[cmdName]
	if b == nil {
		b = &durationBucket{}
		m.cmdDurations[cmdName] = b
	}
	b.count++
	b.sumNsec += dur.Nanoseconds()
	m.cmdDurationMu.Unlock()
}

func (m *Metrics) IncrNetInputBytes(n int64)  { m.netInputBytes.Add(n) }
func (m *Metrics) IncrNetOutputBytes(n int64) { m.netOutputBytes.Add(n) }
func (m *Metrics) IncrRejectedConnections()   { m.rejectedConns.Add(1) }
func (m *Metrics) IncrPartialResync()         { m.partialResync.Add(1) }
func (m *Metrics) IncrPartialResyncDenied()   { m.partialResyncDenied.Add(1) }
func (m *Metrics) IncrFullResyncs()           { m.fullResyncs.Add(1) }
func (m *Metrics) IncrSlowLog()               { m.slowLogCount.Add(1) }
func (m *Metrics) IncrExpiredKeys()           { m.expiredKeys.Add(1) }
func (m *Metrics) IncrEvictedKeys()           { m.evictedKeys.Add(1) }

func (m *Metrics) tickLoop() {
	for {
		time.Sleep(1 * time.Second)
		oc := atomic.LoadInt64(&m.opsCount)
		atomic.StoreInt64(&m.opsCount, 0)
		atomic.StoreInt64(&m.opsLastSec, oc)
	}
}

func (m *Metrics) hotKeyResetLoop() {
	for {
		time.Sleep(hotKeyResetSeconds * time.Second)
		m.hotKeysMu.Lock()
		m.hotKeyCount = make(map[string]int64)
		m.hotKeysMu.Unlock()
	}
}

// --- Counters ---

func (m *Metrics) IncrCommands()       { m.totalCommands.Add(1); m.opsCount++ }
func (m *Metrics) IncrConnections()    { m.totalConnections.Add(1); m.currConnections.Add(1) }
func (m *Metrics) DecrConnections()    { m.currConnections.Add(-1) }
func (m *Metrics) IncrKeyspaceHits()   { m.keyspaceHits.Add(1) }
func (m *Metrics) IncrKeyspaceMisses() { m.keyspaceMisses.Add(1) }

// --- Hot key tracking ---

// RecordKeyAccess records a key access for hot key detection.
func (m *Metrics) RecordKeyAccess(key string, dbIndex int) {
	m.hotKeysMu.Lock()
	m.hotKeyCount[key]++
	m.hotKeysMu.Unlock()
}

// --- Big key tracking ---

// RecordBigKey records a big key observation.
func (m *Metrics) RecordBigKey(key string, dbIndex int, keyType string, size, bytes int64) {
	m.bigKeysMu.Lock()
	defer m.bigKeysMu.Unlock()

	info := BigKeyInfo{Key: key, DB: dbIndex, Type: keyType, Size: size, Bytes: bytes}
	m.bigKeys = append(m.bigKeys, info)
	// Keep only top N
	if len(m.bigKeys) > bigKeyTopN {
		sort.Slice(m.bigKeys, func(i, j int) bool {
			return m.bigKeys[i].Bytes > m.bigKeys[j].Bytes
		})
		m.bigKeys = m.bigKeys[:bigKeyTopN]
	}
}

// ResetBigKeys clears the big key cache (called after each scrape).
func (m *Metrics) ResetBigKeys() {
	m.bigKeysMu.Lock()
	m.bigKeys = nil
	m.bigKeysMu.Unlock()
}

// --- HTTP handler ---

// ServeHTTP implements http.Handler for the /metrics endpoint.
// Compatible with redis_exporter Prometheus metric naming.
func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)

	uptime := int64(time.Since(m.startTime).Seconds())

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	bi, ok := debug.ReadBuildInfo()
	version := "1.3.5"
	if ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		version = bi.Main.Version
	}

	dbStats := m.dbStatsFn()
	opsPerSec := atomic.LoadInt64(&m.opsLastSec)

	// ---- Standard Redis metrics (redis_exporter compatible) ----

	fmt.Fprintf(w, "# HELP redis_up Information about the Redis instance\n")
	fmt.Fprintf(w, "# TYPE redis_up gauge\n")
	fmt.Fprintf(w, "redis_up 1\n")

	fmt.Fprintf(w, "# HELP redis_server_version Redis server version\n")
	fmt.Fprintf(w, "# TYPE redis_server_version gauge\n")
	fmt.Fprintf(w, "redis_server_version{version=\"%s\"} 1\n", version)

	fmt.Fprintf(w, "# HELP redis_uptime_in_seconds Number of seconds since Redis server start\n")
	fmt.Fprintf(w, "# TYPE redis_uptime_in_seconds gauge\n")
	fmt.Fprintf(w, "redis_uptime_in_seconds %d\n", uptime)

	fmt.Fprintf(w, "# HELP redis_connected_clients Number of client connections\n")
	fmt.Fprintf(w, "# TYPE redis_connected_clients gauge\n")
	fmt.Fprintf(w, "redis_connected_clients %d\n", m.currConnections.Load())

	fmt.Fprintf(w, "# HELP redis_total_connections_received_total Total connections received\n")
	fmt.Fprintf(w, "# TYPE redis_total_connections_received_total counter\n")
	fmt.Fprintf(w, "redis_total_connections_received_total %d\n", m.totalConnections.Load())

	fmt.Fprintf(w, "# HELP redis_total_commands_processed_total Total commands processed\n")
	fmt.Fprintf(w, "# TYPE redis_total_commands_processed_total counter\n")
	fmt.Fprintf(w, "redis_total_commands_processed_total %d\n", m.totalCommands.Load())

	fmt.Fprintf(w, "# HELP redis_instantaneous_ops_per_sec Instantaneous operations per second\n")
	fmt.Fprintf(w, "# TYPE redis_instantaneous_ops_per_sec gauge\n")
	fmt.Fprintf(w, "redis_instantaneous_ops_per_sec %d\n", opsPerSec)

	fmt.Fprintf(w, "# HELP redis_used_memory_bytes Used memory in bytes\n")
	fmt.Fprintf(w, "# TYPE redis_used_memory_bytes gauge\n")
	fmt.Fprintf(w, "redis_used_memory_bytes %d\n", memStats.Alloc)

	fmt.Fprintf(w, "# HELP redis_used_memory_rss_bytes RSS memory in bytes\n")
	fmt.Fprintf(w, "# TYPE redis_used_memory_rss_bytes gauge\n")
	fmt.Fprintf(w, "redis_used_memory_rss_bytes %d\n", memStats.Sys)

	fmt.Fprintf(w, "# HELP redis_used_memory_peak_bytes Peak memory used in bytes\n")
	fmt.Fprintf(w, "# TYPE redis_used_memory_peak_bytes gauge\n")
	fmt.Fprintf(w, "redis_used_memory_peak_bytes %d\n", memStats.TotalAlloc)

	fmt.Fprintf(w, "# HELP redis_keyspace_hits_total Keyspace hits\n")
	fmt.Fprintf(w, "# TYPE redis_keyspace_hits_total counter\n")
	fmt.Fprintf(w, "redis_keyspace_hits_total %d\n", m.keyspaceHits.Load())

	fmt.Fprintf(w, "# HELP redis_keyspace_misses_total Keyspace misses\n")
	fmt.Fprintf(w, "# TYPE redis_keyspace_misses_total counter\n")
	fmt.Fprintf(w, "redis_keyspace_misses_total %d\n", m.keyspaceMisses.Load())

	fmt.Fprintf(w, "# HELP redis_cpu_sys_seconds_total System CPU consumed\n")
	fmt.Fprintf(w, "# TYPE redis_cpu_sys_seconds_total counter\n")
	fmt.Fprintf(w, "redis_cpu_sys_seconds_total %d\n", int64(float64(memStats.Sys)*memStats.GCCPUFraction))

	// ---- Per-DB key/expire stats ----
	for _, db := range dbStats {
		fmt.Fprintf(w, "redis_db_keys{db=\"%d\"} %d\n", db.Index, db.Keys)
		fmt.Fprintf(w, "redis_db_expires{db=\"%d\"} %d\n", db.Index, db.Expires)
	}

	// ---- Key length histogram ----
	fmt.Fprintf(w, "# HELP redis_key_length Approximate number of elements per key\n")
	fmt.Fprintf(w, "# TYPE redis_key_length gauge\n")
	for _, db := range dbStats {
		fmt.Fprintf(w, "redis_key_length{db=\"%d\"} %d\n", db.Index, db.Keys)
	}

	// ---- Big key monitoring ----
	fmt.Fprintf(w, "# HELP redis_big_key_info Big keys exceeding size thresholds\n")
	fmt.Fprintf(w, "# TYPE redis_big_key_info gauge\n")
	m.bigKeysMu.RLock()
	for _, bk := range m.bigKeys {
		fmt.Fprintf(w, "redis_big_key_info{key=\"%s\",db=\"%d\",type=\"%s\"} %d\n",
			bk.Key, bk.DB, bk.Type, bk.Bytes)
	}
	// Snapshot and clear after scrape
	bigKeysSnapshot := make([]BigKeyInfo, len(m.bigKeys))
	copy(bigKeysSnapshot, m.bigKeys)
	m.bigKeysMu.RUnlock()
	m.bigKeysMu.Lock()
	m.bigKeys = nil
	m.bigKeysMu.Unlock()

	// ---- Hot key monitoring ----
	fmt.Fprintf(w, "# HELP redis_hot_key_info Frequently accessed keys\n")
	fmt.Fprintf(w, "# TYPE redis_hot_key_info gauge\n")
	m.hotKeysMu.RLock()
	type kv struct {
		key   string
		count int64
	}
	var sorted []kv
	for k, c := range m.hotKeyCount {
		sorted = append(sorted, kv{k, c})
	}
	m.hotKeysMu.RUnlock()
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].count > sorted[j].count
	})
	if len(sorted) > hotKeyTopN {
		sorted = sorted[:hotKeyTopN]
	}
	for _, hk := range sorted {
		fmt.Fprintf(w, "redis_hot_key_info{key=\"%s\"} %d\n", hk.key, hk.count)
	}

	// ---- Replication metrics (godis_* , dashboard 15286 aligned) ----
	if m.replStatsFn != nil {
		rs := m.replStatsFn()
		fmt.Fprintf(w, "# HELP godis_connected_slaves Number of connected replicas\n")
		fmt.Fprintf(w, "# TYPE godis_connected_slaves gauge\n")
		fmt.Fprintf(w, "godis_connected_slaves %d\n", rs.ConnectedSlaves)

		fmt.Fprintf(w, "# HELP godis_master_link_up Whether the link to master is up (slave only)\n")
		fmt.Fprintf(w, "# TYPE godis_master_link_up gauge\n")
		link := 0
		if rs.MasterLinkUp {
			link = 1
		}
		fmt.Fprintf(w, "godis_master_link_up %d\n", link)

		fmt.Fprintf(w, "# HELP godis_master_repl_offset Master replication offset\n")
		fmt.Fprintf(w, "# TYPE godis_master_repl_offset gauge\n")
		fmt.Fprintf(w, "godis_master_repl_offset %d\n", rs.ReplOffset)

		fmt.Fprintf(w, "# HELP godis_repl_backlog_len Length of the replication backlog window\n")
		fmt.Fprintf(w, "# TYPE godis_repl_backlog_len gauge\n")
		fmt.Fprintf(w, "godis_repl_backlog_len %d\n", rs.BacklogLen)
	}

	fmt.Fprintf(w, "# HELP godis_replica_partial_resync_accepted_total Accepted partial resyncs\n")
	fmt.Fprintf(w, "# TYPE godis_replica_partial_resync_accepted_total counter\n")
	fmt.Fprintf(w, "godis_replica_partial_resync_accepted_total %d\n", m.partialResync.Load())

	fmt.Fprintf(w, "# HELP godis_replica_resyncs_full_total Full resyncs served\n")
	fmt.Fprintf(w, "# TYPE godis_replica_resyncs_full_total counter\n")
	fmt.Fprintf(w, "godis_replica_resyncs_full_total %d\n", m.fullResyncs.Load())

	// ---- Production metrics (godis_* , dashboard 15286 aligned) ----
	fmt.Fprintf(w, "# HELP godis_commands_total Total commands processed\n")
	fmt.Fprintf(w, "# TYPE godis_commands_total counter\n")
	fmt.Fprintf(w, "godis_commands_total %d\n", m.totalCommands.Load())

	fmt.Fprintf(w, "# HELP godis_commands_duration_seconds_total Cumulative command execution time\n")
	fmt.Fprintf(w, "# TYPE godis_commands_duration_seconds_total counter\n")
	m.cmdDurationMu.Lock()
	for name, b := range m.cmdDurations {
		fmt.Fprintf(w, "godis_commands_duration_seconds_total{cmd=\"%s\"} %v\n",
			sanitizeLabel(name), float64(b.sumNsec)/1e9)
	}
	m.cmdDurationMu.Unlock()

	fmt.Fprintf(w, "# HELP godis_net_input_bytes_total Total bytes received from clients\n")
	fmt.Fprintf(w, "# TYPE godis_net_input_bytes_total counter\n")
	fmt.Fprintf(w, "godis_net_input_bytes_total %d\n", m.netInputBytes.Load())

	fmt.Fprintf(w, "# HELP godis_net_output_bytes_total Total bytes sent to clients\n")
	fmt.Fprintf(w, "# TYPE godis_net_output_bytes_total counter\n")
	fmt.Fprintf(w, "godis_net_output_bytes_total %d\n", m.netOutputBytes.Load())

	fmt.Fprintf(w, "# HELP godis_rejected_connections_total Rejected connections\n")
	fmt.Fprintf(w, "# TYPE godis_rejected_connections_total counter\n")
	fmt.Fprintf(w, "godis_rejected_connections_total %d\n", m.rejectedConns.Load())

	fmt.Fprintf(w, "# HELP godis_expired_keys_total Keys expired\n")
	fmt.Fprintf(w, "# TYPE godis_expired_keys_total counter\n")
	fmt.Fprintf(w, "godis_expired_keys_total %d\n", m.expiredKeys.Load())

	fmt.Fprintf(w, "# HELP godis_evicted_keys_total Keys evicted\n")
	fmt.Fprintf(w, "# TYPE godis_evicted_keys_total counter\n")
	fmt.Fprintf(w, "godis_evicted_keys_total %d\n", m.evictedKeys.Load())

	fmt.Fprintf(w, "# HELP godis_slowlog_count Total slow queries logged\n")
	fmt.Fprintf(w, "# TYPE godis_slowlog_count counter\n")
	fmt.Fprintf(w, "godis_slowlog_count %d\n", m.slowLogCount.Load())

	// ---- Kvrocks dashboard 15286 compatible metrics ----
	// Alias the production metrics under the godis_* names that the
	// upstream Grafana dashboard (Kvrocks 15286) queries, so it can be
	// imported unchanged against a godis scrape target.
	cpuSys, cpuUser := cpuSeconds()
	fmt.Fprintf(w, "# HELP godis_uptime_in_seconds Server uptime\n")
	fmt.Fprintf(w, "# TYPE godis_uptime_in_seconds gauge\n")
	fmt.Fprintf(w, "godis_uptime_in_seconds %d\n", uptime)

	fmt.Fprintf(w, "# HELP godis_cpu_sys_seconds_total System CPU seconds\n")
	fmt.Fprintf(w, "# TYPE godis_cpu_sys_seconds_total counter\n")
	fmt.Fprintf(w, "godis_cpu_sys_seconds_total %v\n", cpuSys)
	fmt.Fprintf(w, "# HELP godis_cpu_user_seconds_total User CPU seconds\n")
	fmt.Fprintf(w, "# TYPE godis_cpu_user_seconds_total counter\n")
	fmt.Fprintf(w, "godis_cpu_user_seconds_total %v\n", cpuUser)

	usedDisk, diskCapacity := diskUsage(config.Properties.DataDir)
	fmt.Fprintf(w, "# HELP godis_used_disk_size Used disk space in bytes\n")
	fmt.Fprintf(w, "# TYPE godis_used_disk_size gauge\n")
	fmt.Fprintf(w, "godis_used_disk_size %d\n", usedDisk)
	fmt.Fprintf(w, "# HELP godis_disk_capacity_bytes Disk capacity in bytes\n")
	fmt.Fprintf(w, "# TYPE godis_disk_capacity_bytes gauge\n")
	fmt.Fprintf(w, "godis_disk_capacity_bytes %d\n", diskCapacity)

	fmt.Fprintf(w, "# HELP godis_used_db_size In-memory database size in bytes\n")
	fmt.Fprintf(w, "# TYPE godis_used_db_size gauge\n")
	fmt.Fprintf(w, "godis_used_db_size %d\n", memStats.Alloc)

	fmt.Fprintf(w, "# HELP godis_connected_clients Number of connected clients\n")
	fmt.Fprintf(w, "# TYPE godis_connected_clients gauge\n")
	fmt.Fprintf(w, "godis_connected_clients %d\n", m.currConnections.Load())

	fmt.Fprintf(w, "# HELP godis_monitor_clients Number of MONITOR clients\n")
	fmt.Fprintf(w, "# TYPE godis_monitor_clients gauge\n")
	fmt.Fprintf(w, "godis_monitor_clients 0\n")

	fmt.Fprintf(w, "# HELP godis_loading Whether the server is loading data\n")
	fmt.Fprintf(w, "# TYPE godis_loading gauge\n")
	fmt.Fprintf(w, "godis_loading 0\n")

	if m.replStatsFn != nil {
		rs := m.replStatsFn()
		fmt.Fprintf(w, "# HELP godis_connected_slaves Number of connected replicas\n")
		fmt.Fprintf(w, "# TYPE godis_connected_slaves gauge\n")
		fmt.Fprintf(w, "godis_connected_slaves %d\n", rs.ConnectedSlaves)

		fmt.Fprintf(w, "# HELP godis_instance_info Server role information\n")
		fmt.Fprintf(w, "# TYPE godis_instance_info gauge\n")
		fmt.Fprintf(w, "godis_instance_info{role=\"%s\"} 1\n", rs.Role)

		fmt.Fprintf(w, "# HELP godis_connected_slave_lag_seconds Replication lag per connected slave\n")
		fmt.Fprintf(w, "# TYPE godis_connected_slave_lag_seconds gauge\n")
		for _, sl := range rs.SlaveLags {
			fmt.Fprintf(w, "godis_connected_slave_lag_seconds{slave_ip=\"%s\",slave_port=\"%d\"} %v\n",
				sanitizeLabel(sl.IP), sl.Port, sl.LagSeconds)
		}
	}

	fmt.Fprintf(w, "# HELP godis_commands_total Commands processed per command name\n")
	fmt.Fprintf(w, "# TYPE godis_commands_total counter\n")
	m.cmdDurationMu.Lock()
	totalCmdCount := int64(0)
	for name, b := range m.cmdDurations {
		fmt.Fprintf(w, "godis_commands_total{cmd=\"%s\"} %d\n", sanitizeLabel(name), b.count)
		totalCmdCount += b.count
	}
	m.cmdDurationMu.Unlock()
	fmt.Fprintf(w, "godis_commands_total %d\n", m.totalCommands.Load()+totalCmdCount)

	fmt.Fprintf(w, "# HELP godis_commands_duration_seconds_total Cumulative command execution time per command\n")
	fmt.Fprintf(w, "# TYPE godis_commands_duration_seconds_total counter\n")
	m.cmdDurationMu.Lock()
	for name, b := range m.cmdDurations {
		fmt.Fprintf(w, "godis_commands_duration_seconds_total{cmd=\"%s\"} %v\n",
			sanitizeLabel(name), float64(b.sumNsec)/1e9)
	}
	m.cmdDurationMu.Unlock()

	fmt.Fprintf(w, "# HELP godis_memory_used_rss_bytes RSS memory in bytes\n")
	fmt.Fprintf(w, "# TYPE godis_memory_used_rss_bytes gauge\n")
	fmt.Fprintf(w, "godis_memory_used_rss_bytes %d\n", memStats.Sys)

	totalKeys := int64(0)
	totalExpires := int64(0)
	for _, db := range dbStats {
		totalKeys += db.Keys
		totalExpires += db.Expires
	}
	fmt.Fprintf(w, "# HELP godis_estimate_keys Estimated keys per column family\n")
	fmt.Fprintf(w, "# TYPE godis_estimate_keys gauge\n")
	fmt.Fprintf(w, "godis_estimate_keys{column_family=\"metadata\"} %d\n", totalKeys)
	fmt.Fprintf(w, "# HELP godis_db_keys Keys per database\n")
	fmt.Fprintf(w, "# TYPE godis_db_keys gauge\n")
	for _, db := range dbStats {
		fmt.Fprintf(w, "godis_db_keys{db=\"%d\"} %d\n", db.Index, db.Keys)
	}
	fmt.Fprintf(w, "# HELP godis_db_keys_expiring Expiring keys per database\n")
	fmt.Fprintf(w, "# TYPE godis_db_keys_expiring gauge\n")
	for _, db := range dbStats {
		fmt.Fprintf(w, "godis_db_keys_expiring{db=\"%d\"} %d\n", db.Index, db.Expires)
	}

	fmt.Fprintf(w, "# HELP godis_sequence Write sequence (proxy: total commands processed)\n")
	fmt.Fprintf(w, "# TYPE godis_sequence gauge\n")
	fmt.Fprintf(w, "godis_sequence %d\n", m.totalCommands.Load())

	fmt.Fprintf(w, "# HELP godis_replica_resyncs_full Full resyncs served\n")
	fmt.Fprintf(w, "# TYPE godis_replica_resyncs_full counter\n")
	fmt.Fprintf(w, "godis_replica_resyncs_full %d\n", m.fullResyncs.Load())
	fmt.Fprintf(w, "# HELP godis_replica_partial_resync_accepted Accepted partial resyncs\n")
	fmt.Fprintf(w, "# TYPE godis_replica_partial_resync_accepted counter\n")
	fmt.Fprintf(w, "godis_replica_partial_resync_accepted %d\n", m.partialResync.Load())
	fmt.Fprintf(w, "# HELP godis_replica_partial_resync_denied Denied partial resyncs (fell back to full resync)\n")
	fmt.Fprintf(w, "# TYPE godis_replica_partial_resync_denied counter\n")
	fmt.Fprintf(w, "godis_replica_partial_resync_denied %d\n", m.partialResyncDenied.Load())

	// ---- Cluster metrics (only in cluster mode) ----
	if m.clusterStatsFn != nil {
		cs := m.clusterStatsFn()
		enabled := 0
		if cs.Enabled {
			enabled = 1
		}
		fmt.Fprintf(w, "# HELP godis_cluster_enabled Whether cluster mode is enabled on this node\n")
		fmt.Fprintf(w, "# TYPE godis_cluster_enabled gauge\n")
		fmt.Fprintf(w, "godis_cluster_enabled %d\n", enabled)

		fmt.Fprintf(w, "# HELP godis_cluster_mode Cluster mode: raft, master-slave or standalone\n")
		fmt.Fprintf(w, "# TYPE godis_cluster_mode gauge\n")
		fmt.Fprintf(w, "godis_cluster_mode{mode=\"%s\"} 1\n", cs.Mode)

		fmt.Fprintf(w, "# HELP godis_cluster_role Role of this node: master, slave or none\n")
		fmt.Fprintf(w, "# TYPE godis_cluster_role gauge\n")
		fmt.Fprintf(w, "godis_cluster_role{role=\"%s\"} 1\n", cs.Role)

		fmt.Fprintf(w, "# HELP godis_cluster_master_info Current master of this node\n")
		fmt.Fprintf(w, "# TYPE godis_cluster_master_info gauge\n")
		fmt.Fprintf(w, "godis_cluster_master_info{master_id=\"%s\",master_addr=\"%s\"} 1\n",
			sanitizeLabel(cs.MasterID), sanitizeLabel(cs.MasterAddr))

		fmt.Fprintf(w, "# HELP godis_cluster_node_count Number of nodes in the cluster\n")
		fmt.Fprintf(w, "# TYPE godis_cluster_node_count gauge\n")
		fmt.Fprintf(w, "godis_cluster_node_count %d\n", cs.NodeCount)

		fmt.Fprintf(w, "# HELP godis_cluster_wal_seq_applied Highest WAL segment seq applied locally\n")
		fmt.Fprintf(w, "# TYPE godis_cluster_wal_seq_applied gauge\n")
		fmt.Fprintf(w, "godis_cluster_wal_seq_applied %d\n", cs.WalSeqApplied)

		fmt.Fprintf(w, "# HELP godis_cluster_wal_seq_published Highest WAL segment seq published by master\n")
		fmt.Fprintf(w, "# TYPE godis_cluster_wal_seq_published gauge\n")
		fmt.Fprintf(w, "godis_cluster_wal_seq_published %d\n", cs.WalSeqPublished)

		fmt.Fprintf(w, "# HELP godis_cluster_sync_last_success Unix timestamp of last successful WAL sync\n")
		fmt.Fprintf(w, "# TYPE godis_cluster_sync_last_success gauge\n")
		fmt.Fprintf(w, "godis_cluster_sync_last_success %d\n", cs.SyncLastSuccess)

		fmt.Fprintf(w, "# HELP godis_cluster_sync_lag_seconds Time since last successful WAL sync\n")
		fmt.Fprintf(w, "# TYPE godis_cluster_sync_lag_seconds gauge\n")
		fmt.Fprintf(w, "godis_cluster_sync_lag_seconds %v\n", cs.SyncLagSeconds)

		fmt.Fprintf(w, "# HELP godis_cluster_wal_upload_ms Last WAL segment upload duration in ms\n")
		fmt.Fprintf(w, "# TYPE godis_cluster_wal_upload_ms gauge\n")
		fmt.Fprintf(w, "godis_cluster_wal_upload_ms %v\n", cs.LastUploadMs)

		fmt.Fprintf(w, "# HELP godis_cluster_wal_download_ms Last WAL segment download duration in ms\n")
		fmt.Fprintf(w, "# TYPE godis_cluster_wal_download_ms gauge\n")
		fmt.Fprintf(w, "godis_cluster_wal_download_ms %v\n", cs.LastDownloadMs)
	}

	// ---- Go runtime metrics ----
	fmt.Fprintf(w, "# HELP go_goroutines Number of goroutines\n")
	fmt.Fprintf(w, "# TYPE go_goroutines gauge\n")
	fmt.Fprintf(w, "go_goroutines %d\n", runtime.NumGoroutine())

	fmt.Fprintf(w, "# HELP go_gc_duration_seconds A summary of GC pauses\n")
	fmt.Fprintf(w, "# TYPE go_gc_duration_seconds summary\n")
	fmt.Fprintf(w, "go_gc_duration_seconds{quantile=\"0\"} %v\n", quantile(memStats.PauseNs[:], 0))
	fmt.Fprintf(w, "go_gc_duration_seconds{quantile=\"0.5\"} %v\n", quantile(memStats.PauseNs[:], 0.5))
	fmt.Fprintf(w, "go_gc_duration_seconds{quantile=\"0.9\"} %v\n", quantile(memStats.PauseNs[:], 0.9))
	fmt.Fprintf(w, "go_gc_duration_seconds{quantile=\"1\"} %v\n", quantile(memStats.PauseNs[:], 1))
	fmt.Fprintf(w, "go_gc_duration_seconds_sum %v\n", float64(memStats.PauseTotalNs)/1e9)
	fmt.Fprintf(w, "go_gc_duration_seconds_count %d\n", memStats.NumGC)
}

// sanitizeLabel escapes a Prometheus label value.
func sanitizeLabel(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

func quantile(nums []uint64, q float64) float64 {
	if len(nums) == 0 {
		return 0
	}
	n := int(math.Round(float64(len(nums)-1) * q))
	if n < 0 {
		n = 0
	}
	if n >= len(nums) {
		n = len(nums) - 1
	}
	sorted := make([]uint64, len(nums))
	copy(sorted, nums)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	return float64(sorted[n]) / 1e9
}

// StartMetricsServer starts an HTTP server for Prometheus metrics scraping.
func StartMetricsServer(metrics *Metrics) {
	addr := fmt.Sprintf(":%d", config.Properties.PrometheusPort)
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics)
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	go func() {
		slog.Info("starting Prometheus metrics server", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "error", err)
		}
	}()
}
