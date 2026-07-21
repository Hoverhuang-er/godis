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
	"sync"
	"sync/atomic"
	"time"

	"log/slog"

	"github.com/Hoverhuang-er/godis/internal/config"
)

const (
	hotKeyTopN         = 20
	bigKeyTopN         = 20
	bigKeyStringBytes  = 1024 * 1024  // 1 MB string threshold
	bigKeyListLength   = 10000        // 10k elements
	bigKeySetLength    = 5000         // 5k elements
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

	// Ops per second tracking
	opsLastSec int64
	opsCount   int64

	startTime time.Time

	// DB stats callback
	dbStatsFn func() []DBStat

	// Big key tracking
	bigKeysMu sync.RWMutex
	bigKeys   []BigKeyInfo

	// Hot key tracking (key -> access count)
	hotKeysMu   sync.RWMutex
	hotKeyCount map[string]int64
}

// DBStat holds keys/expires count for one database.
type DBStat struct {
	Index   int
	Keys    int64
	Expires int64
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
	Key     string
	DB      int
	Count   int64
	Type    string // string, list, set, zset, hash
}

// New creates a Metrics instance.
func New(dbStatsFn func() []DBStat) *Metrics {
	m := &Metrics{
		startTime:   time.Now(),
		dbStatsFn:   dbStatsFn,
		hotKeyCount: make(map[string]int64),
	}
	go m.tickLoop()
	go m.hotKeyResetLoop()
	return m
}

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

func (m *Metrics) IncrCommands()         { m.totalCommands.Add(1); m.opsCount++ }
func (m *Metrics) IncrConnections()      { m.totalConnections.Add(1); m.currConnections.Add(1) }
func (m *Metrics) DecrConnections()      { m.currConnections.Add(-1) }
func (m *Metrics) IncrKeyspaceHits()     { m.keyspaceHits.Add(1) }
func (m *Metrics) IncrKeyspaceMisses()   { m.keyspaceMisses.Add(1) }

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
	version := "1.3.1"
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
