// Package web provides a web-based dashboard for godis with query and monitoring.
package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/redis/client"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

//go:embed dashboard.html.tmpl
var templateFS embed.FS

// DashboardServer serves the web dashboard.
type DashboardServer struct {
	client     *client.Client
	httpServer *http.Server
	startTime  time.Time
}

var (
	webHotKeysMu   sync.RWMutex
	webHotKeyCount = make(map[string]int64)
	webBigKeysMu   sync.RWMutex
	webBigKeys     []bigKeyEntry
	webStatsMu     sync.RWMutex
	webOpsSec      int64
	webConnections int64
	webCommands    int64
)

// RecordKeyAccess records a key access for the dashboard hot key tracking.
func RecordKeyAccess(key string) {
	webHotKeysMu.Lock()
	webHotKeyCount[key]++
	webHotKeysMu.Unlock()
}

// RecordCommand records a command execution.
func RecordCommand() {
	webCommands++
}

// SetStats updates the dashboard statistics.
func SetStats(opsSec, connections, commands int64) {
	webOpsSec = opsSec
	webConnections = connections
	webCommands = commands
}

// RecordBigKey records a big key observation for the dashboard.
func RecordBigKey(key string, db int, typ string, size, bytes int64) {
	webBigKeysMu.Lock()
	// Keep last 100 big keys
	if len(webBigKeys) >= 100 {
		webBigKeys = webBigKeys[1:]
	}
	webBigKeys = append(webBigKeys, bigKeyEntry{Key: key, DB: db, Type: typ, Size: size, Bytes: bytes})
	webBigKeysMu.Unlock()
}

type monitorData struct {
	HotKeys []hotKeyEntry `json:"hotKeys"`
	BigKeys []bigKeyEntry `json:"bigKeys"`
	DBStats []dbStatEntry `json:"dbStats"`
}

type hotKeyEntry struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

type bigKeyEntry struct {
	Key   string `json:"key"`
	DB    int    `json:"db"`
	Type  string `json:"type"`
	Size  int64  `json:"size"`
	Bytes int64  `json:"bytes"`
}

type dbStatEntry struct {
	Index   int   `json:"index"`
	Keys    int64 `json:"keys"`
	Expires int64 `json:"expires"`
}

type statsData struct {
	Uptime      int64  `json:"uptime"`
	Goroutines  int    `json:"goroutines"`
	Memory      uint64 `json:"memory"`
	Connections int64  `json:"connections"`
	Commands    int64  `json:"commands"`
	OpsPerSec   int64  `json:"opsPerSec"`
}

// NewDashboard creates a new dashboard server.
func NewDashboard(addr string, c *client.Client) *DashboardServer {
	ds := &DashboardServer{
		client:    c,
		startTime: time.Now(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", ds.serveDashboard)
	mux.HandleFunc("/api/query", ds.handleQuery)
	mux.HandleFunc("/api/monitor", ds.handleMonitor)
	mux.HandleFunc("/api/stats", ds.handleStats)

	ds.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	return ds
}

// Start starts the HTTP server.
func (ds *DashboardServer) Start() {
	go func() {
		slog.Info("starting web dashboard", "addr", ds.httpServer.Addr)
		if err := ds.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("dashboard server error", "error", err)
		}
	}()
}

func (ds *DashboardServer) serveDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	tmpl, err := templateFS.ReadFile("dashboard.html.tmpl")
	if err != nil {
		http.Error(w, "template not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(tmpl)
}

var readOnlyCommands = map[string]bool{
	"get": true, "mget": true, "strlen": true, "exists": true, "type": true,
	"ttl": true, "pttl": true, "keys": true, "scan": true, "randomkey": true,
	"hget": true, "hgetall": true, "hmget": true, "hexists": true, "hlen": true,
	"hkeys": true, "hvals": true, "hstrlen": true,
	"lrange": true, "lindex": true, "llen": true,
	"smembers": true, "scard": true, "sismember": true, "sdiff": true, "sinter": true, "sunion": true,
	"zrange": true, "zrevrange": true, "zrank": true, "zrevrank": true, "zscore": true,
	"zcard": true, "zcount": true, "zrangebyscore": true,
	"xrange": true, "xlen": true, "xread": true,
	"geodist": true, "geohash": true, "geopos": true,
	"info": true, "ping": true, "dbsize": true, "time": true,
	"ft.search": true, "ft.info": true, "ft._list": true,
	"ts.get": true, "ts.info": true, "ts.range": true,
	"json.get": true, "json.type": true, "json.strlen": true, "json.objlen": true, "json.objkeys": true,
}

type queryResult struct {
	Success bool   `json:"success"`
	Result  string `json:"result,omitempty"`
	Error   string `json:"error,omitempty"`
}

func (ds *DashboardServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != "POST" {
		json.NewEncoder(w).Encode(queryResult{Error: "only POST allowed"})
		return
	}
	cmd := r.FormValue("cmd")
	if cmd == "" {
		json.NewEncoder(w).Encode(queryResult{Error: "cmd parameter required"})
		return
	}

	parts := parseCommand(cmd)
	if len(parts) == 0 {
		json.NewEncoder(w).Encode(queryResult{Error: "empty command"})
		return
	}

	cmdName := strings.ToLower(string(parts[0]))
	if !readOnlyCommands[cmdName] {
		json.NewEncoder(w).Encode(queryResult{Error: "read-only commands only (write not allowed)"})
		return
	}

	reply := ds.client.Send(parts)
	result := formatReplyForWeb(reply)
	json.NewEncoder(w).Encode(queryResult{Success: true, Result: result})
}

func parseCommand(line string) [][]byte {
	var parts []string
	var current strings.Builder
	inQuote := false
	for _, ch := range line {
		switch {
		case ch == '"':
			inQuote = !inQuote
		case ch == ' ' && !inQuote:
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(ch)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	result := make([][]byte, len(parts))
	for i, p := range parts {
		result[i] = []byte(p)
	}
	return result
}

func formatReplyForWeb(reply interface{}) string {
	if reply == nil {
		return "(nil)"
	}
	switch v := reply.(type) {
	case *protocol.StatusReply:
		return v.Status
	case *protocol.IntReply:
		return fmt.Sprintf("(integer) %d", v.Code)
	case *protocol.BulkReply:
		return string(v.Arg)
	case *protocol.MultiBulkReply:
		if len(v.Args) == 0 {
			return "(empty list or set)"
		}
		var b strings.Builder
		for i, arg := range v.Args {
			if i > 0 {
				b.WriteString("\n")
			}
			b.WriteString(fmt.Sprintf("%d) \"%s\"", i+1, string(arg)))
		}
		return b.String()
	case *protocol.MultiRawReply:
		var b strings.Builder
		for i, reply := range v.Replies {
			if i > 0 {
				b.WriteString("\n")
			}
			b.WriteString(fmt.Sprintf("%d) ", i+1))
			b.WriteString(formatReplyForWeb(reply))
		}
		return b.String()
	default:
		if rr, ok := reply.(redisReply); ok {
			return string(rr.ToBytes())
		}
		return fmt.Sprintf("%v", reply)
	}
}

type redisReply interface{ ToBytes() []byte }

func (ds *DashboardServer) handleMonitor(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	data := monitorData{}

	// Hot keys
	webHotKeysMu.RLock()
	type kv struct {
		key   string
		count int64
	}
	var sorted []kv
	for k, c := range webHotKeyCount {
		sorted = append(sorted, kv{k, c})
	}
	webHotKeysMu.RUnlock()
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].count > sorted[j].count
	})
	if len(sorted) > 20 {
		sorted = sorted[:20]
	}
	for _, hk := range sorted {
		data.HotKeys = append(data.HotKeys, hotKeyEntry{Key: hk.key, Count: hk.count})
	}

	// Big keys
	webBigKeysMu.RLock()
	data.BigKeys = make([]bigKeyEntry, len(webBigKeys))
	copy(data.BigKeys, webBigKeys)
	webBigKeysMu.RUnlock()

	// DB stats (from server via RecordBigKey)
	data.DBStats = nil

	json.NewEncoder(w).Encode(data)
}

func (ds *DashboardServer) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	webStatsMu.RLock()
	ops := webOpsSec
	conns := webConnections
	cmds := webCommands
	webStatsMu.RUnlock()

	stats := statsData{
		Uptime:      int64(time.Since(ds.startTime).Seconds()),
		Goroutines:  runtime.NumGoroutine(),
		Memory:      memStats.Alloc,
		Connections: conns,
		Commands:    cmds,
		OpsPerSec:   ops,
	}
	json.NewEncoder(w).Encode(stats)
}

// Dependencies used from interface package
var _ = redis.Reply(nil)

// ResetHotKeys clears the hot key tracking (called periodically).
func ResetHotKeys() {
	webHotKeysMu.Lock()
	defer webHotKeysMu.Unlock()
	webHotKeyCount = make(map[string]int64)
}
