package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	rclient "github.com/Hoverhuang-er/godis/internal/redis/client"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"log/slog"
)

// ApiServer provides an HTTP API for godis operations with token-based auth.
type ApiServer struct {
	httpServer   *http.Server
	client       *rclient.Client
	tokenEngine  *TokenEngine
	addr         string
	redisAddr    string
	redisPass    string
	clientReady  chan struct{}
}

// NewApiServer creates a new API server.
// The Redis client connection is established asynchronously when Start() is called.
// addr: HTTP listen address (e.g. ":63790")
// redisAddr: godis TCP address (e.g. "127.0.0.1:6379")
// redisPassword: godis requirepass (may be empty)
func NewApiServer(addr, redisAddr, redisPassword string) *ApiServer {
	te := NewTokenEngine(redisPassword)

	s := &ApiServer{
		addr:        addr,
		redisAddr:   redisAddr,
		redisPass:   redisPassword,
		tokenEngine: te,
		clientReady: make(chan struct{}, 1),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/auth", s.handleAuth)
	mux.HandleFunc("/api/commands", s.authMiddleware(s.handleCommands))

	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s
}

// Start begins serving HTTP requests and establishes a background
// connection to the local godis instance with retry.
func (s *ApiServer) Start() {
	if s == nil || s.httpServer == nil {
		return
	}
	// Start HTTP server
	go func() {
		slog.Info("starting HTTP API server", "addr", s.addr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP API server error", "error", err)
		}
	}()
	// Connect to local godis with retry (TCP server may not be ready yet)
	go s.connectWithRetry()
}

// connectWithRetry connects to the local godis instance with retry.
func (s *ApiServer) connectWithRetry() {
	var c *rclient.Client
	var err error
	for i := 0; i < 10; i++ {
		c, err = rclient.MakeClient(s.redisAddr)
		if err == nil {
			break
		}
		slog.Debug("waiting for godis TCP server...", "attempt", i+1, "error", err)
		time.Sleep(time.Second)
	}
	if c == nil {
		slog.Error("failed to connect to godis after retries", "addr", s.redisAddr)
		return
	}
	c.Start()

	// Authenticate if password is set
	if s.redisPass != "" {
		reply := c.Send(utils.ToCmdLine("AUTH", s.redisPass))
		if errMsg := extractError(reply); errMsg != "" {
			slog.Error("API server AUTH to godis failed", "error", errMsg)
			c.Close()
			return
		}
	}

	s.client = c
	s.clientReady <- struct{}{}
	slog.Info("API server connected to godis", "addr", s.redisAddr)
}

// getClient returns the Redis client, waiting for it to be ready if needed.
func (s *ApiServer) getClient() *rclient.Client {
	if s.client != nil {
		return s.client
	}
	// Wait for the connection to be established
	<-s.clientReady
	return s.client
}

// Stop shuts down the API server gracefully.
func (s *ApiServer) Stop() {
	if s == nil {
		return
	}
	if s.client != nil {
		s.client.Close()
	}
	if s.tokenEngine != nil {
		s.tokenEngine.Stop()
	}
	if s.httpServer != nil {
		_ = s.httpServer.Close()
	}
}

// ---- Auth Handler ----

type authRequest struct {
	Password string `json:"password"`
	Expired  int    `json:"expired"` // hours; 0 = permanent
}

type authResponse struct {
	Token     string  `json:"token"`
	ExpiresAt *string `json:"expires_at,omitempty"`
	Permanent bool    `json:"permanent"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (s *ApiServer) handleAuth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "only POST allowed"})
		return
	}

	var req authRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}

	if !s.tokenEngine.IsAuthConfigured() {
		writeJSON(w, http.StatusForbidden, errorResponse{Error: "no password configured on server"})
		return
	}

	expiredHours := req.Expired
	if expiredHours < 0 {
		expiredHours = DefaultTokenExpiryHours
	} else if expiredHours == 0 {
		// 0 means permanent
	}

	entry := s.tokenEngine.Authenticate(req.Password, expiredHours)
	if entry == nil {
		writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "invalid password"})
		return
	}

	resp := authResponse{
		Token:     entry.Token,
		Permanent: entry.ExpiresAt == nil,
	}
	if entry.ExpiresAt != nil {
		t := entry.ExpiresAt.Format(time.RFC3339)
		resp.ExpiresAt = &t
	}

	writeJSON(w, http.StatusOK, resp)
}

// ---- Auth Middleware ----

const AuthHeader = "X-HEADER-AUTHTOKEN"

func (s *ApiServer) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get(AuthHeader)
		if token == "" {
			writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "missing X-HEADER-AUTHTOKEN header"})
			return
		}
		if !s.tokenEngine.ValidateToken(token) {
			writeJSON(w, http.StatusUnauthorized, errorResponse{Error: "unauthorized - invalid or expired token"})
			return
		}
		next(w, r)
	}
}

// ---- Commands Handler ----

type commandResult struct {
	Success bool        `json:"success"`
	Result  interface{} `json:"result,omitempty"`
	Raw     string      `json:"raw,omitempty"`
	Error   string      `json:"error,omitempty"`
}

func (s *ApiServer) handleCommands(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, commandResult{Error: "only GET allowed"})
		return
	}

	q := r.URL.Query()
	cmdType := q.Get("type")
	key := q.Get("key")
	value := q.Get("value")
	field := q.Get("field")
	member := q.Get("member")
	scoreStr := q.Get("score")
	argsRaw := q.Get("args")

	if cmdType == "" {
		writeJSON(w, http.StatusBadRequest, commandResult{Error: "type parameter is required"})
		return
	}

	cmd, args := buildCommand(cmdType, key, value, field, member, scoreStr, argsRaw)
	if cmd == "" {
		writeJSON(w, http.StatusBadRequest, commandResult{Error: "unable to determine command from type: " + cmdType})
		return
	}

	cliArgs := utils.ToCmdLine(cmd)
	cliArgs = append(cliArgs, utils.ToCmdLine(args...)...)

	// Set database if specified
	dbStr := q.Get("db")
	if dbStr != "" {
		dbIdx, err := strconv.Atoi(dbStr)
		if err == nil && dbIdx >= 0 && dbIdx < config.Properties.Databases {
			s.getClient().Send(utils.ToCmdLine("SELECT", strconv.Itoa(dbIdx)))
		}
	}
	reply := s.getClient().Send(cliArgs)
	result := formatReplyValue(reply)
	raw := string(reply.ToBytes())

	if isErrorReply(reply) {
		writeJSON(w, http.StatusOK, commandResult{
			Success: false,
			Error:   result,
			Raw:     raw,
		})
		return
	}

	writeJSON(w, http.StatusOK, commandResult{
		Success: true,
		Result:  result,
		Raw:     raw,
	})
}

// buildCommand translates query parameters into a Redis command and its arguments.
func buildCommand(cmdType, key, value, field, member, scoreStr, argsRaw string) (string, []string) {
	cmd := strings.ToLower(cmdType)
	var args []string

	// If cmdType is already a valid Redis command, use it directly
	switch cmd {
	// String commands (GET, SET can be inferred from presence of value)
	case "get":
		if key != "" {
			args = append(args, key)
		}
		return cmd, args
	case "set", "setnx", "setex", "psetex", "append", "getset", "getdel":
		if key != "" {
			args = append(args, key)
		}
		if value != "" {
			args = append(args, value)
		}
		if argsRaw != "" {
			args = append(args, strings.Fields(argsRaw)...)
		}
		return cmd, args
	case "incr", "incrby", "incrbyfloat", "decr", "decrby":
		if key != "" {
			args = append(args, key)
		}
		if value != "" {
			args = append(args, value)
		}
		return cmd, args
	case "strlen":
		if key != "" {
			args = append(args, key)
		}
		return cmd, args

	// Hash commands
	case "hget", "hexists", "hlen", "hstrlen", "hkeys", "hvals", "hgetall":
		if key != "" {
			args = append(args, key)
		}
		if field != "" {
			args = append(args, field)
		}
		return cmd, args
	case "hset", "hsetnx":
		if key != "" {
			args = append(args, key)
		}
		if field != "" {
			args = append(args, field)
		}
		if value != "" {
			args = append(args, value)
		}
		return cmd, args
	case "hdel":
		if key != "" {
			args = append(args, key)
		}
		if field != "" {
			args = append(args, field)
		}
		if argsRaw != "" {
			args = append(args, strings.Fields(argsRaw)...)
		}
		return cmd, args
	case "hmget":
		if key != "" {
			args = append(args, key)
		}
		if field != "" {
			args = append(args, field)
		}
		if argsRaw != "" {
			args = append(args, strings.Fields(argsRaw)...)
		}
		return cmd, args
	case "hincrby", "hincrbyfloat":
		if key != "" {
			args = append(args, key)
		}
		if field != "" {
			args = append(args, field)
		}
		if value != "" {
			args = append(args, value)
		}
		return cmd, args

	// List commands
	case "lpush", "rpush", "lpushx", "rpushx":
		if key != "" {
			args = append(args, key)
		}
		if value != "" {
			args = append(args, value)
		}
		return cmd, args
	case "lpop", "rpop", "llen", "lrange", "lindex":
		if key != "" {
			args = append(args, key)
		}
		if value != "" {
			args = append(args, value)
		}
		if argsRaw != "" {
			args = append(args, strings.Fields(argsRaw)...)
		}
		return cmd, args
	case "lrem":
		if key != "" {
			args = append(args, key)
		}
		if value != "" {
			args = append(args, value)
		}
		if member != "" {
			args = append(args, member)
		}
		return cmd, args

	// Set commands
	case "sadd", "srem":
		if key != "" {
			args = append(args, key)
		}
		if member != "" {
			args = append(args, member)
		}
		if argsRaw != "" {
			args = append(args, strings.Fields(argsRaw)...)
		}
		return cmd, args
	case "sismember", "smove", "spop":
		if key != "" {
			args = append(args, key)
		}
		if member != "" {
			args = append(args, member)
		}
		return cmd, args
	case "scard", "smembers", "srandmember":
		if key != "" {
			args = append(args, key)
		}
		return cmd, args

	// Sorted Set commands
	case "zadd":
		if key != "" {
			args = append(args, key)
		}
		if scoreStr != "" {
			args = append(args, scoreStr)
		}
		if member != "" {
			args = append(args, member)
		}
		if argsRaw != "" {
			args = append(args, strings.Fields(argsRaw)...)
		}
		return cmd, args
	case "zrem", "zscore", "zrank", "zrevrank":
		if key != "" {
			args = append(args, key)
		}
		if member != "" {
			args = append(args, member)
		}
		return cmd, args
	case "zcard", "zcount":
		if key != "" {
			args = append(args, key)
		}
		if value != "" {
			args = append(args, value)
		}
		if argsRaw != "" {
			args = append(args, strings.Fields(argsRaw)...)
		}
		return cmd, args
	case "zrange", "zrevrange":
		if key != "" {
			args = append(args, key)
		}
		if value != "" {
			args = append(args, value)
		}
		if argsRaw != "" {
			args = append(args, strings.Fields(argsRaw)...)
		}
		return cmd, args
	case "zincrby":
		if key != "" {
			args = append(args, key)
		}
		if scoreStr != "" {
			args = append(args, scoreStr)
		}
		if member != "" {
			args = append(args, member)
		}
		return cmd, args

	// Key commands
	case "del":
		if key != "" {
			args = append(args, key)
		}
		if argsRaw != "" {
			args = append(args, strings.Fields(argsRaw)...)
		}
		return cmd, args
	case "exists", "expire", "expireat", "pexpire", "pexpireat":
		if key != "" {
			args = append(args, key)
		}
		if value != "" {
			args = append(args, value)
		}
		return cmd, args
	case "ttl", "pttl", "persist", "type":
		if key != "" {
			args = append(args, key)
		}
		return cmd, args
	case "rename", "renamenx":
		if key != "" {
			args = append(args, key)
		}
		if value != "" {
			args = append(args, value)
		}
		return cmd, args
	case "copy":
		if key != "" {
			args = append(args, key)
		}
		if value != "" {
			args = append(args, value)
		}
		if argsRaw != "" {
			args = append(args, strings.Fields(argsRaw)...)
		}
		return cmd, args

	// Server commands
	case "ping":
		return cmd, args
	case "dbsize", "flushdb", "flushall", "save", "bgsave":
		return cmd, args
	case "info":
		if value != "" {
			args = append(args, value)
		}
		return cmd, args
	case "select":
		if key != "" {
			args = append(args, key)
		}
		return cmd, args
	case "keys":
		if value != "" {
			args = append(args, value)
		} else if key != "" {
			// allow "key" positional for keys pattern
			args = append(args, key)
		}
		return cmd, args
	case "config":
		if key != "" {
			args = append(args, key)
		}
		if value != "" {
			args = append(args, value)
		}
		return cmd, args

	// Module commands (pass through)
	default:
		// For unknown types, treat cmdType as the actual command name
		// and use args for all positional params
		if key != "" {
			args = append(args, key)
		}
		if value != "" {
			args = append(args, value)
		}
		if field != "" {
			args = append(args, field)
		}
		if member != "" {
			args = append(args, member)
		}
		if scoreStr != "" {
			args = append(args, scoreStr)
		}
		if argsRaw != "" {
			args = append(args, strings.Fields(argsRaw)...)
		}
		return cmd, args
	}
}

// ---- Reply Formatting ----

func formatReplyValue(reply interface{}) string {
	if reply == nil {
		return "(nil)"
	}
	switch v := reply.(type) {
	case *protocol.StatusReply:
		return v.Status
	case *protocol.IntReply:
		return fmt.Sprintf("%d", v.Code)
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
		for i, sub := range v.Replies {
			if i > 0 {
				b.WriteString("\n")
			}
			b.WriteString(fmt.Sprintf("%d) ", i+1))
			b.WriteString(formatReplyValue(sub))
		}
		return b.String()
	default:
		if rr, ok := reply.(interface{ ToBytes() []byte }); ok {
			return string(rr.ToBytes())
		}
		return fmt.Sprintf("%v", reply)
	}
}

func isErrorReply(reply interface{}) bool {
	if reply == nil {
		return false
	}
	if _, ok := reply.(*protocol.StandardErrReply); ok {
		return true
	}
	if rr, ok := reply.(interface{ ToBytes() []byte }); ok {
		return len(rr.ToBytes()) > 0 && rr.ToBytes()[0] == '-'
	}
	return false
}

func extractError(reply interface{}) string {
	if reply == nil {
		return ""
	}
	if err, ok := reply.(*protocol.StandardErrReply); ok {
		return err.Status
	}
	if _, ok := reply.(*protocol.StatusReply); ok {
		return ""
	}
	if rr, ok := reply.(interface{ ToBytes() []byte }); ok {
		b := rr.ToBytes()
		if len(b) > 0 && b[0] == '-' {
			return string(b[1 : len(b)-2])
		}
	}
	return ""
}

