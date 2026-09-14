package web

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hoverhuang-er/godis/internal/redis/parser"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"log/slog"
)

// sseClient is one SSE subscriber.
type sseClient struct {
	id       int64
	channels map[string]bool
	msgCh    chan sseMessage
	done     chan struct{}
}

type sseMessage struct {
	Channel string `json:"channel"`
	Message string `json:"message"`
}

var sseClientSeq atomic.Int64

// SseHub manages SSE subscribers and a dedicated Redis pubsub connection.
type SseHub struct {
	mu         sync.Mutex
	clients    map[int64]*sseClient
	subscribed map[string]int // channel -> refcount
	redisAddr  string
	redisPass  string
	stopCh     chan struct{}
	running    atomic.Bool
}

var defaultSseHub *SseHub

// InitSseHub creates the global SSE hub.
func InitSseHub(redisAddr, redisPass string) *SseHub {
	h := &SseHub{
		clients:    make(map[int64]*sseClient),
		subscribed: make(map[string]int),
		redisAddr:  redisAddr,
		redisPass:  redisPass,
		stopCh:     make(chan struct{}),
	}
	defaultSseHub = h
	return h
}

// Stop shuts down the SSE hub.
func (h *SseHub) Stop() {
	close(h.stopCh)
	h.mu.Lock()
	for _, c := range h.clients {
		close(c.done)
	}
	h.mu.Unlock()
}

func (h *SseHub) addClient(c *sseClient) {
	h.mu.Lock()
	h.clients[c.id] = c
	for ch := range c.channels {
		h.subscribed[ch]++
	}
	h.mu.Unlock()
	h.ensurePubsubRunning()
}

func (h *SseHub) removeClient(c *sseClient) {
	h.mu.Lock()
	delete(h.clients, c.id)
	for ch := range c.channels {
		h.subscribed[ch]--
		if h.subscribed[ch] <= 0 {
			delete(h.subscribed, ch)
		}
	}
	h.mu.Unlock()
}

func (h *SseHub) broadcast(channel, message string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.clients {
		if c.channels[channel] {
			select {
			case c.msgCh <- sseMessage{Channel: channel, Message: message}:
			default:
			}
		}
	}
}

// ensurePubsubRunning starts the pubsub reader goroutine if not already running.
func (h *SseHub) ensurePubsubRunning() {
	if h.running.CompareAndSwap(false, true) {
		go h.pubsubReaderLoop()
	}
}

// pubsubReaderLoop maintains a dedicated TCP connection to godis,
// subscribes to all needed channels, and reads push messages.
// On disconnect, it reconnects and resubscribes.
func (h *SseHub) pubsubReaderLoop() {
	defer h.running.Store(false)

	for {
		select {
		case <-h.stopCh:
			return
		default:
		}

		conn := h.connectAndSubscribe()
		if conn == nil {
			select {
			case <-h.stopCh:
				return
			case <-time.After(3 * time.Second):
			}
			continue
		}

		h.readLoop(conn)
		conn.Close()

		select {
		case <-h.stopCh:
			return
		case <-time.After(1 * time.Second):
		}
	}
}

// connectAndSubscribe opens a TCP connection, authenticates, and subscribes to all needed channels.
func (h *SseHub) connectAndSubscribe() net.Conn {
	conn, err := net.DialTimeout("tcp", h.redisAddr, 5*time.Second)
	if err != nil {
		slog.Error("SSE pubsub: dial failed", "addr", h.redisAddr, "error", err)
		return nil
	}

	// Auth
	if h.redisPass != "" {
		cmd := protocol.MakeMultiBulkReply(toCmdLine("AUTH", h.redisPass)).ToBytes()
		if _, err := conn.Write(cmd); err != nil {
			conn.Close()
			return nil
		}
		// Read AUTH reply
		ch := parser.ParseStream(conn)
		resp := <-ch
		if resp.Err != nil || !protocol.IsOKReply(resp.Data) {
			slog.Error("SSE pubsub: auth failed")
			conn.Close()
			return nil
		}
	}

	// Collect current channels
	h.mu.Lock()
	channels := make([]string, 0, len(h.subscribed))
	for ch, cnt := range h.subscribed {
		if cnt > 0 {
			channels = append(channels, ch)
		}
	}
	h.mu.Unlock()

	if len(channels) == 0 {
		conn.Close()
		return nil
	}

	// Subscribe
	args := make([]string, 0, len(channels)+1)
	args = append(args, "SUBSCRIBE")
	args = append(args, channels...)
	cmd := protocol.MakeMultiBulkReply(toCmdLine(args...)).ToBytes()
	if _, err := conn.Write(cmd); err != nil {
		conn.Close()
		return nil
	}

	// Read subscribe ACKs
	ch := parser.ParseStream(conn)
	for range channels {
		resp := <-ch
		if resp.Err != nil {
			conn.Close()
			return nil
		}
	}

	slog.Info("SSE pubsub: connected", "channels", channels)
	return conn
}

// readLoop reads push messages from the connection and broadcasts to SSE clients.
func (h *SseHub) readLoop(conn net.Conn) {
	ch := parser.ParseStream(conn)
	for {
		select {
		case <-h.stopCh:
			return
		case resp, ok := <-ch:
			if !ok || resp.Err != nil {
				return
			}
			h.handlePushMessage(resp.Data)
		}
	}
}

// handlePushMessage parses a Redis push message (message/smessage/pmessage) and broadcasts.
func (h *SseHub) handlePushMessage(reply interface{ ToBytes() []byte }) {
	arr, ok := reply.(*protocol.MultiBulkReply)
	if !ok {
		return
	}
	lines := arr.Args
	if len(lines) < 3 {
		return
	}
	typ := strings.ToLower(string(lines[0]))
	if typ != "message" {
		return
	}
	channel := string(lines[1])
	message := string(lines[2])
	h.broadcast(channel, message)
}

// handleSubscribe is the SSE endpoint: GET /api/subscribe?channels=ch1,ch2
func (s *ApiServer) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	if defaultSseHub == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "SSE hub not initialized"})
		return
	}

	chStr := r.URL.Query().Get("channels")
	if chStr == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "channels parameter required (comma-separated)"})
		return
	}

	channels := make(map[string]bool)
	for _, ch := range strings.Split(chStr, ",") {
		ch = strings.TrimSpace(ch)
		if ch != "" {
			channels[ch] = true
		}
	}
	if len(channels) == 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "at least one channel required"})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "streaming not supported"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	client := &sseClient{
		id:       sseClientSeq.Add(1),
		channels: channels,
		msgCh:    make(chan sseMessage, 64),
		done:     make(chan struct{}),
	}

	defaultSseHub.addClient(client)
	defer defaultSseHub.removeClient(client)

	chNames := make([]string, 0, len(channels))
	for ch := range channels {
		chNames = append(chNames, ch)
	}
	fmt.Fprintf(w, "event: connected\ndata: {\"client_id\":%d,\"channels\":%s}\n\n",
		client.id, mustMarshal(chNames))
	flusher.Flush()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-client.done:
			return
		case <-ticker.C:
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
		case msg := <-client.msgCh:
			data, _ := json.Marshal(msg)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// ---- Publish HTTP Handler ----

type publishRequest struct {
	Channel string `json:"channel"`
	Message string `json:"message"`
}

type publishResponse struct {
	Success   bool `json:"success"`
	Receivers int  `json:"receivers"`
}

// handlePublish handles POST /api/publish
func (s *ApiServer) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "only POST allowed"})
		return
	}

	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}
	if req.Channel == "" || req.Message == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "channel and message are required"})
		return
	}

	client := s.getClient()
	if client == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "redis connection not ready"})
		return
	}

	reply := client.Send(toCmdLine("PUBLISH", req.Channel, req.Message))
	if protocol.IsErrorReply(reply) {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: string(reply.ToBytes())})
		return
	}

	intReply, ok := reply.(*protocol.IntReply)
	if !ok {
		writeJSON(w, http.StatusOK, publishResponse{Success: true, Receivers: 0})
		return
	}

	writeJSON(w, http.StatusOK, publishResponse{Success: true, Receivers: int(intReply.Code)})
}

// ---- Pipeline HTTP Handler ----

type pipelineRequest struct {
	Commands []pipelineCommand `json:"commands"`
}

type pipelineCommand struct {
	Type   string `json:"type"`
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
	Field  string `json:"field,omitempty"`
	Member string `json:"member,omitempty"`
	Score  string `json:"score,omitempty"`
	Args   string `json:"args,omitempty"`
	DB     string `json:"db,omitempty"`
}

type pipelineResponse struct {
	Results []commandResult `json:"results"`
}

// handlePipeline handles POST /api/pipeline — execute multiple commands in one request.
func (s *ApiServer) handlePipeline(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "only POST allowed"})
		return
	}

	var req pipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}
	if len(req.Commands) == 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "commands array is required"})
		return
	}
	if len(req.Commands) > 100 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "max 100 commands per pipeline request"})
		return
	}

	client := s.getClient()
	if client == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "redis connection not ready"})
		return
	}

	results := make([]commandResult, len(req.Commands))
	for i, cmd := range req.Commands {
		redisCmd, redisArgs := buildCommand(cmd.Type, cmd.Key, cmd.Value, cmd.Field, cmd.Member, cmd.Score, cmd.Args)
		fullArgs := append([]string{redisCmd}, redisArgs...)

		if cmd.DB != "" && cmd.DB != "0" {
			// Execute SELECT first, then the command
			client.Send(toCmdLine("SELECT", cmd.DB))
		}

		reply := client.Send(toCmdLine(fullArgs...))

		if isErrorReply(reply) {
			results[i] = commandResult{
				Success: false,
				Result:  extractError(reply),
				Raw:     string(reply.ToBytes()),
				Error:   extractError(reply),
			}
		} else {
			results[i] = commandResult{
				Success: true,
				Result:  formatReplyValue(reply),
				Raw:     string(reply.ToBytes()),
			}
		}
	}

	writeJSON(w, http.StatusOK, pipelineResponse{Results: results})
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// toCmdLine converts string args to [][]byte for protocol.MakeMultiBulkReply.
func toCmdLine(args ...string) [][]byte {
	result := make([][]byte, len(args))
	for i, a := range args {
		result[i] = []byte(a)
	}
	return result
}
