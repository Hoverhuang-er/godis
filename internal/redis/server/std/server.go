package std

/*
 * A tcp.Handler implements redis protocol
 */

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	stdatomic "sync/atomic"

	"github.com/Hoverhuang-er/godis/internal/cluster"
	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/database"
	idatabase "github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/lib/sync/atomic"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/parser"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"github.com/Hoverhuang-er/godis/internal/tcp"
	"log/slog"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// Handler implements tcp.Handler and serves as a redis server
type Handler struct {
	activeConn sync.Map // *client -> placeholder
	// dbPtr is swapped atomically so a running node can switch between
	// standalone and cluster mode without dropping client connections:
	// each command loads the pointer once and executes against that view.
	dbPtr   stdatomic.Pointer[idatabase.DB]
	closing atomic.Boolean // refusing new client and new request
}

func (h *Handler) setDB(db idatabase.DB) {
	h.dbPtr.Store(&db)
}

func (h *Handler) getDB() idatabase.DB {
	return *h.dbPtr.Load()
}

// CurrentDB returns the active database engine (see DBSwapper in the cluster
// package, used for the runtime standalone-to-cluster switch).
func (h *Handler) CurrentDB() idatabase.DB {
	return h.getDB()
}

// MakeHandler creates a Handler instance
func MakeHandler() *Handler {
	h := &Handler{}
	h.setDB(makeDB())
	return h
}

// makeDB builds the initial database engine from current config.
func makeDB() idatabase.DB {
	if config.Properties.ClusterEnable {
		return cluster.MakeCluster(nil)
	}
	return database.NewStandaloneServer()
}

// SwapDB atomically replaces the active database engine. Existing client
// connections stay open; subsequent commands are routed through the new
// engine.
func (h *Handler) SwapDB(db idatabase.DB) {
	h.setDB(db)
}

func Serve(addr string, handler *Handler, stopCh <-chan struct{}) error {
	return tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: addr,
	}, handler, stopCh)
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.getDB().AfterClientClose(client)
	h.activeConn.Delete(client)
}


// Handle receives and executes redis commands
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn)
	// Track connection for Prometheus metrics
	if srv, ok := h.getDB().(*database.Server); ok && srv.GetMetrics() != nil {
		srv.GetMetrics().IncrConnections()
	}

	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				slog.Info("connection closed: " + client.RemoteAddr())
				return
			}
			// protocol err
			errReply := protocol.MakeErrReply(payload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				slog.Info("connection closed: " + client.RemoteAddr())
				return
			}
			continue
		}
		if payload.Data == nil {
			slog.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			slog.Error("require multi bulk protocol")
			continue
		}
		result := h.getDB().Exec(client, r.Args)
		if result != nil {
			_, _ = client.Write(result.ToBytes())
		} else {
			_, _ = client.Write(unknownErrReplyBytes)
		}
	}
}

// Close stops handler
func (h *Handler) Close() error {
	slog.Info("handler shutting down...")
	h.closing.Set(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.getDB().Close()
	return nil
}
