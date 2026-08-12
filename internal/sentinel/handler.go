package sentinel

import (
	"context"
	"io"
	"net"
	"strconv"
	"strings"

	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/redis/parser"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"github.com/Hoverhuang-er/godis/internal/tcp"
)

// Handler implements tcp.Handler and serves the SENTINEL command surface that
// client libraries use for master discovery and failover awareness.
type Handler struct {
	s *Sentinel
}

// NewHandler creates a SENTINEL handler around the given sentinel runtime.
func NewHandler(s *Sentinel) *Handler {
	return &Handler{s: s}
}

// Serve starts the sentinel RESP server on addr until stopCh closes.
func Serve(addr string, handler *Handler, stopCh <-chan struct{}) error {
	return tcp.ListenAndServeWithSignal(&tcp.Config{Address: addr}, handler, stopCh)
}

// Handle parses RESP commands and dispatches them.
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF || payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				return
			}
			_, _ = conn.Write(protocol.MakeErrReply(payload.Err.Error()).ToBytes())
			continue
		}
		multi, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			_, _ = conn.Write(protocol.MakeErrReply("protocol error: expected array").ToBytes())
			continue
		}
		reply := h.dispatch(multi.Args)
		if reply != nil {
			_, _ = conn.Write(reply.ToBytes())
		}
	}
}

// Close is a no-op; the probe loop lifecycle is owned by the sentinel runtime.
func (h *Handler) Close() error { return nil }

func (h *Handler) dispatch(args [][]byte) redis.Reply {
	if len(args) == 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}
	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case "PING":
		return &protocol.PongReply{}
	case "INFO":
		// minimal info reply; client libs only use it for diagnostics
		return protocol.MakeStatusReply("# Sentinel\r\nsentinel_version:1.0.0\r\n")
	case "SENTINEL":
		return h.dispatchSentinel(args[1:])
	default:
		return protocol.MakeErrReply("ERR unknown command '" + string(args[0]) + "'")
	}
}

func (h *Handler) dispatchSentinel(args [][]byte) redis.Reply {
	if len(args) == 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'sentinel' command")
	}
	sub := strings.ToUpper(string(args[0]))
	rest := args[1:]
	switch sub {
	case "MASTERS":
		return masterListReply(h.s.monitorList())
	case "MASTER":
		if len(rest) < 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments")
		}
		m, err := h.s.monitorSnapshot(string(rest[0]))
		if err != nil {
			return protocol.MakeErrReply("ERR No such master with that name")
		}
		return masterInfoReply(m)
	case "REPLICAS", "SLAVES":
		if len(rest) < 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments")
		}
		m, err := h.s.monitorSnapshot(string(rest[0]))
		if err != nil {
			return protocol.MakeErrReply("ERR No such master with that name")
		}
		return replicasReply(m)
	case "SENTINELS":
		// single-sentinel deployments report no peer sentinels
		return protocol.MakeMultiBulkReply([][]byte{})
	case "GET-MASTER-ADDR-BY-NAME":
		if len(rest) < 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments")
		}
		m, err := h.s.monitorSnapshot(string(rest[0]))
		if err != nil {
			return protocol.MakeMultiBulkReply(nil) // null multi-bulk
		}
		return protocol.MakeMultiBulkReply([][]byte{
			[]byte(m.Host), []byte(strconv.Itoa(m.Port)),
		})
	case "IS-MASTER-DOWN-BY-ADDR":
		if len(rest) < 4 {
			return protocol.MakeErrReply("ERR wrong number of arguments")
		}
		down, found := h.s.isMasterDownByAddr(string(rest[0]), string(rest[1]))
		if !found {
			// unknown master: report up, as real sentinel does for non-monitored addrs
			down = false
		}
		var downFlag []byte
		if down {
			downFlag = []byte("1")
		} else {
			downFlag = []byte("0")
		}
		// [down_state, leader, leader_epoch]
		return protocol.MakeMultiBulkReply([][]byte{downFlag, []byte("*"), []byte("0")})
	case "CKQUORUM":
		m, err := h.s.monitorSnapshot(string(rest[0]))
		if err != nil {
			return protocol.MakeErrReply("ERR No such master with that name")
		}
		if m.oDown {
			return protocol.MakeStatusReply("NOQUORUM")
		}
		return protocol.MakeStatusReply("OK 1 usable Sentinels. Quorum: 1")
	case "FAILOVER":
		if len(rest) < 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments")
		}
		if err := h.s.requestFailover(string(rest[0])); err != nil {
			return protocol.MakeErrReply("ERR " + err.Error())
		}
		return protocol.MakeOkReply()
	case "RESET":
		return protocol.MakeIntReply(0)
	default:
		return protocol.MakeErrReply("ERR unknown subcommand '" + string(args[0]) + "'")
	}
}

// masterListReply flattens each monitor into field/value pairs.
func masterListReply(monitors []*Monitor) redis.Reply {
	var args [][]byte
	for _, m := range monitors {
		args = append(args, masterInfoPairs(m)...)
	}
	if args == nil {
		args = [][]byte{}
	}
	return protocol.MakeMultiBulkReply(args)
}

func masterInfoReply(m *Monitor) redis.Reply {
	return protocol.MakeMultiBulkReply(masterInfoPairs(m))
}

func masterInfoPairs(m *Monitor) [][]byte {
	host := m.Host
	port := strconv.Itoa(m.Port)
	state := "ok"
	if m.oDown {
		state = "odown"
	} else if m.sDown {
		state = "sdown"
	}
	return [][]byte{
		[]byte("name"), []byte(m.Name),
		[]byte("ip"), []byte(host),
		[]byte("port"), []byte(port),
		[]byte("runid"), []byte(""),
		[]byte("flags"), []byte(state),
		[]byte("quorum"), []byte(strconv.Itoa(m.Quorum)),
		[]byte("config-epoch"), []byte(strconv.FormatInt(m.configEpoch, 10)),
	}
}

func replicasReply(m *Monitor) redis.Reply {
	var args [][]byte
	for addr, r := range m.replicas {
		host, portStr, _ := net.SplitHostPort(addr)
		args = append(args, [][]byte{
			[]byte("name"), []byte(addr),
			[]byte("ip"), []byte(host),
			[]byte("port"), []byte(portStr),
			[]byte("runid"), []byte(""),
			[]byte("flags"), []byte("slave"),
			[]byte("master-link-status"), []byte("ok"),
			[]byte("slave-repl-offset"), []byte(strconv.FormatInt(r.Offset, 10)),
		}...)
	}
	if args == nil {
		args = [][]byte{}
	}
	return protocol.MakeMultiBulkReply(args)
}
