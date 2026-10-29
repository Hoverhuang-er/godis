package client

import (
	"bufio"
	"bytes"
	"context"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Hoverhuang-er/godis/internal/lib/logger"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol/asserts"
)

func TestClient(t *testing.T) {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	client, err := MakeClient("localhost:6379")
	if err != nil {
		t.Error(err)
	}
	client.Start()

	result := client.Send([][]byte{
		[]byte("PING"),
	})
	if statusRet, ok := result.(*protocol.StatusReply); ok {
		if statusRet.Status != "PONG" {
			t.Error("`ping` failed, result: " + statusRet.Status)
		}
	}

	result = client.Send([][]byte{
		[]byte("SET"),
		[]byte("a"),
		[]byte("a"),
	})
	if statusRet, ok := result.(*protocol.StatusReply); ok {
		if statusRet.Status != "OK" {
			t.Error("`set` failed, result: " + statusRet.Status)
		}
	}

	result = client.Send([][]byte{
		[]byte("GET"),
		[]byte("a"),
	})
	if bulkRet, ok := result.(*protocol.BulkReply); ok {
		if string(bulkRet.Arg) != "a" {
			t.Error("`get` failed, result: " + string(bulkRet.Arg))
		}
	}

	result = client.Send([][]byte{
		[]byte("DEL"),
		[]byte("a"),
	})
	if intRet, ok := result.(*protocol.IntReply); ok {
		if intRet.Code != 1 {
			t.Error("`del` failed, result: " + strconv.FormatInt(intRet.Code, 10))
		}
	}

	client.doHeartbeat() // random do heartbeat
	result = client.Send([][]byte{
		[]byte("GET"),
		[]byte("a"),
	})
	if _, ok := result.(*protocol.NullBulkReply); !ok {
		t.Error("`get` failed, result: " + string(result.ToBytes()))
	}

	result = client.Send([][]byte{
		[]byte("DEL"),
		[]byte("arr"),
	})

	result = client.Send([][]byte{
		[]byte("RPUSH"),
		[]byte("arr"),
		[]byte("1"),
		[]byte("2"),
		[]byte("c"),
	})
	if intRet, ok := result.(*protocol.IntReply); ok {
		if intRet.Code != 3 {
			t.Error("`rpush` failed, result: " + strconv.FormatInt(intRet.Code, 10))
		}
	}

	result = client.Send([][]byte{
		[]byte("LRANGE"),
		[]byte("arr"),
		[]byte("0"),
		[]byte("-1"),
	})
	if multiBulkRet, ok := result.(*protocol.MultiBulkReply); ok {
		if len(multiBulkRet.Args) != 3 ||
			string(multiBulkRet.Args[0]) != "1" ||
			string(multiBulkRet.Args[1]) != "2" ||
			string(multiBulkRet.Args[2]) != "c" {
			t.Error("`lrange` failed, result: " + string(multiBulkRet.ToBytes()))
		}
	}

	client.Close()
	ret := client.Send(utils.ToCmdLine("ping"))
	asserts.AssertErrReply(t, ret, "client closed")
}

func TestReconnect(t *testing.T) {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "godis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	client, err := MakeClient("localhost:6379")
	if err != nil {
		t.Error(err)
	}
	client.Start()

	_ = client.conn.Close()
	time.Sleep(time.Second) // wait for reconnecting
	success := false
	for i := 0; i < 3; i++ {
		result := client.Send([][]byte{
			[]byte("PING"),
		})
		if bytes.Equal(result.ToBytes(), []byte("+PONG\r\n")) {
			success = true
			break
		}
	}
	if !success {
		t.Error("reconnect error")
	}
}

// TestReconnectBackoff guards against a hot reconnect loop: a peer that
// accepts and immediately hangs up must not be redialed at full speed.
func TestReconnectBackoff(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var dials int32
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			atomic.AddInt32(&dials, 1)
			_ = conn.Close() // accept, then hang up immediately
		}
	}()

	countLogs(t)
	client, err := MakeClient(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	client.Start()
	defer client.Close()

	time.Sleep(3 * time.Second)
	// 1 initial dial + reconnect after 1s + reconnect after 2s.
	if n := atomic.LoadInt32(&dials); n > 5 {
		t.Fatalf("reconnect hot loop: %d dials in 3s", n)
	}
	// The outage is reported exactly once; a peer that never stays up must not
	// trigger a "reconnected" announcement.
	if errs, infos := logCounts(); errs != 1 || infos != 0 {
		t.Fatalf("log spam: %d errors, %d info (want 1, 0)", errs, infos)
	}
}

// TestReconnectRecovers proves the silent retry loop brings the client back
// once the peer returns, and announces it with a single INFO line.
func TestReconnectRecovers(t *testing.T) {
	countLogs(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	var mu sync.Mutex
	var conns []net.Conn
	track := func(c net.Conn) {
		mu.Lock()
		conns = append(conns, c)
		mu.Unlock()
	}
	go servePING(ln, track)

	client, err := MakeClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	client.Start()
	defer client.Close()

	if got := client.Send(utils.ToCmdLine("PING")).ToBytes(); !bytes.Equal(got, []byte("+PONG\r\n")) {
		t.Fatalf("initial PING: %q", got)
	}

	// Outage: stop accepting and drop the live connection.
	_ = ln.Close()
	mu.Lock()
	for _, c := range conns {
		_ = c.Close()
	}
	mu.Unlock()
	time.Sleep(300 * time.Millisecond)

	// The server comes back on the same address.
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer ln2.Close()
	go servePING(ln2, track)

	recovered := false
	for i := 0; i < 10 && !recovered; i++ {
		recovered = bytes.Equal(client.Send(utils.ToCmdLine("PING")).ToBytes(), []byte("+PONG\r\n"))
		if !recovered {
			time.Sleep(time.Second)
		}
	}
	if !recovered {
		t.Fatal("client did not recover after the peer came back")
	}
	if errs, infos := logCounts(); errs != 1 || infos != 1 {
		t.Fatalf("want 1 error + 1 reconnected info, got %d errors, %d infos", errs, infos)
	}
}

// servePING replies +PONG to every command received on ln.
func servePING(ln net.Listener, track func(net.Conn)) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		track(c)
		go func(c net.Conn) {
			defer c.Close()
			r := bufio.NewReader(c)
			for {
				line, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if !strings.HasPrefix(line, "*") {
					continue
				}
				n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
				if err != nil {
					return
				}
				for i := 0; i < 2*n; i++ { // bulk length line + bulk payload
					if _, err := r.ReadString('\n'); err != nil {
						return
					}
				}
				if _, err := c.Write([]byte("+PONG\r\n")); err != nil {
					return
				}
			}
		}(c)
	}
}

var logCountMu sync.Mutex
var logErrs, logInfos int32

type countingHandler struct{}

func (countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (countingHandler) Handle(_ context.Context, r slog.Record) error {
	logCountMu.Lock()
	defer logCountMu.Unlock()
	switch {
	case r.Level >= slog.LevelError:
		logErrs++
	case r.Level == slog.LevelInfo:
		logInfos++
	}
	return nil
}
func (h countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h countingHandler) WithGroup(string) slog.Handler      { return h }

// countLogs installs a counting slog handler for the duration of a test.
func countLogs(t *testing.T) {
	t.Helper()
	logCountMu.Lock()
	logErrs, logInfos = 0, 0
	logCountMu.Unlock()
	prev := slog.Default()
	slog.SetDefault(slog.New(countingHandler{}))
	t.Cleanup(func() { slog.SetDefault(prev) })
}

func logCounts() (int32, int32) {
	logCountMu.Lock()
	defer logCountMu.Unlock()
	return logErrs, logInfos
}
