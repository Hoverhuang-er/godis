package sentinel

import (
	"io"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func startRedis(t *testing.T, port int, replicaOf string) *exec.Cmd {
	t.Helper()
	args := []string{"--port", strconv.Itoa(port), "--save", "", "--appendonly", "no", "--daemonize", "no"}
	if replicaOf != "" {
		args = append(args, "--replicaof", replicaOf)
	}
	cmd := exec.Command("redis-server", args...)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis on %d: %v", port, err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	waitReady(t, port)
	if cmd.ProcessState != nil {
		t.Fatalf("redis on port %d exited during startup: %v", port, cmd.ProcessState)
	}
	return cmd
}

func waitReady(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("redis on port %d did not become ready", port)
}

func TestParseReplicationInfo(t *testing.T) {
	info := parseInfoBody([]byte(
		"# Replication\r\nrole:master\r\nconnected_slaves:1\r\n" +
			"slave0:ip=127.0.0.1,port=6392,state=online,offset=42,lag=0\r\nmaster_repl_offset:42\r\n"))
	if info["role"] != "master" {
		t.Errorf("role = %q, want master", info["role"])
	}
	if info["connected_slaves"] != "1" {
		t.Errorf("connected_slaves = %q, want 1", info["connected_slaves"])
	}
	if !strings.Contains(info["slave0"], "port=6392") {
		t.Errorf("slave0 = %q, want it to contain port=6392", info["slave0"])
	}

	m := &Monitor{replicas: make(map[string]*ReplicaInfo)}
	m.updateReplicas(info)
	if len(m.replicas) != 1 {
		t.Fatalf("replicas = %d, want 1", len(m.replicas))
	}
	r, ok := m.replicas["127.0.0.1:6392"]
	if !ok || r.Offset != 42 {
		t.Errorf("replica 127.0.0.1:6392 = %+v, want offset 42", r)
	}
}

// TestFailover spins up a real master + replica, lets the sentinel observe
// them, kills the master and asserts the sentinel promotes the replica.
func TestFailover(t *testing.T) {
	masterPort := freePort(t)
	replicaPort := freePort(t)

	masterCmd := startRedis(t, masterPort, "")
	startRedis(t, replicaPort, "127.0.0.1 "+strconv.Itoa(masterPort))

	s := New(Config{Monitors: []MonitorConfig{
		{Name: "mymaster", Host: "127.0.0.1", Port: masterPort, Quorum: 1, DownAfterMs: 300},
	}})
	s.probeEvery = 100 * time.Millisecond
	s.Start()
	defer s.Stop()

	h := NewHandler(s)
	getMaster := func() string {
		reply := h.dispatch([][]byte{[]byte("SENTINEL"), []byte("get-master-addr-by-name"), []byte("mymaster")})
		mb, ok := reply.(*protocol.MultiBulkReply)
		if !ok || len(mb.Args) != 2 {
			return ""
		}
		return string(mb.Args[0]) + ":" + string(mb.Args[1])
	}

	// sentinel discovers the master and its replica
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		m, err := s.monitorSnapshot("mymaster")
		if err == nil && getMaster() == "127.0.0.1:"+strconv.Itoa(masterPort) && len(m.replicas) == 1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got := getMaster(); got != "127.0.0.1:"+strconv.Itoa(masterPort) {
		t.Fatalf("master not discovered, get-master-addr = %q", got)
	}

	// kill the master; sentinel should promote the replica
	_ = masterCmd.Process.Kill()

	deadline = time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if getMaster() == "127.0.0.1:"+strconv.Itoa(replicaPort) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got := getMaster(); got != "127.0.0.1:"+strconv.Itoa(replicaPort) {
		t.Fatalf("failover did not happen, get-master-addr = %q, want replica", got)
	}

	// the promoted replica now reports role=master
	_, info := s.probe("127.0.0.1:" + strconv.Itoa(replicaPort))
	if info == nil || info["role"] != "master" {
		t.Fatalf("promoted replica role = %q, want master", info["role"])
	}
}
