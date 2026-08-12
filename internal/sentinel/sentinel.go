// Package sentinel implements a Redis Sentinel compatible monitor.
//
// godis can act as a Sentinel for external Redis/KeyDB masters: it probes
// monitored masters and their replicas, tracks S_DOWN/O_DOWN state, and on
// O_DOWN promotes a replica via REPLICAOF NO ONE, then repoints the other
// replicas at the new master. It exposes the SENTINEL command subset that
// client libraries (redis-py, lettuce, etc.) use for master discovery.
package sentinel

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/redis/client"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"log/slog"
)

// CRLF is the RESP line terminator.
const CRLF = "\r\n"

// Monitor describes one monitored master.
type Monitor struct {
	Name        string
	Host        string
	Port        int
	Quorum      int
	DownAfterMs int

	mu            sync.Mutex
	sDown         bool
	oDown         bool
	downSinceMs   int64 // unix ms when the master went silent, 0 if up
	configEpoch   int64
	failingOver   bool
	replicas      map[string]*ReplicaInfo // addr -> info
	lastSwitchMsg string
}

// ReplicaInfo is the last known state of one replica of a monitored master.
type ReplicaInfo struct {
	Addr   string
	Offset int64
	lastOK time.Time
}

// Sentinel is the sentinel runtime: it owns the monitors and serves the
// SENTINEL command surface over a plain RESP server.
type Sentinel struct {
	mu       sync.Mutex
	monitors map[string]*Monitor
	selfAddr string

	probeEvery time.Duration
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

// MonitorConfig is the static per-monitor configuration from the config file.
type MonitorConfig struct {
	Name        string
	Host        string
	Port        int
	Quorum      int
	DownAfterMs int
}

// Config is the runtime configuration for a sentinel instance.
type Config struct {
	Enabled  bool
	Port     int
	Monitors []MonitorConfig
	SelfAddr string
}

// New creates a Sentinel from configuration.
func New(cfg Config) *Sentinel {
	s := &Sentinel{
		monitors:   make(map[string]*Monitor),
		selfAddr:   cfg.SelfAddr,
		probeEvery: time.Second,
		stopCh:     make(chan struct{}),
	}
	for _, mc := range cfg.Monitors {
		s.addMonitor(mc)
	}
	return s
}

func (s *Sentinel) addMonitor(mc MonitorConfig) {
	if mc.Quorum < 1 {
		mc.Quorum = 1
	}
	if mc.DownAfterMs <= 0 {
		mc.DownAfterMs = 30000
	}
	m := &Monitor{
		Name:        mc.Name,
		Host:        mc.Host,
		Port:        mc.Port,
		Quorum:      mc.Quorum,
		DownAfterMs: mc.DownAfterMs,
		replicas:    make(map[string]*ReplicaInfo),
	}
	s.mu.Lock()
	s.monitors[mc.Name] = m
	s.mu.Unlock()
}

// Start launches the probe loop.
func (s *Sentinel) Start() {
	s.wg.Add(1)
	go s.probeLoop()
}

// Stop terminates the probe loop.
func (s *Sentinel) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

func (s *Sentinel) probeLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.probeEvery)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.probeAll()
		}
	}
}

func (s *Sentinel) probeAll() {
	s.mu.Lock()
	monitors := make([]*Monitor, 0, len(s.monitors))
	for _, m := range s.monitors {
		monitors = append(monitors, m)
	}
	s.mu.Unlock()
	for _, m := range monitors {
		s.probeMonitor(m)
	}
}

// probeMonitor PINGs the master and refreshes replica state from INFO. A
// master that stays silent for down-after-milliseconds is marked S_DOWN; a
// S_DOWN master whose quorum is satisfied is O_DOWN and triggers failover.
func (s *Sentinel) probeMonitor(m *Monitor) {
	addr := net.JoinHostPort(m.Host, strconv.Itoa(m.Port))
	up, info := s.probe(addr)
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	if up {
		if m.downSinceMs != 0 {
			slog.Info("sentinel: master back up", "master", m.Name, "addr", addr)
			m.downSinceMs = 0
			m.sDown = false
			m.oDown = false
		}
		m.updateReplicas(info)
		return
	}
	if m.downSinceMs == 0 {
		m.downSinceMs = now.UnixMilli()
	}
	if now.Sub(time.UnixMilli(m.downSinceMs)) >= time.Duration(m.DownAfterMs)*time.Millisecond {
		if !m.sDown {
			slog.Warn("sentinel: master S_DOWN", "master", m.Name, "addr", addr)
			m.sDown = true
		}
		// quorum: with a single sentinel the local S_DOWN vote is enough;
		// multi-sentinel deployments count is-master-down-by-addr votes.
		if m.Quorum <= 1 && !m.oDown {
			m.oDown = true
			slog.Warn("sentinel: master O_DOWN", "master", m.Name)
		}
	}
	if m.oDown {
		s.startFailoverLocked(m)
	}
}

// probe dials addr, PINGs it and fetches INFO replication with a one-shot
// RESP exchange (no reconnect machinery: a dead master must not spawn retry
// goroutines). Returns whether the server is alive and the parsed INFO fields.
func (s *Sentinel) probe(addr string) (bool, map[string]string) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false, nil
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	br := bufio.NewReader(conn)

	if err := sendCmd(conn, "PING"); err != nil {
		return false, nil
	}
	if line, err := readLine(br); err != nil || !strings.HasPrefix(string(line), "+") {
		return false, nil
	}

	if err := sendCmd(conn, "INFO", "replication"); err != nil {
		return true, nil
	}
	body, err := readBulk(br)
	if err != nil {
		return true, nil
	}
	return true, parseInfoBody(body)
}

func sendCmd(conn net.Conn, args ...string) error {
	var sb strings.Builder
	sb.WriteString("*" + strconv.Itoa(len(args)) + CRLF)
	for _, a := range args {
		sb.WriteString("$" + strconv.Itoa(len(a)) + CRLF + a + CRLF)
	}
	_, err := conn.Write([]byte(sb.String()))
	return err
}

func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	return line[:len(line)-2], nil
}

// readBulk reads a $N\r\n...\r\n bulk reply body.
func readBulk(br *bufio.Reader) ([]byte, error) {
	line, err := readLine(br)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '$' {
		return nil, fmt.Errorf("expected bulk reply, got %q", line)
	}
	n, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return nil, err
	}
	body := make([]byte, n+2)
	if _, err := io.ReadFull(br, body); err != nil {
		return nil, err
	}
	return body[:n], nil
}

// parseInfoBody extracts key/value pairs from an INFO reply body.
func parseInfoBody(body []byte) map[string]string {
	res := make(map[string]string)
	for _, line := range strings.Split(string(body), "\r\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		kv := strings.SplitN(line, ":", 2)
		if len(kv) == 2 {
			res[kv[0]] = kv[1]
		}
	}
	return res
}

// updateReplicas refreshes the replica table from an INFO replication dump.
func (m *Monitor) updateReplicas(info map[string]string) {
	if info == nil {
		return
	}
	n, _ := strconv.Atoi(info["connected_slaves"])
	current := make(map[string]bool)
	for i := 0; i < n; i++ {
		fields := parseCommaFields(info[fmt.Sprintf("slave%d", i)])
		ip := fields["ip"]
		port, _ := strconv.Atoi(fields["port"])
		if ip == "" {
			continue
		}
		addr := net.JoinHostPort(ip, strconv.Itoa(port))
		current[addr] = true
		offset, _ := strconv.ParseInt(fields["offset"], 10, 64)
		ri, ok := m.replicas[addr]
		if !ok {
			ri = &ReplicaInfo{Addr: addr}
			m.replicas[addr] = ri
		}
		ri.Offset = offset
		ri.lastOK = time.Now()
	}
	for addr := range m.replicas {
		if !current[addr] {
			delete(m.replicas, addr)
		}
	}
}

func parseCommaFields(line string) map[string]string {
	res := make(map[string]string)
	for _, f := range strings.Split(line, ",") {
		kv := strings.SplitN(f, "=", 2)
		if len(kv) == 2 {
			res[kv[0]] = kv[1]
		}
	}
	return res
}

// startFailoverLocked promotes the most up-to-date replica when the master is
// O_DOWN. Caller holds m.mu.
func (s *Sentinel) startFailoverLocked(m *Monitor) {
	if m.failingOver {
		return
	}
	m.failingOver = true
	slog.Warn("sentinel: starting failover", "master", m.Name)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.performFailover(m)
	}()
}

// performFailover promotes the replica with the highest replication offset
// via REPLICAOF NO ONE, then repoints every other replica at it.
func (s *Sentinel) performFailover(m *Monitor) {
	m.mu.Lock()
	replicas := make([]*ReplicaInfo, 0, len(m.replicas))
	for _, r := range m.replicas {
		replicas = append(replicas, r)
	}
	m.mu.Unlock()
	sort.Slice(replicas, func(i, j int) bool {
		return replicas[i].Offset > replicas[j].Offset
	})
	if len(replicas) == 0 {
		slog.Warn("sentinel: no replica to promote", "master", m.Name)
		m.mu.Lock()
		m.failingOver = false
		m.mu.Unlock()
		return
	}

	newMaster := replicas[0]
	slog.Info("sentinel: promoting replica", "master", m.Name, "replica", newMaster.Addr)

	cli, err := client.MakeClient(newMaster.Addr)
	if err != nil {
		slog.Error("sentinel: cannot connect to replica", "error", err)
		m.mu.Lock()
		m.failingOver = false
		m.mu.Unlock()
		return
	}
	cli.Start()
	if ret := cli.Send([][]byte{[]byte("REPLICAOF"), []byte("NO"), []byte("ONE")}); !isOk(ret) {
		cli.Close()
		slog.Error("sentinel: REPLICAOF NO ONE failed", "replica", newMaster.Addr)
		m.mu.Lock()
		m.failingOver = false
		m.mu.Unlock()
		return
	}
	cli.Close()

	// wait for the promoted replica to become master
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		_, info := s.probe(newMaster.Addr)
		if info != nil && info["role"] == "master" {
			break
		}
	}

	// repoint the other replicas at the new master
	newHost, newPortStr, _ := net.SplitHostPort(newMaster.Addr)
	newPort, _ := strconv.Atoi(newPortStr)
	for _, r := range replicas[1:] {
		cli2, err := client.MakeClient(r.Addr)
		if err != nil {
			continue
		}
		cli2.Start()
		_ = cli2.Send([][]byte{
			[]byte("REPLICAOF"), []byte(newHost), []byte(strconv.Itoa(newPort)),
		})
		cli2.Close()
	}

	m.mu.Lock()
	m.sDown = false
	m.oDown = false
	m.downSinceMs = 0
	m.configEpoch++
	m.lastSwitchMsg = fmt.Sprintf("+switch-master %s %s %d %s",
		m.Name, m.Host, m.Port, newMaster.Addr)
	// repoint the monitored address at the promoted replica
	m.Host, m.Port = newHost, newPort
	m.failingOver = false
	m.mu.Unlock()
	slog.Warn("sentinel: failover complete", "master", m.Name, "new_master", newMaster.Addr)
}

// isOk reports whether a reply is +OK. The RESP parser produces StatusReply
// for status lines, so both forms are accepted.
func isOk(reply redis.Reply) bool {
	if reply == nil {
		return false
	}
	switch r := reply.(type) {
	case *protocol.OkReply:
		return true
	case *protocol.StatusReply:
		return r.Status == "OK"
	}
	return false
}

// ErrNoSuchMaster is returned for unknown monitor names.
var ErrNoSuchMaster = errors.New("no such master with that name")

// monitorSnapshot returns a copy of the named monitor for reply building.
func (s *Sentinel) monitorSnapshot(name string) (*Monitor, error) {
	s.mu.Lock()
	m := s.monitors[name]
	s.mu.Unlock()
	if m == nil {
		return nil, ErrNoSuchMaster
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := &Monitor{
		Name:          m.Name,
		Host:          m.Host,
		Port:          m.Port,
		Quorum:        m.Quorum,
		DownAfterMs:   m.DownAfterMs,
		sDown:         m.sDown,
		oDown:         m.oDown,
		downSinceMs:   m.downSinceMs,
		configEpoch:   m.configEpoch,
		failingOver:   m.failingOver,
		replicas:      make(map[string]*ReplicaInfo, len(m.replicas)),
		lastSwitchMsg: m.lastSwitchMsg,
	}
	for k, v := range m.replicas {
		r := *v
		cp.replicas[k] = &r
	}
	return cp, nil
}

// monitorList returns copies of all monitors.
func (s *Sentinel) monitorList() []*Monitor {
	s.mu.Lock()
	names := make([]string, 0, len(s.monitors))
	for name := range s.monitors {
		names = append(names, name)
	}
	s.mu.Unlock()
	sort.Strings(names)
	res := make([]*Monitor, 0, len(names))
	for _, name := range names {
		if m, err := s.monitorSnapshot(name); err == nil {
			res = append(res, m)
		}
	}
	return res
}

// isMasterDownByAddr reports whether the master at ip:port is S_DOWN and
// whether the address belongs to a monitored master.
func (s *Sentinel) isMasterDownByAddr(ip, port string) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.monitors {
		if m.Host == ip && strconv.Itoa(m.Port) == port {
			m.mu.Lock()
			down := m.sDown
			m.mu.Unlock()
			return down, true
		}
	}
	return false, false
}

// requestFailover starts a failover for the named master, even if it is up
// (matches SENTINEL FAILOVER semantics).
func (s *Sentinel) requestFailover(name string) error {
	m, err := s.monitorSnapshot(name)
	if err != nil {
		return err
	}
	s.startFailoverLocked(m)
	return nil
}
