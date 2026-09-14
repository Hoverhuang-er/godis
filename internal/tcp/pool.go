package tcp

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"log/slog"
)

// ConnPool manages per-IP TCP connection pools for connection multiplexing.
// Localhost connections (127.0.0.1, ::1) bypass pooling entirely.
// Remote IPs get a pool of up to 10 connections, auto-scaling at 70% utilization.
type ConnPool struct {
	mu     sync.Mutex
	pools  map[string]*ipPool
	config PoolConfig
}

// PoolConfig holds configuration for connection pooling.
type PoolConfig struct {
	MaxPerIP     int           // max connections per IP (default 10)
	ScaleUpAtPct float64       // utilization threshold to scale up (default 0.70)
	ScaleUpByPct float64       // scale up factor (default 0.50)
	IdleTimeout  time.Duration // idle connection timeout (default 5min)
}

// ipPool tracks connections to a single remote IP.
type ipPool struct {
	conns   []*pooledConn
	maxSize int32
	inUse   int32
	mu      sync.Mutex
}

type pooledConn struct {
	conn     net.Conn
	lastUsed time.Time
	inUse    bool
}

// DefaultPoolConfig returns default pooling configuration.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxPerIP:     10,
		ScaleUpAtPct: 0.70,
		ScaleUpByPct: 0.50,
		IdleTimeout:  5 * time.Minute,
	}
}

// NewConnPool creates a new connection pool manager.
func NewConnPool(cfg PoolConfig) *ConnPool {
	return &ConnPool{
		pools:  make(map[string]*ipPool),
		config: cfg,
	}
}

// isLocalhost returns true if the address is a loopback address.
func isLocalhost(addr string) bool {
	ip := extractIP(addr)
	return ip == "127.0.0.1" || ip == "::1" || ip == "localhost"
}

// getOrCreatePool returns the pool for an IP, creating one if needed.
func (cp *ConnPool) getOrCreatePool(ip string) *ipPool {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if p, ok := cp.pools[ip]; ok {
		return p
	}
	p := &ipPool{
		conns:   make([]*pooledConn, 0, cp.config.MaxPerIP),
		maxSize: int32(cp.config.MaxPerIP),
	}
	cp.pools[ip] = p
	return p
}

// Acquire gets a connection for the given address. If pooling is active
// for this address, it reuses an existing connection or creates a new one.
// Returns the connection and whether a new connection should be created.
func (cp *ConnPool) Acquire(addr string) net.Conn {
	// Localhost bypasses pooling entirely — always use direct connections
	if isLocalhost(addr) {
		return nil // nil means "create a fresh connection"
	}

	pool := cp.getOrCreatePool(extractIP(addr))
	pool.mu.Lock()
	defer pool.mu.Unlock()

	// Try to find an idle connection
	for _, pc := range pool.conns {
		if !pc.inUse {
			pc.inUse = true
			pc.lastUsed = time.Now()
			atomic.AddInt32(&pool.inUse, 1)
			return pc.conn
		}
	}

	// Check if we need to scale up
	currentSize := int32(len(pool.conns))
	if currentSize > 0 {
		utilPct := float64(atomic.LoadInt32(&pool.inUse)) / float64(currentSize)
		if utilPct >= cp.config.ScaleUpAtPct {
			newMax := int32(float64(currentSize) * (1.0 + cp.config.ScaleUpByPct))
			if newMax > currentSize {
				pool.maxSize = newMax
				slog.Info("tcp pool scaled up",
					"ip", extractIP(addr),
					"old_max", currentSize,
					"new_max", newMax,
					"utilization_pct", utilPct)
			}
		}
	}

	// Mark that we're creating a new connection
	atomic.AddInt32(&pool.inUse, 1)
	return nil // nil means "create a fresh connection"
}

// Register adds a newly created pooled connection to the pool.
func (cp *ConnPool) Register(addr string, conn net.Conn) {
	if isLocalhost(addr) {
		return // localhost connections don't go into the pool
	}

	ip := extractIP(addr)
	pool := cp.getOrCreatePool(ip)
	pool.mu.Lock()
	defer pool.mu.Unlock()

	pc := &pooledConn{
		conn:     conn,
		lastUsed: time.Now(),
		inUse:    true,
	}
	pool.conns = append(pool.conns, pc)
}

// Release marks a connection as idle in the pool.
func (cp *ConnPool) Release(addr string, conn net.Conn) {
	if isLocalhost(addr) {
		conn.Close()
		return
	}

	ip := extractIP(addr)
	cp.mu.Lock()
	pool, ok := cp.pools[ip]
	cp.mu.Unlock()
	if !ok {
		conn.Close()
		return
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	for _, pc := range pool.conns {
		if pc.conn == conn && pc.inUse {
			pc.inUse = false
			pc.lastUsed = time.Now()
			atomic.AddInt32(&pool.inUse, -1)
			return
		}
	}
	// Connection not found in pool — close it
	conn.Close()
}

// CloseIdle closes idle connections older than the idle timeout.
func (cp *ConnPool) CloseIdle() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for ip, pool := range cp.pools {
		pool.mu.Lock()
		var kept []*pooledConn
		for _, pc := range pool.conns {
			if pc.inUse || time.Since(pc.lastUsed) < cp.config.IdleTimeout {
				kept = append(kept, pc)
			} else {
				pc.conn.Close()
				atomic.AddInt32(&pool.inUse, -1)
			}
		}
		pool.conns = kept
		pool.mu.Unlock()

		// Remove empty pools
		if len(kept) == 0 {
			delete(cp.pools, ip)
			slog.Debug("tcp pool removed", "ip", ip)
		}
	}
}

// Stats returns pool statistics for monitoring.
func (cp *ConnPool) Stats() map[string]any {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	stats := map[string]any{
		"total_pools": len(cp.pools),
	}
	var totalConns, totalInUse int32
	for ip, pool := range cp.pools {
		pool.mu.Lock()
		totalConns += int32(len(pool.conns))
		totalInUse += atomic.LoadInt32(&pool.inUse)
		pool.mu.Unlock()
		_ = ip
	}
	stats["total_connections"] = totalConns
	stats["total_in_use"] = totalInUse
	return stats
}

// StartCleanup starts a background goroutine that periodically closes idle connections.
func (cp *ConnPool) StartCleanup(interval time.Duration, stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				cp.CloseIdle()
			}
		}
	}()
}
