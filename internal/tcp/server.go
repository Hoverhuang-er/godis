package tcp

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hoverhuang-er/godis/internal/interface/tcp"
	"github.com/panjf2000/ants/v2"
	"log/slog"
)

// Config stores tcp server properties
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// ClientCounter Record the number of clients in the current Godis server
var ClientCounter int32

// handlerPool is the ants goroutine pool used for dispatching connection handlers.
var handlerPool *ants.Pool

// InitPool initializes the global ants goroutine pool for connection handling.
func InitPool() {
	var err error
	handlerPool, err = ants.NewPool(300,
		ants.WithPreAlloc(true),
		ants.WithNonblocking(false),
		ants.WithPanicHandler(func(err interface{}) {
			slog.Error("tcp handler pool panic", "error", err)
		}),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to create tcp handler pool: %v", err))
	}
	slog.Info("tcp handler pool initialized", "size", 300)
}

// GetPoolRunning returns the number of currently running goroutines.
func GetPoolRunning() int {
	if handlerPool == nil {
		return 0
	}
	return handlerPool.Running()
}

// GetPoolFree returns the number of free goroutines in the pool.
func GetPoolFree() int {
	if handlerPool == nil {
		return 0
	}
	return handlerPool.Free()
}

// ListenAndServeWithSignal binds port, serves until stopCh is closed.
// The caller is responsible for signal handling and graceful shutdown.
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler, stopCh <-chan struct{}) error {
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	slog.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))

	// Close listener when stopCh fires
	go func() {
		<-stopCh
		slog.Info("shutting down tcp listener...")
		_ = listener.Close()
	}()

	ListenAndServe(listener, handler)
	return nil
}

// ListenAndServe binds port and handle requests, blocking until close
func ListenAndServe(listener net.Listener, handler tcp.Handler) {
	if handlerPool == nil {
		InitPool()
	}

	ctx := context.Background()
	var waitDone sync.WaitGroup

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				slog.Info(fmt.Sprintf("accept occurs temporary error: %v, retry in 5ms", err))
				time.Sleep(5 * time.Millisecond)
				continue
			}
			slog.Info("listener closed, stopping accept loop")
			break
		}

		atomic.AddInt32(&ClientCounter, 1)
		waitDone.Add(1)
		err = handlerPool.Submit(func() {
			defer func() {
				waitDone.Done()
				atomic.AddInt32(&ClientCounter, -1)
			}()
			handler.Handle(ctx, conn)
		})
		if err != nil {
			slog.Warn("tcp pool submit failed, fallback to raw goroutine", "error", err)
			go func() {
				defer func() {
					waitDone.Done()
					atomic.AddInt32(&ClientCounter, -1)
				}()
				handler.Handle(ctx, conn)
			}()
		}
	}

	// Wait for all in-flight handlers to complete, then close the handler
	waitDone.Wait()
	slog.Info("all connections drained, closing handler...")
	_ = handler.Close()
}

// extractIP extracts the IP address from a "host:port" string.
func extractIP(addr string) string {
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}
