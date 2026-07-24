package tcp

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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

// ListenAndServeWithSignal binds port and handle requests, blocking until receive stop signal
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	slog.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// ListenAndServe binds port and handle requests, blocking until close
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	if handlerPool == nil {
		InitPool()
	}

	errCh := make(chan error, 1)
	defer close(errCh)
	go func() {
		select {
		case <-closeChan:
			slog.Info("get exit signal")
		case er := <-errCh:
			slog.Info(fmt.Sprintf("accept error: %s", er.Error()))
		}
		slog.Info("shutting down...")
		_ = listener.Close()
		_ = handler.Close()
	}()

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
			errCh <- err
			break
		}

		ClientCounter++
		waitDone.Add(1)

		err = handlerPool.Submit(func() {
			defer func() {
				waitDone.Done()
				atomic.AddInt32(&ClientCounter, -1)
			}()
			handler.Handle(ctx, conn)
		})
		if err != nil {
			// Pool full or closed — fallback to raw goroutine
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
	waitDone.Wait()
}

// extractIP extracts the IP address from a "host:port" string.
func extractIP(addr string) string {
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}
