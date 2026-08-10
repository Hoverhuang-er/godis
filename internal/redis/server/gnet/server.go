package gnet

import (	"fmt"

	"context"
	"sync/atomic"

	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/parser"
	"github.com/panjf2000/gnet/v2"
	"log/slog"
)

type GnetServer struct {
	gnet.BuiltinEventEngine
	eng       gnet.Engine
	connected int32
	// dbPtr is swapped atomically to switch between standalone and cluster
	// mode at runtime without dropping connections (see SwapDB).
	dbPtr atomic.Pointer[database.DB]
}

func (s *GnetServer) setDB(db database.DB) {
	s.dbPtr.Store(&db)
}

func (s *GnetServer) getDB() database.DB {
	return *s.dbPtr.Load()
}

// CurrentDB returns the active database engine (see DBSwapper in the cluster
// package, used for the runtime standalone-to-cluster switch).
func (s *GnetServer) CurrentDB() database.DB {
	return s.getDB()
}

// SwapDB atomically replaces the active database engine.
func (s *GnetServer) SwapDB(db database.DB) {
	s.setDB(db)
}

func NewGnetServer(db database.DB) *GnetServer {
	s := &GnetServer{}
	s.setDB(db)
	return s
}

func (s *GnetServer) Run(listenAddr string) error {
	return gnet.Run(s, "tcp://"+listenAddr, gnet.WithMulticore(true))
}

func (s *GnetServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	s.eng = eng
	return
}

func (s *GnetServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	client := connection.NewConn(c)
	c.SetContext(client)
	atomic.AddInt32(&s.connected, 1)
	return
}

func (s *GnetServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	if err != nil {
		slog.Info(fmt.Sprintf("error occurred on connection=%s, %v\n", c.RemoteAddr().String(), err))
	}
	atomic.AddInt32(&s.connected, -1)
	conn := c.Context().(redis.Connection)
	s.getDB().AfterClientClose(conn)
	return
}

func (s *GnetServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	conn := c.Context().(redis.Connection)
	cmdLine, err := parser.ParseV2(c)
	if err != nil {
		slog.Info(fmt.Sprintf("parse command line failed: %v", err))
		return gnet.Close
	}
	if len(cmdLine) == 0 {
		return gnet.None
	}
	result := s.getDB().Exec(conn, cmdLine)
	buffer := result.ToBytes()
	if len(buffer) > 0 {
		c.Write(buffer)
	}
	return gnet.None
}

func (s *GnetServer) Close() {
	s.eng.Stop(context.Background())
}