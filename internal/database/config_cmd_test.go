package database

import (
	"testing"

	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol/asserts"
)

func TestConfigGetSet(t *testing.T) {
	prevAppendOnly := config.Properties.AppendOnly
	defer func() { config.Properties.AppendOnly = prevAppendOnly }()
	server := NewStandaloneServer()
	conn := connection.NewFakeConn()

	ret := server.Exec(conn, utils.ToCmdLine("CONFIG", "SET", "appendonly", "yes"))
	asserts.AssertNotError(t, ret)
	if !config.Properties.AppendOnly {
		t.Fatal("CONFIG SET did not update Properties")
	}
	raw, ok := server.mustSelectDB(0).GetEntity("conf:appendonly")
	if !ok || string(raw.Data.([]byte)) != "yes" {
		t.Fatal("CONFIG SET did not write conf key")
	}

	ret = server.Exec(conn, utils.ToCmdLine("CONFIG", "GET", "appendonly"))
	asserts.AssertMultiBulkReply(t, ret, []string{"appendonly", "yes"})

	ret = server.Exec(conn, utils.ToCmdLine("CONFIG", "SET", "bind", "10.0.0.1"))
	asserts.AssertErrReply(t, ret, "ERR CONFIG SET is not supported for node-local parameter 'bind'")

	ret = server.Exec(conn, utils.ToCmdLine("CONFIG", "GET", "*"))
	multi, ok := ret.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("CONFIG GET * returned %T", ret)
	}
	found := false
	for _, item := range multi.Args {
		if string(item) == "appendonly" {
			found = true
		}
	}
	if !found {
		t.Fatal("CONFIG GET * missing appendonly")
	}
}
