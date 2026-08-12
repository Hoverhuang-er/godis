package database

import (
	"os"
	"testing"

	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol/asserts"
)

func TestIsConfKey(t *testing.T) {
	if !IsConfKey("conf:requirepass") {
		t.Error("conf:requirepass should be a conf key")
	}
	if IsConfKey("requirepass") || IsConfKey("xconf:y") || IsConfKey("") {
		t.Error("non-conf keys detected as conf keys")
	}
}

func TestConfWriteGuard(t *testing.T) {
	server := NewStandaloneServer()
	conn := connection.NewFakeConn()
	ret := server.Exec(conn, utils.ToCmdLine("SET", "conf:requirepass", "x"))
	asserts.AssertErrReply(t, ret, "ERR reserved key prefix \"conf:\"")
	ret = server.Exec(conn, utils.ToCmdLine("MSET", "conf:a", "1", "b", "2"))
	asserts.AssertErrReply(t, ret, "ERR reserved key prefix \"conf:\"")
	// normal keys unaffected
	ret = server.Exec(conn, utils.ToCmdLine("SET", "k", "v"))
	asserts.AssertNotError(t, ret)
}

func TestConfKeysHiddenFromIteration(t *testing.T) {
	server := NewStandaloneServer()
	conn := connection.NewFakeConn()
	db := server.mustSelectDB(0)
	db.PutEntity("conf:x", &database.DataEntity{Data: []byte("v")})
	db.PutEntity("user", &database.DataEntity{Data: []byte("v")})

	ret := server.Exec(conn, utils.ToCmdLine("KEYS", "*"))
	multi, ok := ret.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("unexpected KEYS reply type %T", ret)
	}
	foundUser, foundConf := false, false
	for _, item := range multi.Args {
		switch string(item) {
		case "user":
			foundUser = true
		case "conf:x":
			foundConf = true
		}
	}
	if !foundUser {
		t.Fatal("KEYS did not return user key")
	}
	if foundConf {
		t.Fatal("KEYS returned conf key")
	}

	ret = server.Exec(conn, utils.ToCmdLine("SCAN", "0"))
	raw, ok := ret.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("unexpected SCAN reply type %T", ret)
	}
	bulk, ok := raw.Replies[1].(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("unexpected SCAN items reply type %T", raw.Replies[1])
	}
	for _, item := range bulk.Args {
		if string(item) == "conf:x" {
			t.Fatal("SCAN returned conf key")
		}
	}
}

func TestFlushDBPreservesConfKeys(t *testing.T) {
	server := NewStandaloneServer()
	conn := connection.NewFakeConn()
	db := server.mustSelectDB(0)
	db.PutEntity("conf:x", &database.DataEntity{Data: []byte("v")})
	db.PutEntity("user", &database.DataEntity{Data: []byte("v")})

	ret := server.Exec(conn, utils.ToCmdLine("FLUSHDB"))
	asserts.AssertNotError(t, ret)

	if _, ok := server.mustSelectDB(0).GetEntity("conf:x"); !ok {
		t.Fatal("conf key lost by FLUSHDB")
	}
	if _, ok := server.mustSelectDB(0).GetEntity("user"); ok {
		t.Fatal("user key survived FLUSHDB")
	}
}

func TestDBSizeExcludesConfKeys(t *testing.T) {
	server := NewStandaloneServer()
	conn := connection.NewFakeConn()
	db := server.mustSelectDB(0)
	db.PutEntity("conf:x", &database.DataEntity{Data: []byte("v")})
	db.PutEntity("user", &database.DataEntity{Data: []byte("v")})

	ret := server.Exec(conn, utils.ToCmdLine("DBSIZE"))
	asserts.AssertIntReply(t, ret, 1)
}

func TestBootstrapConfNamespace(t *testing.T) {
	config.Properties.AppendOnly = true
	config.Properties.RequirePass = "pw"
	config.Properties.Dir = t.TempDir()
	config.Properties.DataDir = t.TempDir()
	aofFile, err := os.CreateTemp(t.TempDir(), "bootstrap-*.aof")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(aofFile.Name())
	config.Properties.AppendFilename = aofFile.Name()
	defer func() {
		config.Properties.AppendOnly = false
		config.Properties.RequirePass = ""
	}()
	server := NewStandaloneServer()
	BootstrapConfNamespace(server)
	db := server.mustSelectDB(0)
	if raw, ok := db.GetEntity("conf:appendonly"); !ok || string(raw.Data.([]byte)) != "true" {
		t.Fatalf("conf:appendonly not bootstrapped: %v %v", ok, raw)
	}
	if raw, ok := db.GetEntity("conf:requirepass"); !ok || string(raw.Data.([]byte)) != "pw" {
		t.Fatalf("conf:requirepass not bootstrapped")
	}
	if _, ok := db.GetEntity("conf:bind"); ok {
		t.Fatal("node-local param bootstrapped into namespace")
	}
	// existing conf keys are never overwritten (conf key wins on later boots)
	db.PutEntity("conf:appendonly", &database.DataEntity{Data: []byte("false")})
	BootstrapConfNamespace(server)
	if raw, _ := db.GetEntity("conf:appendonly"); string(raw.Data.([]byte)) != "false" {
		t.Fatal("bootstrap overwrote existing conf key")
	}
}
