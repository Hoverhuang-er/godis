package database

import (
	"bytes"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/Hoverhuang-er/godis/internal/aof"
	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/parser"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol/asserts"
	rdbcore "github.com/hdt3213/rdb/core"
)

// TestConfKeyReplicatesToSlave proves the conf namespace rides the existing
// master-slave full sync: a CONFIG SET on the master lands on the slave.
func TestConfKeyReplicatesToSlave(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "godis")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	aofFilename := path.Join(tmpDir, "a.aof")

	master := mockServer()
	aofHandler, err := NewPersister(master, aofFilename, true, aof.FsyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	defer aofHandler.Close()
	master.bindPersister(aofHandler)

	prevMaxClients := config.Properties.MaxClients
	defer func() { config.Properties.MaxClients = prevMaxClients }()
	masterConn := connection.NewFakeConn()
	ret := master.Exec(masterConn, utils.ToCmdLine("CONFIG", "SET", "maxclients", "999"))
	asserts.AssertNotError(t, ret)
	time.Sleep(100 * time.Millisecond) // wait for AOF write

	// full re-sync
	replConn := connection.NewFakeConn()
	master.Exec(replConn, utils.ToCmdLine("psync", "?", "-1"))
	masterChan := parser.ParseStream(replConn)

	psyncPayload := <-masterChan
	if psyncPayload.Err != nil {
		t.Fatal(psyncPayload.Err)
	}
	psyncHeader, ok := psyncPayload.Data.(*protocol.StatusReply)
	if !ok {
		t.Fatalf("psync header is not a status reply: %T", psyncPayload.Data)
	}
	headers := strings.Split(psyncHeader.Status, " ")
	if len(headers) != 3 {
		t.Fatalf("illegal psync header: %s", psyncHeader.Status)
	}

	rdbPayload := <-masterChan
	if rdbPayload.Err != nil {
		t.Fatal("read response failed: " + rdbPayload.Err.Error())
	}
	rdbReply, ok := rdbPayload.Data.(*protocol.BulkReply)
	if !ok {
		t.Fatal("illegal payload header: " + string(rdbPayload.Data.ToBytes()))
	}

	slave := mockServer()
	rdbDec := rdbcore.NewDecoder(bytes.NewReader(rdbReply.Arg))
	if err := slave.LoadRDB(rdbDec); err != nil {
		t.Fatal("import rdb failed: " + err.Error())
	}

	raw, ok := slave.mustSelectDB(0).GetEntity("conf:maxclients")
	if !ok || string(raw.Data.([]byte)) != "999" {
		t.Fatalf("conf key missing on slave after full sync: ok=%v", ok)
	}
}
