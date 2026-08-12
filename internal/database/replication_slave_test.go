package database

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Hoverhuang-er/godis/internal/aof"
	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/client"
	"github.com/Hoverhuang-er/godis/internal/redis/connection"
	"github.com/Hoverhuang-er/godis/internal/redis/parser"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol/asserts"
)

// masterPort returns the port of the real redis master used by replication
// tests. Set GODIS_TEST_MASTER_PORT to run against an existing redis;
// otherwise a throwaway redis-server is started for the test run (the old
// "default to 6379" probe was fragile: 6379 may be anything, e.g. a tunnel).
func masterPort() string {
	if p := os.Getenv("GODIS_TEST_MASTER_PORT"); p != "" {
		return p
	}
	testMasterOnce.Do(func() {
		port := strconv.Itoa(freePort())
		cmd := exec.Command("redis-server", "--port", port, "--save", "", "--appendonly", "no", "--daemonize", "no")
		cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
		if err := cmd.Start(); err != nil {
			panic("start test redis: " + err.Error())
		}
		testMasterCmd = cmd
		testMasterPort = port
		waitRedisReady("127.0.0.1:" + port)
	})
	return testMasterPort
}

var (
	testMasterOnce sync.Once
	testMasterPort string
	testMasterCmd  *exec.Cmd
)

func TestMain(m *testing.M) {
	code := m.Run()
	if testMasterCmd != nil {
		_ = testMasterCmd.Process.Kill()
	}
	os.Exit(code)
}

func waitRedisReady(addr string) {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	panic("test redis at " + addr + " did not become ready")
}

func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// masterAddr returns host:port of the real redis master for replication tests.
func masterAddr() string {
	return "127.0.0.1:" + masterPort()
}

func TestReplicationSlaveSide(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "godis")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := path.Join(tmpDir, "a.aof")
	defer func() {
		_ = os.Remove(aofFilename)
	}()
	config.Properties = &config.ServerProperties{
		Databases:      16,
		AppendOnly:     true,
		AppendFilename: aofFilename,
	}
	conn := connection.NewFakeConn()
	server := mockServer()
	masterCli, err := client.MakeClient(masterAddr())
	if err != nil {
		t.Error(err)
		return
	}
	aofHandler, err := NewPersister(server, config.Properties.AppendFilename, true, aof.FsyncNo)
	if err != nil {
		t.Error(err)
		return
	}
	server.bindPersister(aofHandler)
	server.Exec(conn, utils.ToCmdLine("set", "zz", "zz"))
	masterCli.Start()

	// sync with master
	ret := masterCli.Send(utils.ToCmdLine("set", "1", "1"))
	asserts.AssertStatusReply(t, ret, "OK")
	ret = server.Exec(conn, utils.ToCmdLine("SLAVEOF", "127.0.0.1", masterPort()))
	asserts.AssertStatusReply(t, ret, "OK")
	success := false
	for i := 0; i < 30; i++ {
		// wait for sync
		time.Sleep(time.Second)
		ret = server.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("1")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}

	// receive aof
	ret = masterCli.Send(utils.ToCmdLine("set", "1", "2"))
	asserts.AssertStatusReply(t, ret, "OK")
	success = false
	for i := 0; i < 10; i++ {
		// wait for sync
		time.Sleep(time.Second)
		ret = server.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("2")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}
	err = server.slaveStatus.sendAck2Master()
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(3 * time.Second)

	// test reconnect
	config.Properties.ReplTimeout = 1
	_ = server.slaveStatus.masterConn.Close()
	server.slaveStatus.lastRecvTime = time.Now().Add(-time.Hour) // mock timeout
	server.slaveCron()
	time.Sleep(3 * time.Second)
	ret = masterCli.Send(utils.ToCmdLine("set", "1", "3"))
	asserts.AssertStatusReply(t, ret, "OK")
	success = false
	for i := 0; i < 10; i++ {
		// wait for sync
		time.Sleep(time.Second)
		ret = server.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("3")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}

	// test slave of no one
	ret = server.Exec(conn, utils.ToCmdLine("SLAVEOF", "NO", "ONE"))
	asserts.AssertStatusReply(t, ret, "OK")
	ret = masterCli.Send(utils.ToCmdLine("set", "1", "4"))
	asserts.AssertStatusReply(t, ret, "OK")
	ret = server.Exec(conn, utils.ToCmdLine("GET", "1"))
	asserts.AssertBulkReply(t, ret, "3")
	ret = server.Exec(conn, utils.ToCmdLine("SLAVEOF", "127.0.0.1", masterPort()))
	asserts.AssertStatusReply(t, ret, "OK")
	success = false
	for i := 0; i < 30; i++ {
		// wait for sync
		time.Sleep(time.Second)
		ret = server.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("4")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}

	// check slave aof file
	aofCheckServer := MakeAuxiliaryServer()
	aofHandler2, err := NewPersister(aofCheckServer, config.Properties.AppendFilename, true, aof.FsyncNo)
	if err != nil {
		t.Error("create persister failed")
	}
	aofCheckServer.bindPersister(aofHandler2)
	ret = aofCheckServer.Exec(conn, utils.ToCmdLine("get", "zz"))
	asserts.AssertNullBulk(t, ret)
	ret = aofCheckServer.Exec(conn, utils.ToCmdLine("get", "1"))
	asserts.AssertBulkReply(t, ret, "4")

	err = server.slaveStatus.close()
	if err != nil {
		t.Error("cannot close")
	}
}

func TestReplicationFailover(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "godis")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := path.Join(tmpDir, "a.aof")
	defer func() {
		_ = os.Remove(aofFilename)
	}()
	config.Properties = &config.ServerProperties{
		Databases:      16,
		AppendOnly:     true,
		AppendFilename: aofFilename,
	}
	conn := connection.NewFakeConn()
	server := mockServer()
	aofHandler, err := NewPersister(server, aofFilename, true, aof.FsyncAlways)
	if err != nil {
		t.Error(err)
		return
	}
	server.bindPersister(aofHandler)

	masterCli, err := client.MakeClient(masterAddr())
	if err != nil {
		t.Error(err)
		return
	}
	masterCli.Start()

	// sync with master
	ret := masterCli.Send(utils.ToCmdLine("set", "1", "1"))
	asserts.AssertStatusReply(t, ret, "OK")
	ret = server.Exec(conn, utils.ToCmdLine("SLAVEOF", "127.0.0.1", masterPort()))
	asserts.AssertStatusReply(t, ret, "OK")
	success := false
	for i := 0; i < 30; i++ {
		// wait for sync
		time.Sleep(time.Second)
		ret = server.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("1")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}

	t.Log("slave of no one")
	ret = server.Exec(conn, utils.ToCmdLine("SLAVEOF", "no", "one"))
	asserts.AssertStatusReply(t, ret, "OK")
	server.Exec(conn, utils.ToCmdLine("set", "2", "2"))

	replConn := connection.NewFakeConn()
	server.Exec(replConn, utils.ToCmdLine("psync", "?", "-1"))
	masterChan := parser.ParseStream(replConn)
	serverB := mockServer()
	serverB.slaveStatus.masterChan = masterChan
	serverB.slaveStatus.configVersion = 0
	serverB.parsePsyncHandshake()
	serverB.loadMasterRDB(0)
	server.masterCron()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go serverB.receiveAOF(ctx, 0)

	time.Sleep(3 * time.Second)
	ret = serverB.Exec(conn, utils.ToCmdLine("get", "1"))
	asserts.AssertBulkReply(t, ret, "1")
	ret = serverB.Exec(conn, utils.ToCmdLine("get", "2"))
	asserts.AssertBulkReply(t, ret, "2")
}
