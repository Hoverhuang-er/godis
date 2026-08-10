package database

import (
	"errors"
	"fmt"
	"encoding/binary"
	"path/filepath"

	"github.com/boltdb/bolt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Hoverhuang-er/godis/internal/aof"
	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/monitoring"
	"github.com/Hoverhuang-er/godis/internal/pubsub"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"log/slog"
)

var godisVersion = "1.3.4" // do not modify

// Version returns the godis server version (e.g. "1.3.4").
// It is used to enforce the cluster max major-version gap policy.
func Version() string {
	return godisVersion
}

var connIDCounter int64

// Server is a redis-server with full capabilities including multiple database, rdb loader, replication
// Server is a redis-server with full capabilities
type Server struct {
	dbSet []*atomic.Value // *DB

	hub        *pubsub.Hub
	persister  *aof.Persister

	role         int32
	slaveStatus  *slaveStatus
	masterStatus *masterStatus

	insertCallback database.KeyEventCallback
	deleteCallback database.KeyEventCallback

	// Bolt-backed persistent store, auto-saved on graceful shutdown
	boltDB *bolt.DB
	slogLogger *SlowLogger

	// Prometheus metrics
	metrics *monitoring.Metrics
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

// NewStandaloneServer creates a standalone redis server, with multi database and all other funtions
func NewStandaloneServer() *Server {
	server := &Server{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	// creat tmp dir
	err := os.MkdirAll(config.GetTmpDir(), os.ModePerm)
	if err != nil {
		panic(fmt.Errorf("create tmp dir failed: %v", err))
	}
	// make db set
	server.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range server.dbSet {
		singleDB := makeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}
	server.hub = pubsub.MakeHub()
	// record aof
	validAof := false
	if config.Properties.AppendOnly {
		validAof = fileExists(config.Properties.AppendFilename)
		aofHandler, err := NewPersister(server,
			config.Properties.AppendFilename, true, config.Properties.AppendFsync)
		if err != nil {
			panic(err)
		}
		server.bindPersister(aofHandler)
	}
	if config.Properties.RDBFilename != "" && !validAof {
		// load rdb
		err := server.loadRdbFile()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				slog.Debug("rdb file not found, starting fresh", "path", config.Properties.RDBFilename)
			} else {
				slog.Error(err.Error())
			}
		}
	}
	server.slaveStatus = initReplSlaveStatus()
	server.initMasterStatus()
	server.startReplCron()
	server.role = masterRole

	// record slow log
	server.slogLogger = NewSlowLogger(config.Properties.SlowLogMaxLen, config.Properties.SlowLogSlowerThan)

	// Initialize Prometheus metrics
	if config.Properties.PrometheusEnabled {
		server.metrics = monitoring.New(server.getDBStats)
		monitoring.StartMetricsServer(server.metrics)
	}

	// Open or create bolt DB in the godis data directory
	boltPath := filepath.Join(config.Properties.DataDir, "godis.db")
	boltDB, err := bolt.Open(boltPath, 0o600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		slog.Error("failed to open bolt db, starting without persistence", "path", boltPath, "error", err)
	} else {
		server.boltDB = boltDB
		// Load persisted data from bolt on startup
		server.loadFromBolt()
	}
	return server
}

// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
func (server *Server) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			slog.Warn("error occurs: %v\n%s", err, string(debug.Stack()))
			result = &protocol.UnknownErrReply{}
		}
	}()
	// Record the start time of command execution
	GodisExecCommandStartUnixTime := time.Now()
	// Increment command counter for Prometheus metrics
	if server.metrics != nil {
		server.metrics.IncrCommands()
	}


	cmdName := strings.ToLower(string(cmdLine[0]))
	// ping
	if cmdName == "ping" {
		return Ping(c, cmdLine[1:])
	}
	// authenticate
	if cmdName == "auth" {
		return Auth(c, cmdLine[1:])
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}
	// hello
	if cmdName == "hello" {
		return execHello(server, c, cmdLine[1:])
	}

	// client
	if cmdName == "client" {
		return execClient(c, cmdLine[1:])
	}

	// readonly / readwrite (accepted for client compatibility)
	if cmdName == "readonly" || cmdName == "readwrite" {
		return &protocol.OkReply{}
	}

	// info
	if cmdName == "info" {
		return Info(server, cmdLine[1:])
	}

	// slowlog
	if cmdName == "slowlog" {
		return server.slogLogger.HandleSlowlogCommand(cmdLine)
	}

	if cmdName == "dbsize" {
		return DbSize(c, server)
	}
	if cmdName == "slaveof" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("cannot use slave of database within multi")
		}
		if len(cmdLine) != 3 {
			return protocol.MakeArgNumErrReply("SLAVEOF")
		}
		return server.execSlaveOf(c, cmdLine[1:])
	} else if cmdName == "command" {
		return execCommand(cmdLine[1:])
	}

	// read only slave
	role := atomic.LoadInt32(&server.role)
	if role == slaveRole && !c.IsMaster() {
		if !isReadOnlyCommand(cmdName) {
			return protocol.MakeErrReply("READONLY You can't write against a read only slave.")
		}
	}

	// special commands which cannot execute within transaction
	if cmdName == "subscribe" {
		if len(cmdLine) < 2 {
			return protocol.MakeArgNumErrReply("subscribe")
		}
		return pubsub.Subscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "publish" {
		return pubsub.Publish(server.hub, cmdLine[1:])
	} else if cmdName == "unsubscribe" {
		return pubsub.UnSubscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "bgrewriteaof" {
		if !config.Properties.AppendOnly {
			return protocol.MakeErrReply("AppendOnly is false, you can't rewrite aof file")
		}
		return BGRewriteAOF(server, cmdLine[1:])
	} else if cmdName == "rewriteaof" {
		if !config.Properties.AppendOnly {
			return protocol.MakeErrReply("AppendOnly is false, you can't rewrite aof file")
		}
		return RewriteAOF(server, cmdLine[1:])
	} else if cmdName == "flushall" {
		return server.flushAll()
	} else if cmdName == "flushdb" {
		if !validateArity(1, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		if c.InMultiState() {
			return protocol.MakeErrReply("ERR command 'FlushDB' cannot be used in MULTI")
		}
		return server.execFlushDB(c.GetDBIndex())
	} else if cmdName == "save" {
		return SaveRDB(server, cmdLine[1:])
	} else if cmdName == "bgsave" {
		return BGSaveRDB(server, cmdLine[1:])
	} else if cmdName == "select" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("cannot select database within multi")
		}
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrReply("select")
		}
		return execSelect(c, server, cmdLine[1:])
	} else if cmdName == "copy" {
		if len(cmdLine) < 3 {
			return protocol.MakeArgNumErrReply("copy")
		}
		return execCopy(server, c, cmdLine[1:])
	} else if cmdName == "replconf" {
		return server.execReplConf(c, cmdLine[1:])
	} else if cmdName == "psync" {
		return server.execPSync(c, cmdLine[1:])
	}

	// normal commands
	dbIndex := c.GetDBIndex()
	selectedDB, errReply := server.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	// Record key access for hot key tracking
	if server.metrics != nil {
		for i := 1; i < len(cmdLine); i++ {
			server.metrics.RecordKeyAccess(string(cmdLine[i]), dbIndex)
		}
	}

	exec := selectedDB.Exec(c, cmdLine)
	// Record slow query logs
	server.slogLogger.Record(GodisExecCommandStartUnixTime, cmdLine, c.Name())
	return exec
}

// GetMetrics returns the Prometheus metrics instance.
func (server *Server) GetMetrics() *monitoring.Metrics {
	return server.metrics
}


// Close graceful shutdown database
func (server *Server) Close() {
	// Flush all data to bolt before shutdown
	server.saveToBolt()
	if server.boltDB != nil {
		server.boltDB.Close()
	}
	server.slaveStatus.close()
	if server.persister != nil {
		server.persister.Close()
	}
	server.stopMaster()
}

func (server *Server) AfterClientClose(c redis.Connection) {
	pubsub.UnsubscribeAll(server.hub, c)
	if server.metrics != nil {
		server.metrics.DecrConnections()
	}
}

// boltKey encodes a dbIndex + key into a bolt bucket key.
func boltKey(dbIndex int, key string) []byte {
	buf := make([]byte, 2+len(key))
	binary.BigEndian.PutUint16(buf, uint16(dbIndex))
	copy(buf[2:], key)
	return buf
}

// saveToBolt persists all in-memory data to the bolt DB.
func (server *Server) saveToBolt() {
	if server.boltDB == nil {
		return
	}

	// Count total keys first for progress tracking
	totalKeys := 0
	for dbIndex := range config.Properties.Databases {
		keyCount, _ := server.GetDBSize(dbIndex)
		totalKeys += keyCount
	}

	start := time.Now()
	slog.Info("saving data to bolt for graceful shutdown...",
		"total_keys", totalKeys,
		"path", filepath.Join(config.Properties.DataDir, "godis.db"))

	count := 0
	lastPct := 0
	err := server.boltDB.Update(func(tx *bolt.Tx) error {
		_ = tx.DeleteBucket([]byte("data"))
		bucket, err := tx.CreateBucket([]byte("data"))
		if err != nil {
			return err
		}
		for dbIndex := range config.Properties.Databases {
			server.ForEach(dbIndex, func(key string, data *database.DataEntity, expiration *time.Time) bool {
				val := DataToBytes(data.Data)
				bk := boltKey(dbIndex, key)
				if err := bucket.Put(bk, val); err != nil {
					return false
				}
				count++
				if totalKeys > 0 {
					pct := count * 100 / totalKeys
					if pct >= lastPct+10 || count%10000 == 0 {
						lastPct = pct
						elapsed := time.Since(start)
						var eta time.Duration
						if count > 0 {
							eta = time.Duration(float64(elapsed)/float64(count)*float64(totalKeys)) - elapsed
						}
						slog.Info("bolt save progress",
							"keys", count,
							"total", totalKeys,
							"pct", pct,
							"elapsed", elapsed.Round(time.Millisecond).String(),
							"eta", eta.Round(time.Second).String())
					}
				}
				return true
			})
		}
		return nil
	})

	elapsed := time.Since(start).Round(time.Millisecond)
	if err != nil {
		slog.Error("bolt save failed", "keys", count, "total", totalKeys, "elapsed", elapsed, "error", err)
	} else {
		slog.Info("bolt save complete",
			"keys", count,
			"total", totalKeys,
			"elapsed", elapsed.String(),
			"path", filepath.Join(config.Properties.DataDir, "godis.db"))
	}
}

// DataToBytes converts a database value to raw bytes for persistence.
func DataToBytes(data interface{}) []byte {
	switch v := data.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	case interface{ ToBytes() []byte }:
		return v.ToBytes()
	default:
		return []byte(fmt.Sprint(v))
	}
}

// loadFromBolt restores in-memory data from the bolt DB on startup.
func (server *Server) loadFromBolt() {
	if server.boltDB == nil {
		return
	}
	slog.Info("loading data from bolt...")
	count := 0
	err := server.boltDB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("data"))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			if len(k) < 3 {
				return nil
			}
			dbIndex := int(binary.BigEndian.Uint16(k[:2]))
			key := string(k[2:])
			if dbIndex >= config.Properties.Databases {
				return nil
			}
			db := server.mustSelectDB(dbIndex)
			entity := &database.DataEntity{
				Data: protocol.MakeBulkReply(v),
			}
			db.PutEntity(key, entity)
			count++
			return nil
		})
	})
	if err != nil {
		slog.Error("bolt load failed", "error", err)
	} else {
		slog.Info("bolt load complete", "keys", count)
	}
}

// getDBStats collects per-database key and expiry counts for Prometheus metrics.
func (server *Server) getDBStats() []monitoring.DBStat {
	stats := make([]monitoring.DBStat, 0, len(server.dbSet))
	for i, holder := range server.dbSet {
		db := holder.Load().(*DB)
		keys := int64(db.data.Len())
		expires := int64(db.ttlMap.Len())
		stats = append(stats, monitoring.DBStat{
			Index:   i,
			Keys:    keys,
			Expires: expires,
		})
	}
	return stats
}

// SetClusterStatsFn registers the callback providing cluster-mode stats for
// /metrics. It is a no-op when Prometheus metrics are disabled.
func (server *Server) SetClusterStatsFn(fn func() monitoring.ClusterStats) {
	if server.metrics != nil {
		server.metrics.SetClusterStatsFn(fn)
	}
}

func execSelect(c redis.Connection, mdb *Server, args [][]byte) redis.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return protocol.MakeOkReply()
}

func (server *Server) execFlushDB(dbIndex int) redis.Reply {
	if server.persister != nil {
		server.persister.SaveCmdLine(dbIndex, utils.ToCmdLine("FlushDB"))
	}
	return server.flushDB(dbIndex)
}

// flushDB flushes the selected database
func (server *Server) flushDB(dbIndex int) redis.Reply {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	newDB := makeDB()
	server.loadDB(dbIndex, newDB)
	return &protocol.OkReply{}
}

func (server *Server) loadDB(dbIndex int, newDB *DB) redis.Reply {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	oldDB := server.mustSelectDB(dbIndex)
	newDB.index = dbIndex
	newDB.addAof = oldDB.addAof // inherit oldDB
	server.dbSet[dbIndex].Store(newDB)
	return &protocol.OkReply{}
}

// flushAll flushes all databases.
func (server *Server) flushAll() redis.Reply {
	for i := range server.dbSet {
		server.flushDB(i)
	}
	if server.persister != nil {
		server.persister.SaveCmdLine(0, utils.ToCmdLine("FlushAll"))
	}
	return &protocol.OkReply{}
}

// selectDB returns the database with the given index, or an error if the index is out of range.
func (server *Server) selectDB(dbIndex int) (*DB, *protocol.StandardErrReply) {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return nil, protocol.MakeErrReply("ERR DB index is out of range")
	}
	return server.dbSet[dbIndex].Load().(*DB), nil
}

// mustSelectDB is like selectDB, but panics if an error occurs.
func (server *Server) mustSelectDB(dbIndex int) *DB {
	selectedDB, err := server.selectDB(dbIndex)
	if err != nil {
		panic(err)
	}
	return selectedDB
}

// ForEach traverses all the keys in the given database
func (server *Server) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	server.mustSelectDB(dbIndex).ForEach(cb)
}

// GetEntity returns the data entity to the given key
func (server *Server) GetEntity(dbIndex int, key string) (*database.DataEntity, bool) {
	return server.mustSelectDB(dbIndex).GetEntity(key)
}

func (server *Server) GetExpiration(dbIndex int, key string) *time.Time {
	raw, ok := server.mustSelectDB(dbIndex).ttlMap.Get(key)
	if !ok {
		return nil
	}
	expireTime, _ := raw.(time.Time)
	return &expireTime
}

// ExecMulti executes multi commands transaction Atomically and Isolated
func (server *Server) ExecMulti(conn redis.Connection, watching map[string]uint32, cmdLines []CmdLine) redis.Reply {
	selectedDB, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return selectedDB.ExecMulti(conn, watching, cmdLines)
}

// RWLocks lock keys for writing and reading
func (server *Server) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	server.mustSelectDB(dbIndex).RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (server *Server) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	server.mustSelectDB(dbIndex).RWUnLocks(writeKeys, readKeys)
}

// GetUndoLogs return rollback commands
func (server *Server) GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine {
	return server.mustSelectDB(dbIndex).GetUndoLogs(cmdLine)
}

// ExecWithLock executes normal commands, invoker should provide locks
func (server *Server) ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	db, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return db.execWithLock(cmdLine)
}

// BGRewriteAOF asynchronously rewrites Append-Only-File
func BGRewriteAOF(db *Server, args [][]byte) redis.Reply {
	go db.persister.Rewrite()
	return protocol.MakeStatusReply("Background append only file rewriting started")
}

// RewriteAOF start Append-Only-File rewriting and blocked until it finished
func RewriteAOF(db *Server, args [][]byte) redis.Reply {
	err := db.persister.Rewrite()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

// SaveRDB start RDB writing and blocked until it finished
func SaveRDB(db *Server, args [][]byte) redis.Reply {
	if db.persister == nil {
		return protocol.MakeErrReply("please enable aof before using save")
	}
	rdbFilename := config.Properties.RDBFilename
	if rdbFilename == "" {
		rdbFilename = "dump.rdb"
	}
	err := db.persister.GenerateRDB(rdbFilename)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

// BGSaveRDB asynchronously save RDB
func BGSaveRDB(db *Server, args [][]byte) redis.Reply {
	if db.persister == nil {
		return protocol.MakeErrReply("please enable aof before using save")
	}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				slog.Error(fmt.Sprintf("%v", err))
			}
		}()
		rdbFilename := config.Properties.RDBFilename
		if rdbFilename == "" {
			rdbFilename = "dump.rdb"
		}
		err := db.persister.GenerateRDB(rdbFilename)
		if err != nil {
			slog.Error(err.Error())
		}
	}()
	return protocol.MakeStatusReply("Background saving started")
}

// GetDBSize returns keys count and ttl key count
func (server *Server) GetDBSize(dbIndex int) (int, int) {
	db := server.mustSelectDB(dbIndex)
	return db.data.Len(), db.ttlMap.Len()
}

func (server *Server) startReplCron() {
	go func(mdb *Server) {
		ticker := time.Tick(time.Second * 10)
		for range ticker {
			mdb.slaveCron()
			mdb.masterCron()
		}
	}(server)
}

// GetAvgTTL Calculate the average expiration time of keys
func (server *Server) GetAvgTTL(dbIndex, randomKeyCount int) int64 {
	var ttlCount int64
	db := server.mustSelectDB(dbIndex)
	keys := db.data.RandomKeys(randomKeyCount)
	for _, k := range keys {
		t := time.Now()
		rawExpireTime, ok := db.ttlMap.Get(k)
		if !ok {
			continue
		}
		expireTime, _ := rawExpireTime.(time.Time)
		// if the key has already reached its expiration time during calculation, ignore it
		if expireTime.Sub(t).Microseconds() > 0 {
			ttlCount += expireTime.Sub(t).Microseconds()
		}
	}
	return ttlCount / int64(len(keys))
}

func (server *Server) SetHashFieldChangedCallback(cb database.HashFieldChangeCallback) {
	for i := range server.dbSet {
		db := server.mustSelectDB(i)
		db.hashFieldCallback = cb
	}
}

func (server *Server) SetKeyInsertedCallback(cb database.KeyEventCallback) {
	server.insertCallback = cb
	for i := range server.dbSet {
		db := server.mustSelectDB(i)
		db.insertCallback = cb
	}

}

func (server *Server) SetKeyDeletedCallback(cb database.KeyEventCallback) {
	server.deleteCallback = cb
	for i := range server.dbSet {
		db := server.mustSelectDB(i)
		db.deleteCallback = cb
	}
}

func execHello(server *Server, c redis.Connection, args [][]byte) redis.Reply {
	proto := 2
	if len(args) >= 1 {
		v, err := strconv.Atoi(string(args[0]))
		if err != nil {
			return protocol.MakeErrReply("ERR protocol version is not an integer")
		}
		if v < 2 || v > 3 {
			return protocol.MakeErrReply("ERR protocol version is not supported")
		}
		proto = v
	}

	i := 1
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "AUTH":
			if i+2 >= len(args) {
				return protocol.MakeErrReply("ERR AUTH option requires username and password arguments")
			}
			password := string(args[i+2])
			if config.Properties.RequirePass != "" {
				if config.Properties.RequirePass != password {
					return protocol.MakeErrReply("ERR invalid password")
				}
				c.SetPassword(password)
			}
			i += 3
		case "SETNAME":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR SETNAME option requires a client name argument")
			}
			i += 2
		default:
			return protocol.MakeErrReply("ERR unknown option " + opt)
		}
	}

	c.SetRespVersion(redis.RespVersion(proto))

	var role string
	if c.IsSlave() {
		role = "slave"
	} else {
		role = "master"
	}
	mode := "standalone"

	if proto == 3 {
		// RESP3 Map reply
		keys := []redis.Reply{
			protocol.MakeBulkReply([]byte("server")),
			protocol.MakeBulkReply([]byte("version")),
			protocol.MakeBulkReply([]byte("proto")),
			protocol.MakeBulkReply([]byte("id")),
			protocol.MakeBulkReply([]byte("mode")),
			protocol.MakeBulkReply([]byte("role")),
			protocol.MakeBulkReply([]byte("modules")),
		}
		values := []redis.Reply{
			protocol.MakeBulkReply([]byte("godis")),
			protocol.MakeBulkReply([]byte(godisVersion)),
			protocol.MakeIntReply(int64(proto)),
			protocol.MakeIntReply(atomic.AddInt64(&connIDCounter, 1)),
			protocol.MakeBulkReply([]byte(mode)),
			protocol.MakeBulkReply([]byte(role)),
			protocol.MakeMultiBulkReply([][]byte{}), // empty modules list
		}
		return protocol.MakeMapReply(keys, values)
	}

	// RESP2 Array reply (flat key-value pairs)
	infoPairs := [][]byte{
		[]byte("server"), []byte("godis"),
		[]byte("version"), []byte(godisVersion),
		[]byte("proto"), []byte(strconv.Itoa(proto)),
		[]byte("id"), []byte(strconv.FormatInt(atomic.AddInt64(&connIDCounter, 1), 10)),
		[]byte("mode"), []byte(mode),
		[]byte("role"), []byte(role),
		[]byte("modules"), []byte{},
	}
	return protocol.MakeMultiBulkReply(infoPairs)
}

func execClient(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client' command")
	}
	sub := strings.ToUpper(string(args[0]))
	switch sub {
	case "SETINFO":
		return &protocol.OkReply{}
	case "SETNAME":
		return &protocol.OkReply{}
	case "GETNAME":
		return protocol.MakeNullBulkReply()
	case "NO-EVICT":
		if len(args) >= 2 {
			return &protocol.OkReply{}
		}
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|no-evict' command")
	case "ID":
		return protocol.MakeIntReply(atomic.AddInt64(&connIDCounter, 1))
	case "INFO":
		return protocol.MakeBulkReply([]byte(""))
	case "LIST":
		return protocol.MakeBulkReply([]byte(""))
	case "KILL":
		return &protocol.OkReply{}
	case "PAUSE":
		return &protocol.OkReply{}
	case "UNPAUSE":
		return &protocol.OkReply{}
	case "TRACKING":
		return &protocol.OkReply{}
	case "CACHING":
		return &protocol.OkReply{}
	default:
		return protocol.MakeErrReply("ERR unknown subcommand or wrong number of arguments for 'client|" + sub + "' command")
	}
}
