package database

import (
	"log/slog"
	"strconv"
	"strings"

	"github.com/Hoverhuang-er/godis/internal/datastruct/cms"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

func getAsCMS(db *DB, key string) (*cms.CMS, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return nil, nil
	}
	c, ok := entity.Data.(*cms.CMS)
	if !ok {
		return nil, protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return c, nil
}

// CMS.INITBYDIM key width depth
func execCMSInitByDim(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeArgNumErrReply("CMS.INITBYDIM")
	}
	key := string(args[0])
	width, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR width must be an integer")
	}
	depth, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR depth must be an integer")
	}
	sketch := cms.NewByDim(uint(width), uint(depth))
	db.PutEntity(key, &database.DataEntity{Data: sketch})
	db.addAof(utils.ToCmdLine3("cms.initbydim", args...))
	return protocol.MakeOkReply()
}

// CMS.INITBYPROB key error_rate probability
func execCMSInitByProb(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeArgNumErrReply("CMS.INITBYPROB")
	}
	key := string(args[0])
	errorRate, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR error_rate must be a float")
	}
	probability, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR probability must be a float")
	}
	sketch := cms.NewByProb(errorRate, probability)
	db.PutEntity(key, &database.DataEntity{Data: sketch})
	db.addAof(utils.ToCmdLine3("cms.initbyprob", args...))
	return protocol.MakeOkReply()
}

// CMS.INCRBY key item increment [item increment ...]
func execCMSIncrBy(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return protocol.MakeArgNumErrReply("CMS.INCRBY")
	}
	key := string(args[0])
	sketch, errReply := getAsCMS(db, key)
	if errReply != nil {
		return errReply
	}
	if sketch == nil {
		sketch = cms.NewByDim(100, 7)
	}
	results := make([]redis.Reply, (len(args)-1)/2)
	for i := 1; i+1 < len(args); i += 2 {
		item := string(args[i])
		incr, err := strconv.ParseUint(string(args[i+1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR increment must be a non-negative integer")
		}
		sketch.IncrBy(item, incr)
		results[(i-1)/2] = protocol.MakeIntReply(int64(sketch.Query(item)))
	}
	db.PutEntity(key, &database.DataEntity{Data: sketch})
	db.addAof(utils.ToCmdLine3("cms.incrby", args...))
	return protocol.MakeMultiRawReply(results)
}

// CMS.QUERY key item [item ...]
func execCMSQuery(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("CMS.QUERY")
	}
	key := string(args[0])
	sketch, errReply := getAsCMS(db, key)
	if errReply != nil {
		return errReply
	}
	if sketch == nil {
		result := make([]redis.Reply, len(args)-1)
		for i := range args[1:] {
			result[i] = protocol.MakeIntReply(0)
		}
		return protocol.MakeMultiRawReply(result)
	}
	result := make([]redis.Reply, len(args)-1)
	for i, item := range args[1:] {
		count := sketch.Query(string(item))
		result[i] = protocol.MakeIntReply(int64(count))
	}
	return protocol.MakeMultiRawReply(result)
}

// CMS.MERGE dest src [src ...] [WEIGHTS w ...]
func execCMSMerge(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("CMS.MERGE")
	}
	destKey := string(args[0])
	var srcKeys []string
	for _, arg := range args[1:] {
		s := string(arg)
		if strings.EqualFold(s, "WEIGHTS") {
			break
		}
		srcKeys = append(srcKeys, s)
	}
	if len(srcKeys) == 0 {
		return protocol.MakeErrReply("ERR at least one source key is required")
	}
	merged := cms.NewByDim(1000, 7)
	for _, sk := range srcKeys {
		sketch, errReply := getAsCMS(db, sk)
		if errReply != nil {
			return errReply
		}
		if sketch != nil {
			merged.Merge(sketch)
		}
	}
	db.PutEntity(destKey, &database.DataEntity{Data: merged})
	db.addAof(utils.ToCmdLine3("cms.merge", args...))
	return protocol.MakeOkReply()
}

// CMS.INFO key
func execCMSInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeArgNumErrReply("CMS.INFO")
	}
	key := string(args[0])
	sketch, errReply := getAsCMS(db, key)
	if errReply != nil {
		return errReply
	}
	if sketch == nil {
		return protocol.MakeErrReply("ERR not found")
	}
	width, depth, count := sketch.Info()
	result := [][]byte{
		[]byte("width"), []byte(strconv.FormatUint(uint64(width), 10)),
		[]byte("depth"), []byte(strconv.FormatUint(uint64(depth), 10)),
		[]byte("count"), []byte(strconv.FormatUint(count, 10)),
	}
	return protocol.MakeMultiBulkReply(result)
}

func init() {
	registerCommand("CMS.INITBYDIM", execCMSInitByDim, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("CMS.INITBYPROB", execCMSInitByProb, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("CMS.INCRBY", execCMSIncrBy, writeFirstKey, rollbackFirstKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("CMS.QUERY", execCMSQuery, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("CMS.MERGE", execCMSMerge, writeFirstKey, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("CMS.INFO", execCMSInfo, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	slog.Info("Count-min sketch commands registered (6 commands)")
}
