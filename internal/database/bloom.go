package database

import (
	"log/slog"
	"strconv"

	"github.com/Hoverhuang-er/godis/internal/datastruct/bloom"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

func getAsBloom(db *DB, key string) (*bloom.Bloom, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return nil, nil
	}
	b, ok := entity.Data.(*bloom.Bloom)
	if !ok {
		return nil, protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return b, nil
}

func execBFReserve(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeArgNumErrReply("BF.RESERVE")
	}
	key := string(args[0])
	errorRate, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR error_rate must be a float")
	}
	capacity, err := strconv.ParseUint(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR capacity must be an integer")
	}
	b := bloom.New(capacity, errorRate)
	db.PutEntity(key, &database.DataEntity{Data: b})
	db.addAof(utils.ToCmdLine3("bf.reserve", args...))
	return protocol.MakeOkReply()
}

func execBFAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("BF.ADD")
	}
	key := string(args[0])
	b, errReply := getAsBloom(db, key)
	if errReply != nil {
		return errReply
	}
	if b == nil {
		b = bloom.New(100, 0.01)
	}
	added := b.InsertCount(args[1])
	db.PutEntity(key, &database.DataEntity{Data: b})
	db.addAof(utils.ToCmdLine3("bf.add", args...))
	return protocol.MakeIntReply(int64(added))
}

func execBFExists(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("BF.EXISTS")
	}
	key := string(args[0])
	b, errReply := getAsBloom(db, key)
	if errReply != nil {
		return errReply
	}
	if b == nil {
		return protocol.MakeIntReply(0)
	}
	if b.Exists(args[1]) {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

func execBFInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeArgNumErrReply("BF.INFO")
	}
	key := string(args[0])
	b, errReply := getAsBloom(db, key)
	if errReply != nil {
		return errReply
	}
	if b == nil {
		return protocol.MakeErrReply("ERR not found")
	}
	capacity, count, errorRate, _ := b.Info()
	result := [][]byte{
		[]byte("Capacity"), []byte(strconv.FormatUint(capacity, 10)),
		[]byte("Size"), []byte(strconv.FormatUint(uint64(len(b.Bytes())), 10)),
		[]byte("Number of items"), []byte(strconv.FormatUint(count, 10)),
		[]byte("Error rate"), []byte(strconv.FormatFloat(errorRate, 'f', 6, 64)),
	}
	return protocol.MakeMultiBulkReply(result)
}

func execBFMAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("BF.MADD")
	}
	key := string(args[0])
	b, errReply := getAsBloom(db, key)
	if errReply != nil {
		return errReply
	}
	if b == nil {
		b = bloom.New(100, 0.01)
	}
	result := make([]redis.Reply, len(args)-1)
	for i, item := range args[1:] {
		added := b.InsertCount(item)
		if added > 0 {
			result[i] = protocol.MakeIntReply(1)
		} else {
			result[i] = protocol.MakeIntReply(0)
		}
	}
	db.PutEntity(key, &database.DataEntity{Data: b})
	return protocol.MakeMultiRawReply(result)
}

func execBFMExists(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("BF.MEXISTS")
	}
	key := string(args[0])
	b, errReply := getAsBloom(db, key)
	if errReply != nil {
		return errReply
	}
	result := make([]redis.Reply, len(args)-1)
	var exists int64
	for i, item := range args[1:] {
		if b != nil && b.Exists(item) {
			exists = 1
		} else {
			exists = 0
		}
		result[i] = protocol.MakeIntReply(exists)
	}
	return protocol.MakeMultiRawReply(result)
}

func init() {
	registerCommand("BF.RESERVE", execBFReserve, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("BF.ADD", execBFAdd, writeFirstKey, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("BF.EXISTS", execBFExists, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("BF.INFO", execBFInfo, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("BF.MADD", execBFMAdd, writeFirstKey, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("BF.MEXISTS", execBFMExists, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	slog.Info("Bloom filter commands registered (6 commands)")
}
