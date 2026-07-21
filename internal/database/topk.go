package database

import (
	"log/slog"
	"strconv"

	"github.com/Hoverhuang-er/godis/internal/datastruct/topk"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

func getAsTopK(db *DB, key string) (*topk.TopK, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return nil, nil
	}
	t, ok := entity.Data.(*topk.TopK)
	if !ok {
		return nil, protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return t, nil
}

func execTopKReserve(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("TOPK.RESERVE")
	}
	key := string(args[0])
	k, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR k must be an integer")
	}
	tk := topk.New(uint(k))
	db.PutEntity(key, &database.DataEntity{Data: tk})
	db.addAof(utils.ToCmdLine3("topk.reserve", args...))
	return protocol.MakeOkReply()
}

func execTopKAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("TOPK.ADD")
	}
	key := string(args[0])
	tk, errReply := getAsTopK(db, key)
	if errReply != nil {
		return errReply
	}
	if tk == nil {
		tk = topk.New(10)
	}
	result := make([]redis.Reply, len(args)-1)
	for i, item := range args[1:] {
		added, dropped, _ := tk.Add(string(item))
		if added && dropped != "" {
			result[i] = protocol.MakeBulkReply([]byte(dropped))
		} else {
			result[i] = protocol.MakeNullBulkReply()
		}
	}
	db.PutEntity(key, &database.DataEntity{Data: tk})
	return protocol.MakeMultiRawReply(result)
}

func execTopKCount(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("TOPK.COUNT")
	}
	key := string(args[0])
	tk, errReply := getAsTopK(db, key)
	if errReply != nil {
		return errReply
	}
	if tk == nil {
		result := make([]redis.Reply, len(args)-1)
		for i := range args[1:] {
			result[i] = protocol.MakeIntReply(0)
		}
		return protocol.MakeMultiRawReply(result)
	}
	result := make([]redis.Reply, len(args)-1)
	for i, item := range args[1:] {
		count := tk.Count(string(item))
		result[i] = protocol.MakeIntReply(int64(count))
	}
	return protocol.MakeMultiRawReply(result)
}

func execTopKQuery(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("TOPK.QUERY")
	}
	key := string(args[0])
	tk, errReply := getAsTopK(db, key)
	if errReply != nil {
		return errReply
	}
	result := make([]redis.Reply, len(args)-1)
	for i, item := range args[1:] {
		exists := int64(0)
		if tk != nil && tk.Query(string(item)) {
			exists = 1
		}
		result[i] = protocol.MakeIntReply(exists)
	}
	return protocol.MakeMultiRawReply(result)
}

func execTopKList(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeArgNumErrReply("TOPK.LIST")
	}
	key := string(args[0])
	tk, errReply := getAsTopK(db, key)
	if errReply != nil {
		return errReply
	}
	if tk == nil {
		return protocol.MakeMultiBulkReply([][]byte{})
	}
	items := tk.List()
	result := make([][]byte, len(items))
	for i, item := range items {
		result[i] = []byte(item)
	}
	return protocol.MakeMultiBulkReply(result)
}

func init() {
	registerCommand("TOPK.RESERVE", execTopKReserve, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("TOPK.ADD", execTopKAdd, writeFirstKey, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("TOPK.COUNT", execTopKCount, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("TOPK.QUERY", execTopKQuery, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("TOPK.LIST", execTopKList, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	slog.Info("Top-K commands registered (5 commands)")
}
