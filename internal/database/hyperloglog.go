package database

import (
	"hash/fnv"
	"log/slog"

	"github.com/Hoverhuang-er/godis/internal/datastruct/hyperloglog"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

func getAsHLL(key string) *hyperloglog.HLL {
	return nil
}

func execPFAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("PFADD")
	}
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	var hll *hyperloglog.HLL
	if exists && entity.Data != nil {
		hll = entity.Data.(*hyperloglog.HLL)
	} else {
		hll = hyperloglog.New()
	}
	changed := false
	for _, elem := range args[1:] {
		h := fnv.New64a()
		h.Write(elem)
		oldCount := hll.Count()
		hll.Add(h.Sum64())
		if hll.Count() != oldCount {
			changed = true
		}
	}
	db.PutEntity(key, &database.DataEntity{Data: hll})
	if changed {
		db.addAof(utils.ToCmdLine3("pfadd", args...))
	}
	if changed {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

func execPFCount(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeArgNumErrReply("PFCOUNT")
	}
	// Merge all HLLs
	merged := hyperloglog.New()
	for _, keyArg := range args {
		key := string(keyArg)
		entity, exists := db.GetEntity(key)
		if exists && entity.Data != nil {
			if hll, ok := entity.Data.(*hyperloglog.HLL); ok {
				merged.Merge(hll)
			}
		}
	}
	return protocol.MakeIntReply(int64(merged.Count()))
}

func execPFMerge(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("PFMERGE")
	}
	destKey := string(args[0])
	merged := hyperloglog.New()
	for _, srcArg := range args[1:] {
		key := string(srcArg)
		entity, exists := db.GetEntity(key)
		if exists && entity.Data != nil {
			if hll, ok := entity.Data.(*hyperloglog.HLL); ok {
				merged.Merge(hll)
			}
		}
	}
	db.PutEntity(destKey, &database.DataEntity{Data: merged})
	db.addAof(utils.ToCmdLine3("pfmerge", args...))
	return protocol.MakeOkReply()
}

func init() {
	registerCommand("PFADD", execPFAdd, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("PFCOUNT", execPFCount, readAllKeys, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("PFMERGE", execPFMerge, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	slog.Info("HyperLogLog commands registered (3 commands)")
}
