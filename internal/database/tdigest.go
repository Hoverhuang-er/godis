package database

import (
	"log/slog"
	"strconv"
	"strings"

	"github.com/Hoverhuang-er/godis/internal/datastruct/tdigest"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

func getAsTDigest(db *DB, key string) (*tdigest.TDigest, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return nil, nil
	}
	t, ok := entity.Data.(*tdigest.TDigest)
	if !ok {
		return nil, protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return t, nil
}

// TDIGEST.CREATE key [COMPRESSION c]
func execTDCreate(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeArgNumErrReply("TDIGEST.CREATE")
	}
	key := string(args[0])
	td := tdigest.New()
	if len(args) > 2 && strings.ToUpper(string(args[1])) == "COMPRESSION" {
		if c, err := strconv.ParseFloat(string(args[2]), 64); err == nil {
			td.Compression(c)
		}
	}
	db.PutEntity(key, &database.DataEntity{Data: td})
	db.addAof(utils.ToCmdLine3("tdigest.create", args...))
	return protocol.MakeOkReply()
}

// TDIGEST.ADD key value [weight]
func execTDAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("TDIGEST.ADD")
	}
	key := string(args[0])
	td, errReply := getAsTDigest(db, key)
	if errReply != nil {
		return errReply
	}
	if td == nil {
		td = tdigest.New()
	}
	value, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value must be a float")
	}
	weight := uint64(1)
	if len(args) > 2 {
		if w, err := strconv.ParseUint(string(args[2]), 10, 64); err == nil {
			weight = w
		}
	}
	td.Add(value, weight)
	db.PutEntity(key, &database.DataEntity{Data: td})
	db.addAof(utils.ToCmdLine3("tdigest.add", args...))
	return protocol.MakeOkReply()
}

// TDIGEST.QUANTILE key quantile [quantile ...]
func execTDQuantile(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("TDIGEST.QUANTILE")
	}
	key := string(args[0])
	td, errReply := getAsTDigest(db, key)
	if errReply != nil {
		return errReply
	}
	if td == nil {
		return &protocol.NullBulkReply{}
	}
	results := make([][]byte, len(args)-1)
	for i, qArg := range args[1:] {
		q, err := strconv.ParseFloat(string(qArg), 64)
		if err != nil {
			return protocol.MakeErrReply("ERR quantile must be a float")
		}
		val := td.Quantile(q)
		results[i] = []byte(strconv.FormatFloat(val, 'f', 6, 64))
	}
	return protocol.MakeMultiBulkReply(results)
}

// TDIGEST.INFO key
func execTDInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeArgNumErrReply("TDIGEST.INFO")
	}
	key := string(args[0])
	td, errReply := getAsTDigest(db, key)
	if errReply != nil {
		return errReply
	}
	if td == nil {
		return protocol.MakeErrReply("ERR not found")
	}
	count, centroids, compression := td.Info()
	result := [][]byte{
		[]byte("Compression"), []byte(strconv.FormatFloat(compression, 'f', 2, 64)),
		[]byte("Total Observations"), []byte(strconv.FormatUint(count, 10)),
		[]byte("Centroids"), []byte(strconv.Itoa(centroids)),
	}
	return protocol.MakeMultiBulkReply(result)
}

// TDIGEST.RESET key
func execTDReset(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeArgNumErrReply("TDIGEST.RESET")
	}
	key := string(args[0])
	td, errReply := getAsTDigest(db, key)
	if errReply != nil {
		return errReply
	}
	if td != nil {
		td.Reset()
	}
	return protocol.MakeOkReply()
}

// TDIGEST.MERGE dest src [src ...]
func execTDMerge(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("TDIGEST.MERGE")
	}
	destKey := string(args[0])
	merged := tdigest.New()
	for _, srcArg := range args[1:] {
		key := string(srcArg)
		td, errReply := getAsTDigest(db, key)
		if errReply != nil {
			return errReply
		}
		if td != nil {
			merged.Merge(td)
		}
	}
	db.PutEntity(destKey, &database.DataEntity{Data: merged})
	db.addAof(utils.ToCmdLine3("tdigest.merge", args...))
	return protocol.MakeOkReply()
}

func init() {
	registerCommand("TDIGEST.CREATE", execTDCreate, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("TDIGEST.ADD", execTDAdd, writeFirstKey, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("TDIGEST.QUANTILE", execTDQuantile, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("TDIGEST.INFO", execTDInfo, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("TDIGEST.RESET", execTDReset, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("TDIGEST.MERGE", execTDMerge, writeFirstKey, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	slog.Info("T-Digest commands registered (6 commands)")
}
