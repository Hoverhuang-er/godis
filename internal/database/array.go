package database

import (
	"log/slog"
	"strconv"

	Array "github.com/hdt3213/godis/internal/datastruct/array"
	"github.com/hdt3213/godis/internal/interface/database"
	"github.com/hdt3213/godis/internal/interface/redis"
	"github.com/hdt3213/godis/internal/lib/utils"
	"github.com/hdt3213/godis/internal/redis/protocol"
)

// getAsArray retrieves the array value bound to the given key
func (db *DB) getAsArray(key string) (*Array.Array, protocol.ErrorReply) {
	slog.Debug("getAsArray", "key", key)
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	arr, ok := entity.Data.(*Array.Array)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return arr, nil
}

// getOrInitArray retrieves an existing array or creates a new one if not exists
func (db *DB) getOrInitArray(key string) (arr *Array.Array, isNew bool, errReply protocol.ErrorReply) {
	slog.Debug("getOrInitArray", "key", key)
	arr, errReply = db.getAsArray(key)
	if errReply != nil {
		return nil, false, errReply
	}
	isNew = false
	if arr == nil {
		arr = &Array.Array{}
		db.PutEntity(key, &database.DataEntity{
			Data: arr,
		})
		isNew = true
	}
	return arr, isNew, nil
}

// execARSet sets array element at the given index
func execARSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("ARSet", "key", key)
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		slog.Error("ARSet failed", "error", "ERR value is not an integer or out of range")
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if index < 0 {
		slog.Error("ARSet failed", "error", "ERR index out of range")
		return protocol.MakeErrReply("ERR index out of range")
	}
	arr, _, errReply := db.getOrInitArray(key)
	if errReply != nil {
		slog.Error("ARSet failed", "error", errReply.Error())
		return errReply
	}
	arr.Set(index, args[2])
	db.addAof(utils.ToCmdLine3("ARSET", args...))
	return &protocol.OkReply{}
}

// execARGet gets array element at the given index
func execARGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("ARGet", "key", key)
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		slog.Error("ARGet failed", "error", "ERR value is not an integer or out of range")
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	arr, errReply := db.getAsArray(key)
	if errReply != nil {
		slog.Error("ARGet failed", "error", errReply.Error())
		return errReply
	}
	if arr == nil {
		return protocol.MakeNullBulkReply()
	}
	val := arr.Get(index)
	if val == nil {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeBulkReply(val)
}

// execARMSet sets multiple array elements by index-value pairs
func execARMSet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("ARMSet", "key", key)
	pairs := args[1:]
	if len(pairs)%2 != 0 {
		slog.Error("ARMSet failed", "error", "ERR wrong number of arguments for 'armset' command")
		return protocol.MakeErrReply("ERR wrong number of arguments for 'armset' command")
	}
	arr, _, errReply := db.getOrInitArray(key)
	if errReply != nil {
		slog.Error("ARMSet failed", "error", errReply.Error())
		return errReply
	}
	for i := 0; i < len(pairs); i += 2 {
		index, err := strconv.Atoi(string(pairs[i]))
		if err != nil {
			slog.Error("ARMSet failed", "error", "ERR value is not an integer or out of range")
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if index < 0 {
			slog.Error("ARMSet failed", "error", "ERR index out of range")
			return protocol.MakeErrReply("ERR index out of range")
		}
		arr.Set(index, pairs[i+1])
	}
	db.addAof(utils.ToCmdLine3("ARMSET", args...))
	return &protocol.OkReply{}
}

// execARMGet gets values at the given indices from the array
func execARMGet(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("ARMGet", "key", key)
	indices := make([]int, len(args)-1)
	for i, arg := range args[1:] {
		idx, err := strconv.Atoi(string(arg))
		if err != nil {
			slog.Error("ARMGet failed", "error", "ERR value is not an integer or out of range")
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		indices[i] = idx
	}
	arr, errReply := db.getAsArray(key)
	if errReply != nil {
		slog.Error("ARMGet failed", "error", errReply.Error())
		return errReply
	}
	if arr == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	vals := arr.MultiGet(indices)
	result := make([][]byte, len(vals))
	for i, v := range vals {
		if v != nil {
			result[i] = v
		}
	}
	return protocol.MakeMultiBulkReply(result)
}

// execARLen returns the length of the array
func execARLen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("ARLen", "key", key)
	arr, errReply := db.getAsArray(key)
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(arr.Len()))
}

// execARCount counts elements matching the given value in the array
func execARCount(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("ARCount", "key", key)
	var value []byte
	if len(args) > 1 {
		value = args[1]
	}
	arr, errReply := db.getAsArray(key)
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(arr.Count(value)))
}

// execARInfo returns metadata about the array (length and non-null count)
func execARInfo(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("ARInfo", "key", key)
	arr, errReply := db.getAsArray(key)
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	length, nonNull := arr.Info()
	result := make([][]byte, 0, 4)
	result = append(result, []byte("length"))
	result = append(result, []byte(strconv.Itoa(length)))
	result = append(result, []byte("non_null_count"))
	result = append(result, []byte(strconv.Itoa(nonNull)))
	return protocol.MakeMultiBulkReply(result)
}

// execARAppend appends values to the end of the array
func execARAppend(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("ARAppend", "key", key)
	values := args[1:]
	arr, _, errReply := db.getOrInitArray(key)
	if errReply != nil {
		slog.Error("ARAppend failed", "error", errReply.Error())
		return errReply
	}
	arr.Append(values...)
	db.addAof(utils.ToCmdLine3("ARAPPEND", args...))
	return protocol.MakeIntReply(int64(arr.Len()))
}

// execARInsert inserts values at the given index in the array
func execARInsert(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("ARInsert", "key", key)
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		slog.Error("ARInsert failed", "error", "ERR value is not an integer or out of range")
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	values := args[2:]
	arr, _, errReply := db.getOrInitArray(key)
	if errReply != nil {
		slog.Error("ARInsert failed", "error", errReply.Error())
		return errReply
	}
	arr.Insert(index, values...)
	db.addAof(utils.ToCmdLine3("ARINSERT", args...))
	return protocol.MakeIntReply(int64(arr.Len()))
}

// execARRem removes up to count occurrences of a value from the array
func execARRem(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("ARRem", "key", key)
	count, err := strconv.Atoi(string(args[1]))
	if err != nil {
		slog.Error("ARRem failed", "error", "ERR value is not an integer or out of range")
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	value := args[2]
	arr, errReply := db.getAsArray(key)
	if errReply != nil {
		slog.Error("ARRem failed", "error", errReply.Error())
		return errReply
	}
	if arr == nil {
		return protocol.MakeIntReply(0)
	}
	removed := arr.Remove(value, count)
	if removed > 0 {
		db.addAof(utils.ToCmdLine3("ARREM", args...))
	}
	return protocol.MakeIntReply(int64(removed))
}

// execARPop removes and returns the last n elements from the array
func execARPop(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("ARPop", "key", key)
	n, err := strconv.Atoi(string(args[1]))
	if err != nil {
		slog.Error("ARPop failed", "error", "ERR value is not an integer or out of range")
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	arr, errReply := db.getAsArray(key)
	if errReply != nil {
		slog.Error("ARPop failed", "error", errReply.Error())
		return errReply
	}
	if arr == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	popped := arr.Pop(n)
	if len(popped) > 0 {
		db.addAof(utils.ToCmdLine3("ARPOP", args...))
	}
	result := make([][]byte, len(popped))
	for i, v := range popped {
		if v != nil {
			result[i] = v
		}
	}
	return protocol.MakeMultiBulkReply(result)
}

// execARTrim retains elements within the specified index range in the array
func execARTrim(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("ARTrim", "key", key)
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		slog.Error("ARTrim failed", "error", "ERR value is not an integer or out of range")
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	end, err := strconv.Atoi(string(args[2]))
	if err != nil {
		slog.Error("ARTrim failed", "error", "ERR value is not an integer or out of range")
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	arr, errReply := db.getAsArray(key)
	if errReply != nil {
		slog.Error("ARTrim failed", "error", errReply.Error())
		return errReply
	}
	if arr == nil {
		return &protocol.OkReply{}
	}
	arr.Trim(start, end)
	db.addAof(utils.ToCmdLine3("ARTRIM", args...))
	return &protocol.OkReply{}
}

func init() {
	registerCommand("ARSet", execARSet, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("ARGet", execARGet, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ARMSet", execARMSet, writeFirstKey, rollbackFirstKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("ARMGet", execARMGet, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ARLen", execARLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ARCount", execARCount, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ARInfo", execARInfo, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ARAppend", execARAppend, writeFirstKey, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("ARInsert", execARInsert, writeFirstKey, rollbackFirstKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("ARRem", execARRem, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("ARPop", execARPop, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ARTrim", execARTrim, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
}
