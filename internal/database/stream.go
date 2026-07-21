package database

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	Stream "github.com/Hoverhuang-er/godis/internal/datastruct/stream"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

// getAsStream retrieves the stream entity for a given key, returning an error if the type is wrong.
func (db *DB) getAsStream(key string) (*Stream.Stream, protocol.ErrorReply) {
	slog.Debug("getAsStream", "key", key)
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	stream, ok := entity.Data.(*Stream.Stream)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return stream, nil
}

// getOrInitStream retrieves the stream for a key or creates a new one if it does not exist. Returns the stream, a boolean indicating if it was newly created, and any error.
func (db *DB) getOrInitStream(key string) (*Stream.Stream, bool, protocol.ErrorReply) {
	slog.Debug("getOrInitStream", "key", key)
	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited := false
	if stream == nil {
		stream = Stream.NewStream()
		db.PutEntity(key, &database.DataEntity{Data: stream})
		inited = true
	}
	return stream, inited, nil
}

// execXAdd appends a new entry to a stream and returns its ID.
func execXAdd(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("XAdd", "key", key)
	args = args[1:]

	var id string
	var maxLen int
	var fields map[string]string
	fieldMode := false

	for i := 0; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "MAXLEN":
			i++
			if i >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'XADD' command")
			}
			v, err := strconv.Atoi(string(args[i]))
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			maxLen = v
		case "NOMKSTREAM":
			continue
		default:
			if !fieldMode {
				id = string(args[i])
				fieldMode = true
			} else {
				if fields == nil {
					fields = make(map[string]string)
				}
				if i+1 >= len(args) {
					return protocol.MakeErrReply("ERR wrong number of arguments for 'XADD' command")
				}
				fieldName := string(args[i])
				i++
				fieldVal := string(args[i])
				fields[fieldName] = fieldVal
			}
		}
	}

	if id == "" {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XADD' command")
	}

	stream, _, errReply := db.getOrInitStream(key)
	if errReply != nil {
		return errReply
	}

	entryID, err := stream.Add(id, fields)
	if err != nil {
		slog.Error("XAdd failed", "error", err.Error())
		return protocol.MakeErrReply(err.Error())
	}

	if maxLen > 0 {
		stream.Trim(maxLen)
	}

	db.addAof(utils.ToCmdLine3("xadd", args...))
	return protocol.MakeBulkReply([]byte(entryID))
}

// execXLen returns the number of entries in a stream.
func execXLen(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("XLen", "key", key)
	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if stream == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(stream.Len()))
}

// execXRange returns a range of entries from a stream between start and end IDs.
func execXRange(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("XRange", "key", key)
	start := string(args[1])
	end := string(args[2])
	count := -1
	if len(args) > 3 {
		if strings.ToUpper(string(args[3])) != "COUNT" {
			return protocol.MakeErrReply("ERR syntax error")
		}
		v, err := strconv.Atoi(string(args[4]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count = v
	}

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if stream == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	if start == "-" {
		start = "0-0"
	}
	if end == "+" {
		end = stream.LastID()
	}

	entries := stream.Range(start, end, count)
	return streamEntriesToReply(entries)
}

// streamEntriesToReply converts a slice of stream entries into a bulk reply for the Redis protocol.
func streamEntriesToReply(entries []*Stream.Entry) *protocol.MultiBulkReply {
	slog.Debug("streamEntriesToReply", "count", len(entries))
	result := make([][]byte, 0, len(entries)*2)
	for _, entry := range entries {
		entryArgs := make([][]byte, 0, 2+len(entry.Fields)*2)
		entryArgs = append(entryArgs, []byte(entry.ID))
		fieldCount := protocol.MakeBulkReply([]byte(strconv.Itoa(len(entry.Fields))))
		entryArgs = append(entryArgs, fieldCount.ToBytes())
		for k, v := range entry.Fields {
			entryArgs = append(entryArgs, []byte(k))
			entryArgs = append(entryArgs, []byte(v))
		}
		result = append(result, entryArgs...)
	}
	return protocol.MakeMultiBulkReply(result)
}

// execXRead reads entries from multiple streams after given IDs, optionally limited by COUNT.
func execXRead(db *DB, args [][]byte) redis.Reply {
	slog.Info("XRead")
	count := -1
	argIdx := 0

	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XREAD' command")
	}

	if strings.ToUpper(string(args[0])) == "COUNT" {
		if len(args) < 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'XREAD' command")
		}
		v, err := strconv.Atoi(string(args[1]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count = v
		argIdx = 2
	}

	if argIdx >= len(args) || strings.ToUpper(string(args[argIdx])) != "STREAMS" {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XREAD' command")
	}
	argIdx++

	if argIdx >= len(args) {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XREAD' command")
	}

	streamKeys := make([]string, 0)
	for argIdx < len(args) {
		streamKeys = append(streamKeys, string(args[argIdx]))
		argIdx++
	}
	if len(streamKeys)%2 != 0 {
		return protocol.MakeErrReply("ERR Unbalanced XREAD list of streams")
	}
	half := len(streamKeys) / 2
	streamIDs := streamKeys[half:]
	streamKeys = streamKeys[:half]

	result := make([][]byte, 0)
	for i, key := range streamKeys {
		stream, errReply := db.getAsStream(key)
		if errReply != nil {
			return errReply
		}
		var entries []*Stream.Entry
		if stream == nil {
			entries = make([]*Stream.Entry, 0)
		} else {
			entries = stream.ReadAfter(streamIDs[i], count)
		}
		if len(entries) > 0 {
			keyReply := protocol.MakeBulkReply([]byte(key))
			result = append(result, keyReply.ToBytes())
			entriesReply := streamEntriesToReply(entries)
			result = append(result, entriesReply.ToBytes())
		}
	}

	if len(result) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiRawReply(
		[]redis.Reply{protocol.MakeMultiBulkReply(result)},
	)
}

// execXDel deletes entries from a stream by their IDs and returns the number removed.
func execXDel(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("XDel", "key", key)
	ids := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		ids[i] = string(arg)
	}

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if stream == nil {
		return protocol.MakeIntReply(0)
	}

	deleted := stream.Delete(ids)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("xdel", args...))
	}
	return protocol.MakeIntReply(int64(deleted))
}

// execXTrim trims a stream to at most MAXLEN entries and returns the number removed.
func execXTrim(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	slog.Info("XTrim", "key", key)
	if strings.ToUpper(string(args[1])) != "MAXLEN" {
		return protocol.MakeErrReply("ERR syntax error")
	}
	maxLen, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if stream == nil {
		return protocol.MakeIntReply(0)
	}

	removed := stream.Trim(maxLen)
	if removed > 0 {
		db.addAof(utils.ToCmdLine3("xtrim", args...))
	}
	return protocol.MakeIntReply(int64(removed))
}

// execXGroup dispatches XGROUP subcommands (CREATE, DESTROY, SETID, DELCONSUMER).
func execXGroup(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XGROUP' command")
	}

	subCmd := strings.ToUpper(string(args[0]))

	switch subCmd {
	case "CREATE":
		return execXGroupCreate(db, args[1:])
	case "DESTROY":
		return execXGroupDestroy(db, args[1:])
	case "SETID":
		return execXGroupSetID(db, args[1:])
	case "DELCONSUMER":
		return execXGroupDelConsumer(db, args[1:])
	default:
		return protocol.MakeErrReply("ERR unknown subcommand '" + subCmd + "'")
	}
}

// execXGroupCreate creates a new consumer group for a stream.
func execXGroupCreate(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XGROUP CREATE' command")
	}
	key := string(args[0])
	groupName := string(args[1])
	slog.Info("XGroupCreate", "key", key, "group", groupName)
	lastID := "$"
	if len(args) > 2 {
		lastID = string(args[2])
	}

	if groupName == "" {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XGROUP CREATE' command")
	}

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		slog.Error("XGroupCreate failed", "error", errReply.Error())
		return errReply
	}
	if stream == nil {
		stream = Stream.NewStream()
		db.PutEntity(key, &database.DataEntity{Data: stream})
	}

	if lastID == "$" {
		lastID = stream.LastID()
	}

	err := stream.GroupCreate(groupName, lastID)
	if err != nil {
		slog.Error("XGroupCreate failed", "error", err.Error())
		return protocol.MakeErrReply(err.Error())
	}
	db.addAof(utils.ToCmdLine3("xgroup", args...))
	return protocol.MakeOkReply()
}

// execXGroupDestroy destroys a consumer group for a stream.
func execXGroupDestroy(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XGROUP DESTROY' command")
	}
	key := string(args[0])
	groupName := string(args[1])
	slog.Info("XGroupDestroy", "key", key, "group", groupName)

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		slog.Error("XGroupDestroy failed", "error", errReply.Error())
		return errReply
	}
	if stream == nil {
		return protocol.MakeIntReply(0)
	}

	result := stream.GroupDestroy(groupName)
	if result {
		db.addAof(utils.ToCmdLine3("xgroup", args...))
	}
	return protocol.MakeIntReply(boolToInt(result))
}

// boolToInt converts a boolean to an int64 (1 for true, 0 for false).
func boolToInt(b bool) int64 {
	slog.Debug("boolToInt", "value", b)
	if b {
		return 1
	}
	return 0
}

// execXGroupSetID sets the last delivered ID for a consumer group.
func execXGroupSetID(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XGROUP SETID' command")
	}
	key := string(args[0])
	groupName := string(args[1])
	slog.Info("XGroupSetID", "key", key, "group", groupName)
	lastID := "$"
	if len(args) > 2 {
		lastID = string(args[2])
	}

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		slog.Error("XGroupSetID failed", "error", errReply.Error())
		return errReply
	}
	if stream == nil {
		return protocol.MakeErrReply("ERR no such stream for key")
	}

	if lastID == "$" {
		lastID = stream.LastID()
	}

	err := stream.GroupSetID(groupName, lastID)
	if err != nil {
		slog.Error("XGroupSetID failed", "error", err.Error())
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

// execXGroupDelConsumer removes a consumer from a consumer group and returns the number of pending entries it had.
func execXGroupDelConsumer(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XGROUP DELCONSUMER' command")
	}
	key := string(args[0])
	groupName := string(args[1])
	consumerName := string(args[2])
	slog.Info("XGroupDelConsumer", "key", key, "group", groupName, "consumer", consumerName)

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		slog.Error("XGroupDelConsumer failed", "error", errReply.Error())
		return errReply
	}
	if stream == nil {
		return protocol.MakeIntReply(0)
	}

	pending := stream.GroupDelConsumer(groupName, consumerName)
	return protocol.MakeIntReply(int64(pending))
}

// execXReadGroup reads entries from a stream as a consumer group consumer.
func execXReadGroup(db *DB, args [][]byte) redis.Reply {
	slog.Info("XReadGroup")
	count := -1
	argIdx := 0

	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XREADGROUP' command")
	}

	if strings.ToUpper(string(args[0])) == "GROUP" {
		if len(args) < 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'XREADGROUP' command")
		}
		argIdx = 2
	} else {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XREADGROUP' command")
	}

	groupName := string(args[1])
	consumerName := string(args[2])

	if strings.ToUpper(string(args[argIdx])) == "COUNT" {
		if argIdx+1 >= len(args) {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'XREADGROUP' command")
		}
		v, err := strconv.Atoi(string(args[argIdx+1]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count = v
		argIdx += 2
	}

	for argIdx < len(args) && strings.ToUpper(string(args[argIdx])) != "STREAMS" {
		argIdx++
	}
	argIdx++

	if argIdx >= len(args) {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XREADGROUP' command")
	}

	streamKeys := make([]string, 0)
	for argIdx < len(args) {
		streamKeys = append(streamKeys, string(args[argIdx]))
		argIdx++
	}
	if len(streamKeys)%2 != 0 {
		return protocol.MakeErrReply("ERR Unbalanced XREADGROUP list of streams")
	}
	half := len(streamKeys) / 2
	streamIDs := streamKeys[half:]
	streamKeys = streamKeys[:half]

	key := streamKeys[0]
	lastID := streamIDs[0]

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if stream == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	entries, _, err := stream.ReadGroup(groupName, consumerName, lastID, count)
	if err != nil {
		slog.Error("XReadGroup failed", "error", err.Error())
		return protocol.MakeErrReply(err.Error())
	}

	if len(entries) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	keyReply := protocol.MakeBulkReply([]byte(key))
	entriesReply := streamEntriesToReply(entries)
	result := protocol.MakeMultiRawReply(
		[]redis.Reply{
			protocol.MakeMultiBulkReply([][]byte{keyReply.ToBytes(), entriesReply.ToBytes()}),
		},
	)
	return result
}

// execXAck acknowledges messages in a consumer group, removing them from the pending entries list.
func execXAck(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XACK' command")
	}
	key := string(args[0])
	groupName := string(args[1])
	slog.Info("XAck", "key", key, "group", groupName)
	ids := make([]string, len(args)-2)
	for i, arg := range args[2:] {
		ids[i] = string(arg)
	}

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		slog.Error("XAck failed", "error", errReply.Error())
		return errReply
	}
	if stream == nil {
		return protocol.MakeIntReply(0)
	}

	acked := stream.Ack(groupName, ids)
	return protocol.MakeIntReply(int64(acked))
}

// execXNack moves messages back to pending for re-delivery in a consumer group.
func execXNack(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XNACK' command")
	}
	key := string(args[0])
	groupName := string(args[1])
	slog.Info("XNack", "key", key, "group", groupName)
	ids := make([]string, len(args)-2)
	for i, arg := range args[2:] {
		ids[i] = string(arg)
	}

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		slog.Error("XNack failed", "error", errReply.Error())
		return errReply
	}
	if stream == nil {
		return protocol.MakeIntReply(0)
	}

	nacked := stream.Nack(groupName, ids)
	return protocol.MakeIntReply(int64(nacked))
}

// execXInfo dispatches XINFO subcommands (STREAM, GROUPS, CONSUMERS).
func execXInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XINFO' command")
	}
	subCmd := strings.ToUpper(string(args[0]))

	switch subCmd {
	case "STREAM":
		return execXInfoStream(db, args[1:])
	case "GROUPS":
		return execXInfoGroups(db, args[1:])
	case "CONSUMERS":
		return execXInfoConsumers(db, args[1:])
	default:
		return protocol.MakeErrReply("ERR unknown subcommand or wrong number of arguments for 'XINFO' command")
	}
}

// execXInfoStream returns metadata about a stream.
func execXInfoStream(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XINFO STREAM' command")
	}
	key := string(args[0])
	slog.Info("XInfoStream", "key", key)

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if stream == nil {
		return protocol.MakeErrReply("ERR no such key")
	}

	info := stream.Info()
	result := make([]redis.Reply, 0, len(info)*2)
	result = append(result, protocol.MakeBulkReply([]byte("length")))
	result = append(result, protocol.MakeIntReply(int64(info["length"].(int))))
	result = append(result, protocol.MakeBulkReply([]byte("groups")))
	result = append(result, protocol.MakeIntReply(int64(info["groups"].(int))))

	return protocol.MakeMultiRawReply(result)
}

// execXInfoGroups returns information about all consumer groups for a stream.
func execXInfoGroups(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XINFO GROUPS' command")
	}
	key := string(args[0])
	slog.Info("XInfoGroups", "key", key)

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if stream == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	groups := stream.GroupInfo()
	result := make([][]byte, 0, len(groups))
	for _, g := range groups {
		groupReply := make([][]byte, 0, 8)
		groupReply = append(groupReply, []byte("name"))
		groupReply = append(groupReply, []byte(g["name"].(string)))
		groupReply = append(groupReply, []byte("consumers"))
		groupReply = append(groupReply, []byte(strconv.Itoa(g["consumers"].(int))))
		groupReply = append(groupReply, []byte("pending"))
		groupReply = append(groupReply, []byte(strconv.Itoa(g["pending"].(int))))
		groupReply = append(groupReply, []byte("last-delivered-id"))
		groupReply = append(groupReply, []byte(g["last-delivered-id"].(string)))
		result = append(result, protocol.MakeMultiBulkReply(groupReply).ToBytes())
	}
	return protocol.MakeMultiBulkReply(result)
}

// execXInfoConsumers returns information about all consumers in a given consumer group.
func execXInfoConsumers(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XINFO CONSUMERS' command")
	}
	key := string(args[0])
	groupName := string(args[1])
	slog.Info("XInfoConsumers", "key", key, "group", groupName)

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if stream == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	consumers := stream.ConsumerInfo(groupName)
	if consumers == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	result := make([][]byte, 0, len(consumers))
	for _, c := range consumers {
		cReply := make([][]byte, 0, 4)
		cReply = append(cReply, []byte("name"))
		cReply = append(cReply, []byte(c["name"].(string)))
		cReply = append(cReply, []byte("pending"))
		cReply = append(cReply, []byte(strconv.Itoa(c["pending"].(int))))
		result = append(result, protocol.MakeMultiBulkReply(cReply).ToBytes())
	}
	return protocol.MakeMultiBulkReply(result)
}

// execXPending returns pending entries for a consumer group with optional ID range and count filtering.
func execXPending(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'XPENDING' command")
	}
	key := string(args[0])
	groupName := string(args[1])
	slog.Info("XPending", "key", key, "group", groupName)

	stream, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if stream == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	start := "-"
	end := "+"
	count := 10

	if len(args) > 2 {
		start = string(args[2])
	}
	if len(args) > 3 {
		end = string(args[3])
	}
	if len(args) > 4 {
		v, err := strconv.Atoi(string(args[4]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count = v
	}

	pending, err := stream.PendingInfo(groupName, start, end, count)
	if err != nil {
		slog.Error("XPending failed", "error", err.Error())
		return protocol.MakeErrReply(err.Error())
	}
	if len(pending) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	result := make([][]byte, 0, len(pending)*4)
	for _, pe := range pending {
		result = append(result, []byte(pe.ID))
		result = append(result, []byte(pe.ConsumerName))
		result = append(result, []byte(fmt.Sprintf("%d", pe.DeliveryTime.UnixMilli())))
		result = append(result, []byte(strconv.FormatInt(pe.DeliveryCount, 10)))
	}
	return protocol.MakeMultiBulkReply(result)
}

func init() {
	registerCommand("XAdd", execXAdd, writeFirstKey, rollbackFirstKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("XLen", execXLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("XRange", execXRange, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("XRead", execXRead, readAllKeys, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("XDel", execXDel, writeFirstKey, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("XTrim", execXTrim, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("XGroup", execXGroup, writeFirstKey, rollbackFirstKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("XReadGroup", execXReadGroup, readAllKeys, nil, -6, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("XAck", execXAck, writeFirstKey, rollbackFirstKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("XNack", execXNack, writeFirstKey, rollbackFirstKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("XInfo", execXInfo, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagLoading, redisFlagStale}, 1, 1, 1)
	registerCommand("XPending", execXPending, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
