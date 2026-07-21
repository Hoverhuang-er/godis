package database

import (
	"log/slog"
	"strconv"
	"strings"

	TS "github.com/hdt3213/godis/datastruct/timeseries"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// execTSCreate creates a new time series with optional RETENTION and LABELS
func execTSCreate(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'TS.CREATE' command")
	}
	key := string(args[0])
	slog.Info("TS.CREATE", "key", key)
	args = args[1:]

	var retention int64
	labels := make(map[string]string)

	for i := 0; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "RETENTION":
			i++
			if i >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'TS.CREATE' command")
			}
			v, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR RETENTION must be an integer")
			}
			retention = v
		case "LABELS":
			i++
			for i < len(args) {
				if strings.ToUpper(string(args[i])) == "RETENTION" {
					break
				}
				if i+1 >= len(args) {
					return protocol.MakeErrReply("ERR wrong number of arguments for 'TS.CREATE' command")
				}
				labelKey := string(args[i])
				i++
				labelVal := string(args[i])
				i++
				labels[labelKey] = labelVal
				if i >= len(args) {
					break
				}
			}
			i--
		}
	}

	ts := TS.NewTimeSeries(key, retention, labels)
	TS.RegisterTimeSeries(key, ts)
	db.PutEntity(key, &database.DataEntity{Data: ts})

	return protocol.MakeOkReply()
}

// execTSAdd appends a sample to a time series, creating it if needed
func execTSAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'TS.ADD' command")
	}
	key := string(args[0])
	slog.Info("TS.ADD", "key", key)
	timestampStr := string(args[1])
	valueStr := string(args[2])

	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}

	var timestamp int64
	if strings.ToUpper(timestampStr) == "*" {
		timestamp = 0
	} else {
		timestamp, err = strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR timestamp is not a valid integer")
		}
	}

	ts := TS.GetTimeSeries(key)
	if ts == nil {
		ts = TS.NewTimeSeries(key, 0, nil)
		TS.RegisterTimeSeries(key, ts)
		db.PutEntity(key, &database.DataEntity{Data: ts})
	}

	err = ts.Add(timestamp, value)
	if err != nil {
		slog.Error("TS.ADD failed", "key", key, "error", err)
		return protocol.MakeErrReply(err.Error())
	}

	if timestamp == 0 {
		timestamp = ts.LastTimestamp()
	}
	return protocol.MakeIntReply(timestamp)
}

// execTSGet returns the last sample of a time series
func execTSGet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'TS.GET' command")
	}
	key := string(args[0])
	slog.Info("TS.GET", "key", key)

	ts := TS.GetTimeSeries(key)
	if ts == nil {
		slog.Error("TS.GET failed", "key", key, "error", "key does not exist")
		return protocol.MakeErrReply("ERR key does not exist")
	}

	info := ts.Info()
	result := make([][]byte, 0, 4)
	result = append(result, []byte(strconv.FormatInt(info["lastTimestamp"].(int64), 10)))
	result = append(result, []byte(strconv.FormatFloat(info["lastValue"].(float64), 'f', -1, 64)))
	return protocol.MakeMultiBulkReply(result)
}

// execTSRange returns samples within the given time range with optional aggregation
func execTSRange(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'TS.RANGE' command")
	}
	key := string(args[0])
	slog.Info("TS.RANGE", "key", key)
	fromStr := string(args[1])
	toStr := string(args[2])

	var from, to int64
	var err error

	if strings.ToUpper(fromStr) == "-" {
		from = 0
	} else {
		from, err = strconv.ParseInt(fromStr, 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR from timestamp is not a valid integer")
		}
	}

	if strings.ToUpper(toStr) == "+" {
		to = 0
	} else {
		to, err = strconv.ParseInt(toStr, 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR to timestamp is not a valid integer")
		}
	}

	count := 0
	aggregation := ""
	bucketDuration := int64(0)

	for i := 3; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "COUNT":
			i++
			if i >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'TS.RANGE' command")
			}
			v, err := strconv.Atoi(string(args[i]))
			if err != nil {
				return protocol.MakeErrReply("ERR COUNT must be an integer")
			}
			count = v
		case "AGGREGATION":
			i++
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'TS.RANGE' command")
			}
			aggregation = strings.ToUpper(string(args[i]))
			i++
			dur, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR aggregation bucket duration must be an integer")
			}
			bucketDuration = dur
		}
	}

	ts := TS.GetTimeSeries(key)
	if ts == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	samples := ts.Range(from, to, count, aggregation, bucketDuration)
	result := make([][]byte, 0, len(samples)*2)
	for _, s := range samples {
		result = append(result, []byte(strconv.FormatInt(s.Timestamp, 10)))
		result = append(result, []byte(strconv.FormatFloat(s.Value, 'f', -1, 64)))
	}
	return protocol.MakeMultiBulkReply(result)
}

// execTSInfo returns metadata and statistics for a time series
func execTSInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'TS.INFO' command")
	}
	key := string(args[0])
	slog.Info("TS.INFO", "key", key)

	ts := TS.GetTimeSeries(key)
	if ts == nil {
		slog.Error("TS.INFO failed", "key", key, "error", "key does not exist")
		return protocol.MakeErrReply("ERR key does not exist")
	}

	info := ts.Info()
	result := make([][]byte, 0, 16)
	result = append(result, []byte("totalSamples"))
	result = append(result, []byte(strconv.Itoa(info["totalSamples"].(int))))
	result = append(result, []byte("retention"))
	result = append(result, []byte(strconv.FormatInt(info["retention"].(int64), 10)))
	result = append(result, []byte("lastTimestamp"))
	result = append(result, []byte(strconv.FormatInt(info["lastTimestamp"].(int64), 10)))
	result = append(result, []byte("lastValue"))
	result = append(result, []byte(strconv.FormatFloat(info["lastValue"].(float64), 'f', -1, 64)))

	return protocol.MakeMultiBulkReply(result)
}

func init() {
	registerCommand("TS.CREATE", execTSCreate, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("TS.ADD", execTSAdd, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("TS.GET", execTSGet, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("TS.RANGE", execTSRange, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("TS.INFO", execTSInfo, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
