package database

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

// jsonPathSplit splits a RedisJSON path into segments.
// Supports: $, ., .key, [N], .key.subkey[0].name
func jsonPathSplit(path string) []string {
	if path == "" || path == "$" || path == "." {
		return nil
	}
	// Strip leading $ or .
	if path[0] == '$' || path[0] == '.' {
		path = path[1:]
	}
	if path == "" {
		return nil
	}

	var segs []string
	i := 0
	for i < len(path) {
		if path[i] == '[' {
			// Array index: [N]
			j := i + 1
			for j < len(path) && path[j] != ']' {
				j++
			}
			if j > i+1 {
				segs = append(segs, path[i:j+1])
			}
			i = j + 1
			if i < len(path) && path[i] == '.' {
				i++
			}
		} else {
			// Object key
			j := i
			for j < len(path) && path[j] != '.' && path[j] != '[' {
				j++
			}
			if j > i {
				segs = append(segs, path[i:j])
			}
			i = j
			if i < len(path) && path[i] == '.' {
				i++
			}
		}
	}
	return segs
}

// jsonGetValue resolves a path against a json value.
func jsonGetValue(val interface{}, segs []string) (interface{}, error) {
	if len(segs) == 0 {
		return val, nil
	}

	current := val
	for _, seg := range segs {
		if current == nil {
			return nil, nil
		}
		if strings.HasPrefix(seg, "[") && strings.HasSuffix(seg, "]") {
			// Array index access
			idxStr := seg[1 : len(seg)-1]
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				return nil, fmt.Errorf("ERR invalid array index: %s", seg)
			}
			arr, ok := current.([]interface{})
			if !ok {
				return nil, fmt.Errorf("ERR not an array")
			}
			if idx < 0 || idx >= len(arr) {
				return nil, nil
			}
			current = arr[idx]
		} else {
			// Object key access
			obj, ok := current.(map[string]interface{})
			if !ok {
				return nil, nil
			}
			current, ok = obj[seg]
			if !ok {
				return nil, nil
			}
		}
	}
	return current, nil
}

// jsonSetValue sets a value at the given path, creating intermediate objects/arrays as needed.
func jsonSetValue(root interface{}, segs []string, newVal interface{}) (interface{}, error) {
	if len(segs) == 0 {
		return newVal, nil
	}

	if root == nil {
		root = make(map[string]interface{})
	}

	current := root
	for i, seg := range segs {
		isLast := i == len(segs)-1
		if strings.HasPrefix(seg, "[") && strings.HasSuffix(seg, "]") {
			idxStr := seg[1 : len(seg)-1]
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				return nil, fmt.Errorf("ERR invalid array index: %s", seg)
			}
			arr, ok := current.([]interface{})
			if !ok {
				return nil, fmt.Errorf("ERR path does not point to an array")
			}
			if idx < 0 || idx >= len(arr) {
				return nil, fmt.Errorf("ERR array index out of bounds")
			}
			if isLast {
				arr[idx] = newVal
			} else {
				if arr[idx] == nil {
					if strings.HasPrefix(segs[i+1], "[") {
						arr[idx] = make([]interface{}, 0)
					} else {
						arr[idx] = make(map[string]interface{})
					}
				}
				sub, err := jsonSetValue(arr[idx], segs[i+1:], newVal)
				if err != nil {
					return nil, err
				}
				arr[idx] = sub
			}
			return root, nil
		} else {
			obj, ok := current.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("ERR path does not point to an object")
			}
			if isLast {
				if newVal != nil {
					obj[seg] = newVal
				} else {
					delete(obj, seg)
				}
			} else {
				existing, exists := obj[seg]
				if !exists || existing == nil {
					if strings.HasPrefix(segs[i+1], "[") {
						obj[seg] = make([]interface{}, 0)
					} else {
						obj[seg] = make(map[string]interface{})
					}
					existing = obj[seg]
				}
				sub, err := jsonSetValue(existing, segs[i+1:], newVal)
				if err != nil {
					return nil, err
				}
				obj[seg] = sub
			}
			return root, nil
		}
	}
	return root, nil
}

// jsonDelValue deletes a value at the given path.
func jsonDelValue(root interface{}, segs []string) (interface{}, bool, error) {
	if len(segs) == 0 {
		return nil, true, nil
	}

	if root == nil {
		return nil, false, nil
	}

	current := root
	for i, seg := range segs {
		isLast := i == len(segs)-1
		if strings.HasPrefix(seg, "[") && strings.HasSuffix(seg, "]") {
			idxStr := seg[1 : len(seg)-1]
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				return nil, false, fmt.Errorf("ERR invalid array index: %s", seg)
			}
			arr, ok := current.([]interface{})
			if !ok {
				return root, false, nil
			}
			if idx < 0 || idx >= len(arr) {
				return root, false, nil
			}
			if isLast {
				arr = append(arr[:idx], arr[idx+1:]...)
				return arr, true, nil
			}
			sub, deleted, err := jsonDelValue(arr[idx], segs[i+1:])
			if err != nil {
				return nil, false, err
			}
			if deleted {
				arr[idx] = sub
			}
			return root, deleted, nil
		} else {
			obj, ok := current.(map[string]interface{})
			if !ok {
				return root, false, nil
			}
			if isLast {
				if _, exists := obj[seg]; exists {
					delete(obj, seg)
					return root, true, nil
				}
				return root, false, nil
			}
			next, exists := obj[seg]
			if !exists {
				return root, false, nil
			}
			sub, deleted, err := jsonDelValue(next, segs[i+1:])
			if err != nil {
				return nil, false, err
			}
			if deleted {
				obj[seg] = sub
			}
			return root, deleted, nil
		}
	}
	return root, false, nil
}

// jsonType returns the RedisJSON type string for a Go value.
func jsonType(val interface{}) string {
	if val == nil {
		return "null"
	}
	switch val.(type) {
	case bool:
		return "boolean"
	case float64:
		return "number"
	case string:
		return "string"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "unknown"
	}
}

// jsonMarshal marshals a Go value to JSON bytes.
func jsonMarshal(val interface{}) ([]byte, error) {
	if val == nil {
		return []byte("null"), nil
	}
	return json.Marshal(val)
}

// --- Command handlers ---

// execJSONSet sets a JSON value at the given path.
// JSON.SET key [path] json [NX | XX]
func execJSONSet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.SET' command")
	}
	key := string(args[0])
	path := "$"
	jsonIdx := 1
	hasNX := false
	hasXX := false

	if len(args) > 2 {
		// Check if second arg starts with $ or .
		arg := string(args[1])
		if strings.HasPrefix(arg, "$") || strings.HasPrefix(arg, ".") {
			path = arg
			jsonIdx = 2
		}
	}

	// Parse NX/XX flags
	for i := jsonIdx + 1; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "NX":
			hasNX = true
		case "XX":
			hasXX = true
		}
	}

	if jsonIdx >= len(args) {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.SET' command")
	}

	var parsed interface{}
	if err := json.Unmarshal(args[jsonIdx], &parsed); err != nil {
		return protocol.MakeErrReply("ERR invalid JSON: " + err.Error())
	}

	segs := jsonPathSplit(path)

	// Check existing key
	entity, exists := db.GetEntity(key)
	if hasNX && exists {
		return &protocol.NullBulkReply{}
	}
	if hasXX && !exists {
		return &protocol.NullBulkReply{}
	}

	var root interface{}
	if exists {
		if entity.Data != nil {
			root = entity.Data
		}
	}

	newRoot, err := jsonSetValue(root, segs, parsed)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	db.PutEntity(key, &database.DataEntity{Data: newRoot})
	db.addAof(utils.ToCmdLine3("json.set", args...))
	return protocol.MakeOkReply()
}

// execJSONGet returns a JSON value at the given path.
// JSON.GET key [path ...]
func execJSONGet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.GET' command")
	}
	key := string(args[0])

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	root := entity.Data

	if len(args) == 1 {
		// Return entire document
		data, err := jsonMarshal(root)
		if err != nil {
			return protocol.MakeErrReply("ERR failed to serialize JSON")
		}
		return protocol.MakeBulkReply(data)
	}

	// Multiple paths - return a JSON object with path->value mappings
	if len(args) > 2 {
		result := make(map[string]interface{})
		for i := 1; i < len(args); i++ {
			path := string(args[i])
			segs := jsonPathSplit(path)
			val, err := jsonGetValue(root, segs)
			if err != nil {
				return protocol.MakeErrReply(err.Error())
			}
			result[path] = val
		}
		data, err := jsonMarshal(result)
		if err != nil {
			return protocol.MakeErrReply("ERR failed to serialize JSON")
		}
		return protocol.MakeBulkReply(data)
	}

	// Single path
	path := string(args[1])
	segs := jsonPathSplit(path)
	val, err := jsonGetValue(root, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	data, err := jsonMarshal(val)
	if err != nil {
		return protocol.MakeErrReply("ERR failed to serialize JSON")
	}
	return protocol.MakeBulkReply(data)
}

// execJSONDel deletes a JSON value at the given path.
// JSON.DEL key [path]
func execJSONDel(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.DEL' command")
	}
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return protocol.MakeIntReply(0)
	}

	segs := jsonPathSplit(path)
	if len(segs) == 0 {
		// Delete entire key
		db.Remove(key)
		db.addAof(utils.ToCmdLine3("json.del", args...))
		return protocol.MakeIntReply(1)
	}

	newRoot, deleted, err := jsonDelValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if deleted {
		db.PutEntity(key, &database.DataEntity{Data: newRoot})
		db.addAof(utils.ToCmdLine3("json.del", args...))
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execJSONType returns the type of a JSON value at the given path.
// JSON.TYPE key [path]
func execJSONType(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.TYPE' command")
	}
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	segs := jsonPathSplit(path)
	val, err := jsonGetValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeStatusReply(jsonType(val))
}

// execJSONStrLen returns the length of a JSON string at the given path.
// JSON.STRLEN key [path]
func execJSONStrLen(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.STRLEN' command")
	}
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	segs := jsonPathSplit(path)
	val, err := jsonGetValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	s, ok := val.(string)
	if !ok {
		return protocol.MakeErrReply("ERR path does not point to a string")
	}
	return protocol.MakeIntReply(int64(len(s)))
}

// execJSONNumIncrBy increments a JSON number at the given path.
// JSON.NUMINCRBY key path number
func execJSONNumIncrBy(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.NUMINCRBY' command")
	}
	key := string(args[0])
	path := string(args[1])
	incrStr := string(args[2])
	incr, err := strconv.ParseFloat(incrStr, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR invalid number")
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	segs := jsonPathSplit(path)
	val, err := jsonGetValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	num, ok := val.(float64)
	if !ok {
		return protocol.MakeErrReply("ERR path does not point to a number")
	}

	num += incr
	newRoot, err := jsonSetValue(entity.Data, segs, num)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	db.PutEntity(key, &database.DataEntity{Data: newRoot})
	db.addAof(utils.ToCmdLine3("json.numincrby", args...))

	data, _ := jsonMarshal(num)
	return protocol.MakeBulkReply(data)
}

// execJSONNumMultBy multiplies a JSON number at the given path.
// JSON.NUMMULTBY key path number
func execJSONNumMultBy(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.NUMMULTBY' command")
	}
	key := string(args[0])
	path := string(args[1])
	multStr := string(args[2])
	mult, err := strconv.ParseFloat(multStr, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR invalid number")
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	segs := jsonPathSplit(path)
	val, err := jsonGetValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	num, ok := val.(float64)
	if !ok {
		return protocol.MakeErrReply("ERR path does not point to a number")
	}

	num *= mult
	newRoot, err := jsonSetValue(entity.Data, segs, num)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	db.PutEntity(key, &database.DataEntity{Data: newRoot})
	db.addAof(utils.ToCmdLine3("json.nummultby", args...))

	data, _ := jsonMarshal(num)
	return protocol.MakeBulkReply(data)
}

// execJSONObjLen returns the number of keys in a JSON object at the given path.
// JSON.OBJLEN key [path]
func execJSONObjLen(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.OBJLEN' command")
	}
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	segs := jsonPathSplit(path)
	val, err := jsonGetValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	obj, ok := val.(map[string]interface{})
	if !ok {
		return protocol.MakeErrReply("ERR path does not point to an object")
	}
	return protocol.MakeIntReply(int64(len(obj)))
}

// execJSONObjKeys returns the keys of a JSON object at the given path.
// JSON.OBJKEYS key [path]
func execJSONObjKeys(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.OBJKEYS' command")
	}
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	segs := jsonPathSplit(path)
	val, err := jsonGetValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	obj, ok := val.(map[string]interface{})
	if !ok {
		return protocol.MakeErrReply("ERR path does not point to an object")
	}

	result := make([][]byte, 0, len(obj))
	for k := range obj {
		result = append(result, []byte(k))
	}
	return protocol.MakeMultiBulkReply(result)
}

// execJSONArrAppend appends JSON values to a JSON array at the given path.
// JSON.ARRAPPEND key [path] value [value ...]
func execJSONArrAppend(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.ARRAPPEND' command")
	}
	key := string(args[0])
	path := "$"
	valStart := 1
	if len(args) > 2 && (strings.HasPrefix(string(args[1]), "$") || strings.HasPrefix(string(args[1]), ".")) {
		path = string(args[1])
		valStart = 2
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	segs := jsonPathSplit(path)
	val, err := jsonGetValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	arr, ok := val.([]interface{})
	if !ok {
		return protocol.MakeErrReply("ERR path does not point to an array")
	}

	for i := valStart; i < len(args); i++ {
		var parsed interface{}
		if err := json.Unmarshal(args[i], &parsed); err != nil {
			return protocol.MakeErrReply("ERR invalid JSON: " + err.Error())
		}
		arr = append(arr, parsed)
	}

	newRoot, err := jsonSetValue(entity.Data, segs, arr)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	db.PutEntity(key, &database.DataEntity{Data: newRoot})
	db.addAof(utils.ToCmdLine3("json.arrappend", args...))
	return protocol.MakeIntReply(int64(len(arr)))
}

// execJSONArrPop pops an element from a JSON array at the given path.
// JSON.ARRPOP key [path [index]]
func execJSONArrPop(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.ARRPOP' command")
	}
	key := string(args[0])
	path := "$"
	index := -1 // default: pop last

	argIdx := 1
	if len(args) > 1 && (strings.HasPrefix(string(args[1]), "$") || strings.HasPrefix(string(args[1]), ".")) {
		path = string(args[1])
		argIdx = 2
	}
	if len(args) > argIdx {
		idx, err := strconv.Atoi(string(args[argIdx]))
		if err == nil {
			index = idx
		}
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	segs := jsonPathSplit(path)
	val, err := jsonGetValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	arr, ok := val.([]interface{})
	if !ok {
		return protocol.MakeErrReply("ERR path does not point to an array")
	}
	if len(arr) == 0 {
		return &protocol.NullBulkReply{}
	}

	if index < 0 {
		index = len(arr) + index
	}
	if index < 0 || index >= len(arr) {
		return &protocol.NullBulkReply{}
	}

	popped := arr[index]
	arr = append(arr[:index], arr[index+1:]...)

	newRoot, err := jsonSetValue(entity.Data, segs, arr)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	db.PutEntity(key, &database.DataEntity{Data: newRoot})
	db.addAof(utils.ToCmdLine3("json.arrpop", args...))

	data, err := jsonMarshal(popped)
	if err != nil {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeBulkReply(data)
}

// execJSONArrTrim trims a JSON array to the specified range.
// JSON.ARRTRIM key path start stop
func execJSONArrTrim(db *DB, args [][]byte) redis.Reply {
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.ARRTRIM' command")
	}
	key := string(args[0])
	path := string(args[1])
	startStr := string(args[2])
	stopStr := string(args[3])

	start, err := strconv.Atoi(startStr)
	if err != nil {
		return protocol.MakeErrReply("ERR start must be an integer")
	}
	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		return protocol.MakeErrReply("ERR stop must be an integer")
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	segs := jsonPathSplit(path)
	val, err := jsonGetValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	arr, ok := val.([]interface{})
	if !ok {
		return protocol.MakeErrReply("ERR path does not point to an array")
	}

	// Normalize start/stop like Python slicing
	if start < 0 {
		start = len(arr) + start
	}
	if stop < 0 {
		stop = len(arr) + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= len(arr) {
		stop = len(arr) - 1
	}
	if start > stop || start >= len(arr) {
		arr = arr[:0]
	} else {
		arr = arr[start : stop+1]
	}

	newRoot, err := jsonSetValue(entity.Data, segs, arr)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	db.PutEntity(key, &database.DataEntity{Data: newRoot})
	db.addAof(utils.ToCmdLine3("json.arrtrim", args...))
	return protocol.MakeIntReply(int64(len(arr)))
}

// execJSONArrInsert inserts JSON values at a specified index in a JSON array.
// JSON.ARRINSERT key path index value [value ...]
func execJSONArrInsert(db *DB, args [][]byte) redis.Reply {
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.ARRINSERT' command")
	}
	key := string(args[0])
	path := string(args[1])
	idxStr := string(args[2])
	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		return protocol.MakeErrReply("ERR index must be an integer")
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	segs := jsonPathSplit(path)
	val, err := jsonGetValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	arr, ok := val.([]interface{})
	if !ok {
		return protocol.MakeErrReply("ERR path does not point to an array")
	}
	if idx < 0 || idx > len(arr) {
		return protocol.MakeErrReply("ERR array index out of bounds")
	}

	var items []interface{}
	for i := 3; i < len(args); i++ {
		var parsed interface{}
		if err := json.Unmarshal(args[i], &parsed); err != nil {
			return protocol.MakeErrReply("ERR invalid JSON: " + err.Error())
		}
		items = append(items, parsed)
	}

	arr = append(arr[:idx], append(items, arr[idx:]...)...)

	newRoot, err := jsonSetValue(entity.Data, segs, arr)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	db.PutEntity(key, &database.DataEntity{Data: newRoot})
	db.addAof(utils.ToCmdLine3("json.arrinsert", args...))
	return protocol.MakeIntReply(int64(len(arr)))
}

// execJSONClear clears a JSON container (array or object) at the given path.
// JSON.CLEAR key [path]
func execJSONClear(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.CLEAR' command")
	}
	key := string(args[0])
	path := "$"
	if len(args) > 1 {
		path = string(args[1])
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	segs := jsonPathSplit(path)
	val, err := jsonGetValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}

	switch v := val.(type) {
	case []interface{}:
		v = v[:0]
		newRoot, err := jsonSetValue(entity.Data, segs, v)
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		db.PutEntity(key, &database.DataEntity{Data: newRoot})
	case map[string]interface{}:
		for k := range v {
			delete(v, k)
		}
	default:
		return protocol.MakeErrReply("ERR path does not point to an array or object")
	}
	db.addAof(utils.ToCmdLine3("json.clear", args...))
	return protocol.MakeIntReply(1)
}

// execJSONToggle toggles a JSON boolean at the given path.
// JSON.TOGGLE key path
func execJSONToggle(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.TOGGLE' command")
	}
	key := string(args[0])
	path := string(args[1])

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	segs := jsonPathSplit(path)
	val, err := jsonGetValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	b, ok := val.(bool)
	if !ok {
		return protocol.MakeErrReply("ERR path does not point to a boolean")
	}

	newVal := !b
	newRoot, err := jsonSetValue(entity.Data, segs, newVal)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	db.PutEntity(key, &database.DataEntity{Data: newRoot})
	db.addAof(utils.ToCmdLine3("json.toggle", args...))

	if newVal {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execJSONStrAppend appends a JSON string to a JSON string at the given path.
// JSON.STRAPPEND key [path] value
func execJSONStrAppend(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.STRAPPEND' command")
	}
	key := string(args[0])
	path := "$"
	valIdx := 1
	if len(args) > 2 && (strings.HasPrefix(string(args[1]), "$") || strings.HasPrefix(string(args[1]), ".")) {
		path = string(args[1])
		valIdx = 2
	}

	if valIdx >= len(args) {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'JSON.STRAPPEND' command")
	}
	var appendVal string
	if err := json.Unmarshal(args[valIdx], &appendVal); err != nil {
		return protocol.MakeErrReply("ERR invalid JSON string")
	}

	entity, exists := db.GetEntity(key)
	if !exists || entity.Data == nil {
		return &protocol.NullBulkReply{}
	}

	segs := jsonPathSplit(path)
	val, err := jsonGetValue(entity.Data, segs)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	if val == nil {
		return &protocol.NullBulkReply{}
	}
	s, ok := val.(string)
	if !ok {
		return protocol.MakeErrReply("ERR path does not point to a string")
	}

	s += appendVal
	newRoot, err := jsonSetValue(entity.Data, segs, s)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	db.PutEntity(key, &database.DataEntity{Data: newRoot})
	db.addAof(utils.ToCmdLine3("json.strappend", args...))
	return protocol.MakeIntReply(int64(len(s)))
}

func init() {
	registerCommand("JSON.SET", execJSONSet, writeFirstKey, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("JSON.GET", execJSONGet, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("JSON.DEL", execJSONDel, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.TYPE", execJSONType, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("JSON.STRLEN", execJSONStrLen, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("JSON.NUMINCRBY", execJSONNumIncrBy, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.NUMMULTBY", execJSONNumMultBy, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.OBJLEN", execJSONObjLen, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("JSON.OBJKEYS", execJSONObjKeys, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("JSON.ARRAPPEND", execJSONArrAppend, writeFirstKey, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("JSON.ARRPOP", execJSONArrPop, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.ARRTRIM", execJSONArrTrim, writeFirstKey, rollbackFirstKey, 5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.ARRINSERT", execJSONArrInsert, writeFirstKey, rollbackFirstKey, -5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("JSON.CLEAR", execJSONClear, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.TOGGLE", execJSONToggle, writeFirstKey, rollbackFirstKey, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("JSON.STRAPPEND", execJSONStrAppend, writeFirstKey, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)

	slog.Info("RedisJSON commands registered (16 commands)")
}
