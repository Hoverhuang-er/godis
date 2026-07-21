package database

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/Hoverhuang-er/godis/internal/datastruct/search"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

// parseFieldType parses a field type string (TEXT, TAG, NUMERIC, VECTOR) into a FieldType constant.
func parseFieldType(s string) (search.FieldType, error) {
	slog.Debug("parseFieldType", "type", s)
	switch strings.ToUpper(s) {
	case "TEXT":
		return search.FieldTypeText, nil
	case "TAG":
		return search.FieldTypeTag, nil
	case "NUMERIC":
		return search.FieldTypeNumeric, nil
	case "VECTOR":
		return search.FieldTypeVector, nil
	default:
		return 0, fmt.Errorf("unsupported field type: %s", s)
	}
}

// execFTCreate creates a new search index with the given schema definition.
func execFTCreate(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
	}

	idxName := string(args[0])
	slog.Info("FT.CREATE", "index", idxName)
	schema := search.IndexSchema{
		Name: idxName,
	}
	args = args[1:]

	for i := 0; i < len(args); {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "ON":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
			}
			structure := strings.ToUpper(string(args[i+1]))
			if structure != "HASH" {
				return protocol.MakeErrReply("ERR only HASH structure is supported")
			}
			i += 2
			if i < len(args) && strings.ToUpper(string(args[i])) == "PREFIX" {
				i++
				if i >= len(args) {
					return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
				}
				count, err := strconv.Atoi(string(args[i]))
				if err != nil {
					return protocol.MakeErrReply("ERR PREFIX count must be an integer")
				}
				i++
				for j := 0; j < count && i < len(args); j++ {
					schema.Prefixes = append(schema.Prefixes, string(args[i]))
					i++
				}
			}
		case "SCHEMA":
			i++
			for i < len(args) {
				if i+1 >= len(args) {
					return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
				}
				fieldName := string(args[i])
				i++
				fieldTypeStr := string(args[i])
				i++
				ft, err := parseFieldType(fieldTypeStr)
				if err != nil {
					return protocol.MakeErrReply(err.Error())
				}
				fs := search.FieldSchema{
					Name: fieldName,
					Type: ft,
				}
				if ft == search.FieldTypeVector {
					if i >= len(args) {
						return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
					}
					algoStr := string(args[i])
					i++
					var algo search.VectorAlgo
					switch strings.ToUpper(algoStr) {
					case "FLAT":
						algo = search.VectorAlgoFlat
					case "HNSW":
						algo = search.VectorAlgoHNSW
					default:
						return protocol.MakeErrReply("ERR unsupported vector algorithm: " + algoStr)
					}
					if i >= len(args) {
						return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
					}
					count, err := strconv.Atoi(string(args[i]))
					if err != nil {
						return protocol.MakeErrReply("ERR vector attribute count must be an integer")
					}
					i++
					opts := &search.VectorFieldOpts{Algo: algo}
					for j := 0; j < count && i < len(args); j++ {
						if i+1 >= len(args) {
							return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.CREATE' command")
						}
						attr := strings.ToUpper(string(args[i]))
						i++
						val := string(args[i])
						i++
						switch attr {
						case "TYPE":
							opts.Type = val
						case "DIM":
							dim, err := strconv.Atoi(val)
							if err != nil {
								return protocol.MakeErrReply("ERR DIM must be an integer")
							}
							opts.Dim = dim
						case "DISTANCE_METRIC":
							opts.DistanceMetric = val
						}
					}
					fs.VectorOpts = opts
				}
				schema.Fields = append(schema.Fields, fs)
			}
		default:
			i++
		}
	}

	if len(schema.Fields) == 0 {
		return protocol.MakeErrReply("ERR SCHEMA is required for FT.CREATE")
	}

	err := search.RegisterIndex(schema)
	if err != nil {
		slog.Error("FT.CREATE failed", "index", idxName, "error", err)
		return protocol.MakeErrReply(err.Error())
	}

	db.addAof(utils.ToCmdLine3("ft.create", args...))
	return protocol.MakeOkReply()
}

// execFTSearch performs full-text and/or vector similarity search using a registered index.
func execFTSearch(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.SEARCH' command")
	}

	idxName := string(args[0])
	query := string(args[1])
	slog.Info("FT.SEARCH", "index", idxName, "query", query)

	idx := search.GetIndex(idxName)
	if idx == nil {
		return protocol.MakeErrReply("ERR unknown index name")
	}

	limit := 10
	var queryVec []float32
	var vectorField string
	knnK := 0

	if len(args) > 2 {
		for i := 2; i < len(args); i++ {
			arg := strings.ToUpper(string(args[i]))
			switch arg {
			case "LIMIT":
				if i+2 >= len(args) {
					return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.SEARCH' command")
				}
				i += 2
				v, err := strconv.Atoi(string(args[i]))
				if err != nil {
					return protocol.MakeErrReply("ERR LIMIT must be an integer")
				}
				limit = v
			case "RETURN":
				i++
			case "PARAMS":
				i++
				if i >= len(args) {
					return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.SEARCH' command")
				}
				n, err := strconv.Atoi(string(args[i]))
				if err != nil || n%2 != 0 {
					return protocol.MakeErrReply("ERR PARAMS count must be an even integer")
				}
				i++
				for j := 0; j < n && i < len(args); j += 2 {
					paramName := string(args[i])
					i++
					paramVal := string(args[i])
					i++
					if strings.EqualFold(paramName, "query_vec") {
						vec, err := search.ParseFloat32Vec(paramVal)
						if err == nil {
							queryVec = vec
						}
					}
				}
				i--
			}
		}
	}

	if strings.Contains(query, "=>[KNN") {
		parts := strings.SplitN(query, "=>[KNN ", 2)
		if len(parts) == 2 {
			endParts := strings.SplitN(parts[1], "]", 2)
			knnPart := endParts[0]
			knnFields := strings.Fields(knnPart)
			if len(knnFields) >= 2 {
				k, err := strconv.Atoi(knnFields[0])
				if err == nil {
					knnK = k
				}
				fieldName := strings.TrimPrefix(knnFields[1], "@")
				if fieldName != "" {
					vectorField = fieldName
				}
			}
			query = strings.TrimSpace(strings.Replace(query, "=>[KNN "+knnPart+"]", "", 1))
		}
	}

	results, err := idx.Search(query, queryVec, vectorField, knnK)
	if err != nil {
		slog.Error("FT.SEARCH failed", "index", idxName, "query", query, "error", err)
		return protocol.MakeErrReply(err.Error())
	}

	total := len(results)
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	// Redis Stack FT.SEARCH response format:
	// [total, key1, score1, [field1, val1, ...], key2, score2, [field2, val2, ...]]
	resp := make([][]byte, 0, 1+len(results)*3)
	resp = append(resp, []byte(strconv.Itoa(total)))

	for _, doc := range results {
		resp = append(resp, []byte(doc.Key))
		scoreStr := strconv.FormatFloat(doc.Score, 'f', 4, 64)
		resp = append(resp, []byte(scoreStr))
		fieldPairs := make([][]byte, 0, len(doc.Fields)*2)
		for k, v := range doc.Fields {
			fieldPairs = append(fieldPairs, []byte(k))
			fieldPairs = append(fieldPairs, []byte(v))
		}
		resp = append(resp, protocol.MakeMultiBulkReply(fieldPairs).ToBytes())
	}

	return protocol.MakeMultiBulkReply(resp)
}

// execFTDropIndex drops a registered search index by name.
func execFTDropIndex(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.DROPINDEX' command")
	}
	idxName := string(args[0])
	slog.Info("FT.DROPINDEX", "index", idxName)

	ok := search.DropIndex(idxName)
	if !ok {
		slog.Error("FT.DROPINDEX failed", "index", idxName, "error", "unknown index")
		return protocol.MakeErrReply("ERR unknown index name")
	}

	db.addAof(utils.ToCmdLine3("ft.dropindex", args...))
	return protocol.MakeOkReply()
}

// execFTInfo returns metadata about a registered search index.
func execFTInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'FT.INFO' command")
	}
	idxName := string(args[0])
	slog.Info("FT.INFO", "index", idxName)

	idx := search.GetIndex(idxName)
	if idx == nil {
		return protocol.MakeErrReply("ERR unknown index name")
	}

	schema := idx.Schema()
	result := make([][]byte, 0)
	result = append(result, []byte("index_name"))
	result = append(result, []byte(schema.Name))
	result = append(result, []byte("index_definition"))
	defParts := make([][]byte, 0)
	defParts = append(defParts, []byte("key_type"))
	defParts = append(defParts, []byte("HASH"))
	defParts = append(defParts, []byte("prefixes"))
	prefixParts := make([][]byte, len(schema.Prefixes))
	for i, p := range schema.Prefixes {
		prefixParts[i] = []byte(p)
	}
	defParts = append(defParts, protocol.MakeMultiBulkReply(prefixParts).ToBytes())
	result = append(result, protocol.MakeMultiBulkReply(defParts).ToBytes())
	result = append(result, []byte("num_docs"))
	result = append(result, []byte(strconv.Itoa(idx.DocCount())))
	result = append(result, []byte("num_terms"))
	result = append(result, []byte(strconv.Itoa(idx.TermCount())))

	return protocol.MakeMultiBulkReply(result)
}

// execFTList returns the names of all registered search indexes.
func execFTList(db *DB, args [][]byte) redis.Reply {
	slog.Info("FT._LIST")
	names := search.ListIndexes()
	result := make([][]byte, len(names))
	for i, n := range names {
		result[i] = []byte(n)
	}
	return protocol.MakeMultiBulkReply(result)
}

func init() {
	registerCommand("FT.CREATE", execFTCreate, readAllKeys, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("FT.SEARCH", execFTSearch, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("FT.DROPINDEX", execFTDropIndex, readAllKeys, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FT.INFO", execFTInfo, readAllKeys, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("FT._LIST", execFTList, readAllKeys, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
