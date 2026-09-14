package database

import (
	"strconv"
	"strings"
	"time"

	"github.com/Hoverhuang-er/godis/internal/datastruct/search"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

// MEMORY.* stores an agent's conversation as retrievable long-term memory.
//
// A session's messages live in ordinary hashes and share one index, so a caller
// can pull the recent tail of a conversation for immediate context and search
// the older part by meaning when the recent window is not enough.
//
// Keys:
//
//	mem:{session}:{seq}   one message
//	memseq:{session}      the per-session sequence counter

const (
	memoryPrefix = "mem:"
	memoryIndex  = "memory"
)

func memorySchema(dim int) search.IndexSchema {
	return search.IndexSchema{
		Name:     memoryIndex,
		KeyType:  search.KeyTypeHash,
		Prefixes: []string{memoryPrefix},
		Fields: []search.FieldSchema{
			tagField("session"),
			tagField("role"),
			textField("content"),
			tagField("tags"),
			numericField("seq"),
			numericField("ts"),
			vectorSchemaField("embedding", dim, "COSINE"),
		},
	}
}

func memoryMessageKey(session string, seq int) string {
	return memoryPrefix + session + ":" + strconv.Itoa(seq)
}

func memorySeqKey(session string) string { return "memseq:" + session }

var memoryRoles = map[string]bool{"user": true, "assistant": true, "system": true, "tool": true}

// execMemoryAdd appends a message to a session.
//
// MEMORY.ADD session role content [TAG t ...]
func execMemoryAdd(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 3, "MEMORY.ADD"); errReply != nil {
		return errReply
	}
	session, role, content := string(args[0]), strings.ToLower(string(args[1])), string(args[2])
	if !memoryRoles[role] {
		return protocol.MakeErrReply("ERR role must be one of user, assistant, system, tool")
	}
	tags, _ := collectValues(args, 3)

	vecs, errReply := embedTexts([]string{content})
	if errReply != nil {
		return errReply
	}

	memoryLock.Lock()
	defer memoryLock.Unlock()

	seq := memorySeq(db, session) + 1
	fields := []hash{
		{"session", session},
		{"role", role},
		{"content", content},
		{"tags", joinTags(tags)},
		{"seq", strconv.Itoa(seq)},
		{"ts", strconv.FormatInt(time.Now().Unix(), 10)},
		{"embedding", encodeVector(vecs[0])},
	}
	if errReply := storeDocument(db, memoryMessageKey(session, seq), fields, memorySchema(embedDim())); errReply != nil {
		return errReply
	}
	if errReply := putHashFields(db, memorySeqKey(session), []hash{{"seq", strconv.Itoa(seq)}}); errReply != nil {
		return errReply
	}
	return protocol.MakeIntReply(int64(seq))
}

func memorySeq(db *DB, session string) int {
	raw, _ := hashField(db, memorySeqKey(session), "seq")
	seq, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return seq
}

// execMemoryGet returns the recent tail of a session, oldest message first.
//
// MEMORY.GET session [LIMIT n] [ROLE r]
func execMemoryGet(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 1, "MEMORY.GET"); errReply != nil {
		return errReply
	}
	session := string(args[0])
	limit, role := 20, ""

	for i := 1; i < len(args); {
		switch strings.ToUpper(string(args[i])) {
		case "LIMIT":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR LIMIT requires a value")
			}
			v, errReply := parseIntArg(args[i+1], "LIMIT")
			if errReply != nil {
				return errReply
			}
			limit, i = v, i+2
		case "ROLE":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR ROLE requires a value")
			}
			role, i = strings.ToLower(string(args[i+1])), i+2
		default:
			return protocol.MakeErrReply("ERR unknown argument `" + string(args[i]) + "`")
		}
	}

	memoryLock.RLock()
	defer memoryLock.RUnlock()

	seq := memorySeq(db, session)
	var resp [][]byte
	// Walk backwards from the newest message so LIMIT counts the most recent
	// matching messages, then reverse to hand back conversation order.
	for n := seq; n > 0 && len(resp) < limit*2; n-- {
		raw, exists := hashField(db, memoryMessageKey(session, n), "role")
		if !exists {
			continue
		}
		if role != "" && raw != role {
			continue
		}
		content, _ := hashField(db, memoryMessageKey(session, n), "content")
		resp = append(resp, []byte(raw), []byte(content))
	}
	for i, j := 0, len(resp)-2; i < j; i, j = i+2, j-2 {
		resp[i], resp[j] = resp[j], resp[i]
		resp[i+1], resp[j+1] = resp[j+1], resp[i+1]
	}
	return protocol.MakeMultiBulkReply(resp)
}

// execMemorySearch finds the messages in a session that are closest in meaning to
// a query — the older context that still matters even though it has scrolled out
// of the recent window.
//
// MEMORY.SEARCH session text [K n]
//
// Results come back most relevant first, not in conversation order: this is a
// search, and a caller wanting chronology has MEMORY.GET.
func execMemorySearch(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 2, "MEMORY.SEARCH"); errReply != nil {
		return errReply
	}
	session, text := string(args[0]), string(args[1])
	k := 5
	if len(args) >= 4 && strings.EqualFold(string(args[2]), "K") {
		v, errReply := parseIntArg(args[3], "K")
		if errReply != nil {
			return errReply
		}
		k = v
	}

	idx := indexFor(db, memorySchema(embedDim()))
	if idx == nil {
		return protocol.MakeMultiBulkReply(nil)
	}
	vecs, errReply := embedTexts([]string{text})
	if errReply != nil {
		return errReply
	}

	memoryLock.RLock()
	defer memoryLock.RUnlock()

	candidates, errReply := filterCandidates(idx, "session", []string{session})
	if errReply != nil {
		return errReply
	}
	hits, err := idx.KNNSearch("embedding", vecs[0], k, candidates)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}

	resp := make([][]byte, 0, len(hits)*2)
	for _, hit := range hits {
		resp = append(resp,
			[]byte(firstOr(hit.Fields["role"], "")),
			[]byte(firstOr(hit.Fields["content"], "")))
	}
	return protocol.MakeMultiBulkReply(resp)
}

// execMemoryClear drops a session and its messages.
// MEMORY.CLEAR session
func execMemoryClear(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 1, "MEMORY.CLEAR"); errReply != nil {
		return errReply
	}
	session := string(args[0])

	memoryLock.Lock()
	defer memoryLock.Unlock()

	removed := 0
	for _, key := range prefixKeys(db, memoryPrefix+session+":") {
		if deleteDocument(db, key) {
			removed++
		}
	}
	deleteDocument(db, memorySeqKey(session))
	return protocol.MakeIntReply(int64(removed))
}

// execMemorySessions lists sessions that hold messages.
// MEMORY.SESSIONS
func execMemorySessions(db *DB, args [][]byte) redis.Reply {
	var sessions []string
	for _, key := range prefixKeys(db, "memseq:") {
		sessions = append(sessions, strings.TrimPrefix(key, "memseq:"))
	}
	return makeStringListReply(sessions)
}

// execMemoryInfo reports one session's shape.
// MEMORY.INFO session
func execMemoryInfo(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 1, "MEMORY.INFO"); errReply != nil {
		return errReply
	}
	session := string(args[0])
	count, first, last := 0, "", ""
	for _, key := range prefixKeys(db, memoryPrefix+session+":") {
		count++
		if first == "" {
			first, _ = hashField(db, key, "ts")
		}
		last, _ = hashField(db, key, "ts")
	}
	resp := [][]byte{
		[]byte("session"), []byte(session),
		[]byte("messages"), []byte(strconv.Itoa(count)),
		[]byte("first_ts"), []byte(first),
		[]byte("last_ts"), []byte(last),
	}
	return protocol.MakeMultiBulkReply(resp)
}

func init() {
	registerCommand("MEMORY.ADD", execMemoryAdd, noPrepare, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("MEMORY.GET", execMemoryGet, noPrepare, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("MEMORY.SEARCH", execMemorySearch, noPrepare, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("MEMORY.CLEAR", execMemoryClear, noPrepare, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("MEMORY.SESSIONS", execMemorySessions, noPrepare, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("MEMORY.INFO", execMemoryInfo, noPrepare, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
