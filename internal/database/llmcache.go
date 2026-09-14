package database

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/Hoverhuang-er/godis/internal/datastruct/search"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

// LLMCACHE.* is a semantic cache for LLM answers: a prompt that means the same
// thing as a cached one returns the stored answer instead of paying for
// generation again. Wording does not have to match, which is the whole point —
// "how do I tune recall?" and "improving vector recall" hit the same entry.
//
// Keys:
//
//	llmc:{sha256(prompt)}   one cached answer, with a TTL
//	llmcache_stats          hit and miss counters
//
// Entries expire, so a hit is only reported after the key is confirmed to still
// exist; an expired entry is evicted from the index on the way past.

const (
	llmCachePrefix   = "llmc:"
	llmCacheIndex    = "llmcache"
	llmCacheStatsKey = "llmcache_stats"
	// DefaultCacheThreshold is the cosine similarity a cached prompt must reach
	// to count as the same question.
	//
	// It sits in the gap the built-in local embedder leaves between a reworded
	// prompt (~0.78) and an unrelated one (below 0.2). A remote embedding model
	// separates the two far more sharply, so raise this towards 0.85-0.9 when
	// using one; a threshold set for the local embedder is too permissive there.
	DefaultCacheThreshold = 0.75
)

func llmCacheSchema(dim int) search.IndexSchema {
	return search.IndexSchema{
		Name:     llmCacheIndex,
		KeyType:  search.KeyTypeHash,
		Prefixes: []string{llmCachePrefix},
		Fields: []search.FieldSchema{
			textField("prompt"),
			tagField("tags"),
			numericField("created"),
			numericField("hits"),
			vectorSchemaField("embedding", dim, "COSINE"),
		},
	}
}

func llmCacheKey(prompt string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(prompt)))
	return llmCachePrefix + hex.EncodeToString(sum[:16])
}

// execLLMCacheSet stores an answer.
//
// LLMCACHE.SET prompt response [TTL seconds] [TAG t ...]
func execLLMCacheSet(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 2, "LLMCACHE.SET"); errReply != nil {
		return errReply
	}
	prompt, response := string(args[0]), string(args[1])
	ttl := 0
	var tags []string

	for i := 2; i < len(args); {
		switch strings.ToUpper(string(args[i])) {
		case "TTL":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR TTL requires a value")
			}
			v, errReply := parseIntArg(args[i+1], "TTL")
			if errReply != nil {
				return errReply
			}
			ttl, i = v, i+2
		case "TAG", "TAGS":
			var values []string
			values, i = collectValues(args, i+1)
			tags = append(tags, values...)
		default:
			return protocol.MakeErrReply("ERR unknown argument `" + string(args[i]) + "`")
		}
	}

	vecs, errReply := embedTexts([]string{prompt})
	if errReply != nil {
		return errReply
	}
	key := llmCacheKey(prompt)

	llmCacheLock.Lock()
	defer llmCacheLock.Unlock()

	fields := []hash{
		{"prompt", prompt},
		{"response", response},
		{"tags", joinTags(tags)},
		{"created", strconv.FormatInt(time.Now().Unix(), 10)},
		{"hits", "0"},
		{"embedding", encodeVector(vecs[0])},
	}
	if errReply := storeDocument(db, key, fields, llmCacheSchema(embedDim())); errReply != nil {
		return errReply
	}
	if ttl > 0 {
		expiresAt := time.Now().Add(time.Duration(ttl) * time.Second)
		db.Expire(key, expiresAt)
		db.addAof(utils.ToCmdLine("pexpireat", key, strconv.FormatInt(expiresAt.UnixMilli(), 10)))
	}
	return protocol.MakeOkReply()
}

// execLLMCacheGet returns the cached answer for a prompt that means the same
// thing as a stored one.
//
// LLMCACHE.GET prompt [THRESHOLD x] [TAG t ...]
//
// The reply is HIT or MISS followed by the answer and the similarity, so a
// caller can branch on a single round trip.
func execLLMCacheGet(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 1, "LLMCACHE.GET"); errReply != nil {
		return errReply
	}
	prompt := string(args[0])
	threshold := DefaultCacheThreshold
	var tags []string

	for i := 1; i < len(args); {
		switch strings.ToUpper(string(args[i])) {
		case "THRESHOLD":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR THRESHOLD requires a value")
			}
			v, errReply := parseFloatArg(args[i+1], "THRESHOLD")
			if errReply != nil {
				return errReply
			}
			threshold, i = v, i+2
		case "TAG", "TAGS":
			var values []string
			values, i = collectValues(args, i+1)
			tags = append(tags, values...)
		default:
			return protocol.MakeErrReply("ERR unknown argument `" + string(args[i]) + "`")
		}
	}

	vecs, errReply := embedTexts([]string{prompt})
	if errReply != nil {
		return errReply
	}

	llmCacheLock.Lock()
	defer llmCacheLock.Unlock()

	idx := indexFor(db, llmCacheSchema(embedDim()))
	if idx == nil {
		return incrementCacheStats(db, cacheMissReply(0))
	}

	candidates, errReply := filterCandidates(idx, "tags", tags)
	if errReply != nil {
		return errReply
	}
	hits, err := idx.KNNSearch("embedding", vecs[0], 1, candidates)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	if len(hits) == 0 {
		return incrementCacheStats(db, cacheMissReply(0))
	}

	hit := hits[0]
	sim := similarity(idx, "embedding", hit.Score)
	// An entry whose TTL elapsed is gone from the database but may still be in the
	// index; confirm before reporting a hit, and evict it while we are here.
	fields := hashFields(db, hit.Key)
	if fields == nil {
		idx.RemoveDocument(hit.Key)
		return incrementCacheStats(db, cacheMissReply(sim))
	}
	if sim < threshold {
		return incrementCacheStats(db, cacheMissReply(sim))
	}

	hitCount, _ := strconv.Atoi(fields["hits"])
	if errReply := putHashFields(db, hit.Key, []hash{{"hits", strconv.Itoa(hitCount + 1)}}); errReply != nil {
		return errReply
	}
	resp := [][]byte{
		[]byte("HIT"),
		[]byte(fields["response"]),
		[]byte(strconv.FormatFloat(sim, 'f', 6, 64)),
		[]byte(hit.Key),
	}
	return incrementCacheStats(db, protocol.MakeMultiBulkReply(resp))
}

func cacheMissReply(sim float64) redis.Reply {
	return protocol.MakeMultiBulkReply([][]byte{
		[]byte("MISS"),
		{},
		[]byte(strconv.FormatFloat(sim, 'f', 6, 64)),
		{},
	})
}

// incrementCacheStats bumps the counter the reply corresponds to and returns it
// unchanged.
func incrementCacheStats(db *DB, reply redis.Reply) redis.Reply {
	multi, ok := reply.(*protocol.MultiBulkReply)
	if !ok || len(multi.Args) == 0 {
		return reply
	}
	field := "misses"
	if string(multi.Args[0]) == "HIT" {
		field = "hits"
	}
	current := hashFields(db, llmCacheStatsKey)
	count, _ := strconv.Atoi(current[field])
	if errReply := putHashFields(db, llmCacheStatsKey, []hash{{field, strconv.Itoa(count + 1)}}); errReply != nil {
		return errReply
	}
	return reply
}

// execLLMCacheDel removes one entry by its key.
// LLMCACHE.DEL key
func execLLMCacheDel(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 1, "LLMCACHE.DEL"); errReply != nil {
		return errReply
	}
	key := string(args[0])
	if !strings.HasPrefix(key, llmCachePrefix) {
		key = llmCacheKey(key)
	}

	llmCacheLock.Lock()
	defer llmCacheLock.Unlock()

	if !deleteDocument(db, key) {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(1)
}

// execLLMCacheStats reports cache effectiveness.
// LLMCACHE.STATS
func execLLMCacheStats(db *DB, args [][]byte) redis.Reply {
	fields := hashFields(db, llmCacheStatsKey)
	hits, _ := strconv.Atoi(fields["hits"])
	misses, _ := strconv.Atoi(fields["misses"])
	entries := 0
	if idx := search.GetIndex(llmCacheIndex); idx != nil {
		entries = idx.DocCount()
	}
	rate := 0.0
	if hits+misses > 0 {
		rate = float64(hits) / float64(hits+misses)
	}
	resp := [][]byte{
		[]byte("hits"), []byte(strconv.Itoa(hits)),
		[]byte("misses"), []byte(strconv.Itoa(misses)),
		[]byte("hit_rate"), []byte(strconv.FormatFloat(rate, 'f', 4, 64)),
		[]byte("entries"), []byte(strconv.Itoa(entries)),
	}
	return protocol.MakeMultiBulkReply(resp)
}

// execLLMCacheClear drops every cached answer.
// LLMCACHE.CLEAR
func execLLMCacheClear(db *DB, args [][]byte) redis.Reply {
	llmCacheLock.Lock()
	defer llmCacheLock.Unlock()

	removed := 0
	for _, key := range prefixKeys(db, llmCachePrefix) {
		if deleteDocument(db, key) {
			removed++
		}
	}
	deleteDocument(db, llmCacheStatsKey)
	return protocol.MakeIntReply(int64(removed))
}

func init() {
	registerCommand("LLMCACHE.SET", execLLMCacheSet, noPrepare, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("LLMCACHE.GET", execLLMCacheGet, noPrepare, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("LLMCACHE.DEL", execLLMCacheDel, noPrepare, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("LLMCACHE.STATS", execLLMCacheStats, noPrepare, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("LLMCACHE.CLEAR", execLLMCacheClear, noPrepare, nil, -1, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
}
