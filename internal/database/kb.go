package database

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Hoverhuang-er/godis/internal/ai"
	"github.com/Hoverhuang-er/godis/internal/datastruct/search"
	"github.com/Hoverhuang-er/godis/internal/interface/database"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

// This file holds the plumbing shared by the knowledge-base command families
// (RAG.*, WIKI.*, MEMORY.*, LLMCACHE.*). They all store ordinary Redis hashes
// under a reserved prefix and lean on an ordinary FT index, so persistence,
// replication and reindexing come from machinery that already exists.
//
// Locking: these commands derive their keys dynamically (chunk counts, link
// graphs), so the per-key RWMutex table the command dispatcher maintains cannot
// cover them. Each family takes its own coarse lock instead, and embedding runs
// outside it so only the store is serial.
//
// ponytail: one lock per family serialises writes across all collections of that
// family. Move to per-collection locks if write throughput ever matters.

var (
	ragLock      sync.RWMutex
	wikiLock     sync.RWMutex
	memoryLock   sync.RWMutex
	llmCacheLock sync.RWMutex
)

// hash is a field/value pair used when writing documents.
type hash [2]string

// putHashFields writes every field of a hash, refreshes the search indexes that
// claim the key, and records a single HMSET in the AOF.
//
// The embedding travels through the AOF as data rather than being recomputed on
// replay, which keeps replay deterministic even with a remote embedder and
// avoids paying for embeddings twice.
func putHashFields(db *DB, key string, fields []hash) protocol.ErrorReply {
	if len(fields) == 0 {
		return protocol.MakeErrReply("ERR refusing to store an empty document")
	}
	d, _, errReply := db.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}

	cmdLine := make([][]byte, 0, 2+len(fields)*2)
	cmdLine = append(cmdLine, []byte("hmset"), []byte(key))
	for _, f := range fields {
		d.Put(f[0], []byte(f[1]))
		cmdLine = append(cmdLine, []byte(f[0]), []byte(f[1]))
	}
	reindexHash(key, d)
	db.addAof(cmdLine)
	return nil
}

// storeDocument creates the index when needed, then writes the document hash, so
// the first ingest into a fresh collection is queryable immediately.
func storeDocument(db *DB, key string, fields []hash, schema search.IndexSchema) protocol.ErrorReply {
	if _, errReply := ensureIndex(db, schema); errReply != nil {
		return errReply
	}
	return putHashFields(db, key, fields)
}

// ensureIndex registers the schema if absent and records the implicit FT.CREATE
// in the AOF, so replay rebuilds the index before the documents that fill it.
//
// A newly created index is backfilled from the keys already present. That matters
// after an AOF rewrite: rewriting regenerates the log from data entities alone,
// so the FT.CREATE that built an index does not survive it. Because a
// knowledge-base schema is fixed by its command family, the index can simply be
// rebuilt from the data rather than the database losing its search ability.
func ensureIndex(db *DB, schema search.IndexSchema) (*search.InvertedIndex, protocol.ErrorReply) {
	idx, created, err := search.EnsureIndex(schema)
	if err != nil {
		return nil, protocol.MakeErrReply("ERR " + err.Error())
	}
	if created {
		db.addAof(schema.ToCommandLine())
		backfillIndex(db, schema)
	}
	return idx, nil
}

// deleteDocument removes a key and its index entry.
func deleteDocument(db *DB, key string) bool {
	if _, exists := db.GetEntity(key); !exists {
		return false
	}
	db.Remove(key)
	db.addAof(utils.ToCmdLine("del", key))
	return true
}

// hashField reads a single field of a hash document.
func hashField(db *DB, key, field string) (string, bool) {
	d, errReply := db.getAsDict(key)
	if errReply != nil || d == nil {
		return "", false
	}
	raw, exists := d.Get(field)
	if !exists {
		return "", false
	}
	value, _ := raw.([]byte)
	return string(value), true
}

// hashFields reads every field of a hash document as strings.
func hashFields(db *DB, key string) map[string]string {
	d, errReply := db.getAsDict(key)
	if errReply != nil || d == nil {
		return nil
	}
	out := make(map[string]string, d.Len())
	d.ForEach(func(field string, val any) bool {
		if raw, ok := val.([]byte); ok {
			out[field] = string(raw)
		}
		return true
	})
	return out
}

// prefixKeys returns every live key under a prefix, walking real data rather
// than an index so callers can reconcile the two.
func prefixKeys(db *DB, prefix string) []string {
	var keys []string
	db.ForEach(func(key string, entity *database.DataEntity, _ *time.Time) bool {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return true
	})
	sort.Strings(keys)
	return keys
}

// indexFor returns a command family's index, rebuilding it when it is missing
// but its documents are still present — the state an AOF rewrite leaves behind.
//
// The rebuild records the FT.CREATE in the AOF. A read command writing to the log
// is unusual, but here it is a repair: without it the next restart repeats the
// loss. A family whose keys are genuinely absent reports no index rather than
// vivifying one, so a mistyped collection name stays an error.
func indexFor(db *DB, schema search.IndexSchema) *search.InvertedIndex {
	if idx := search.GetIndex(schema.Name); idx != nil {
		return idx
	}
	if len(schema.Prefixes) == 0 || len(prefixKeys(db, schema.Prefixes[0])) == 0 {
		return nil
	}
	idx, errReply := ensureIndex(db, schema)
	if errReply != nil {
		return nil
	}
	return idx
}

// embedDim is the vector length the configured embedder produces, which is what
// a schema needs before any text has been embedded.
func embedDim() int {
	return ai.DefaultEmbedder().Dim()
}

// embedTexts embeds a batch, reporting failure as a command error rather than an
// empty result, which the caller would read as "no matches".
func embedTexts(texts []string) ([][]float32, protocol.ErrorReply) {
	e := ai.DefaultEmbedder()
	vecs, err := e.Embed(texts)
	if err != nil {
		return nil, protocol.MakeErrReply("ERR embedding failed: " + err.Error())
	}
	if len(vecs) != len(texts) {
		return nil, protocol.MakeErrReply("ERR embedding failed: expected " + strconv.Itoa(len(texts)) +
			" vectors, got " + strconv.Itoa(len(vecs)))
	}
	return vecs, nil
}

// encodeVector renders a vector as the space-separated text stored in a document
// field, which is the form the index parses back into floats.
func encodeVector(vec []float32) string {
	parts := make([]string, len(vec))
	for i, v := range vec {
		parts[i] = strconv.FormatFloat(float64(v), 'f', -1, 32)
	}
	return strings.Join(parts, " ")
}

func vectorSchemaField(name string, dim int, metric string) search.FieldSchema {
	return search.FieldSchema{
		Name: name,
		Path: name,
		Type: search.FieldTypeVector,
		VectorOpts: &search.VectorFieldOpts{
			Algo:           search.VectorAlgoFlat,
			Type:           "FLOAT32",
			Dim:            dim,
			DistanceMetric: metric,
		},
	}
}

func textField(name string) search.FieldSchema {
	return search.FieldSchema{Name: name, Path: name, Type: search.FieldTypeText}
}

func tagField(name string) search.FieldSchema {
	return search.FieldSchema{Name: name, Path: name, Type: search.FieldTypeTag}
}

func numericField(name string) search.FieldSchema {
	return search.FieldSchema{Name: name, Path: name, Type: search.FieldTypeNumeric}
}

// similarity converts a KNN distance into the 0..1 score reported to callers.
func similarity(idx *search.InvertedIndex, field string, distance float64) float64 {
	return search.Similarity(idx.Metric(field), distance)
}

// joinTags renders a tag list as the comma-separated form the index splits on.
func joinTags(tags []string) string {
	cleaned := make([]string, 0, len(tags))
	for _, t := range tags {
		if t = strings.TrimSpace(t); t != "" {
			cleaned = append(cleaned, strings.ToLower(t))
		}
	}
	return strings.Join(cleaned, ",")
}

// filterCandidates evaluates an optional tag filter into the candidate set the
// KNN scan is restricted to. A nil map means "no filter".
func filterCandidates(idx *search.InvertedIndex, field string, tags []string) (map[string]float64, protocol.ErrorReply) {
	if len(tags) == 0 {
		return nil, nil
	}
	candidates, err := idx.EvalScores(search.TagQuery(field, tags))
	if err != nil {
		return nil, protocol.MakeErrReply("ERR " + err.Error())
	}
	return candidates, nil
}

// collectValues gathers every argument until the next recognised option keyword
// or the end of the command line.
func collectValues(args [][]byte, i int) ([]string, int) {
	var values []string
	for i < len(args) && !isKBOption(args[i]) {
		values = append(values, string(args[i]))
		i++
	}
	return values, i
}

// kbOptionKeywords terminate a free-form value list.
var kbOptionKeywords = map[string]bool{
	"TAG": true, "TAGS": true, "SOURCE": true, "TITLE": true, "SUMMARY": true,
	"CATEGORY": true, "TTL": true, "K": true, "FORMAT": true, "MINSCORE": true,
	"THRESHOLD": true, "ROLE": true, "LIMIT": true, "META": true, "FORCE": true,
	"CONTENT": true, "BY": true, "PREFIX": true, "KIND": true, "BODY": true,
}

func isKBOption(arg []byte) bool {
	return kbOptionKeywords[strings.ToUpper(string(arg))]
}

// requireArgs validates that a command received at least n arguments.
func requireArgs(args [][]byte, n int, command string) protocol.ErrorReply {
	if len(args) < n {
		return protocol.MakeErrReply("ERR wrong number of arguments for '" + command + "' command")
	}
	return nil
}

// parseIntArg parses a non-negative integer option value.
func parseIntArg(raw []byte, name string) (int, protocol.ErrorReply) {
	v, err := strconv.Atoi(string(raw))
	if err != nil || v < 0 {
		return 0, protocol.MakeErrReply("ERR " + name + " must be a non-negative integer")
	}
	return v, nil
}

// parseFloatArg parses a float option value.
func parseFloatArg(raw []byte, name string) (float64, protocol.ErrorReply) {
	v, err := strconv.ParseFloat(string(raw), 64)
	if err != nil {
		return 0, protocol.MakeErrReply("ERR " + name + " must be numeric")
	}
	return v, nil
}

// makeBulkOrNil replies with a bulk string, or a null bulk when the value is
// absent, matching how Redis reports a missing key.
func makeBulkOrNil(value string, exists bool) redis.Reply {
	if !exists {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeBulkReply([]byte(value))
}

// formatDoc renders one retrieved document as a markdown block: the shape an
// agent pastes straight into a prompt. meta is a short provenance line, already
// formatted, because a cosine similarity and a fused rank score are not the same
// quantity and must not be labelled alike.
func formatDoc(rank int, title, body, meta string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "### [%d] %s\n", rank, title)
	if meta != "" {
		b.WriteString("_" + meta + "_\n\n")
	}
	b.WriteString(strings.TrimSpace(body))
	b.WriteString("\n")
	return b.String()
}

// metaLine joins provenance fragments with a separator.
func metaLine(parts ...string) string {
	kept := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			kept = append(kept, p)
		}
	}
	return strings.Join(kept, " · ")
}

// replyOKOrErr converts a nil protocol error reply into an OK reply.
func replyOKOrErr(errReply protocol.ErrorReply) redis.Reply {
	if errReply != nil {
		return errReply
	}
	return protocol.MakeOkReply()
}
