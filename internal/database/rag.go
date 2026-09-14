package database

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Hoverhuang-er/godis/internal/ai"
	"github.com/Hoverhuang-er/godis/internal/datastruct/search"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

// RAG.* is the retrieval half of a RAG pipeline, run inside the server: chunk a
// document, embed it, store it, and answer a natural-language query with the
// passages that match it.
//
// Generation is deliberately not here. The caller is an LLM already, so RAG.QUERY
// returns the assembled context with citations and lets the caller generate —
// no generation API key has to live inside the database.
//
// Layout: chunk n of document id in collection c is the hash
//
//	rag:{c}:{id}:{n}
//
// chunk counts live in the hash ragmeta:{c} (field id -> chunk count), which sits
// outside the index prefix so metadata never shows up as a search hit.

const ragIndexPrefix = "rag:"

// ragSchema is the index backing one collection.
func ragSchema(collection string, dim int) search.IndexSchema {
	return search.IndexSchema{
		Name:     ragIndexName(collection),
		KeyType:  search.KeyTypeHash,
		Prefixes: []string{ragChunkPrefix(collection)},
		Fields: []search.FieldSchema{
			textField("content"),
			textField("title"),
			tagField("source"),
			tagField("tags"),
			numericField("chunk"),
			vectorSchemaField("embedding", dim, "COSINE"),
		},
	}
}

func ragIndexName(collection string) string   { return "rag:" + collection }
func ragChunkPrefix(collection string) string { return "rag:" + collection + ":" }
func ragMetaKey(collection string) string     { return "ragmeta:" + collection }
func ragChunkKey(collection, id string, n int) string {
	return ragChunkPrefix(collection) + id + ":" + strconv.Itoa(n)
}

// execRAGIngest chunks a document, embeds every chunk, and stores them.
//
// RAG.INGEST collection id text [TITLE t] [SOURCE s] [TAG t ...]
func execRAGIngest(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 3, "RAG.INGEST"); errReply != nil {
		return errReply
	}
	collection, id, text := string(args[0]), string(args[1]), string(args[2])
	title, source := "", ""
	var tags []string

	for i := 3; i < len(args); {
		switch strings.ToUpper(string(args[i])) {
		case "TITLE":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR TITLE requires a value")
			}
			title, i = string(args[i+1]), i+2
		case "SOURCE":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR SOURCE requires a value")
			}
			source, i = string(args[i+1]), i+2
		case "TAG", "TAGS":
			var values []string
			values, i = collectValues(args, i+1)
			tags = append(tags, values...)
		default:
			return protocol.MakeErrReply("ERR unknown argument `" + string(args[i]) + "`")
		}
	}

	chunks := ai.ChunkWithDefaults(text)
	if len(chunks) == 0 {
		return protocol.MakeErrReply("ERR document is empty after chunking")
	}
	// Embed before taking the lock: a remote embedder is a network round trip we
	// must not hold the collection lock across.
	vecs, errReply := embedTexts(chunks)
	if errReply != nil {
		return errReply
	}
	dim := embedDim()

	ragLock.Lock()
	defer ragLock.Unlock()

	schema := ragSchema(collection, dim)
	if errReply := storeDocument(db, ragChunkKey(collection, id, 0), chunkFields(0, chunks[0], vecs[0], title, source, tags), schema); errReply != nil {
		return errReply
	}
	for n := 1; n < len(chunks); n++ {
		if errReply := putHashFields(db, ragChunkKey(collection, id, n),
			chunkFields(n, chunks[n], vecs[n], title, source, tags)); errReply != nil {
			return errReply
		}
	}

	// Re-ingesting a shorter document must not leave its old tail searchable.
	prev, _ := strconv.Atoi(hashFieldOr(db, ragMetaKey(collection), id, "0"))
	for n := len(chunks); n < prev; n++ {
		deleteDocument(db, ragChunkKey(collection, id, n))
	}
	if errReply := putHashFields(db, ragMetaKey(collection), []hash{{id, strconv.Itoa(len(chunks))}, {"updated", strconv.FormatInt(time.Now().Unix(), 10)}}); errReply != nil {
		return errReply
	}
	return protocol.MakeIntReply(int64(len(chunks)))
}

func chunkFields(n int, content string, vec []float32, title, source string, tags []string) []hash {
	return []hash{
		{"content", content},
		{"title", title},
		{"source", source},
		{"tags", joinTags(tags)},
		{"chunk", strconv.Itoa(n)},
		{"embedding", encodeVector(vec)},
	}
}

// execRAGQuery answers a natural-language query with the closest passages.
//
// RAG.QUERY collection text [K n] [TAG t ...] [MINSCORE x] [FORMAT CONTEXT|DETAIL]
func execRAGQuery(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 2, "RAG.QUERY"); errReply != nil {
		return errReply
	}
	collection, question := string(args[0]), string(args[1])
	k, minScore, detailed := 5, 0.0, false
	var tags []string

	for i := 2; i < len(args); {
		switch strings.ToUpper(string(args[i])) {
		case "K":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR K requires a value")
			}
			v, errReply := parseIntArg(args[i+1], "K")
			if errReply != nil {
				return errReply
			}
			k, i = v, i+2
		case "MINSCORE":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR MINSCORE requires a value")
			}
			v, errReply := parseFloatArg(args[i+1], "MINSCORE")
			if errReply != nil {
				return errReply
			}
			minScore, i = v, i+2
		case "FORMAT":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR FORMAT requires a value")
			}
			switch strings.ToUpper(string(args[i+1])) {
			case "CONTEXT":
				detailed = false
			case "DETAIL":
				detailed = true
			default:
				return protocol.MakeErrReply("ERR FORMAT must be CONTEXT or DETAIL")
			}
			i += 2
		case "TAG", "TAGS":
			var values []string
			values, i = collectValues(args, i+1)
			tags = append(tags, values...)
		default:
			return protocol.MakeErrReply("ERR unknown argument `" + string(args[i]) + "`")
		}
	}

	vecs, errReply := embedTexts([]string{question})
	if errReply != nil {
		return errReply
	}

	ragLock.Lock()
	defer ragLock.Unlock()

	idx := indexFor(db, ragSchema(collection, embedDim()))
	if idx == nil {
		return protocol.MakeErrReply("ERR no such collection '" + collection + "'")
	}

	candidates, errReply := filterCandidates(idx, "tags", tags)
	if errReply != nil {
		return errReply
	}
	hits, err := idx.KNNSearch("embedding", vecs[0], k, candidates)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}

	kept := make([]*search.DocInfo, 0, len(hits))
	for _, hit := range hits {
		if similarity(idx, "embedding", hit.Score) >= minScore {
			kept = append(kept, hit)
		}
	}
	if detailed {
		return makeRAGDetailReply(idx, kept)
	}
	if len(kept) == 0 {
		return protocol.MakeBulkReply([]byte("No passages matched the query.\n"))
	}
	return protocol.MakeBulkReply([]byte(renderContext(question, idx, kept)))
}

func renderContext(question string, idx *search.InvertedIndex, hits []*search.DocInfo) string {
	var b strings.Builder
	b.WriteString("# Retrieved context\n\n")
	b.WriteString("Query: " + question + "\n\n")
	for i, hit := range hits {
		fields := hit.Fields
		title := firstOr(fields["title"], chunkSourceLabel(hit.Key))
		meta := metaLine(
			sourceLabel(firstOr(fields["source"], "")),
			fmt.Sprintf("similarity: %.3f", similarity(idx, "embedding", hit.Score)),
			chunkSourceLabel(hit.Key),
		)
		b.WriteString(formatDoc(i+1, title, firstOr(fields["content"], ""), meta))
		b.WriteString("\n")
	}
	return b.String()
}

// sourceLabel prefixes a provider-supplied source name for the provenance line.
func sourceLabel(source string) string {
	if source == "" {
		return ""
	}
	return "source: " + source
}

// chunkSourceLabel turns "rag:kb:doc-1:3" into "doc-1 · chunk 3".
func chunkSourceLabel(key string) string {
	parts := strings.Split(key, ":")
	if len(parts) < 2 {
		return key
	}
	id := strings.Join(parts[1:len(parts)-1], ":")
	return id + " · chunk " + parts[len(parts)-1]
}

func makeRAGDetailReply(idx *search.InvertedIndex, hits []*search.DocInfo) redis.Reply {
	resp := make([][]byte, 0, len(hits)*4+1)
	resp = append(resp, []byte(strconv.Itoa(len(hits))))
	for _, hit := range hits {
		resp = append(resp, []byte(hit.Key))
		resp = append(resp, []byte(strconv.FormatFloat(similarity(idx, "embedding", hit.Score), 'f', 4, 64)))
		pairs := make([][]byte, 0, 8)
		for _, name := range []string{"title", "source", "tags", "chunk"} {
			if v := hit.Fields[name]; len(v) > 0 {
				pairs = append(pairs, []byte(name), []byte(v[0]))
			}
		}
		resp = append(resp, protocol.MakeMultiBulkReply(pairs).ToBytes())
	}
	return protocol.MakeMultiBulkReply(resp)
}

// execRAGDel removes one document and all of its chunks.
// RAG.DEL collection id
func execRAGDel(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 2, "RAG.DEL"); errReply != nil {
		return errReply
	}
	collection, id := string(args[0]), string(args[1])

	ragLock.Lock()
	defer ragLock.Unlock()

	removed := 0
	for _, key := range prefixKeys(db, ragChunkPrefix(collection)+id+":") {
		if deleteDocument(db, key) {
			removed++
		}
	}
	meta := ragMetaKey(collection)
	if _, exists := db.GetEntity(meta); exists {
		if d, errReply := db.getAsDict(meta); errReply == nil && d != nil {
			d.Remove(id)
		}
		db.addAof(utils.ToCmdLine("hdel", meta, id))
	}
	return protocol.MakeIntReply(int64(removed))
}

// execRAGDocs lists the documents in a collection.
// RAG.DOCS collection
func execRAGDocs(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 1, "RAG.DOCS"); errReply != nil {
		return errReply
	}
	fields := hashFields(db, ragMetaKey(string(args[0])))
	if len(fields) == 0 {
		return protocol.MakeMultiBulkReply(nil)
	}
	ids := make([]string, 0, len(fields))
	for _, id := range sortedKeys(fields) {
		if id == "updated" {
			continue
		}
		ids = append(ids, id)
	}
	resp := make([][]byte, 0, len(ids)*2)
	for _, id := range ids {
		resp = append(resp, []byte(id), []byte(fields[id]))
	}
	return protocol.MakeMultiBulkReply(resp)
}

// execRAGInfo reports how a collection is indexed.
// RAG.INFO collection
func execRAGInfo(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 1, "RAG.INFO"); errReply != nil {
		return errReply
	}
	collection := string(args[0])
	idx := indexFor(db, ragSchema(collection, embedDim()))
	if idx == nil {
		return protocol.MakeErrReply("ERR no such collection '" + collection + "'")
	}
	schema := idx.Schema()
	dim, metric := 0, "COSINE"
	if f := schema.FieldByName("embedding"); f != nil && f.VectorOpts != nil {
		dim, metric = f.VectorOpts.Dim, f.VectorOpts.DistanceMetric
	}
	docs := hashFields(db, ragMetaKey(collection))
	delete(docs, "updated")

	resp := [][]byte{
		[]byte("collection"), []byte(collection),
		[]byte("documents"), []byte(strconv.Itoa(len(docs))),
		[]byte("chunks"), []byte(strconv.Itoa(idx.DocCount())),
		[]byte("dim"), []byte(strconv.Itoa(dim)),
		[]byte("distance_metric"), []byte(metric),
		[]byte("embedder"), []byte(ai.DefaultEmbedder().Name()),
	}
	return protocol.MakeMultiBulkReply(resp)
}

func hashFieldOr(db *DB, key, field, fallback string) string {
	if v, ok := hashField(db, key, field); ok {
		return v
	}
	return fallback
}

func firstOr(values []string, fallback string) string {
	if len(values) > 0 && values[0] != "" {
		return values[0]
	}
	return fallback
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func init() {
	registerCommand("RAG.INGEST", execRAGIngest, noPrepare, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("RAG.QUERY", execRAGQuery, noPrepare, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("RAG.DEL", execRAGDel, noPrepare, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("RAG.DOCS", execRAGDocs, noPrepare, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("RAG.INFO", execRAGInfo, noPrepare, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
