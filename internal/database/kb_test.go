package database

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Hoverhuang-er/godis/internal/datastruct/search"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

func replyText(t *testing.T, reply redis.Reply) string {
	t.Helper()
	if errReply, ok := reply.(protocol.ErrorReply); ok {
		t.Fatalf("command failed: %s", errReply.Error())
	}
	bulk, ok := reply.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("expected a bulk reply, got %T: %s", reply, reply.ToBytes())
	}
	return string(bulk.Arg)
}

func replyList(t *testing.T, reply redis.Reply) []string {
	t.Helper()
	if errReply, ok := reply.(protocol.ErrorReply); ok {
		t.Fatalf("command failed: %s", errReply.Error())
	}
	multi, ok := reply.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("expected a multi bulk reply, got %T: %s", reply, reply.ToBytes())
	}
	out := make([]string, len(multi.Args))
	for i, a := range multi.Args {
		out[i] = string(a)
	}
	return out
}

func replyInt(t *testing.T, reply redis.Reply) int64 {
	t.Helper()
	if errReply, ok := reply.(protocol.ErrorReply); ok {
		t.Fatalf("command failed: %s", errReply.Error())
	}
	n, ok := reply.(*protocol.IntReply)
	if !ok {
		t.Fatalf("expected an int reply, got %T: %s", reply, reply.ToBytes())
	}
	return n.Code
}

// TestRAGIngestAndQuery covers the retrieval half of a RAG pipeline end to end:
// chunk, embed, store, then answer a query with cited passages.
func TestRAGIngestAndQuery(t *testing.T) {
	db := makeTestDB()
	defer db.Flush()

	docs := map[string]string{
		"hnsw": "HNSW builds a multi-layer proximity graph. Query cost grows " +
			"logarithmically with the number of vectors, and recall is traded " +
			"against build time through the efConstruction and efRuntime parameters.",
		"flat": "A flat index compares the query against every vector. It gives " +
			"exact recall and is the right default for collections that fit in memory, " +
			"because exhaustive search over a small set is both fast and precise.",
		"bread": "Sourdough needs a starter, flour, water and salt. Fold the dough " +
			"gently and let it proof overnight so the crust develops properly.",
	}
	for id, text := range docs {
		n := replyInt(t, mustExec(t, db, "RAG.INGEST", "kb", id, text,
			"TITLE", "note about "+id, "TAG", "engineering", "SOURCE", "handbook.md"))
		if n < 1 {
			t.Fatalf("RAG.INGEST %s reported %d chunks", id, n)
		}
	}

	// The index is created implicitly on first ingest and must be queryable at once.
	context := replyText(t, mustExec(t, db, "RAG.QUERY", "kb", "how is recall traded against speed?", "K", "1"))
	if !strings.Contains(context, "HNSW") {
		t.Fatalf("the nearest passage should be the graph-index note, got:\n%s", context)
	}
	if !strings.Contains(context, "Query: how is recall traded against speed?") {
		t.Error("the assembled context must restate the query so it can be pasted into a prompt")
	}
	if !strings.Contains(context, "source: handbook.md") {
		t.Error("passages must carry their provenance for citation")
	}
	// Ranking, not merely membership: the unrelated document must sort last.
	ranked := replyText(t, mustExec(t, db, "RAG.QUERY", "kb", "how is recall traded against speed?"))
	if strings.Index(ranked, "HNSW") > strings.Index(ranked, "Sourdough") {
		t.Fatalf("the relevant passage must outrank the unrelated one:\n%s", ranked)
	}
	if !strings.Contains(ranked, "Sourdough") {
		t.Fatalf("K defaults to 5 and there are only 3 chunks, so all should appear:\n%s", ranked)
	}
	// MINSCORE is how a caller asks only for genuinely close passages.
	if filtered := replyText(t, mustExec(t, db, "RAG.QUERY", "kb", "how is recall traded against speed?", "MINSCORE", "0.25")); strings.Contains(filtered, "Sourdough") {
		t.Fatalf("MINSCORE 0.25 must drop the unrelated passage:\n%s", filtered)
	}

	// A tag filter must exclude documents outside the tag.
	if context := replyText(t, mustExec(t, db, "RAG.QUERY", "kb", "exact recall", "TAG", "recipes")); !strings.Contains(context, "No passages") {
		t.Fatalf("a tag with no matches must say so, got:\n%s", context)
	}

	// DETAIL gives a client the structured form.
	detail := replyList(t, mustExec(t, db, "RAG.QUERY", "kb", "flat index", "FORMAT", "DETAIL"))
	if len(detail) < 4 {
		t.Fatalf("DETAIL must return [total, key, score, fields, ...], got %v", detail)
	}

	info := replyList(t, mustExec(t, db, "RAG.INFO", "kb"))
	if got := infoValue(info, "documents"); got != strconv.Itoa(len(docs)) {
		t.Fatalf("RAG.INFO documents = %s, want %d", got, len(docs))
	}
	if got := infoValue(info, "dim"); got == "0" {
		t.Fatal("RAG.INFO must report the embedding dimension")
	}

	// Re-ingesting a shorter document must not leave the old tail searchable.
	long := strings.Repeat("alpha bravo charlie delta echo foxtrot. ", 200)
	mustExec(t, db, "RAG.INGEST", "kb", "rewritten", long)
	mustExec(t, db, "RAG.INGEST", "kb", "rewritten", "now short")
	if n := replyInt(t, mustExec(t, db, "RAG.INGEST", "kb", "rewritten", "still short")); n != 1 {
		t.Fatalf("a short document must produce one chunk, got %d", n)
	}
	if infoValue(replyList(t, mustExec(t, db, "RAG.INFO", "kb")), "chunks") == "" {
		t.Fatal("chunk count missing from RAG.INFO")
	}

	removed := replyInt(t, mustExec(t, db, "RAG.DEL", "kb", "rewritten"))
	if removed != 1 {
		t.Fatalf("RAG.DEL should have removed the single remaining chunk, removed %d", removed)
	}
	docsList := replyList(t, mustExec(t, db, "RAG.DOCS", "kb"))
	if len(docsList) != len(docs)*2 {
		t.Fatalf("RAG.DOCS should pair id and chunk count, got %v", docsList)
	}
}

func infoValue(pairs []string, key string) string {
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i] == key {
			return pairs[i+1]
		}
	}
	return ""
}

// TestWikiLifecycle covers the LLM-wiki operations: pages, the link graph, the
// generated index and log, and the lint pass.
func TestWikiLifecycle(t *testing.T) {
	db := makeTestDB()
	defer db.Flush()

	stored := map[string]string{
		"vector-search": "## Overview\n\nVector search matches on meaning. " +
			"It relates to [[hnsw]] and [[embeddings]].\n\n## Details\n\nDistance metrics decide what \"near\" means.",
		"hnsw":       "A layered proximity graph. Used by [[vector-search]].\n\n## Recall\n\nTraded against build time.",
		"embeddings": "Dense vectors from a model. Produced for [[vector-search]].\n\n## Models\n\nDimension must match the index.",
		"orphan":     "A page nobody links to.\n\n## Notes\n\nShould be reported by lint.",
	}
	for name, content := range stored {
		mustExec(t, db, "WIKI.SET", name, content, "TITLE", name, "CATEGORY", "search")
	}

	page := replyText(t, mustExec(t, db, "WIKI.GET", "vector-search"))
	if !strings.Contains(page, "matches on meaning") {
		t.Fatalf("WIKI.GET returned the wrong body: %q", page)
	}
	if _, errReply := db.Exec(nil, toBytesArgs("WIKI.GET", "no-such-page")).(protocol.ErrorReply); !errReply {
		// a missing page is a null bulk, not an error
	}
	if reply, ok := db.Exec(nil, toBytesArgs("WIKI.GET", "no-such-page")).(*protocol.NullBulkReply); !ok {
		t.Fatalf("a missing page must be a null bulk, got %T", reply)
	}

	// Links and backlinks come from the link graph the server maintains.
	links := replyList(t, mustExec(t, db, "WIKI.LINKS", "vector-search"))
	if len(links) != 2 || !contains(links, "hnsw") || !contains(links, "embeddings") {
		t.Fatalf("WIKI.LINKS = %v", links)
	}
	back := replyList(t, mustExec(t, db, "WIKI.BACKLINKS", "vector-search"))
	if len(back) != 2 || !contains(back, "hnsw") || !contains(back, "embeddings") {
		t.Fatalf("WIKI.BACKLINKS = %v", back)
	}
	if back := replyList(t, mustExec(t, db, "WIKI.BACKLINKS", "hnsw")); len(back) != 1 || back[0] != "vector-search" {
		t.Fatalf("WIKI.BACKLINKS hnsw = %v", back)
	}

	// index.md is the catalogue the pattern uses as its first retrieval step.
	index := replyText(t, mustExec(t, db, "WIKI.INDEX"))
	for _, name := range []string{"vector-search", "hnsw", "embeddings", "orphan"} {
		if !strings.Contains(index, "[["+name+"]]") {
			t.Fatalf("index must catalogue %s:\n%s", name, index)
		}
	}
	if !strings.Contains(index, "## search") {
		t.Fatalf("index must group by category:\n%s", index)
	}

	// Lint finds the orphan and the page without a summary.
	lint := replyText(t, mustExec(t, db, "WIKI.LINT"))
	if !strings.Contains(lint, "## Orphan pages (1)") || !strings.Contains(lint, "- orphan") {
		t.Fatalf("lint must report the orphan page:\n%s", lint)
	}
	if !strings.Contains(lint, "## Broken links (0)") {
		t.Fatalf("lint must report broken links, even at zero:\n%s", lint)
	}

	// A link to a missing page is a broken link.
	mustExec(t, db, "WIKI.SET", "dangling", "Refers to [[nowhere]].")
	lint = replyText(t, mustExec(t, db, "WIKI.LINT"))
	if !strings.Contains(lint, "## Broken links (1)") || !strings.Contains(lint, "[[nowhere]] ← dangling") {
		t.Fatalf("lint must report the broken link:\n%s", lint)
	}

	// The log is append-only and its entries are greppable.
	replyInt(t, mustExec(t, db, "WIKI.LOG.ADD", "query", "Why is recall low?"))
	log := replyText(t, mustExec(t, db, "WIKI.LOG", "3"))
	if !strings.Contains(log, "] query | Why is recall low?") {
		t.Fatalf("the log entry must start with a greppable prefix:\n%s", log)
	}
	if strings.Count(log, "## [") < 2 {
		t.Fatalf("the log must return several entries, newest first:\n%s", log)
	}

	// Raw sources are immutable unless forced.
	if _, ok := mustExecRaw(t, db, toBytesArgs("WIKI.SOURCE.ADD", "paper", "the source text")).(*protocol.OkReply); !ok {
		t.Fatal("WIKI.SOURCE.ADD should succeed the first time")
	}
	if _, ok := db.Exec(nil, toBytesArgs("WIKI.SOURCE.ADD", "paper", "revised")).(protocol.ErrorReply); !ok {
		t.Fatal("re-adding a source without FORCE must be refused")
	}
	if got := replyText(t, mustExec(t, db, "WIKI.SOURCE.GET", "paper")); got != "the source text" {
		t.Fatalf("SOURCE.GET = %q", got)
	}
	if list := replyList(t, mustExec(t, db, "WIKI.SOURCE.LIST")); len(list) != 1 || list[0] != "paper" {
		t.Fatalf("SOURCE.LIST = %v", list)
	}

	// Export renders the wiki as committable markdown.
	exported := replyText(t, mustExec(t, db, "WIKI.EXPORT", "PREFIX", "wiki/"))
	if !strings.Contains(exported, "----- wiki/vector-search.md") || !strings.Contains(exported, "category: search") {
		t.Fatalf("WIKI.EXPORT must emit front-mattered files:\n%s", exported)
	}
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// TestWikiQueryFusesMeaningAndWording checks that a page is reachable both by the
// words it uses and by what it means.
func TestWikiQueryFusesMeaningAndWording(t *testing.T) {
	db := makeTestDB()
	defer db.Flush()

	mustExec(t, db, "WIKI.SET", "recall", "## Recall\n\nRecall is traded against build time and the candidate window.")
	mustExec(t, db, "WIKI.SET", "latency", "## Latency\n\nLatency grows with the number of vectors scanned per query.")
	mustExec(t, db, "WIKI.SET", "cooking", "## Cooking\n\nSourdough needs a starter and an overnight proof.")

	answer := replyText(t, mustExec(t, db, "WIKI.QUERY", "recall build time", "K", "1"))
	if !strings.Contains(answer, "recall") || strings.Contains(answer, "Sourdough") {
		t.Fatalf("the page using those exact words must rank first:\n%s", answer)
	}

	full := replyText(t, mustExec(t, db, "WIKI.QUERY", "recall", "K", "1", "FORMAT", "FULL"))
	if !strings.Contains(full, "traded against build time") {
		t.Fatalf("FORMAT FULL must include the page body:\n%s", full)
	}
	if !strings.Contains(full, "links:") && !strings.Contains(full, "page: recall") {
		t.Fatalf("a hit must identify its page:\n%s", full)
	}

	// The scan always returns k neighbours, so MINSCORE is how a caller asks
	// "does the wiki actually cover this?" and gets a usable no.
	if got := replyText(t, mustExec(t, db, "WIKI.QUERY", "nonexistent zebra topic", "MINSCORE", "0.4")); !strings.Contains(got, "No wiki page") {
		t.Fatalf("MINSCORE must turn an uncovered question into a clear miss, got:\n%s", got)
	}
	if ranked := replyText(t, mustExec(t, db, "WIKI.QUERY", "recall")); strings.Index(ranked, "page: recall") > strings.Index(ranked, "page: cooking") {
		t.Fatalf("the matching page must outrank an unrelated one:\n%s", ranked)
	}
}

// TestMemorySession covers recent-window retrieval and meaning-based recall of
// older turns.
func TestMemorySession(t *testing.T) {
	db := makeTestDB()
	defer db.Flush()

	turns := [][2]string{
		{"user", "I am working on the vector search index."},
		{"assistant", "Start with a flat index; it gives exact recall for small sets."},
		{"user", "What about the weather in Lisbon?"},
		{"assistant", "I have no data on that."},
		{"user", "Back to embeddings: which dimension should I use?"},
		{"assistant", "Match the dimension of the model that produced the vectors."},
	}
	for _, turn := range turns {
		replyInt(t, mustExec(t, db, "MEMORY.ADD", "s1", turn[0], turn[1]))
	}

	recent := replyList(t, mustExec(t, db, "MEMORY.GET", "s1", "LIMIT", "2"))
	if len(recent) != 4 {
		t.Fatalf("LIMIT 2 must return two role/content pairs, got %v", recent)
	}
	if recent[0] != "user" || !strings.Contains(recent[1], "Back to embeddings") {
		t.Fatalf("MEMORY.GET must return the newest turns in conversation order, got %v", recent)
	}

	all := replyList(t, mustExec(t, db, "MEMORY.GET", "s1"))
	if len(all) != len(turns)*2 {
		t.Fatalf("MEMORY.GET without LIMIT returned %d entries, want %d", len(all), len(turns)*2)
	}
	if all[0] != "user" || !strings.Contains(all[1], "vector search index") {
		t.Fatalf("MEMORY.GET must start at the oldest turn, got %v", all[:2])
	}

	assistantOnly := replyList(t, mustExec(t, db, "MEMORY.GET", "s1", "LIMIT", "10", "ROLE", "assistant"))
	for i := 0; i < len(assistantOnly); i += 2 {
		if assistantOnly[i] != "assistant" {
			t.Fatalf("ROLE filter leaked %q", assistantOnly[i])
		}
	}

	// The relevant older turn must be found by meaning, not recency.
	found := replyList(t, mustExec(t, db, "MEMORY.SEARCH", "s1", "what dimension do the vectors need", "K", "1"))
	if len(found) != 2 {
		t.Fatalf("MEMORY.SEARCH K 1 must return one role/content pair, got %v", found)
	}
	if !strings.Contains(found[1], "dimension") {
		t.Fatalf("the best match must be the dimension turn, got %q", found[1])
	}
	if strings.Contains(found[1], "Lisbon") {
		t.Fatalf("the weather turn must not outrank the dimension turn, got %q", found[1])
	}

	if sessions := replyList(t, mustExec(t, db, "MEMORY.SESSIONS")); len(sessions) != 1 || sessions[0] != "s1" {
		t.Fatalf("MEMORY.SESSIONS = %v", sessions)
	}
	info := replyList(t, mustExec(t, db, "MEMORY.INFO", "s1"))
	if infoValue(info, "messages") != strconv.Itoa(len(turns)) {
		t.Fatalf("MEMORY.INFO = %v", info)
	}

	if n := replyInt(t, mustExec(t, db, "MEMORY.CLEAR", "s1")); n != int64(len(turns)) {
		t.Fatalf("MEMORY.CLEAR removed %d of %d messages", n, len(turns))
	}
	if sessions := replyList(t, mustExec(t, db, "MEMORY.SESSIONS")); len(sessions) != 0 {
		t.Fatalf("the session must be gone, got %v", sessions)
	}
}

// TestLLMCacheSemanticHit covers the semantic cache: a reworded prompt hits, an
// unrelated prompt misses, and the hit rate is reported.
func TestLLMCacheSemanticHit(t *testing.T) {
	db := makeTestDB()
	defer db.Flush()

	prompt := "how do I tune vector recall?"
	answer := "Raise the candidate window and prefer a graph index for large sets."
	mustExec(t, db, "LLMCACHE.SET", prompt, answer, "TAG", "vectors")

	hit := replyList(t, mustExec(t, db, "LLMCACHE.GET", prompt))
	if hit[0] != "HIT" || hit[1] != answer {
		t.Fatalf("an identical prompt must hit, got %v", hit)
	}
	if sim, _ := strconv.ParseFloat(hit[2], 64); sim < 0.99 {
		t.Fatalf("an identical prompt should score ~1.0, got %s", hit[2])
	}

	// Reworded but the same question.
	reworded := replyList(t, mustExec(t, db, "LLMCACHE.GET", "how do I tune vector recall for the index?"))
	if reworded[0] != "HIT" {
		t.Fatalf("a reworded prompt should hit a semantic cache, got %v", reworded)
	}
	if sim, _ := strconv.ParseFloat(reworded[2], 64); sim < DefaultCacheThreshold {
		t.Fatalf("a reported hit must clear the threshold, got %s", reworded[2])
	}

	// Unrelated prompts must miss rather than return a wrong answer.
	if miss := replyList(t, mustExec(t, db, "LLMCACHE.GET", "how do I bake sourdough bread?")); miss[0] != "MISS" {
		t.Fatalf("an unrelated prompt must miss, got %v", miss)
	}
	// A threshold above the observed similarity also misses.
	if miss := replyList(t, mustExec(t, db, "LLMCACHE.GET", "how do I tune vector recall for the index?", "THRESHOLD", "0.999")); miss[0] != "MISS" {
		t.Fatalf("a strict threshold must miss, got %v", miss)
	}

	// A tag filter isolates cached entries.
	mustExec(t, db, "LLMCACHE.SET", "what is a flat index?", "Exhaustive scan.", "TAG", "vectors")
	if miss := replyList(t, mustExec(t, db, "LLMCACHE.GET", prompt, "TAG", "recipes")); miss[0] != "MISS" {
		t.Fatalf("a tag with no cached entries must miss, got %v", miss)
	}

	stats := replyList(t, mustExec(t, db, "LLMCACHE.STATS"))
	if infoValue(stats, "entries") != "2" {
		t.Fatalf("LLMCACHE.STATS entries = %v", stats)
	}
	hits, _ := strconv.Atoi(infoValue(stats, "hits"))
	if hits < 2 {
		t.Fatalf("LLMCACHE.STATS should count the hits, got %v", stats)
	}
	if rate, _ := strconv.ParseFloat(infoValue(stats, "hit_rate"), 64); rate <= 0 || rate >= 1 {
		t.Fatalf("hit_rate should be a fraction, got %v", stats)
	}

	// Expiry must not leave a phantom hit behind in the index.
	mustExec(t, db, "LLMCACHE.SET", "short lived prompt", "gone soon", "TTL", "1")
	key := llmCacheKey("short lived prompt")
	db.Expire(key, time.Now().Add(-time.Second))
	if reply := replyList(t, mustExec(t, db, "LLMCACHE.GET", "short lived prompt")); reply[0] != "MISS" {
		t.Fatalf("an expired entry must not be served, got %v", reply)
	}
	if idx := search.GetIndex(llmCacheIndex); idx != nil && idx.Doc(key) != nil {
		t.Fatal("an expired entry must be evicted from the index")
	}

	if n := replyInt(t, mustExec(t, db, "LLMCACHE.CLEAR")); n < 2 {
		t.Fatalf("LLMCACHE.CLEAR removed %d entries", n)
	}
	if entries := infoValue(replyList(t, mustExec(t, db, "LLMCACHE.STATS")), "entries"); entries != "0" {
		t.Fatalf("entries after CLEAR = %s", entries)
	}
}

// TestKnowledgeBasePersistsThroughAOF is the guard that makes restart safe: the
// stored commands must rebuild the index without re-embedding, and the index must
// exist in the AOF before the documents that fill it.
func TestKnowledgeBasePersistsThroughAOF(t *testing.T) {
	db := makeTestDB()
	defer db.Flush()

	var aof [][][]byte
	db.addAof = func(line CmdLine) {
		cloned := make([][]byte, len(line))
		for i, arg := range line {
			cloned[i] = append([]byte(nil), arg...)
		}
		aof = append(aof, cloned)
	}

	mustExec(t, db, "RAG.INGEST", "kb", "one", "a document about indexing")

	sawCreate, sawHset := false, false
	for i, line := range aof {
		switch strings.ToLower(string(line[0])) {
		case "ft.create":
			sawCreate = true
		case "hmset":
			if !sawCreate {
				t.Fatalf("document written at AOF position %d before its FT.CREATE", i)
			}
			sawHset = true
		}
	}
	if !sawCreate || !sawHset {
		t.Fatalf("AOF must contain both the index creation and the documents, got %v", aof)
	}

	// Replaying the recorded commands into a fresh database must reproduce a
	// searchable collection without calling the embedder again.
	fresh := makeTestDB()
	defer fresh.Flush()
	// Indexes are process-wide, so the source database's index has to go before
	// replaying into a second one; a real replay starts in a fresh process.
	search.ResetAll()
	for _, line := range aof {
		mustExecRaw(t, fresh, line)
	}
	if got := replyText(t, mustExec(t, fresh, "RAG.QUERY", "kb", "indexing")); !strings.Contains(got, "a document about indexing") {
		t.Fatalf("replay did not rebuild a queryable collection:\n%s", got)
	}
}

// TestKnowledgeBaseSurvivesLostIndex covers the AOF-rewrite path, which discards
// every in-memory index: rewriting regenerates the log from data entities, so a
// knowledge base would otherwise come back with its documents intact and its
// search permanently broken.
func TestKnowledgeBaseSurvivesLostIndex(t *testing.T) {
	db := makeTestDB()
	defer db.Flush()

	mustExec(t, db, "RAG.INGEST", "kb", "one", "a document about index tuning")
	mustExec(t, db, "WIKI.SET", "recall", "## Recall\n\nRecall is traded against build time.")
	mustExec(t, db, "MEMORY.ADD", "s1", "user", "remember the index tuning note")
	mustExec(t, db, "LLMCACHE.SET", "how do I tune the index?", "raise the candidate window")
	mustExec(t, db, "FT.CREATE", "sessions", "ON", "HASH", "PREFIX", "1", "mem:", "SCHEMA",
		"session", "TAG", "role", "TAG", "content", "TEXT")

	// Simulate a rewrite-and-restart: the data survives, every index does not.
	search.ResetAll()

	if got := replyText(t, mustExec(t, db, "RAG.QUERY", "kb", "index tuning")); !strings.Contains(got, "a document about index tuning") {
		t.Fatalf("RAG lost its index and did not rebuild it:\n%s", got)
	}
	if got := replyText(t, mustExec(t, db, "WIKI.QUERY", "recall")); !strings.Contains(got, "page: recall") {
		t.Fatalf("WIKI lost its index and did not rebuild it:\n%s", got)
	}
	if got := replyList(t, mustExec(t, db, "MEMORY.SEARCH", "s1", "index tuning", "K", "1")); len(got) != 2 || !strings.Contains(got[1], "index tuning") {
		t.Fatalf("MEMORY lost its index and did not rebuild it: %v", got)
	}
	if got := replyList(t, mustExec(t, db, "LLMCACHE.GET", "how do I tune the index?")); got[0] != "HIT" || got[1] != "raise the candidate window" {
		t.Fatalf("LLMCACHE lost its index and did not rebuild it: %v", got)
	}
}

// TestHSetVariadic covers the multi-pair form: Redis accepts it, and a hash
// document is how an ON HASH search index gets its fields.
func TestHSetVariadic(t *testing.T) {
	db := makeTestDB()
	defer db.Flush()

	if n := replyInt(t, db.Exec(nil, toBytesArgs("HSET", "h", "a", "1", "b", "2", "c", "3"))); n != 3 {
		t.Fatalf("HSET with three new fields returned %d, want 3", n)
	}
	if got := replyList(t, db.Exec(nil, toBytesArgs("HGETALL", "h"))); len(got) != 6 {
		t.Fatalf("HGETALL = %v", got)
	}

	// The reply counts only newly added fields, not overwritten ones.
	if n := replyInt(t, db.Exec(nil, toBytesArgs("HSET", "h", "a", "9", "d", "4"))); n != 1 {
		t.Fatalf("HSET with one new and one existing field returned %d, want 1", n)
	}
	if got := replyText(t, db.Exec(nil, toBytesArgs("HGET", "h", "a"))); got != "9" {
		t.Fatalf("existing field must be overwritten, got %q", got)
	}

	// A field without a value is a syntax error, not a partial write.
	if _, ok := db.Exec(nil, toBytesArgs("HSET", "h", "a", "1", "b")).(protocol.ErrorReply); !ok {
		t.Fatal("an odd argument count must be rejected")
	}

	// Fields written together must be searchable together.
	mustExec(t, db, "FT.CREATE", "hidx", "ON", "HASH", "PREFIX", "1", "h", "SCHEMA", "a", "TEXT", "b", "TEXT", "c", "TEXT")
	_, keys, _, _ := searchResult(t, mustExec(t, db, "FT.SEARCH", "hidx", "9", "NOCONTENT"), false, true)
	if len(keys) != 1 || keys[0] != "h" {
		t.Fatalf("a field value written by a multi-pair HSET must be indexed, got %v", keys)
	}
	mustExec(t, db, "FT.DROPINDEX", "hidx")
}
