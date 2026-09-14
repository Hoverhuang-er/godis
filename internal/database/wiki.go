package database

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Hoverhuang-er/godis/internal/datastruct/search"
	"github.com/Hoverhuang-er/godis/internal/interface/redis"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

// WIKI.* implements the LLM-wiki pattern: a set of interlinked markdown pages an
// agent maintains, layered between immutable raw sources and the questions asked
// of them.
//
// The server does the bookkeeping that no one wants to do by hand — link graph,
// backlinks, orphan and broken-link detection, the index and the log — so the
// agent only has to write prose. Everything lives in ordinary hashes, which means
// the whole wiki is dumpable, replicable, and exportable as files you can commit.
//
// Keys:
//
//	wiki:page:{name}   one page: title, content, summary, category, tags, links, updated, embedding
//	wiki:raw:{name}    an immutable source document
//	wiki:log:{seq}     one log entry, newest seq first
//	wiki:logseq        the log sequence counter

const (
	wikiPagePrefix  = "wiki:page:"
	wikiRawPrefix   = "wiki:raw:"
	wikiLogPrefix   = "wiki:log:"
	wikiLogSeqKey   = "wiki:logseq"
	wikiIndexPrefix = "wiki:index"
	// wikiEmbedBudget bounds how much page text is embedded. A page's identity
	// is in its title and opening, and embedding the whole body of a long page
	// costs latency without improving which page is retrieved.
	wikiEmbedBudget = 2000
	wikiLogMaxSeq   = 1000
)

// wikiSchema indexes pages by title and body text plus an embedding, so a query
// reaches a page by wording and by meaning.
func wikiSchema(dim int) search.IndexSchema {
	return search.IndexSchema{
		Name:     wikiIndexPrefix,
		KeyType:  search.KeyTypeHash,
		Prefixes: []string{wikiPagePrefix},
		Fields: []search.FieldSchema{
			textField("title"),
			textField("content"),
			textField("summary"),
			tagField("category"),
			tagField("tags"),
			tagField("links"),
			numericField("updated"),
			vectorSchemaField("embedding", dim, "COSINE"),
		},
	}
}

func wikiPageKey(name string) string { return wikiPagePrefix + normalizePageName(name) }

// normalizePageName makes page names case- and separator-insensitive so that
// [[Vector Search]], [[vector-search]] and wiki:page:vector-search all resolve
// to the same page.
func normalizePageName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.ReplaceAll(name, " ", "-")
	name = strings.ReplaceAll(name, "_", "-")
	return strings.Trim(name, "-")
}

var wikiLinkPattern = regexp.MustCompile(`\[\[([^\[\]|]+)(?:\|[^\[\]]*)?\]\]`)

// extractLinks returns the distinct page names a body links to.
func extractLinks(content string) []string {
	seen := map[string]bool{}
	var links []string
	for _, m := range wikiLinkPattern.FindAllStringSubmatch(content, -1) {
		name := normalizePageName(m[1])
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		links = append(links, name)
	}
	sort.Strings(links)
	return links
}

// execWikiSet writes a page and refreshes its links.
//
// WIKI.SET page content [TITLE t] [SUMMARY s] [CATEGORY c] [TAG t ...]
func execWikiSet(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 2, "WIKI.SET"); errReply != nil {
		return errReply
	}
	name, content := string(args[0]), string(args[1])
	title := name
	var summary, category string
	var tags []string

	for i := 2; i < len(args); {
		switch strings.ToUpper(string(args[i])) {
		case "TITLE":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR TITLE requires a value")
			}
			title, i = string(args[i+1]), i+2
		case "SUMMARY":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR SUMMARY requires a value")
			}
			summary, i = string(args[i+1]), i+2
		case "CATEGORY":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR CATEGORY requires a value")
			}
			category, i = string(args[i+1]), i+2
		case "TAG", "TAGS":
			var values []string
			values, i = collectValues(args, i+1)
			tags = append(tags, values...)
		default:
			return protocol.MakeErrReply("ERR unknown argument `" + string(args[i]) + "`")
		}
	}

	if summary == "" {
		summary = deriveSummary(content)
	}
	links := extractLinks(content)
	vec, errReply := embedTexts([]string{embedText(title, summary, content)})
	if errReply != nil {
		return errReply
	}

	wikiLock.Lock()
	defer wikiLock.Unlock()

	fields := []hash{
		{"title", title},
		{"content", content},
		{"summary", summary},
		{"category", strings.ToLower(strings.TrimSpace(category))},
		{"tags", joinTags(tags)},
		{"links", joinTags(links)},
		{"updated", strconv.FormatInt(time.Now().Unix(), 10)},
		{"embedding", encodeVector(vec[0])},
	}
	return replyOKOrErr(storeDocument(db, wikiPageKey(name), fields, wikiSchema(len(vec[0]))))
}

// embedText builds the text a page is represented by: title and summary carry the
// page's identity, the body adds detail.
func embedText(title, summary, content string) string {
	head := strings.TrimSpace(title + "\n" + summary)
	if len(head) >= wikiEmbedBudget {
		return head[:wikiEmbedBudget]
	}
	body := content
	if remaining := wikiEmbedBudget - len(head); len(body) > remaining {
		body = body[:remaining]
	}
	return head + "\n" + body
}

// deriveSummary picks the one-line description for a page when the caller did not
// supply one. Headings, list items and code fences are structural, not
// descriptive, so the first line of prose is the useful answer.
func deriveSummary(content string) string {
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "":
			continue
		case strings.HasPrefix(trimmed, "#"):
			continue
		case strings.HasPrefix(trimmed, "-"), strings.HasPrefix(trimmed, "*"),
			strings.HasPrefix(trimmed, ">"), strings.HasPrefix(trimmed, "```"),
			strings.HasPrefix(trimmed, "|"):
			continue
		}
		return truncateRunes(stripInlineMarkdown(trimmed), 160)
	}
	return ""
}

// stripInlineMarkdown removes the emphasis and link syntax that would read as
// noise in the index catalogue.
func stripInlineMarkdown(line string) string {
	line = wikiLinkPattern.ReplaceAllString(line, "$1")
	replacer := strings.NewReplacer("**", "", "__", "", "`", "", "*", "", "_", "")
	return strings.TrimSpace(replacer.Replace(line))
}

func truncateRunes(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return strings.TrimSpace(string(runes[:n])) + "…"
}

// execWikiGet returns a page body.
// WIKI.GET page [CONTENT|FIELD field]
func execWikiGet(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 1, "WIKI.GET"); errReply != nil {
		return errReply
	}
	key := wikiPageKey(string(args[0]))
	if len(args) >= 3 && strings.EqualFold(string(args[1]), "FIELD") {
		value, exists := hashField(db, key, strings.ToLower(string(args[2])))
		return makeBulkOrNil(value, exists)
	}
	value, exists := hashField(db, key, "content")
	return makeBulkOrNil(value, exists)
}

// execWikiDel removes a page. Raw sources and the log are left alone.
// WIKI.DEL page
func execWikiDel(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 1, "WIKI.DEL"); errReply != nil {
		return errReply
	}
	wikiLock.Lock()
	defer wikiLock.Unlock()

	if !deleteDocument(db, wikiPageKey(string(args[0]))) {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(1)
}

// execWikiList lists page names, optionally narrowed to a category.
// WIKI.LIST [CATEGORY c]
func execWikiList(db *DB, args [][]byte) redis.Reply {
	category := ""
	if len(args) >= 2 && strings.EqualFold(string(args[0]), "CATEGORY") {
		category = normalizePageName(string(args[1]))
	}

	idx := indexFor(db, wikiSchema(embedDim()))
	if idx == nil {
		return protocol.MakeMultiBulkReply(nil)
	}
	var names []string
	for _, key := range idx.Keys() {
		if category != "" {
			doc := idx.Doc(key)
			if doc == nil || !containsValue(doc.Fields["category"], category) {
				continue
			}
		}
		names = append(names, strings.TrimPrefix(key, wikiPagePrefix))
	}
	resp := make([][]byte, len(names))
	for i, n := range names {
		resp[i] = []byte(n)
	}
	return protocol.MakeMultiBulkReply(resp)
}

func containsValue(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// execWikiLinks returns the pages a page links to.
// WIKI.LINKS page
func execWikiLinks(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 1, "WIKI.LINKS"); errReply != nil {
		return errReply
	}
	raw, _ := hashField(db, wikiPageKey(string(args[0])), "links")
	return makeStringListReply(splitCSV(raw))
}

// execWikiBacklinks returns the pages that link to a page.
// WIKI.BACKLINKS page
func execWikiBacklinks(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 1, "WIKI.BACKLINKS"); errReply != nil {
		return errReply
	}
	idx := indexFor(db, wikiSchema(embedDim()))
	if idx == nil {
		return protocol.MakeMultiBulkReply(nil)
	}
	candidates, err := idx.EvalScores(search.TagQuery("links", []string{normalizePageName(string(args[0]))}))
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	names := make([]string, 0, len(candidates))
	for key := range candidates {
		names = append(names, strings.TrimPrefix(key, wikiPagePrefix))
	}
	sort.Strings(names)
	return makeStringListReply(names)
}

func splitCSV(raw string) []string {
	if raw == "" {
		return nil
	}
	out := make([]string, 0)
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func makeStringListReply(values []string) redis.Reply {
	resp := make([][]byte, len(values))
	for i, v := range values {
		resp[i] = []byte(v)
	}
	return protocol.MakeMultiBulkReply(resp)
}

// execWikiIndex renders index.md: the content catalogue the pattern uses as its
// first retrieval step at moderate scale.
// WIKI.INDEX
func execWikiIndex(db *DB, args [][]byte) redis.Reply {
	idx := indexFor(db, wikiSchema(embedDim()))
	if idx == nil {
		return protocol.MakeBulkReply([]byte("# Wiki index\n\n_(empty)_\n"))
	}

	type entry struct{ name, title, summary, category string }
	byCategory := map[string][]entry{}
	for _, key := range idx.Keys() {
		doc := idx.Doc(key)
		if doc == nil {
			continue
		}
		e := entry{
			name:     strings.TrimPrefix(key, wikiPagePrefix),
			title:    firstOr(doc.Fields["title"], strings.TrimPrefix(key, wikiPagePrefix)),
			summary:  firstOr(doc.Fields["summary"], ""),
			category: firstOr(doc.Fields["category"], "uncategorised"),
		}
		byCategory[e.category] = append(byCategory[e.category], e)
	}

	categories := make([]string, 0, len(byCategory))
	for c := range byCategory {
		categories = append(categories, c)
	}
	sort.Strings(categories)

	var b strings.Builder
	b.WriteString("# Wiki index\n\n")
	b.WriteString(strconv.Itoa(len(idx.Keys())) + " " + plural(len(idx.Keys()), "page", "pages") +
		" in " + strconv.Itoa(len(categories)) + " " + plural(len(categories), "category", "categories") + ".\n")
	for _, c := range categories {
		entries := byCategory[c]
		sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })
		b.WriteString("\n## " + c + "\n\n")
		for _, e := range entries {
			b.WriteString("- [[" + e.name + "]] — " + e.summary + "\n")
		}
	}
	return protocol.MakeBulkReply([]byte(b.String()))
}

// execWikiLog returns the most recent log entries, newest first.
// WIKI.LOG [n]
func execWikiLog(db *DB, args [][]byte) redis.Reply {
	limit := 20
	if len(args) >= 1 {
		v, errReply := parseIntArg(args[0], "count")
		if errReply != nil {
			return errReply
		}
		limit = v
	}
	seq := wikiLogSeq(db)
	if limit > seq {
		limit = seq
	}
	var b strings.Builder
	for i := 0; i < limit; i++ {
		raw, _ := hashField(db, wikiLogPrefix+strconv.Itoa(seq-i), "entry")
		b.WriteString(raw)
	}
	if b.Len() == 0 {
		return protocol.MakeBulkReply([]byte("_(no log entries)_\n"))
	}
	return protocol.MakeBulkReply([]byte(b.String()))
}

// execWikiLogAdd appends a log entry.
//
// WIKI.LOG.ADD kind title [BODY body]
//
// The entry starts with "## [YYYY-MM-DD] kind | title" so the exported log stays
// greppable: grep "^## \[" log.md | tail -5.
func execWikiLogAdd(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 2, "WIKI.LOG.ADD"); errReply != nil {
		return errReply
	}
	kind, title := string(args[0]), string(args[1])
	body := ""
	if len(args) >= 4 && strings.EqualFold(string(args[2]), "BODY") {
		body = string(args[3])
	}

	wikiLock.Lock()
	defer wikiLock.Unlock()
	return protocol.MakeIntReply(appendWikiLog(db, kind, title, body))
}

// appendWikiLog records one entry; the caller holds wikiLock.
func appendWikiLog(db *DB, kind, title, body string) int64 {
	seq := wikiLogSeq(db) + 1
	entry := "## [" + time.Now().Format("2006-01-02") + "] " + kind + " | " + title + "\n\n"
	if body != "" {
		entry += strings.TrimSpace(body) + "\n"
	}
	entry += "\n"

	if errReply := putHashFields(db, wikiLogPrefix+strconv.Itoa(seq), []hash{{"entry", entry}}); errReply != nil {
		return 0
	}
	if errReply := putHashFields(db, wikiLogSeqKey, []hash{{"seq", strconv.Itoa(seq)}}); errReply != nil {
		return 0
	}
	// Trim the tail so the log does not grow without bound.
	if seq > wikiLogMaxSeq {
		deleteDocument(db, wikiLogPrefix+strconv.Itoa(seq-wikiLogMaxSeq))
	}
	return int64(seq)
}

func wikiLogSeq(db *DB) int {
	raw, _ := hashField(db, wikiLogSeqKey, "seq")
	seq, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return seq
}

// execWikiSourceAdd stores an immutable raw source.
//
// WIKI.SOURCE.ADD name content [FORCE]
func execWikiSourceAdd(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 2, "WIKI.SOURCE.ADD"); errReply != nil {
		return errReply
	}
	name := normalizePageName(string(args[0]))
	key := wikiRawPrefix + name
	force := len(args) >= 3 && strings.EqualFold(string(args[2]), "FORCE")

	wikiLock.Lock()
	defer wikiLock.Unlock()

	if _, exists := db.GetEntity(key); exists && !force {
		return protocol.MakeErrReply("ERR source '" + name + "' already exists; sources are immutable, pass FORCE to replace")
	}
	fields := []hash{
		{"content", string(args[1])},
		{"updated", strconv.FormatInt(time.Now().Unix(), 10)},
	}
	return replyOKOrErr(putHashFields(db, key, fields))
}

// execWikiSourceGet returns a stored raw source.
// WIKI.SOURCE.GET name
func execWikiSourceGet(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 1, "WIKI.SOURCE.GET"); errReply != nil {
		return errReply
	}
	value, exists := hashField(db, wikiRawPrefix+normalizePageName(string(args[0])), "content")
	return makeBulkOrNil(value, exists)
}

// execWikiSourceList lists stored raw sources.
// WIKI.SOURCE.LIST
func execWikiSourceList(db *DB, args [][]byte) redis.Reply {
	var names []string
	for _, key := range prefixKeys(db, wikiRawPrefix) {
		names = append(names, strings.TrimPrefix(key, wikiRawPrefix))
	}
	return makeStringListReply(names)
}

// execWikiQuery answers a question from the wiki, by wording and by meaning.
//
// WIKI.QUERY text [K n] [MINSCORE x] [CATEGORY c] [FORMAT CONTEXT|FULL]
//
// Whole pages are returned rather than fragments: a wiki page is already the
// compiled synthesis, so half of one is less useful than the whole thing.
func execWikiQuery(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 1, "WIKI.QUERY"); errReply != nil {
		return errReply
	}
	question := string(args[0])
	k, full, category := 3, false, ""
	minScore := 0.0

	for i := 1; i < len(args); {
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
		case "CATEGORY":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR CATEGORY requires a value")
			}
			category, i = normalizePageName(string(args[i+1])), i+2
		case "FORMAT":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR FORMAT requires a value")
			}
			switch strings.ToUpper(string(args[i+1])) {
			case "CONTEXT":
				full = false
			case "FULL":
				full = true
			default:
				return protocol.MakeErrReply("ERR FORMAT must be CONTEXT or FULL")
			}
			i += 2
		default:
			return protocol.MakeErrReply("ERR unknown argument `" + string(args[i]) + "`")
		}
	}

	wikiLock.Lock()
	defer wikiLock.Unlock()

	idx := indexFor(db, wikiSchema(embedDim()))
	if idx == nil {
		return protocol.MakeBulkReply([]byte("The wiki is empty.\n"))
	}

	vecs, errReply := embedTexts([]string{question})
	if errReply != nil {
		return errReply
	}

	var candidates map[string]float64
	if category != "" {
		candidates, errReply = filterCandidates(idx, "category", []string{category})
		if errReply != nil {
			return errReply
		}
	}

	// Fuse wording and meaning: a page that matches the exact term and a page
	// that merely means the same thing are both useful, and RRF keeps either
	// from dominating.
	// A question is not a RediSearch expression, so it is matched as any-of-words
	// rather than parsed as syntax, which would make every word mandatory.
	textQuery := search.TermsQuery(question)
	textHits, err := idx.TextSearch(textQuery)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	if candidates != nil {
		textHits = search.Restrict(textHits, candidates)
	}
	vectorHits, err := idx.KNNSearch("embedding", vecs[0], k, candidates)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	// The scan always returns k neighbours, however far away. MINSCORE is how a
	// caller turns that into "the wiki does not cover this", which is a useful
	// answer for an agent: it means write a page, not read three irrelevant ones.
	vectorHits = filterBySimilarity(idx, vectorHits, minScore)

	fused := search.FuseRRF(textHits, vectorHits, k)
	if len(fused) == 0 {
		return protocol.MakeBulkReply([]byte("No wiki page matched the question.\n"))
	}
	return protocol.MakeBulkReply([]byte(renderWikiAnswer(question, idx, fused, full)))
}

// filterBySimilarity drops vector hits below a similarity floor.
func filterBySimilarity(idx *search.InvertedIndex, hits []*search.DocInfo, minScore float64) []*search.DocInfo {
	if minScore <= 0 {
		return hits
	}
	kept := make([]*search.DocInfo, 0, len(hits))
	for _, hit := range hits {
		if similarity(idx, "embedding", hit.Score) >= minScore {
			kept = append(kept, hit)
		}
	}
	return kept
}

// plural picks the singular or plural noun for a count.
func plural(n int, singular, pluralForm string) string {
	if n == 1 {
		return singular
	}
	return pluralForm
}

func linkLabel(links []string) string {
	if len(links) == 0 {
		return ""
	}
	return "links: " + strings.Join(links, ", ")
}

func renderWikiAnswer(question string, idx *search.InvertedIndex, hits []*search.DocInfo, full bool) string {
	var b strings.Builder
	b.WriteString("# Wiki answer context\n\n")
	b.WriteString("Question: " + question + "\n\n")
	for i, hit := range hits {
		doc := idx.Doc(hit.Key)
		if doc == nil {
			continue
		}
		name := strings.TrimPrefix(hit.Key, wikiPagePrefix)
		title := firstOr(doc.Fields["title"], name)
		body := firstOr(doc.Fields["summary"], "")
		if full {
			body = firstOr(doc.Fields["content"], body)
		}
		links := splitCSV(firstOr(doc.Fields["links"], ""))
		meta := metaLine(
			"page: "+name,
			fmt.Sprintf("relevance: %.4f", hit.Score),
			linkLabel(links),
		)
		b.WriteString(formatDoc(i+1, title, body, meta))
		b.WriteString("\n")
	}
	return b.String()
}

// execWikiExport renders every page as markdown files, so the wiki can be kept
// in git, diffed, and read outside godis.
// WIKI.EXPORT [PREFIX p]
func execWikiExport(db *DB, args [][]byte) redis.Reply {
	prefix := ""
	if len(args) >= 2 && strings.EqualFold(string(args[0]), "PREFIX") {
		prefix = string(args[1])
	}
	idx := indexFor(db, wikiSchema(embedDim()))
	if idx == nil {
		return protocol.MakeBulkReply([]byte{})
	}

	var b strings.Builder
	for _, key := range idx.Keys() {
		doc := idx.Doc(key)
		if doc == nil {
			continue
		}
		name := strings.TrimPrefix(key, wikiPagePrefix)
		b.WriteString("----- " + prefix + name + ".md\n")
		b.WriteString("---\n")
		b.WriteString("title: " + firstOr(doc.Fields["title"], name) + "\n")
		if c := firstOr(doc.Fields["category"], ""); c != "" {
			b.WriteString("category: " + c + "\n")
		}
		if t := firstOr(doc.Fields["tags"], ""); t != "" {
			b.WriteString("tags: " + t + "\n")
		}
		b.WriteString("updated: " + firstOr(doc.Fields["updated"], "") + "\n")
		b.WriteString("---\n\n")
		b.WriteString(firstOr(doc.Fields["content"], ""))
		b.WriteString("\n\n")
	}
	return protocol.MakeBulkReply([]byte(b.String()))
}

// execWikiIngest registers a raw source, resolves the pages it touches, records
// a log entry, and hands back the maintenance checklist.
//
// WIKI.INGEST name content [TITLE t]
//
// Ingest is a multi-page edit, so the writing stays with the agent. What the
// server can do is the part that gets forgotten: file the immutable source,
// surface the pages already containing related terms, and append the log entry.
func execWikiIngest(db *DB, args [][]byte) redis.Reply {
	if errReply := requireArgs(args, 2, "WIKI.INGEST"); errReply != nil {
		return errReply
	}
	name, content := normalizePageName(string(args[0])), string(args[1])
	title := name
	if len(args) >= 4 && strings.EqualFold(string(args[2]), "TITLE") {
		title = string(args[3])
	}

	idx := indexFor(db, wikiSchema(embedDim()))
	var related []string
	if idx != nil {
		// Surface the pages that already mention this source's vocabulary, which
		// is the set an ingest most likely has to revise.
		if hits, err := idx.TextSearch(search.TermsQuery(content)); err == nil {
			for _, hit := range hits {
				related = append(related, strings.TrimPrefix(hit.Key, wikiPagePrefix))
				if len(related) >= 15 {
					break
				}
			}
		}
	}

	wikiLock.Lock()
	defer wikiLock.Unlock()

	if _, exists := db.GetEntity(wikiRawPrefix + name); exists {
		return protocol.MakeErrReply("ERR source '" + name + "' already exists; sources are immutable, pass FORCE via WIKI.SOURCE.ADD to replace")
	}
	if errReply := putHashFields(db, wikiRawPrefix+name, []hash{
		{"content", content},
		{"updated", strconv.FormatInt(time.Now().Unix(), 10)},
	}); errReply != nil {
		return errReply
	}
	if seq := appendWikiLog(db, "ingest", title, "source: `"+name+"`"); seq == 0 {
		return protocol.MakeErrReply("ERR failed to append the log entry")
	}

	var b strings.Builder
	b.WriteString("# Ingest checklist for `" + name + "`\n\n")
	b.WriteString("Raw source stored. The log entry is written.\n\n")
	if len(related) == 0 {
		b.WriteString("No existing page mentions this source's vocabulary — expect to create pages rather than revise them.\n")
	} else {
		b.WriteString("Pages already using related terms; review each and update or contradict it:\n\n")
		for _, page := range related {
			b.WriteString("- [[" + page + "]]\n")
		}
	}
	b.WriteString("\nWhen the edits are done, write a summary page and re-run `WIKI.INDEX` to refresh the catalogue.\n")
	return protocol.MakeBulkReply([]byte(b.String()))
}

// execWikiLint runs the deterministic health checks and logs the pass.
// WIKI.LINT
func execWikiLint(db *DB, args [][]byte) redis.Reply {
	idx := indexFor(db, wikiSchema(embedDim()))
	if idx == nil {
		return protocol.MakeBulkReply([]byte("# Wiki lint\n\nNo pages.\n"))
	}

	pages := map[string]bool{}
	var ordered []string
	for _, key := range idx.Keys() {
		name := strings.TrimPrefix(key, wikiPagePrefix)
		pages[name] = true
		ordered = append(ordered, name)
	}

	linked := map[string]bool{}
	var orphans, broken, missingSummary []string
	var duplicates []string
	seenTitles := map[string]string{}

	for _, name := range ordered {
		doc := idx.Doc(wikiPagePrefix + name)
		if doc == nil {
			continue
		}
		links := splitCSV(firstOr(doc.Fields["links"], ""))
		for _, link := range links {
			linked[link] = true
			if !pages[link] {
				broken = append(broken, "[["+link+"]] ← "+name)
			}
		}
		if firstOr(doc.Fields["summary"], "") == "" {
			missingSummary = append(missingSummary, name)
		}
		title := normalizePageName(firstOr(doc.Fields["title"], name))
		if prev, ok := seenTitles[title]; ok {
			duplicates = append(duplicates, prev+" / "+name)
		} else {
			seenTitles[title] = name
		}
	}
	for _, name := range ordered {
		if !linked[name] {
			orphans = append(orphans, name)
		}
	}

	sort.Strings(orphans)
	sort.Strings(broken)
	sort.Strings(missingSummary)
	sort.Strings(duplicates)

	var b strings.Builder
	b.WriteString("# Wiki lint\n\n")
	b.WriteString(strconv.Itoa(len(ordered)) + " pages checked.\n")

	writeSection := func(title, hint string, items []string) {
		b.WriteString("\n## " + title + " (" + strconv.Itoa(len(items)) + ")\n\n")
		if len(items) == 0 {
			b.WriteString("None.\n")
			return
		}
		b.WriteString(hint + "\n\n")
		for _, item := range items {
			b.WriteString("- " + item + "\n")
		}
	}
	writeSection("Orphan pages", "No page links to these; either link them in or merge them.", orphans)
	writeSection("Broken links", "These links point at pages that do not exist.", broken)
	writeSection("Missing summaries", "Add a SUMMARY so the index catalogue stays readable.", missingSummary)
	writeSection("Duplicated titles", "Two pages share a title; consider merging.", duplicates)

	issues := len(orphans) + len(broken) + len(missingSummary) + len(duplicates)
	report := b.String()
	wikiLock.Lock()
	appendWikiLog(db, "lint",
		strconv.Itoa(len(ordered))+" pages, "+strconv.Itoa(issues)+" issues", "")
	wikiLock.Unlock()
	return protocol.MakeBulkReply([]byte(report))
}

func init() {
	registerCommand("WIKI.SET", execWikiSet, noPrepare, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("WIKI.GET", execWikiGet, noPrepare, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("WIKI.DEL", execWikiDel, noPrepare, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("WIKI.LIST", execWikiList, noPrepare, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("WIKI.LINKS", execWikiLinks, noPrepare, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("WIKI.BACKLINKS", execWikiBacklinks, noPrepare, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("WIKI.INDEX", execWikiIndex, noPrepare, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("WIKI.LOG", execWikiLog, noPrepare, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("WIKI.LOG.ADD", execWikiLogAdd, noPrepare, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("WIKI.SOURCE.ADD", execWikiSourceAdd, noPrepare, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("WIKI.SOURCE.GET", execWikiSourceGet, noPrepare, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("WIKI.SOURCE.LIST", execWikiSourceList, noPrepare, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("WIKI.QUERY", execWikiQuery, noPrepare, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("WIKI.INGEST", execWikiIngest, noPrepare, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("WIKI.LINT", execWikiLint, noPrepare, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("WIKI.EXPORT", execWikiExport, noPrepare, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
