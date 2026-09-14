package search

import (
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// indexedDoc is a document stored in an index: its raw field values and the
// decoded vectors of every VECTOR field it carries.
type indexedDoc struct {
	key     string
	fields  map[string][]string
	vectors map[string][]float32
}

// DocInfo is a search hit: the document key, its score, and its field values.
// Score is a distance for KNN results and a term-frequency relevance for
// full-text results.
type DocInfo struct {
	Key     string
	Score   float64
	Fields  map[string][]string
	Vectors map[string][]float32
}

// InvertedIndex implements an inverted index for full-text search with vector search support.
type InvertedIndex struct {
	mu       sync.RWMutex
	schema   IndexSchema
	inverted map[string]map[string]float64
	docs     map[string]*indexedDoc
}

// NewInvertedIndex creates a new inverted index with the given schema.
func NewInvertedIndex(schema IndexSchema) *InvertedIndex {
	return &InvertedIndex{
		schema:   schema,
		inverted: make(map[string]map[string]float64),
		docs:     make(map[string]*indexedDoc),
	}
}

// Schema returns the index schema.
func (idx *InvertedIndex) Schema() IndexSchema {
	return idx.schema
}

// tokenize splits a text string into lowercase tokens using common punctuation as delimiters.
func (idx *InvertedIndex) tokenize(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), isTokenDelimiter)
}

func isTokenDelimiter(r rune) bool {
	switch r {
	case ' ', '\t', '\n', '\r', ',', '.', '!', '?', ':', ';', '"', '\'', '`',
		'(', ')', '[', ']', '{', '}', '<', '>', '/', '\\', '|', '~',
		'@', '#', '$', '%', '^', '&', '*', '+', '=', '-', '_':
		return true
	}
	return false
}

// splitTags splits a tag field value into individual tags. JSON array fields
// arrive as one value per element; hash fields carry comma-separated tags.
func splitTags(value string) []string {
	parts := strings.Split(value, ",")
	tags := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			tags = append(tags, strings.ToLower(p))
		}
	}
	return tags
}

// IndexDocument indexes a document with the given field values, replacing any
// existing document with the same key. Fields are keyed by schema alias.
func (idx *InvertedIndex) IndexDocument(key string, fields map[string][]string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.indexLocked(key, fields)
}

func (idx *InvertedIndex) indexLocked(key string, fields map[string][]string) {
	idx.removeLocked(key)

	doc := &indexedDoc{
		key:     key,
		fields:  fields,
		vectors: make(map[string][]float32),
	}
	for i := range idx.schema.Fields {
		f := &idx.schema.Fields[i]
		values, ok := fields[f.Name]
		if !ok || len(values) == 0 {
			continue
		}
		switch f.Type {
		case FieldTypeVector:
			vec, err := idx.decodeFieldVector(f, values)
			if err != nil {
				slog.Warn("search: skipping vector field", "key", key, "field", f.Name, "error", err)
				continue
			}
			doc.vectors[f.Name] = vec
		case FieldTypeText:
			for _, v := range values {
				for _, tok := range idx.tokenize(v) {
					idx.addPosting(tok, key)
				}
			}
		case FieldTypeTag:
			for _, v := range values {
				for _, tag := range splitTags(v) {
					idx.addPosting(tag, key)
				}
			}
		}
	}
	idx.docs[key] = doc
}

func (idx *InvertedIndex) decodeFieldVector(f *FieldSchema, values []string) ([]float32, error) {
	vec, err := ParseFloat32Vec(values[0])
	if err != nil {
		return nil, err
	}
	if f.VectorOpts != nil && f.VectorOpts.Dim > 0 && len(vec) != f.VectorOpts.Dim {
		return nil, fmt.Errorf("vector dimension mismatch: index expects %d, got %d", f.VectorOpts.Dim, len(vec))
	}
	return vec, nil
}

func (idx *InvertedIndex) addPosting(term, key string) {
	postings := idx.inverted[term]
	if postings == nil {
		postings = make(map[string]float64)
		idx.inverted[term] = postings
	}
	postings[key]++
}

// RemoveDocument removes a document from the index by its key.
func (idx *InvertedIndex) RemoveDocument(key string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.removeLocked(key)
}

func (idx *InvertedIndex) removeLocked(key string) {
	doc, exists := idx.docs[key]
	if !exists {
		return
	}
	for i := range idx.schema.Fields {
		f := &idx.schema.Fields[i]
		for _, v := range doc.fields[f.Name] {
			var terms []string
			switch f.Type {
			case FieldTypeText:
				terms = idx.tokenize(v)
			case FieldTypeTag:
				terms = splitTags(v)
			default:
				continue
			}
			for _, term := range terms {
				if postings := idx.inverted[term]; postings != nil {
					delete(postings, key)
					if len(postings) == 0 {
						delete(idx.inverted, term)
					}
				}
			}
		}
	}
	delete(idx.docs, key)
}

// docSet is an evaluated query result. all short-circuits the common
// "no filter" case so a match-all query never materialises a key set.
type docSet struct {
	all    bool
	scores map[string]float64
}

// EvalScores evaluates a query's filter expression and returns every matching
// document key with its relevance score. Documents matched only by filter
// clauses (tag, numeric) score 0; text terms contribute their term frequency.
func (idx *InvertedIndex) EvalScores(q *Query) (map[string]float64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if q == nil || q.Root == nil {
		return idx.allScoresLocked(), nil
	}
	set, err := idx.evalLocked(q.Root)
	if err != nil {
		return nil, err
	}
	if set.all {
		return idx.allScoresLocked(), nil
	}
	return set.scores, nil
}

func (idx *InvertedIndex) allScoresLocked() map[string]float64 {
	scores := make(map[string]float64, len(idx.docs))
	for key := range idx.docs {
		scores[key] = 0
	}
	return scores
}

func (idx *InvertedIndex) evalLocked(n *Node) (docSet, error) {
	if n == nil {
		return docSet{all: true}, nil
	}
	switch n.Op {
	case OpAll:
		return docSet{all: true}, nil
	case OpTerm:
		return idx.evalTermLocked(n.Term)
	case OpAnd:
		result := docSet{all: true}
		for _, child := range n.Children {
			next, err := idx.evalLocked(child)
			if err != nil {
				return docSet{}, err
			}
			result = intersectSets(result, next)
		}
		return result, nil
	case OpOr:
		result := docSet{scores: map[string]float64{}}
		for _, child := range n.Children {
			next, err := idx.evalLocked(child)
			if err != nil {
				return docSet{}, err
			}
			result = unionSets(result, next)
		}
		return result, nil
	case OpNot:
		if len(n.Children) != 1 {
			return docSet{}, fmt.Errorf("Syntax error: negation takes one operand")
		}
		child, err := idx.evalLocked(n.Children[0])
		if err != nil {
			return docSet{}, err
		}
		return idx.complementLocked(child), nil
	}
	return docSet{}, fmt.Errorf("internal error: unknown query node")
}

func intersectSets(a, b docSet) docSet {
	if a.all {
		return b
	}
	if b.all {
		return a
	}
	out := make(map[string]float64, len(a.scores))
	for key, score := range a.scores {
		if other, ok := b.scores[key]; ok {
			out[key] = score + other
		}
	}
	return docSet{scores: out}
}

func unionSets(a, b docSet) docSet {
	if a.all || b.all {
		return docSet{all: true}
	}
	out := make(map[string]float64, len(a.scores)+len(b.scores))
	for key, score := range a.scores {
		out[key] = score
	}
	for key, score := range b.scores {
		out[key] += score
	}
	return docSet{scores: out}
}

func (idx *InvertedIndex) complementLocked(excluded docSet) docSet {
	out := make(map[string]float64, len(idx.docs))
	for key := range idx.docs {
		if !excluded.all {
			if _, skip := excluded.scores[key]; skip {
				continue
			}
		}
		out[key] = 0
	}
	return docSet{scores: out}
}

func (idx *InvertedIndex) evalTermLocked(t *Term) (docSet, error) {
	if t == nil {
		return docSet{all: true}, nil
	}
	switch t.Kind {
	case TermTag:
		return idx.evalTagLocked(t)
	case TermNumeric:
		return idx.evalNumericLocked(t)
	default:
		return idx.evalWordLocked(t)
	}
}

func (idx *InvertedIndex) evalWordLocked(t *Term) (docSet, error) {
	if t.Field == "" {
		matches := map[string]float64{}
		for key, tf := range idx.inverted[t.Word] {
			doc := idx.docs[key]
			if doc != nil && idx.wordInTextFieldsLocked(doc, "", t.Word) {
				matches[key] = tf
			}
		}
		return docSet{scores: matches}, nil
	}

	f := idx.schema.FieldByName(t.Field)
	if f == nil {
		return docSet{}, fmt.Errorf("Unknown field '%s'", t.Field)
	}
	switch f.Type {
	case FieldTypeText:
		matches := map[string]float64{}
		for key, tf := range idx.inverted[t.Word] {
			if idx.wordInTextFieldsLocked(idx.docs[key], f.Name, t.Word) {
				matches[key] = tf
			}
		}
		return docSet{scores: matches}, nil
	case FieldTypeTag:
		return idx.evalTagLocked(&Term{Field: t.Field, Tags: []string{t.Word}})
	case FieldTypeNumeric:
		return docSet{}, fmt.Errorf("Syntax error: numeric field '%s' requires a range query", t.Field)
	default:
		return docSet{}, fmt.Errorf("Syntax error: vector field '%s' requires a KNN clause", t.Field)
	}
}

func (idx *InvertedIndex) wordInTextFieldsLocked(doc *indexedDoc, field, word string) bool {
	if doc == nil {
		return false
	}
	check := func(name string) bool {
		for _, v := range doc.fields[name] {
			for _, tok := range idx.tokenize(v) {
				if tok == word {
					return true
				}
			}
		}
		return false
	}
	if field != "" {
		return check(field)
	}
	for i := range idx.schema.Fields {
		if idx.schema.Fields[i].Type == FieldTypeText && check(idx.schema.Fields[i].Name) {
			return true
		}
	}
	return false
}

func (idx *InvertedIndex) evalTagLocked(t *Term) (docSet, error) {
	f := idx.schema.FieldByName(t.Field)
	if f == nil {
		return docSet{}, fmt.Errorf("Unknown field '%s'", t.Field)
	}
	if f.Type != FieldTypeTag {
		return docSet{}, fmt.Errorf("Syntax error: field '%s' is not a TAG field", t.Field)
	}

	matches := map[string]float64{}
	for _, tag := range t.Tags {
		for key := range idx.inverted[tag] {
			doc := idx.docs[key]
			if doc == nil {
				continue
			}
			for _, v := range doc.fields[f.Name] {
				if containsTag(splitTags(v), tag) {
					matches[key] = 1
					break
				}
			}
		}
	}
	return docSet{scores: matches}, nil
}

func containsTag(tags []string, tag string) bool {
	for _, t := range tags {
		if t == tag {
			return true
		}
	}
	return false
}

func (idx *InvertedIndex) evalNumericLocked(t *Term) (docSet, error) {
	f := idx.schema.FieldByName(t.Field)
	if f == nil {
		return docSet{}, fmt.Errorf("Unknown field '%s'", t.Field)
	}
	if f.Type != FieldTypeNumeric {
		return docSet{}, fmt.Errorf("Syntax error: field '%s' is not a NUMERIC field", t.Field)
	}

	matches := map[string]float64{}
	for key, doc := range idx.docs {
		for _, v := range doc.fields[f.Name] {
			num, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
			if err != nil {
				continue
			}
			if t.HasMin && num < t.Min {
				continue
			}
			if t.HasMax && num > t.Max {
				continue
			}
			matches[key] = 0
			break
		}
	}
	return docSet{scores: matches}, nil
}

// TextSearch evaluates the full-text filter of a query and returns the matching
// documents ordered by descending relevance, then by key for a stable order.
func (idx *InvertedIndex) TextSearch(q *Query) ([]*DocInfo, error) {
	scores, err := idx.EvalScores(q)
	if err != nil {
		return nil, err
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make([]*DocInfo, 0, len(scores))
	for key, score := range scores {
		doc := idx.docs[key]
		if doc == nil {
			continue
		}
		result = append(result, doc.info(score))
	}
	sortDocInfos(result, false)
	return result, nil
}

// KNNSearch returns the k documents whose vector for field is closest to vec.
// candidates restricts the scan, implementing the dialect-2 pre-filter; a nil
// candidates map searches every document carrying the field.
func (idx *InvertedIndex) KNNSearch(field string, vec []float32, k int, candidates map[string]float64) ([]*DocInfo, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	f := idx.findVectorField(field)
	if f == nil {
		return nil, fmt.Errorf("Unknown field '%s'", field)
	}
	if f.VectorOpts != nil && f.VectorOpts.Dim > 0 && len(vec) != f.VectorOpts.Dim {
		return nil, fmt.Errorf("Vector dimension mismatch: index expects %d, got %d", f.VectorOpts.Dim, len(vec))
	}
	metric := "COSINE"
	if f.VectorOpts != nil && f.VectorOpts.DistanceMetric != "" {
		metric = f.VectorOpts.DistanceMetric
	}

	type hit struct {
		doc  *indexedDoc
		dist float64
	}
	hits := make([]hit, 0, len(idx.docs))
	for key, doc := range idx.docs {
		if candidates != nil {
			if _, ok := candidates[key]; !ok {
				continue
			}
		}
		stored, ok := doc.vectors[f.Name]
		if !ok {
			continue
		}
		hits = append(hits, hit{doc: doc, dist: vectorDistance(metric, stored, vec)})
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].dist != hits[j].dist {
			return hits[i].dist < hits[j].dist
		}
		return hits[i].doc.key < hits[j].doc.key
	})
	if k > 0 && k < len(hits) {
		hits = hits[:k]
	}

	result := make([]*DocInfo, len(hits))
	for i, h := range hits {
		result[i] = h.doc.info(h.dist)
	}
	return result, nil
}

func (d *indexedDoc) info(score float64) *DocInfo {
	return &DocInfo{Key: d.key, Score: score, Fields: d.fields, Vectors: d.vectors}
}

func sortDocInfos(docs []*DocInfo, ascending bool) {
	sort.Slice(docs, func(i, j int) bool {
		if docs[i].Score != docs[j].Score {
			if ascending {
				return docs[i].Score < docs[j].Score
			}
			return docs[i].Score > docs[j].Score
		}
		return docs[i].Key < docs[j].Key
	})
}

// findVectorField finds and returns the field schema for a vector field by name.
func (idx *InvertedIndex) findVectorField(name string) *FieldSchema {
	f := idx.schema.FieldByName(name)
	if f == nil || f.Type != FieldTypeVector {
		return nil
	}
	return f
}

// DocCount returns the number of documents in the index.
func (idx *InvertedIndex) DocCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.docs)
}

// Keys returns every indexed document key in sorted order.
func (idx *InvertedIndex) Keys() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	keys := make([]string, 0, len(idx.docs))
	for key := range idx.docs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Doc returns a copy of the stored information for a key, or nil.
func (idx *InvertedIndex) Doc(key string) *DocInfo {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	doc, ok := idx.docs[key]
	if !ok {
		return nil
	}
	return doc.info(0)
}

// Metric returns the distance metric of a vector field, defaulting to COSINE.
func (idx *InvertedIndex) Metric(field string) string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	f := idx.findVectorField(field)
	if f == nil || f.VectorOpts == nil || f.VectorOpts.DistanceMetric == "" {
		return "COSINE"
	}
	return f.VectorOpts.DistanceMetric
}

// TermCount returns the number of unique terms in the inverted index.
func (idx *InvertedIndex) TermCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.inverted)
}

// HasPrefix reports whether the index claims the given key.
func (idx *InvertedIndex) HasPrefix(key string) bool {
	if len(idx.schema.Prefixes) == 0 {
		return true
	}
	for _, prefix := range idx.schema.Prefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

var (
	globalMu      sync.RWMutex
	globalIndexes = make(map[string]*InvertedIndex)
)

// RegisterIndex registers a new search index globally by schema.
func RegisterIndex(schema IndexSchema) error {
	globalMu.Lock()
	defer globalMu.Unlock()

	name := strings.ToLower(schema.Name)
	if _, exists := globalIndexes[name]; exists {
		return fmt.Errorf("index already exists")
	}
	globalIndexes[name] = NewInvertedIndex(schema)
	return nil
}

// EnsureIndex registers the schema when no index of that name exists yet and
// returns the live index plus whether it was created. Commands that create
// indexes implicitly use this, then persist the returned command line to the AOF
// so replay rebuilds the same index.
//
// An existing index whose vector fields disagree with the schema on dimension is
// an error, not a silent mix of incompatible embeddings.
func EnsureIndex(schema IndexSchema) (*InvertedIndex, bool, error) {
	globalMu.Lock()
	defer globalMu.Unlock()

	name := strings.ToLower(schema.Name)
	if existing, ok := globalIndexes[name]; ok {
		if err := existing.checkCompatible(schema); err != nil {
			return nil, false, err
		}
		return existing, false, nil
	}
	idx := NewInvertedIndex(schema)
	globalIndexes[name] = idx
	return idx, true, nil
}

// checkCompatible reports schema drift that would corrupt search results.
func (idx *InvertedIndex) checkCompatible(schema IndexSchema) error {
	if schema.KeyType != idx.schema.KeyType {
		return fmt.Errorf("index %s: key type mismatch, index holds %s but %s was requested",
			idx.schema.Name, idx.schema.KeyType, schema.KeyType)
	}
	for _, want := range schema.Fields {
		if want.Type != FieldTypeVector {
			continue
		}
		have := idx.schema.FieldByName(want.Name)
		if have == nil || have.VectorOpts == nil {
			continue
		}
		if have.VectorOpts.Dim != want.VectorOpts.Dim {
			return fmt.Errorf("index %s: vector dimension mismatch for field %s, index holds %d but %d was requested",
				idx.schema.Name, want.Name, have.VectorOpts.Dim, want.VectorOpts.Dim)
		}
	}
	return nil
}

// DropIndex removes a registered search index by name. Returns false if the index did not exist.
func DropIndex(name string) bool {
	globalMu.Lock()
	defer globalMu.Unlock()
	name = strings.ToLower(name)
	if _, exists := globalIndexes[name]; !exists {
		return false
	}
	delete(globalIndexes, name)
	return true
}

// GetIndex retrieves a registered search index by name.
func GetIndex(name string) *InvertedIndex {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalIndexes[strings.ToLower(name)]
}

// ResetAll drops every index and every indexed document.
//
// ponytail: the registry is process-wide rather than per-database, so trimming
// one database resets indexes globally. Without this, FLUSHDB would leave
// phantom hits pointing at documents that no longer exist. Make the registry
// per-database if godis ever needs independent search state per DB.
func ResetAll() {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalIndexes = make(map[string]*InvertedIndex)
}

// ListIndexes returns the names of all registered search indexes.
func ListIndexes() []string {
	globalMu.RLock()
	defer globalMu.RUnlock()
	names := make([]string, 0, len(globalIndexes))
	for name := range globalIndexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// IndexDocByPrefix indexes a HASH document into every matching HASH index.
func IndexDocByPrefix(key string, fields map[string]string) {
	values := make(map[string][]string, len(fields))
	for name, v := range fields {
		values[name] = []string{v}
	}
	for _, idx := range matchingIndexes(key, KeyTypeHash) {
		idx.IndexDocument(key, values)
	}
}

// IndexJSONByPrefix indexes a JSON document into every matching JSON index.
func IndexJSONByPrefix(key string, doc any) {
	for _, idx := range matchingIndexes(key, KeyTypeJSON) {
		idx.IndexDocument(key, idx.schema.ExtractFields(doc))
	}
}

// RemoveDocByPrefix removes a document from all matching indexes by its key prefix.
func RemoveDocByPrefix(key string) {
	for _, idx := range matchingIndexes(key, -1) {
		idx.RemoveDocument(key)
	}
}

func matchingIndexes(key string, keyType KeyType) []*InvertedIndex {
	globalMu.RLock()
	defer globalMu.RUnlock()

	var matched []*InvertedIndex
	for _, idx := range globalIndexes {
		if keyType >= 0 && idx.schema.KeyType != keyType {
			continue
		}
		if idx.HasPrefix(key) {
			matched = append(matched, idx)
		}
	}
	return matched
}
