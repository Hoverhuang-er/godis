package search

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// FieldType represents the type of a field in a search index schema.
type FieldType int

const (
	FieldTypeText FieldType = iota
	FieldTypeTag
	FieldTypeNumeric
	FieldTypeVector
)

// VectorAlgo represents the algorithm used for vector similarity search.
type VectorAlgo int

const (
	VectorAlgoFlat VectorAlgo = iota
	VectorAlgoHNSW
)

// VectorFieldOpts contains configuration options for a vector field in the index.
type VectorFieldOpts struct {
	Algo           VectorAlgo
	Type           string // FLOAT32, FLOAT64
	Dim            int
	DistanceMetric string // L2, IP, COSINE
}

// FieldSchema defines a single field in a search index schema.
type FieldSchema struct {
	Name      string
	Type      FieldType
	VectorOpts *VectorFieldOpts
}

// IndexSchema defines the complete schema for a search index, including name, key prefixes, and fields.
type IndexSchema struct {
	Name     string
	Prefixes []string
	Fields   []FieldSchema
}

// DocInfo stores information about an indexed document including its key, score, fields, and vectors.
type DocInfo struct {
	Key     string
	Score   float64
	Fields  map[string]string
	Vectors map[string][]float32
}

// InvertedIndex implements an inverted index for full-text search with vector search support.
type InvertedIndex struct {
	mu       sync.RWMutex
	schema   IndexSchema
	inverted map[string]map[string]float64
	docs     map[string]*DocInfo
}

// NewInvertedIndex creates a new inverted index with the given schema.
func NewInvertedIndex(schema IndexSchema) *InvertedIndex {
	slog.Debug("NewInvertedIndex", "schemaName", schema.Name)
	return &InvertedIndex{
		schema:   schema,
		inverted: make(map[string]map[string]float64),
		docs:     make(map[string]*DocInfo),
	}
}

// Schema returns the index schema.
func (idx *InvertedIndex) Schema() IndexSchema {
	slog.Debug("InvertedIndex.Schema")
	return idx.schema
}

// tokenize splits a text string into lowercase tokens using common punctuation as delimiters.
func (idx *InvertedIndex) tokenize(text string) []string {
	text = strings.ToLower(text)
	parts := strings.FieldsFunc(text, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == '\r' ||
			r == ',' || r == '.' || r == '!' || r == '?' ||
			r == ':' || r == ';' || r == '"' || r == '\'' ||
			r == '(' || r == ')' || r == '[' || r == ']' ||
			r == '{' || r == '}' || r == '<' || r == '>' ||
			r == '/' || r == '\\' || r == '|' || r == '~' ||
			r == '`' || r == '@' || r == '#' || r == '$' ||
			r == '%' || r == '^' || r == '&' || r == '*' ||
			r == '+' || r == '='
	})
	words := make([]string, 0)
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			words = append(words, p)
		}
	}
	return words
}

// ParseFloat32Vec parses a string representation of a float32 vector (space or comma separated, optionally bracketed).
func ParseFloat32Vec(s string) ([]float32, error) {
	slog.Debug("ParseFloat32Vec")
	parts := strings.Fields(s)
	if len(parts) == 1 {
		parts = strings.Split(s, ",")
	}
	vec := make([]float32, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.TrimPrefix(p, "[")
		p = strings.TrimSuffix(p, "]")
		v, err := strconv.ParseFloat(p, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid vector value: %s", p)
		}
		vec[i] = float32(v)
	}
	return vec, nil
}

// cosineSimilarity computes the cosine similarity between two float32 vectors.
func cosineSimilarity(a, b []float32) float64 {
	slog.Debug("cosineSimilarity")
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// l2Distance computes the Euclidean (L2) distance between two float32 vectors.
func l2Distance(a, b []float32) float64 {
	slog.Debug("l2Distance")
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat64
	}
	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return math.Sqrt(sum)
}

// innerProduct computes the negative inner product between two float32 vectors (for use as a distance metric where smaller is better).
func innerProduct(a, b []float32) float64 {
	slog.Debug("innerProduct")
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat64
	}
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	return -dot
}

// IndexDocument indexes a document with the given key and field values, replacing any existing document with the same key.
func (idx *InvertedIndex) IndexDocument(key string, fields map[string]string) {
	slog.Debug("InvertedIndex.IndexDocument", "key", key)
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if oldDoc, exists := idx.docs[key]; exists {
		for _, field := range idx.schema.Fields {
			if val, ok := oldDoc.Fields[field.Name]; ok && field.Type != FieldTypeVector {
				terms := idx.tokenize(val)
				for _, term := range terms {
					if docMap, exists := idx.inverted[term]; exists {
						delete(docMap, key)
						if len(docMap) == 0 {
							delete(idx.inverted, term)
						}
					}
				}
			}
		}
	}

	score := 1.0
	docFields := make(map[string]string)
	docVectors := make(map[string][]float32)
	for k, v := range fields {
		docFields[k] = v
	}
	for _, field := range idx.schema.Fields {
		if field.Type == FieldTypeVector {
			if val, ok := fields[field.Name]; ok && val != "" {
				vec, err := ParseFloat32Vec(val)
				if err == nil {
					docVectors[field.Name] = vec
				}
			}
		}
	}
	doc := &DocInfo{
		Key:     key,
		Score:   score,
		Fields:  docFields,
		Vectors: docVectors,
	}
	idx.docs[key] = doc

	for _, field := range idx.schema.Fields {
		if val, ok := fields[field.Name]; ok && val != "" && field.Type != FieldTypeVector {
			switch field.Type {
			case FieldTypeText:
				terms := idx.tokenize(val)
				for _, term := range terms {
					if idx.inverted[term] == nil {
						idx.inverted[term] = make(map[string]float64)
					}
					idx.inverted[term][key] = idx.inverted[term][key] + 1.0
				}
			case FieldTypeTag:
				tags := strings.Split(val, ",")
				for _, tag := range tags {
					tag = strings.TrimSpace(tag)
					if tag != "" {
						tagLower := strings.ToLower(tag)
						if idx.inverted[tagLower] == nil {
							idx.inverted[tagLower] = make(map[string]float64)
						}
						idx.inverted[tagLower][key] = 1.0
					}
				}
			}
		}
	}
}

// RemoveDocument removes a document from the index by its key.
func (idx *InvertedIndex) RemoveDocument(key string) {
	slog.Debug("InvertedIndex.RemoveDocument", "key", key)
	idx.mu.Lock()
	defer idx.mu.Unlock()

	oldDoc, exists := idx.docs[key]
	if !exists {
		return
	}

	for _, field := range idx.schema.Fields {
		if val, ok := oldDoc.Fields[field.Name]; ok {
			terms := idx.tokenize(val)
			for _, term := range terms {
				if docMap, exists := idx.inverted[term]; exists {
					delete(docMap, key)
					if len(docMap) == 0 {
						delete(idx.inverted, term)
					}
				}
			}
		}
	}
	delete(idx.docs, key)
}

// Search performs full-text and/or vector similarity search against the index, returning matching documents ordered by relevance.
func (idx *InvertedIndex) Search(query string, queryVec []float32, vectorField string, knnK int) ([]*DocInfo, error) {
	slog.Info("InvertedIndex.Search", "query", query, "knnK", knnK)
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	query = strings.TrimSpace(query)

	if knnK > 0 && vectorField != "" && len(queryVec) > 0 {
		type vecDist struct {
			key  string
			dist float64
		}
		dists := make([]vecDist, 0, len(idx.docs))
		for key, doc := range idx.docs {
			if vec, ok := doc.Vectors[vectorField]; ok {
				var dist float64
				field := idx.findVectorField(vectorField)
				if field != nil && field.VectorOpts != nil {
					switch field.VectorOpts.DistanceMetric {
					case "L2":
						dist = l2Distance(vec, queryVec)
					case "IP":
						dist = innerProduct(vec, queryVec)
					default:
						dist = 1.0 - cosineSimilarity(vec, queryVec)
					}
				} else {
					dist = 1.0 - cosineSimilarity(vec, queryVec)
				}
				dists = append(dists, vecDist{key: key, dist: dist})
			}
		}
		sort.Slice(dists, func(i, j int) bool {
			return dists[i].dist < dists[j].dist
		})
		if knnK > 0 && knnK < len(dists) {
			dists = dists[:knnK]
		}
		result := make([]*DocInfo, len(dists))
		for i, d := range dists {
			doc := idx.docs[d.key]
			doc.Score = 1.0 / (1.0 + d.dist)
			result[i] = doc
		}
		return result, nil
	}

	// Full-text search mode (existing logic)
	if query == "" || query == "*" {
		result := make([]*DocInfo, 0, len(idx.docs))
		for _, doc := range idx.docs {
			result = append(result, doc)
		}
		return result, nil
	}

	parts := strings.Fields(query)
	var required []string
	var optional []string
	var exclude []string

	for _, part := range parts {
		if strings.HasPrefix(part, "-") {
			exclude = append(exclude, strings.ToLower(part[1:]))
		} else if strings.HasPrefix(part, "+") {
			required = append(required, strings.ToLower(part[1:]))
		} else {
			optional = append(optional, strings.ToLower(part))
		}
	}

	scores := make(map[string]float64)

	if len(required) == 0 && len(optional) == 0 && len(exclude) == 0 {
		result := make([]*DocInfo, 0, len(idx.docs))
		for _, doc := range idx.docs {
			result = append(result, doc)
		}
		return result, nil
	}

	hasRequired := len(required) > 0
	var candidateKeys map[string]bool

	if hasRequired {
		for _, term := range required {
			if docMap, exists := idx.inverted[term]; exists {
				if candidateKeys == nil {
					candidateKeys = make(map[string]bool)
					for k := range docMap {
						candidateKeys[k] = true
					}
				} else {
					for k := range candidateKeys {
						if _, found := docMap[k]; !found {
							delete(candidateKeys, k)
						}
					}
				}
			} else {
				return []*DocInfo{}, nil
			}
		}
	}

	for _, term := range optional {
		if docMap, exists := idx.inverted[term]; exists {
			for k, score := range docMap {
				if candidateKeys == nil || candidateKeys[k] {
					scores[k] += score
				}
			}
		}
	}

	if candidateKeys == nil && len(scores) == 0 {
		return []*DocInfo{}, nil
	}

	if hasRequired {
		for k := range candidateKeys {
			if _, exists := scores[k]; !exists {
				scores[k] = 0
			}
		}
	}

	for _, term := range exclude {
		if docMap, exists := idx.inverted[term]; exists {
			for k := range docMap {
				delete(scores, k)
			}
		}
	}

	result := make([]*DocInfo, 0, len(scores))
	for key := range scores {
		if doc, exists := idx.docs[key]; exists {
			result = append(result, doc)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		si := scores[result[i].Key]
		sj := scores[result[j].Key]
		if sj != si {
			return sj < si
		}
		return result[j].Key < result[i].Key
	})

	return result, nil
}

// findVectorField finds and returns the field schema for a vector field by name.
func (idx *InvertedIndex) findVectorField(name string) *FieldSchema {
	for i := range idx.schema.Fields {
		if idx.schema.Fields[i].Name == name && idx.schema.Fields[i].Type == FieldTypeVector {
			return &idx.schema.Fields[i]
		}
	}
	return nil
}

// DocCount returns the number of documents in the index.
func (idx *InvertedIndex) DocCount() int {
	slog.Debug("InvertedIndex.DocCount")
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.docs)
}

// TermCount returns the number of unique terms in the inverted index.
func (idx *InvertedIndex) TermCount() int {
	slog.Debug("InvertedIndex.TermCount")
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.inverted)
}

var (
	globalMu      sync.RWMutex
	globalIndexes = make(map[string]*InvertedIndex)
)

// RegisterIndex registers a new search index globally by schema.
func RegisterIndex(schema IndexSchema) error {
	slog.Debug("RegisterIndex", "schemaName", schema.Name)
	globalMu.Lock()
	defer globalMu.Unlock()

	name := strings.ToLower(schema.Name)
	if _, exists := globalIndexes[name]; exists {
		return fmt.Errorf("index already exists")
	}
	globalIndexes[name] = NewInvertedIndex(schema)
	return nil
}

// DropIndex removes a registered search index by name. Returns false if the index did not exist.
func DropIndex(name string) bool {
	slog.Debug("DropIndex", "name", name)
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
	slog.Debug("GetIndex", "name", name)
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalIndexes[strings.ToLower(name)]
}

// ListIndexes returns the names of all registered search indexes.
func ListIndexes() []string {
	slog.Debug("ListIndexes")
	globalMu.RLock()
	defer globalMu.RUnlock()
	names := make([]string, 0, len(globalIndexes))
	for name := range globalIndexes {
		names = append(names, name)
	}
	return names
}

// IndexDocByPrefix indexes a document by its key prefix, automatically routing to matching registered indexes.
func IndexDocByPrefix(key string, fields map[string]string) {
	slog.Debug("IndexDocByPrefix", "key", key)
	globalMu.RLock()
	defer globalMu.RUnlock()

	for _, idx := range globalIndexes {
		for _, prefix := range idx.schema.Prefixes {
			if strings.HasPrefix(key, prefix) {
				idx.IndexDocument(key, fields)
				break
			}
		}
	}
}

// RemoveDocByPrefix removes a document from all matching indexes by its key prefix.
func RemoveDocByPrefix(key string) {
	slog.Debug("RemoveDocByPrefix", "key", key)
	globalMu.RLock()
	defer globalMu.RUnlock()

	for _, idx := range globalIndexes {
		for _, prefix := range idx.schema.Prefixes {
			if strings.HasPrefix(key, prefix) {
				idx.RemoveDocument(key)
				break
			}
		}
	}
}
