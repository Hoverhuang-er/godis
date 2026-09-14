package search

import (
	"encoding/json"
	"strconv"
	"strings"
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
//
// ponytail: both FLAT and HNSW run the exact linear scan implemented in
// vector.go. Exact recall beats approximate on the collection sizes godis sees,
// and the token is accepted for client compatibility. Swap in a proximity-graph
// index when n grows past ~100k vectors per field.
type VectorAlgo int

const (
	VectorAlgoFlat VectorAlgo = iota
	VectorAlgoHNSW
)

// KeyType is the Redis structure a search index attaches to.
type KeyType int

const (
	KeyTypeHash KeyType = iota
	KeyTypeJSON
)

func (k KeyType) String() string {
	if k == KeyTypeJSON {
		return "JSON"
	}
	return "HASH"
}

// VectorFieldOpts contains configuration options for a vector field in the index.
type VectorFieldOpts struct {
	Algo           VectorAlgo
	Type           string // FLOAT32, FLOAT64
	Dim            int
	DistanceMetric string // L2, IP, COSINE
}

// FieldSchema defines a single field in a search index schema.
//
// Path is the source location inside the document: a JSON path such as
// "$.features[*]" for JSON indexes, or the hash field name for HASH indexes.
// Name is the alias queries use, and equals the last path segment when no AS
// alias is declared.
type FieldSchema struct {
	Name       string
	Path       string
	Type       FieldType
	VectorOpts *VectorFieldOpts
}

// IndexSchema defines the complete schema for a search index, including name, key prefixes, and fields.
type IndexSchema struct {
	Name     string
	KeyType  KeyType
	Prefixes []string
	Fields   []FieldSchema
}

// FieldByName returns the schema entry for a field alias, or nil.
func (s *IndexSchema) FieldByName(name string) *FieldSchema {
	for i := range s.Fields {
		if strings.EqualFold(s.Fields[i].Name, name) {
			return &s.Fields[i]
		}
	}
	return nil
}

// ToCommandLine renders the schema as the equivalent FT.CREATE command, so a
// command that creates an index implicitly can record it in the AOF and have
// replay rebuild an identical index.
func (s IndexSchema) ToCommandLine() [][]byte {
	cmd := [][]byte{[]byte("ft.create"), []byte(s.Name), []byte("ON"), []byte(s.KeyType.String())}
	if len(s.Prefixes) > 0 {
		cmd = append(cmd, []byte("PREFIX"), []byte(strconv.Itoa(len(s.Prefixes))))
		for _, p := range s.Prefixes {
			cmd = append(cmd, []byte(p))
		}
	}
	cmd = append(cmd, []byte("SCHEMA"))
	for _, f := range s.Fields {
		path := f.Path
		if path == "" {
			path = f.Name
		}
		cmd = append(cmd, []byte(path))
		if path != f.Name {
			cmd = append(cmd, []byte("AS"), []byte(f.Name))
		}
		cmd = append(cmd, []byte(FieldTypeName(f.Type)))
		if f.Type != FieldTypeVector || f.VectorOpts == nil {
			continue
		}
		algo := "FLAT"
		if f.VectorOpts.Algo == VectorAlgoHNSW {
			algo = "HNSW"
		}
		typ := f.VectorOpts.Type
		if typ == "" {
			typ = "FLOAT32"
		}
		metric := f.VectorOpts.DistanceMetric
		if metric == "" {
			metric = "COSINE"
		}
		// The VECTOR type token already introduces the algorithm, matching
		// "path AS alias VECTOR FLAT 6 TYPE FLOAT32 DIM n DISTANCE_METRIC m".
		cmd = append(cmd,
			[]byte(algo), []byte("6"),
			[]byte("TYPE"), []byte(typ),
			[]byte("DIM"), []byte(strconv.Itoa(f.VectorOpts.Dim)),
			[]byte("DISTANCE_METRIC"), []byte(metric),
		)
	}
	return cmd
}

// FieldTypeName renders a field type the way FT.CREATE and FT.INFO spell it.
func FieldTypeName(t FieldType) string {
	switch t {
	case FieldTypeText:
		return "TEXT"
	case FieldTypeTag:
		return "TAG"
	case FieldTypeNumeric:
		return "NUMERIC"
	default:
		return "VECTOR"
	}
}

// ExtractFields resolves every schema field for a document into flat string values.
// Vector fields keep their JSON text form and are decoded when the document is indexed.
func (s *IndexSchema) ExtractFields(doc any) map[string][]string {
	out := make(map[string][]string, len(s.Fields))
	for _, f := range s.Fields {
		path := f.Path
		if path == "" {
			path = f.Name
		}
		if f.Type == FieldTypeVector {
			if raw, err := json.Marshal(lookupValue(doc, path)); err == nil && string(raw) != "null" {
				out[f.Name] = []string{string(raw)}
			}
			continue
		}
		if vals := jsonScalars(lookupValues(doc, path)); len(vals) > 0 {
			out[f.Name] = vals
		}
	}
	return out
}

// pathSegments normalises a RedisJSON path into plain key segments, dropping the
// leading "$." and any "[*]" array wildcards.
func pathSegments(path string) []string {
	path = strings.TrimSpace(path)
	if path == "" || path == "$" || path == "." {
		return nil
	}
	path = strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(path, "$"), "."), "[*]")
	segs := strings.Split(path, ".")
	for i, seg := range segs {
		segs[i] = strings.Trim(seg, "[]")
	}
	return segs
}

// lookupValues resolves a path and flattens every array it selects, which is what
// TEXT, TAG and NUMERIC fields need.
func lookupValues(root any, path string) []any {
	return flattenValue(walkValue(root, pathSegments(path)))
}

// lookupValue resolves a path without flattening arrays, so a vector field keeps
// its components together.
func lookupValue(root any, path string) any {
	segs := pathSegments(path)
	if len(segs) == 0 {
		return root
	}
	return walkValue(root, segs)
}

func walkValue(cur any, segs []string) any {
	if len(segs) == 0 {
		return cur
	}
	if seg := segs[0]; seg != "" {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		mid, ok := obj[seg]
		if !ok {
			return nil
		}
		cur = mid
	}
	if arr, ok := cur.([]any); ok {
		out := make([]any, 0, len(arr))
		for _, item := range arr {
			if v := walkValue(item, segs[1:]); v != nil {
				out = append(out, v)
			}
		}
		if len(out) == 1 {
			return out[0]
		}
		return out
	}
	return walkValue(cur, segs[1:])
}

func flattenValue(v any) []any {
	if arr, ok := v.([]any); ok {
		var out []any
		for _, item := range arr {
			out = append(out, flattenValue(item)...)
		}
		return out
	}
	if v == nil {
		return nil
	}
	return []any{v}
}

func jsonScalars(vals []any) []string {
	var out []string
	for _, v := range vals {
		switch t := v.(type) {
		case nil:
			continue
		case string:
			out = append(out, t)
		case float64:
			out = append(out, strconv.FormatFloat(t, 'f', -1, 64))
		case bool:
			out = append(out, strconv.FormatBool(t))
		case json.Number:
			out = append(out, t.String())
		default:
			if raw, err := json.Marshal(v); err == nil {
				out = append(out, string(raw))
			}
		}
	}
	return out
}
