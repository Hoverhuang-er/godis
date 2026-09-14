package search

import (
	"fmt"
	"math"
	"testing"
)

func TestParseQuery(t *testing.T) {
	cases := []struct {
		in    string
		knn   *KNNClause
		check func(t *testing.T, q *Query)
	}{
		{
			in:  "(*)=>[KNN 3 @embedding $query_vector AS score]",
			knn: &KNNClause{K: 3, Field: "embedding", Param: "query_vector", ScoreAlias: "score"},
		},
		{
			in:  "(@category:{Audio})=>[KNN 3 @embedding $query_vector AS score]",
			knn: &KNNClause{K: 3, Field: "embedding", Param: "query_vector", ScoreAlias: "score"},
			check: func(t *testing.T, q *Query) {
				term := q.Root.Term
				if term == nil || term.Kind != TermTag || term.Field != "category" {
					t.Fatalf("expected tag term on category, got %+v", q.Root.Term)
				}
				if len(term.Tags) != 1 || term.Tags[0] != "audio" {
					t.Fatalf("expected tag [audio], got %v", term.Tags)
				}
			},
		},
		{
			in: "wireless -wired",
			check: func(t *testing.T, q *Query) {
				if q.Root.Op != OpAnd || len(q.Root.Children) != 2 {
					t.Fatalf("expected AND of two children, got %+v", q.Root)
				}
				if q.Root.Children[0].Op != OpTerm || q.Root.Children[0].Term.Word != "wireless" {
					t.Fatalf("expected optional term wireless, got %+v", q.Root.Children[0])
				}
				neg := q.Root.Children[1]
				if neg.Op != OpNot || neg.Children[0].Term.Word != "wired" {
					t.Fatalf("expected negation of wired, got %+v", neg)
				}
			},
		},
		{
			in: "@brand:{Aurora|Sonus}",
			check: func(t *testing.T, q *Query) {
				term := q.Root.Term
				if term.Kind != TermTag || term.Field != "brand" {
					t.Fatalf("expected tag term, got %+v", term)
				}
				if len(term.Tags) != 2 || term.Tags[0] != "aurora" || term.Tags[1] != "sonus" {
					t.Fatalf("expected [aurora sonus], got %v", term.Tags)
				}
			},
		},
		{
			in: "@price:[50 200]",
			check: func(t *testing.T, q *Query) {
				term := q.Root.Term
				if term.Kind != TermNumeric || !term.HasMin || !term.HasMax {
					t.Fatalf("expected bounded numeric term, got %+v", term)
				}
				if term.Min != 50 || term.Max != 200 {
					t.Fatalf("expected range 50..200, got %v..%v", term.Min, term.Max)
				}
			},
		},
		{
			in: "@price:[(50 +inf]",
			check: func(t *testing.T, q *Query) {
				term := q.Root.Term
				if term.Min <= 50 || term.HasMax {
					t.Fatalf("expected exclusive lower bound and no upper bound, got %+v", term)
				}
			},
		},
		{
			in: "portable|bluetooth",
			check: func(t *testing.T, q *Query) {
				if q.Root.Op != OpOr {
					t.Fatalf("expected OR root, got %+v", q.Root)
				}
			},
		},
		{
			in: "@name:(wireless bluetooth)",
			check: func(t *testing.T, q *Query) {
				// A space inside a field group is an intersection in dialect 2.
				if q.Root.Op != OpAnd || len(q.Root.Children) != 2 {
					t.Fatalf("expected field-scoped AND, got %+v", q.Root)
				}
				for _, c := range q.Root.Children {
					if c.Term == nil || c.Term.Field != "name" {
						t.Fatalf("expected both terms scoped to name, got %+v", c.Term)
					}
				}
			},
		},
		{
			in: "(kids | small) @condition:{used}",
			check: func(t *testing.T, q *Query) {
				if q.Root.Op != OpAnd || len(q.Root.Children) != 2 {
					t.Fatalf("expected AND of an OR group and a tag, got %+v", q.Root)
				}
				if q.Root.Children[0].Op != OpOr || q.Root.Children[1].Term.Kind != TermTag {
					t.Fatalf("unexpected shape: %+v %+v", q.Root.Children[0], q.Root.Children[1])
				}
			},
		},
		{
			in: "kids small | used new",
			check: func(t *testing.T, q *Query) {
				// Dialect 2: AND binds tighter, so this is (kids AND small) OR (used AND new).
				if q.Root.Op != OpOr {
					t.Fatalf("expected OR at the root, got %+v", q.Root)
				}
				for _, branch := range q.Root.Children {
					if branch.Op != OpAnd || len(branch.Children) != 2 {
						t.Fatalf("expected two AND branches, got %+v", branch)
					}
				}
			},
		},
	}

	for _, c := range cases {
		q, err := ParseQuery(c.in)
		if err != nil {
			t.Errorf("ParseQuery(%q) failed: %v", c.in, err)
			continue
		}
		if c.knn != nil {
			if q.KNN == nil || *q.KNN != *c.knn {
				t.Errorf("ParseQuery(%q) KNN = %+v, want %+v", c.in, q.KNN, c.knn)
			}
		}
		if c.check != nil {
			c.check(t, q)
		}
	}
}

func TestParseQueryErrors(t *testing.T) {
	for _, in := range []string{
		"@price:[10]",
		"@tag:{missing-brace",
		"(@category:{Audio}",
		"=>[KNN 3 @embedding]",
		"term*",
		"@name:",
	} {
		if _, err := ParseQuery(in); err == nil {
			t.Errorf("ParseQuery(%q) should have failed", in)
		}
	}
}

// newTestIndex builds an index whose documents carry a 2-dim vector, a tag
// field and a text field, mirroring the tutorial's catalog shape.
func newTestIndex(t *testing.T) *InvertedIndex {
	t.Helper()
	idx := NewInvertedIndex(IndexSchema{
		Name:    "test",
		KeyType: KeyTypeJSON,
		Fields: []FieldSchema{
			{Name: "name", Path: "$.name", Type: FieldTypeText},
			{Name: "category", Path: "$.category", Type: FieldTypeTag},
			{Name: "embedding", Path: "$.embedding", Type: FieldTypeVector,
				VectorOpts: &VectorFieldOpts{Algo: VectorAlgoFlat, Type: "FLOAT32", Dim: 2, DistanceMetric: "COSINE"}},
		},
	})
	type doc struct {
		key      string
		name     string
		category string
		vec      [2]float32
	}
	for _, d := range []doc{
		{key: "product:1", name: "portable speaker", category: "Audio", vec: [2]float32{1, 0.05}},
		{key: "product:2", name: "wireless earbuds", category: "Audio", vec: [2]float32{0.95, 0.1}},
		{key: "product:3", name: "gaming laptop", category: "Computers", vec: [2]float32{0, 1}},
		{key: "product:4", name: "keyboard wireless", category: "Accessories", vec: [2]float32{0.5, 0.8}},
	} {
		idx.IndexDocument(d.key, map[string][]string{
			"name":      {d.name},
			"category":  {d.category},
			"embedding": {fmt.Sprint(d.vec[:])},
		})
	}
	return idx
}

func TestKNNSearchHonoursPreFilter(t *testing.T) {
	idx := newTestIndex(t)

	q, err := ParseQuery("(@category:{Audio})=>[KNN 3 @embedding $v AS score]")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	candidates, err := idx.EvalScores(q)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("pre-filter should keep 2 Audio docs, got %d (%v)", len(candidates), candidates)
	}

	hits, err := idx.KNNSearch("embedding", []float32{1, 0}, 3, candidates)
	if err != nil {
		t.Fatalf("knn: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("expected 2 hits inside the Audio subset, got %d", len(hits))
	}
	if hits[0].Key != "product:1" || hits[1].Key != "product:2" {
		t.Fatalf("expected [product:1 product:2] by cosine distance, got [%s %s]", hits[0].Key, hits[1].Key)
	}
	if hits[0].Score > hits[1].Score {
		t.Fatalf("distances must ascend: %v then %v", hits[0].Score, hits[1].Score)
	}

	all, err := idx.KNNSearch("embedding", []float32{0, 1}, 1, nil)
	if err != nil {
		t.Fatalf("knn without filter: %v", err)
	}
	if len(all) != 1 || all[0].Key != "product:3" {
		t.Fatalf("expected product:3 nearest to (0,1), got %+v", all)
	}

	if _, err := idx.KNNSearch("embedding", []float32{1, 0, 0}, 1, nil); err == nil {
		t.Error("a dimension mismatch must be reported, not silently ignored")
	}
}

func TestHybridFusionRanking(t *testing.T) {
	idx := newTestIndex(t)

	q, err := ParseQuery("wireless")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	hits, err := idx.Hybrid(&HybridRequest{
		Search:      q,
		Vector:      []float32{1, 0},
		VectorField: "embedding",
		KNN:         3,
		K:           3,
	})
	if err != nil {
		t.Fatalf("hybrid: %v", err)
	}
	if len(hits) != 3 {
		t.Fatalf("expected 3 fused results, got %d", len(hits))
	}
	// product:2 is the only doc matching both the keyword and the vector, so it
	// must outrank product:1 (nearest vector only) and product:4 (keyword only).
	if hits[0].Key != "product:2" {
		t.Fatalf("expected product:2 first, got %s", hits[0].Key)
	}
	if hits[0].Score <= hits[len(hits)-1].Score {
		t.Fatalf("fused scores must descend: %v", hits[0].Score)
	}
	if math.IsNaN(hits[0].Score) || hits[0].Score == 0 {
		t.Fatalf("unexpected fused score %v", hits[0].Score)
	}
}
