package ai

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLocalEmbedderRanksRelatedTextHigher(t *testing.T) {
	e := NewLocalEmbedder(256)
	texts := []string{
		"vector search tuning",
		"tuning vector search parameters",
		"banana bread recipe",
	}
	vecs, err := e.Embed(texts)
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	for i, v := range vecs {
		if len(v) != 256 {
			t.Fatalf("vector %d has length %d, want 256", i, len(v))
		}
	}

	near := cosine(vecs[0], vecs[1])
	far := cosine(vecs[0], vecs[2])
	if !(near > far) {
		t.Fatalf("related text must score higher: related=%.4f unrelated=%.4f", near, far)
	}
	if near <= 0.2 {
		t.Fatalf("related text scored suspiciously low: %.4f", near)
	}
}

func TestLocalEmbedderIsDeterministic(t *testing.T) {
	a, _ := NewLocalEmbedder(128).Embed([]string{"stable across processes"})
	b, _ := NewLocalEmbedder(128).Embed([]string{"stable across processes"})
	if cosine(a[0], b[0]) != 1 {
		t.Fatal("the same text embedded twice must produce the same vector, " +
			"otherwise vectors stored by one process are useless to the next")
	}
	if len(a[0]) != 128 {
		t.Fatalf("dim = %d, want 128", len(a[0]))
	}
}

func TestLocalEmbedderHandlesMorphology(t *testing.T) {
	e := NewLocalEmbedder(512)
	vecs, _ := e.Embed([]string{"embedding", "embeddings", "aerospace"})
	// Character trigrams mean a plural is far closer than an unrelated word.
	if cosine(vecs[0], vecs[1]) <= cosine(vecs[0], vecs[2]) {
		t.Fatal("morphological variants must beat unrelated words")
	}
}

func cosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (sqrt(na) * sqrt(nb))
}

func sqrt(x float64) float64 {
	if x <= 0 {
		return 0
	}
	z := x
	for i := 0; i < 40; i++ {
		z = (z + x/z) / 2
	}
	return z
}

func TestChunkSplitsAndKeepsHeadingContext(t *testing.T) {
	doc := strings.Join([]string{
		"# Vector search",
		"",
		"Vector search finds documents by meaning rather than exact words.",
		"",
		"## Tuning recall",
		"",
		"Recall depends on the index and the candidate window.",
		"",
		"## Tuning latency",
		"",
		"Latency depends on how many vectors are scanned per query.",
	}, "\n")

	chunks := Chunk(doc, 120, 20)
	if len(chunks) < 2 {
		t.Fatalf("expected the document to split, got %d chunk(s): %v", len(chunks), chunks)
	}
	for i, c := range chunks {
		if len(c) > 400 {
			t.Errorf("chunk %d is %d chars, far beyond the requested size: %q", i, len(c), c)
		}
	}
	joined := strings.Join(chunks, "\n")
	if !strings.Contains(joined, "Tuning recall") {
		t.Error("a chunk lifted from a section must carry its heading")
	}
	if !strings.Contains(joined, "Vector search") {
		t.Error("heading breadcrumb lost")
	}
}

func TestChunkBoundsOversizedParagraph(t *testing.T) {
	// A wall of text with no punctuation must still be cut down to size.
	doc := strings.Repeat("a", 5000)
	chunks := Chunk(doc, 400, 50)
	if len(chunks) < 10 {
		t.Fatalf("expected many chunks for a 5000-char run, got %d", len(chunks))
	}
	for i, c := range chunks {
		if len(c) > 400 {
			t.Fatalf("chunk %d is %d chars, exceeds the 400 limit", i, len(c))
		}
	}
	if got := len(strings.Join(chunks, "")); got < len(doc) {
		t.Fatalf("chunking dropped content: %d of %d characters survived", got, len(doc))
	}
}

func TestChunkEmptyInput(t *testing.T) {
	if chunks := Chunk("", 100, 10); len(chunks) != 0 {
		t.Fatalf("empty input must produce no chunks, got %v", chunks)
	}
	if chunks := Chunk("   \n\n  \n", 100, 10); len(chunks) != 0 {
		t.Fatalf("whitespace-only input must produce no chunks, got %v", chunks)
	}
}

func TestHTTPEmbedderRoundTrip(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("Authorization = %q", got)
		}
		var req struct {
			Model string   `json:"model"`
			Input []string `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Model != "test-model" {
			t.Errorf("model = %q", req.Model)
		}
		// Answer out of order to prove the index field is honoured.
		out := map[string]any{"data": []map[string]any{
			{"index": 1, "embedding": []float32{0, 1, 0}},
			{"index": 0, "embedding": []float32{1, 0, 0}},
		}}
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer server.Close()

	e, err := NewHTTPEmbedder(HTTPEmbedderOptions{
		BaseURL: server.URL, APIKey: "test-key", Model: "test-model", Dim: 3,
	})
	if err != nil {
		t.Fatalf("build embedder: %v", err)
	}
	vecs, err := e.Embed([]string{"first", "second"})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	if len(vecs) != 2 || vecs[0][0] != 1 || vecs[1][1] != 1 {
		t.Fatalf("results must be reordered by index, got %v", vecs)
	}
}

func TestHTTPEmbedderReportsProblems(t *testing.T) {
	// A dimension that disagrees with the config would silently produce vectors
	// an existing index cannot compare.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{
			{"index": 0, "embedding": []float32{1, 0}},
		}})
	}))
	defer server.Close()
	e, _ := NewHTTPEmbedder(HTTPEmbedderOptions{BaseURL: server.URL, Model: "m", Dim: 3})
	if _, err := e.Embed([]string{"x"}); err == nil || !strings.Contains(err.Error(), "dimension") {
		t.Fatalf("expected a dimension error, got %v", err)
	}

	errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"message":"bad key"}}`))
	}))
	defer errServer.Close()
	e2, _ := NewHTTPEmbedder(HTTPEmbedderOptions{BaseURL: errServer.URL, Model: "m", Dim: 3})
	if _, err := e2.Embed([]string{"x"}); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected the HTTP status to surface, got %v", err)
	}

	if _, err := NewHTTPEmbedder(HTTPEmbedderOptions{Model: "m", Dim: 3}); err == nil {
		t.Fatal("a missing base URL must be rejected")
	}
	if _, err := NewHTTPEmbedder(HTTPEmbedderOptions{BaseURL: "http://x", Model: "m"}); err == nil {
		t.Fatal("a missing dimension must be rejected")
	}
}

// TestLocalEmbedderSeparatesRelatedFromUnrelated pins the property every
// knowledge-base threshold depends on: the weakest related pair must still
// outrank the strongest unrelated pair, with room between them for a threshold.
//
// The local embedder is lexical, so its absolute scores run low and a short query
// against a long passage can score around 0.2. That is why MINSCORE defaults to 0
// in the retrieval commands and why a remote model is worth configuring: its
// bands are far wider apart.
func TestLocalEmbedderSeparatesRelatedFromUnrelated(t *testing.T) {
	e := NewLocalEmbedder(512)

	related := [][2]string{
		{"how do I tune vector recall?", "how do I tune vector recall for a large index?"},
		{"how is recall traded against speed?", "A graph index trades recall against build time."},
		{"vector recall tuning", "recall is traded against the candidate window"},
	}
	unrelated := [][2]string{
		{"how do I tune vector recall?", "how do I bake sourdough bread?"},
		{"how do I tune vector recall?", "what is the weather in Lisbon?"},
		{"vector recall tuning", "the wifi password is on the fridge"},
		{"how is recall traded against speed?", "Sourdough needs a starter, flour, water and salt."},
	}

	simOf := func(p [2]string) float64 {
		v, err := e.Embed([]string{p[0], p[1]})
		if err != nil {
			t.Fatalf("embed: %v", err)
		}
		return cosine(v[0], v[1])
	}

	weakestRelated := 1.0
	for _, p := range related {
		if got := simOf(p); got < weakestRelated {
			weakestRelated = got
		}
	}
	strongestUnrelated := -1.0
	for _, p := range unrelated {
		if got := simOf(p); got > strongestUnrelated {
			strongestUnrelated = got
		}
	}

	if strongestUnrelated >= weakestRelated {
		t.Fatalf("related and unrelated bands overlap (weakest related %.4f, strongest unrelated %.4f); "+
			"no similarity threshold could separate them", weakestRelated, strongestUnrelated)
	}
	// Without stop word filtering the bands collapse into each other, so require
	// real daylight rather than a hairline.
	if weakestRelated-strongestUnrelated < 0.05 {
		t.Fatalf("bands are too close to discriminate: weakest related %.4f, strongest unrelated %.4f",
			weakestRelated, strongestUnrelated)
	}
}
