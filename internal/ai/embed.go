// Package ai provides the server-side AI primitives the knowledge-base commands
// build on: turning text into vectors, and splitting documents into chunks.
//
// Everything here runs inside the database process, which is what lets a plain
// `redis-cli RAG.QUERY ...` retrieve by meaning without the client embedding
// anything. The default embedder is dependency-free and offline; an
// OpenAI-compatible HTTP provider can be configured instead.
package ai

import (
	"fmt"
	"hash/fnv"
	"math"
	"strings"
)

// Embedder turns text into vectors. Implementations must be safe for concurrent
// use and must return one vector per input, in input order.
type Embedder interface {
	Embed(texts []string) ([][]float32, error)
	// Dim is the vector length every call returns.
	Dim() int
	// Name identifies the implementation, so an index built by one embedder can
	// refuse queries from another.
	Name() string
}

// EmbedOne is a convenience wrapper for the single-text case.
func EmbedOne(e Embedder, text string) ([]float32, error) {
	vecs, err := e.Embed([]string{text})
	if err != nil {
		return nil, err
	}
	if len(vecs) != 1 {
		return nil, fmt.Errorf("embedder %s returned %d vectors for 1 input", e.Name(), len(vecs))
	}
	return vecs[0], nil
}

// LocalEmbedder is a deterministic, dependency-free embedder: a hashing
// vectorizer over word unigrams and character trigrams, L2-normalised and
// compared with cosine similarity.
//
// ponytail: this is lexical, not semantic — it will not match "headphones" to
// "earbuds". The character trigrams give it tolerance for morphology and typos,
// nothing more. It exists so knowledge-base commands work with zero config and
// no network. Configure an HTTP embedding provider for real semantics.
type LocalEmbedder struct {
	dim int
}

// NewLocalEmbedder returns a local embedder producing vectors of length dim.
func NewLocalEmbedder(dim int) *LocalEmbedder {
	if dim <= 0 {
		dim = DefaultLocalDim
	}
	return &LocalEmbedder{dim: dim}
}

// DefaultLocalDim is the local embedder's vector length.
const DefaultLocalDim = 512

func (e *LocalEmbedder) Dim() int { return e.dim }

func (e *LocalEmbedder) Name() string { return fmt.Sprintf("local:%d", e.dim) }

func (e *LocalEmbedder) Embed(texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		out[i] = e.embedOne(text)
	}
	return out, nil
}

// stopWords contribute no discriminative signal to a lexical embedding.
var stopWords = map[string]bool{
	"a": true, "about": true, "an": true, "and": true, "any": true, "are": true,
	"as": true, "at": true, "be": true, "but": true, "by": true, "can": true,
	"could": true, "did": true, "do": true, "does": true, "for": true, "from": true,
	"had": true, "has": true, "have": true, "he": true, "her": true, "him": true,
	"his": true, "how": true, "i": true, "if": true, "in": true, "into": true,
	"is": true, "it": true, "its": true, "me": true, "my": true, "no": true,
	"not": true, "of": true, "on": true, "or": true, "our": true, "out": true,
	"she": true, "should": true, "so": true, "some": true, "such": true,
	"than": true, "that": true, "the": true, "their": true, "them": true,
	"then": true, "there": true, "these": true, "they": true, "this": true,
	"to": true, "up": true, "us": true, "was": true, "we": true, "were": true,
	"what": true, "when": true, "where": true, "which": true, "who": true,
	"why": true, "will": true, "with": true, "would": true, "you": true,
	"your": true,
}

// gramWeight keeps word identity dominant over sub-word features so that an
// exact word match still outranks a coincidental trigram overlap.
const gramWeight = 0.5

func (e *LocalEmbedder) embedOne(text string) []float32 {
	vec := make([]float32, e.dim)
	for _, token := range tokenize(text) {
		// Function words are shared by essentially every English sentence, so
		// they inflate the similarity of unrelated text: "how do I tune vector
		// recall?" and "how do I bake bread?" would otherwise score ~0.5.
		if stopWords[token] {
			continue
		}
		addFeature(vec, token, 1)
		for _, gram := range charGrams(token, trigramLen) {
			addFeature(vec, gram, gramWeight)
		}
	}
	normalize(vec)
	return vec
}

const trigramLen = 3

// addFeature hashes a feature into a signed bucket. The sign comes from a
// different bit than the bucket index so that two colliding features are as
// likely to cancel as to reinforce, which keeps collisions from inflating
// similarity.
func addFeature(vec []float32, feature string, weight float32) {
	sum := hash64(feature)
	vec[sum%uint64(len(vec))] += signOf(sum) * weight
}

func signOf(sum uint64) float32 {
	if sum&(1<<63) != 0 {
		return -1
	}
	return 1
}

func normalize(vec []float32) {
	var norm float64
	for _, v := range vec {
		norm += float64(v) * float64(v)
	}
	if norm == 0 {
		return
	}
	inv := float32(1 / math.Sqrt(norm))
	for i := range vec {
		vec[i] *= inv
	}
}

// hashFeature is FNV-1a: stable across processes and Go versions, so vectors
// stored by one godis process stay comparable in the next.
func hash64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// tokenize lowercases and splits on anything that is not a letter or digit.
func tokenize(text string) []string {
	return strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !isWordRune(r)
	})
}

func isWordRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	case r > 0x7f:
		// keep non-ASCII letters (CJK, accented Latin) as word characters
		return true
	}
	return false
}

// charGrams returns the fixed-width character n-grams of a token, padding short
// tokens with their whole self so single letters still contribute a feature.
func charGrams(token string, n int) []string {
	runes := []rune(token)
	if len(runes) <= n {
		return []string{token}
	}
	grams := make([]string, 0, len(runes)-n+1)
	for i := 0; i+n <= len(runes); i++ {
		grams = append(grams, string(runes[i:i+n]))
	}
	return grams
}
