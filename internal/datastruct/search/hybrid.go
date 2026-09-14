package search

import (
	"fmt"
	"strings"
)

// HybridRequest is a parsed FT.HYBRID query: a full-text SEARCH clause and a
// vector VSIM clause whose rankings are fused into one result list.
type HybridRequest struct {
	Search      *Query
	Filter      *Query // optional extra filter applied to both rankings
	Vector      []float32
	VectorField string
	KNN         int
	K           int
	Combine     string  // RRF (default) or LINEAR
	Alpha       float64 // LINEAR: weight of the text ranking, 0..1
	Window      int     // RRF: rank window per ranking
	Constant    float64 // RRF: rank constant, defaults to RRFRankConstant
}

// Fusion methods supported by FT.HYBRID.
const (
	CombineRRF    = "RRF"
	CombineLinear = "LINEAR"
)

// RRFRankConstant is the k of the Reciprocal Rank Fusion formula, matching the
// RediSearch default.
const RRFRankConstant = 60

// Hybrid runs the SEARCH and VSIM clauses and fuses their rankings.
func (idx *InvertedIndex) Hybrid(req *HybridRequest) ([]*DocInfo, error) {
	switch {
	case req == nil:
		return nil, fmt.Errorf("Syntax error: missing hybrid query")
	case req.VectorField == "":
		return nil, fmt.Errorf("Syntax error: VSIM requires a vector field")
	case len(req.Vector) == 0:
		return nil, fmt.Errorf("Syntax error: VSIM requires a vector parameter")
	case req.KNN <= 0:
		return nil, fmt.Errorf("Syntax error: VSIM KNN requires a positive k")
	}

	var candidates map[string]float64
	if req.Filter != nil && req.Filter.Root != nil {
		var err error
		if candidates, err = idx.EvalScores(req.Filter); err != nil {
			return nil, err
		}
	}

	textHits, err := idx.TextSearch(req.Search)
	if err != nil {
		return nil, err
	}
	vectorHits, err := idx.KNNSearch(req.VectorField, req.Vector, req.KNN, candidates)
	if err != nil {
		return nil, err
	}
	textHits = Restrict(textHits, candidates)

	window := req.Window
	if window <= 0 {
		window = req.K
	}

	constant := req.Constant
	if constant <= 0 {
		constant = RRFRankConstant
	}

	result := make([]*DocInfo, 0, len(textHits)+len(vectorHits))
	seen := make(map[string]bool, len(textHits)+len(vectorHits))
	for key, score := range fuse(req.Combine, req.Alpha, constant, textHits, vectorHits, window) {
		if seen[key] {
			continue
		}
		seen[key] = true
		if doc := idx.getDoc(key); doc != nil {
			result = append(result, doc.info(score))
		}
	}
	sortDocInfos(result, false)
	if req.K > 0 && req.K < len(result) {
		result = result[:req.K]
	}
	return result, nil
}

func (idx *InvertedIndex) getDoc(key string) *indexedDoc {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.docs[key]
}

// FuseRRF combines two ranked lists with Reciprocal Rank Fusion and returns the
// merged ranking, best first. It is exported so callers beyond FT.HYBRID — a
// knowledge-base query fusing wording and meaning — get the same fusion.
func FuseRRF(textHits, vectorHits []*DocInfo, limit int) []*DocInfo {
	scores := map[string]float64{}
	addReciprocalRanks(scores, textHits, 0, RRFRankConstant)
	addReciprocalRanks(scores, vectorHits, 0, RRFRankConstant)

	byKey := make(map[string]*DocInfo, len(textHits)+len(vectorHits))
	for _, hits := range [][]*DocInfo{textHits, vectorHits} {
		for _, h := range hits {
			if _, ok := byKey[h.Key]; !ok {
				byKey[h.Key] = h
			}
		}
	}

	out := make([]*DocInfo, 0, len(scores))
	for key, score := range scores {
		doc := byKey[key]
		if doc == nil {
			continue
		}
		out = append(out, &DocInfo{Key: doc.Key, Score: score, Fields: doc.Fields, Vectors: doc.Vectors})
	}
	sortDocInfos(out, false)
	if limit > 0 && limit < len(out) {
		out = out[:limit]
	}
	return out
}

// fuse combines two ranked lists. RRF sums reciprocal ranks; LINEAR blends the
// normalised scores with Alpha weighting the text ranking.
func fuse(method string, alpha, constant float64, textHits, vectorHits []*DocInfo, window int) map[string]float64 {
	scores := map[string]float64{}
	switch strings.ToUpper(method) {
	case CombineLinear:
		textWeight := min(max(alpha, 0), 1)
		for key, v := range normalize(textHits) {
			scores[key] += textWeight * v
		}
		for key, v := range normalize(vectorHits) {
			scores[key] += (1 - textWeight) * v
		}
	default: // RRF
		addReciprocalRanks(scores, textHits, window, constant)
		addReciprocalRanks(scores, vectorHits, window, constant)
	}
	return scores
}

func addReciprocalRanks(scores map[string]float64, hits []*DocInfo, window int, constant float64) {
	limit := len(hits)
	if window > 0 && window < limit {
		limit = window
	}
	for rank := 0; rank < limit; rank++ {
		scores[hits[rank].Key] += 1 / (constant + float64(rank+1))
	}
}

// normalize maps a ranked list onto 0..1, best result first.
func normalize(hits []*DocInfo) map[string]float64 {
	out := make(map[string]float64, len(hits))
	if len(hits) == 0 {
		return out
	}
	best, worst := hits[0].Score, hits[len(hits)-1].Score
	for i, h := range hits {
		if best == worst {
			out[h.Key] = 1 - float64(i)/float64(len(hits))
			continue
		}
		out[h.Key] = (h.Score - worst) / (best - worst)
	}
	return out
}
