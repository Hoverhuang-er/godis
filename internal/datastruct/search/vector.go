package search

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ParseFloat32Vec parses a text representation of a float vector: whitespace or
// comma separated, optionally bracketed.
func ParseFloat32Vec(s string) ([]float32, error) {
	parts := strings.Fields(s)
	if len(parts) == 1 {
		parts = strings.Split(s, ",")
	}
	vec := make([]float32, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.TrimPrefix(p, "[")
		p = strings.TrimSuffix(p, "]")
		if p == "" {
			return nil, fmt.Errorf("invalid vector value: empty")
		}
		v, err := strconv.ParseFloat(p, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid vector value: %s", p)
		}
		vec[i] = float32(v)
	}
	return vec, nil
}

// DecodeVector decodes a query vector as sent by a client. redis-cli and the
// Redis client libraries send the raw little-endian float bytes inside the RESP
// bulk string; hand-written commands send text. Text is tried first because
// printable ASCII can never carry a valid binary vector payload.
func DecodeVector(raw []byte, dim int) ([]float32, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty vector parameter")
	}

	var vec []float32
	var err error
	switch {
	case isPrintable(raw):
		vec, err = ParseFloat32Vec(string(raw))
	case len(raw)%4 == 0:
		vec = make([]float32, len(raw)/4)
		for i := range vec {
			vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
		}
	default:
		err = fmt.Errorf("vector parameter is neither text nor a float32 blob (%d bytes)", len(raw))
	}
	if err != nil {
		return nil, err
	}
	if dim > 0 && len(vec) != dim {
		return nil, fmt.Errorf("vector dimension mismatch: index expects %d, got %d", dim, len(vec))
	}
	return vec, nil
}

// Similarity maps a distance produced by KNNSearch onto 0..1, where 1 means
// identical. COSINE is exact; L2 is a monotone squash; IP is only a monotone
// proxy because the inner product is unbounded.
func Similarity(metric string, distance float64) float64 {
	switch strings.ToUpper(metric) {
	case "L2":
		if distance < 0 {
			return 0
		}
		return 1 / (1 + distance)
	case "IP":
		return 1 / (1 + math.Max(distance, 0))
	default: // COSINE
		return 1 - distance
	}
}

func isPrintable(b []byte) bool {
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return true
}

// vectorDistance returns the metric distance between two vectors; smaller is
// always more similar, so IP is negated and COSINE is converted to a distance.
func vectorDistance(metric string, a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return math.MaxFloat64
	}
	switch strings.ToUpper(metric) {
	case "L2":
		var sum float64
		for i := range a {
			d := float64(a[i]) - float64(b[i])
			sum += d * d
		}
		return math.Sqrt(sum)
	case "IP":
		var dot float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
		}
		return -dot
	default: // COSINE
		var dot, na, nb float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
			na += float64(a[i]) * float64(a[i])
			nb += float64(b[i]) * float64(b[i])
		}
		if na == 0 || nb == 0 {
			return 1
		}
		return 1 - dot/(math.Sqrt(na)*math.Sqrt(nb))
	}
}
