// Package tdigest provides a T-Digest probabilistic data structure
// for quantile estimation, compatible with Redis T-Digest.
package tdigest

import (
	"math"
	"sort"
)

const maxCentroids = 1000

type centroid struct {
	mean float64
	count uint64
}

// TDigest estimates quantiles from streaming data.
type TDigest struct {
	centroids []centroid
	count     uint64
	compression float64
}

// New creates a new T-Digest.
func New() *TDigest {
	return &TDigest{compression: 100}
}

// Compression sets the compression parameter.
func (t *TDigest) Compression(c float64) {
	if c > 0 {
		t.compression = c
	}
}

// Add adds a weighted observation.
func (t *TDigest) Add(value float64, weight uint64) {
	if weight == 0 {
		weight = 1
	}
	t.centroids = append(t.centroids, centroid{mean: value, count: weight})
	t.count += weight
	if len(t.centroids) >= maxCentroids {
		t.compress()
	}
}

// Merge merges another TDigest into this one.
func (t *TDigest) Merge(other *TDigest) {
	t.centroids = append(t.centroids, other.centroids...)
	t.count += other.count
	t.compress()
}

// Quantile estimates the value at the given quantile (0..1).
func (t *TDigest) Quantile(q float64) float64 {
	if len(t.centroids) == 0 {
		return 0
	}
	if q <= 0 {
		return t.centroids[0].mean
	}
	if q >= 1 {
		return t.centroids[len(t.centroids)-1].mean
	}
	t.compress()

	total := float64(t.count)
	target := q * total
	var cum float64
	for _, c := range t.centroids {
		cum += float64(c.count)
		if cum >= target {
			return c.mean
		}
	}
	return t.centroids[len(t.centroids)-1].mean
}

// compress merges nearby centroids.
func (t *TDigest) compress() {
	if len(t.centroids) <= 1 {
		return
	}
	sort.Slice(t.centroids, func(i, j int) bool {
		return t.centroids[i].mean < t.centroids[j].mean
	})

	compressed := make([]centroid, 0, len(t.centroids))
	for _, c := range t.centroids {
		if len(compressed) == 0 {
			compressed = append(compressed, c)
			continue
		}
		last := &compressed[len(compressed)-1]
		n := float64(last.count + c.count)
		threshold := 4 * float64(t.count) * math.Pi * t.compression / float64(len(compressed))
		delta := last.mean - c.mean
		if delta < 0 {
			delta = -delta
		}
		if float64(last.count+c.count) <= threshold && len(compressed) < maxCentroids {
			last.mean = (last.mean*float64(last.count) + c.mean*float64(c.count)) / n
			last.count += c.count
		} else {
			compressed = append(compressed, c)
		}
	}
	t.centroids = compressed
}

// Count returns the total number of observations.
func (t *TDigest) Count() uint64 { return t.count }

// Info returns metadata.
func (t *TDigest) Info() (totalObservations uint64, totalCentroids int, compression float64) {
	return t.count, len(t.centroids), t.compression
}

// Reset clears all data.
func (t *TDigest) Reset() {
	t.centroids = nil
	t.count = 0
}

// CentroidCount returns the number of centroids.
func (t *TDigest) CentroidCount() int { return len(t.centroids) }
