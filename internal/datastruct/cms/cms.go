// Package cms provides a Count-Min Sketch probabilistic data structure
// for frequency estimation, compatible with Redis CMS.
package cms

import "math"

// CMS is a Count-Min Sketch for frequency estimation.
type CMS struct {
	width uint
	depth uint
	count uint64
	cells []uint64
}

// NewByDim creates a CMS with the given dimensions.
func NewByDim(width, depth uint) *CMS {
	if width < 1 {
		width = 1
	}
	if depth < 1 {
		depth = 1
	}
	return &CMS{
		width: width,
		depth: depth,
		cells: make([]uint64, width*depth),
	}
}

// NewByProb creates a CMS from error probability and confidence.
func NewByProb(errorRate, confidence float64) *CMS {
	width := uint(math.Ceil(math.E / errorRate))
	depth := uint(math.Ceil(-math.Log(1 - confidence)))
	return NewByDim(width, depth)
}

func hash(d []byte, seed uint64) uint64 {
	h := seed
	for _, b := range d {
		h ^= uint64(b)
		h *= 0x9e3779b97f4a7c55
		h ^= h >> 31
	}
	return h
}

func (c *CMS) idx(d []byte, row uint) uint {
	h := hash(d, uint64(row))
	return uint(uint64(row)*uint64(c.width) + (h % uint64(c.width)))
}

// IncrBy increments the count for an item by the given value.
func (c *CMS) IncrBy(item string, increment uint64) {
	data := []byte(item)
	for i := uint(0); i < c.depth; i++ {
		c.cells[c.idx(data, i)] += increment
	}
	c.count += increment
}

// Query returns the estimated count of an item (overestimate, but bounded).
func (c *CMS) Query(item string) uint64 {
	data := []byte(item)
	min := uint64(1<<64 - 1)
	for i := uint(0); i < c.depth; i++ {
		if v := c.cells[c.idx(data, i)]; v < min {
			min = v
		}
	}
	return min
}

// Merge combines another CMS of the same dimensions into this one.
func (c *CMS) Merge(other *CMS) error {
	if c.width != other.width || c.depth != other.depth {
		return ErrDimMismatch
	}
	for i := range c.cells {
		c.cells[i] += other.cells[i]
	}
	c.count += other.count
	return nil
}

// Info returns metadata about the CMS.
func (c *CMS) Info() (width, depth uint, count uint64) {
	return c.width, c.depth, c.count
}

var ErrDimMismatch = &errDimMismatch{}

type errDimMismatch struct{}

func (e *errDimMismatch) Error() string { return "ERR dimension mismatch" }
