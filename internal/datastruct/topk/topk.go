// Package topk provides a Top-K data structure for finding the most frequent
// items in a data stream, compatible with Redis Top-K.
package topk

import (
	"sort"
	"strings"
)

type item struct {
	key   string
	count uint64
	error uint64 // count-min sketch counting error
}

// TopK maintains the top-k most frequent items.
type TopK struct {
	k       uint
	items   []*item
	cms     []uint64
	cmsW    uint
	cmsDepth uint
}

// New creates a new Top-K with the given k.
func New(k uint) *TopK {
	if k < 1 {
		k = 1
	}
	width := k * 8
	depth := uint(7)
	return &TopK{
		k:        k,
		items:    make([]*item, 0, k),
		cms:      make([]uint64, width*depth),
		cmsW:     width,
		cmsDepth: depth,
	}
}

func (t *TopK) cmsIdx(d []byte, row uint) uint {
	h := uint(hash(d, uint64(row)))
	return (row * t.cmsW) + (h % t.cmsW)
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

func (t *TopK) cmsIncr(d []byte, val uint64) {
	for i := uint(0); i < t.cmsDepth; i++ {
		t.cms[t.cmsIdx(d, i)] += val
	}
}

func (t *TopK) cmsQuery(d []byte) uint64 {
	min := uint64(1<<64 - 1)
	for i := uint(0); i < t.cmsDepth; i++ {
		if v := t.cms[t.cmsIdx(d, i)]; v < min {
			min = v
		}
	}
	return min
}

// Add adds an item and returns whether it is in the top-k.
func (t *TopK) Add(key string) (added bool, droppedKey string, droppedCount uint64) {
	data := []byte(key)
	hit := t.cmsQuery(data)

	// Check if already in top-k
	for _, it := range t.items {
		if it.key == key {
			it.count++
			it.error = t.cmsQuery(data) - 1
			t.cmsIncr(data, 1)
			t.rebuild()
			return false, "", 0
		}
	}

	if uint(len(t.items)) < t.k {
		t.items = append(t.items, &item{key: key, count: hit + 1, error: hit})
		t.cmsIncr(data, 1)
		t.rebuild()
		return true, "", 0
	}

	// Find minimum in top-k
	minIdx := 0
	for i := 1; i < len(t.items); i++ {
		if t.items[i].count < t.items[minIdx].count {
			minIdx = i
		}
	}

	min := t.items[minIdx]
	if hit > min.count {
		droppedKey = min.key
		droppedCount = min.count
		t.items[minIdx] = &item{key: key, count: hit + 1, error: hit}
		t.cmsIncr(data, 1)
		t.rebuild()
		return true, droppedKey, droppedCount
	}

	t.cmsIncr(data, 1)
	return false, "", 0
}

// Count returns the estimated count of an item.
func (t *TopK) Count(key string) uint64 {
	for _, it := range t.items {
		if it.key == key {
			return it.count
		}
	}
	return t.cmsQuery([]byte(key))
}

// Query checks if an item is in the top-k.
func (t *TopK) Query(key string) bool {
	hit := t.cmsQuery([]byte(key))
	for _, it := range t.items {
		if it.key == key && hit >= it.count-it.error {
			return true
		}
	}
	return false
}

// List returns all items in the top-k.
func (t *TopK) List() []string {
	result := make([]string, len(t.items))
	for i, it := range t.items {
		result[i] = it.key
	}
	return result
}

// ListWithCount returns all items with their counts.
func (t *TopK) ListWithCount() map[string]uint64 {
	result := make(map[string]uint64, len(t.items))
	for _, it := range t.items {
		result[it.key] = it.count
	}
	return result
}

func (t *TopK) rebuild() {
	if len(t.items) <= 1 {
		return
	}
	sort.Slice(t.items, func(i, j int) bool {
		if t.items[i].count != t.items[j].count {
			return t.items[i].count > t.items[j].count
		}
		return strings.Compare(t.items[i].key, t.items[j].key) < 0
	})
	if uint(len(t.items)) > t.k {
		t.items = t.items[:t.k]
	}
}
