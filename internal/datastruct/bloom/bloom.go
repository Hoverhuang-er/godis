// Package bloom provides a Bloom filter probabilistic data structure
// for membership testing, compatible with Redis Bloom filter.
package bloom

import (
	"encoding/binary"
	"math"
)

// Bloom is a standard Bloom filter.
type Bloom struct {
	bits     []uint64
	m        uint64 // number of bits
	k        uint   // number of hash functions
	count    uint64
	capacity uint64
	errorRate float64
}

// New creates a new Bloom filter with the given capacity and error rate.
func New(capacity uint64, errorRate float64) *Bloom {
	if capacity == 0 {
		capacity = 100
	}
	if errorRate <= 0 || errorRate >= 1 {
		errorRate = 0.01
	}
	m := optimalM(capacity, errorRate)
	k := optimalK(m, capacity)
	return &Bloom{
		bits:      make([]uint64, (m+63)/64),
		m:         m,
		k:         k,
		capacity:  capacity,
		errorRate: errorRate,
	}
}

func optimalM(n uint64, p float64) uint64 {
	return uint64(math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)))
}

func optimalK(m uint64, n uint64) uint {
	return uint(math.Ceil(float64(m) / float64(n) * math.Ln2))
}

// hash computes the i-th hash value for the given data.
func (b *Bloom) hash(data []byte, i uint) uint64 {
	h1 := hash64(data, uint64(i*2+1))
	h2 := hash64(data, uint64(i*2+2))
	return h1 + uint64(i)*h2
}

// hash64 computes a 64-bit hash from data with a seed.
func hash64(data []byte, seed uint64) uint64 {
	h := seed
	for _, b := range data {
		h ^= uint64(b)
		h *= 0x9e3779b97f4a7c55
		h ^= h >> 31
	}
	return h
}

// Add adds an element to the bloom filter.
func (b *Bloom) Add(data []byte) {
	for i := uint(0); i < b.k; i++ {
		bit := b.hash(data, i) % b.m
		b.bits[bit/64] |= 1 << (bit % 64)
	}
	b.count++
}

// Exists checks if an element has been added to the bloom filter.
func (b *Bloom) Exists(data []byte) bool {
	for i := uint(0); i < b.k; i++ {
		bit := b.hash(data, i) % b.m
		if b.bits[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// Info returns metadata about the bloom filter.
func (b *Bloom) Info() (capacity uint64, itemCount uint64, errorRate float64, bitsPerItem float64) {
	return b.capacity, b.count, b.errorRate, float64(b.m) / float64(b.capacity)
}

// Merge merges another Bloom filter of the same size into this one.
func (b *Bloom) Merge(other *Bloom) error {
	if len(b.bits) != len(other.bits) {
		return ErrSizeMismatch
	}
	for i := range b.bits {
		b.bits[i] |= other.bits[i]
	}
	b.count += other.count
	return nil
}

var ErrSizeMismatch = &errSizeMismatch{}

type errSizeMismatch struct{}

func (e *errSizeMismatch) Error() string { return "ERR Bloom filter size mismatch" }

// InsertCount inserts an element and returns whether it might have been newly inserted.
func (b *Bloom) InsertCount(data []byte) uint64 {
	exists := b.Exists(data)
	b.Add(data)
	if exists {
		return 0
	}
	return 1
}

// Bytes returns serialized Bloom filter.
func (b *Bloom) Bytes() []byte {
	bitsLen := len(b.bits)
	buf := make([]byte, 16+bitsLen*8)
	binary.LittleEndian.PutUint64(buf[0:8], b.m)
	binary.LittleEndian.PutUint64(buf[8:16], b.count)
	for i, v := range b.bits {
		binary.LittleEndian.PutUint64(buf[16+i*8:16+(i+1)*8], v)
	}
	return buf
}
