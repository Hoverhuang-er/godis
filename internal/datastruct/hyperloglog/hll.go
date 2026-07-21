// Package hyperloglog provides a HyperLogLog probabilistic data structure
// for cardinality estimation, compatible with Redis HyperLogLog.
package hyperloglog

import (
	"encoding/binary"
	"math"
	"math/bits"
)

const (
	hllRegisters = 16384 // 2^14 registers
	hllAlpha     = 0.7213 / (1 + 1.0/float64(hllRegisters))
	hllPP        = uint8(14)
)

// HLL represents a HyperLogLog data structure with 2^14 registers.
type HLL struct {
	registers [hllRegisters]uint8
}

// New creates a new HyperLogLog.
func New() *HLL {
	return &HLL{}
}

// Add adds a new element to the HLL estimate.
func (h *HLL) Add(hash uint64) {
	idx := hash >> (64 - hllPP)
	leading := bits.LeadingZeros64((hash << hllPP) | (1 << (hllPP - 1))) + 1
	if leading > 63 {
		leading = 63
	}
	r := uint8(leading)
	if r > h.registers[idx] {
		h.registers[idx] = r
	}
}

// Count returns the estimated cardinality.
func (h *HLL) Count() uint64 {
	var sum float64
	var zeroCount int
	for _, r := range h.registers {
		if r == 0 {
			zeroCount++
		}
		sum += 1.0 / float64(uint64(1)<<r)
	}
	est := hllAlpha * float64(hllRegisters*hllRegisters) / sum

	if est <= 2.5*float64(hllRegisters) {
		// Small range correction
		if zeroCount > 0 {
			est = float64(hllRegisters) * math.Log(float64(hllRegisters)/float64(zeroCount))
		}
	} else if est > 1.0/30.0*float64(1<<32) {
		// Large range correction
		est = -float64(1<<32) * math.Log(1.0-est/float64(1<<32))
	}
	return uint64(est + 0.5)
}

// Merge merges another HLL into this one (union).
func (h *HLL) Merge(other *HLL) {
	for i := range h.registers {
		if other.registers[i] > h.registers[i] {
			h.registers[i] = other.registers[i]
		}
	}
}

// Bytes returns the serialized HLL registers for storage/transfer.
func (h *HLL) Bytes() []byte {
	b := make([]byte, 2+hllRegisters)
	binary.BigEndian.PutUint16(b[0:2], hllRegisters)
	copy(b[2:], h.registers[:])
	return b
}

// FromBytes deserializes HLL registers from bytes.
func FromBytes(data []byte) *HLL {
	h := &HLL{}
	if len(data) >= 2+int(hllRegisters) {
		copy(h.registers[:], data[2:2+hllRegisters])
	}
	return h
}
