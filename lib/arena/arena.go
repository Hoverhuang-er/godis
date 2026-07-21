package arena

import (
	"math/bits"
	"sync"
	"sync/atomic"
)

const (
	defaultBlockSize = 64 * 1024  // 64KB per block
	maxBlockSize     = 4 * 1024 * 1024 // 4MB max block
)

// Arena is a bump-pointer allocator that pre-allocates large memory blocks
// and serves allocations from them, drastically reducing GC pressure.
// Thread-safe via atomic operations on the current block cursor.
type Arena struct {
	blocks    []*block
	blockSize int
	mu        sync.Mutex
}

type block struct {
	mem   []byte
	cursor int64
}

// New creates a new Arena with the given minimum block size.
// The arena grows by allocating new blocks when the current one is exhausted.
func New(blockSize int) *Arena {
	if blockSize <= 0 {
		blockSize = defaultBlockSize
	}
	if blockSize > maxBlockSize {
		blockSize = maxBlockSize
	}
	a := &Arena{
		blocks:    make([]*block, 0, 8),
		blockSize: blockSize,
	}
	a.allocBlock()
	return a
}

func (a *Arena) allocBlock() {
	size := a.blockSize
	if len(a.blocks) > 0 {
		// Double block size up to max on growth
		size = a.blockSize << uint(min(len(a.blocks), 6))
		if size > maxBlockSize {
			size = maxBlockSize
		}
	}
	b := &block{
		mem: make([]byte, size),
	}
	a.blocks = append(a.blocks, b)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Alloc allocates n bytes from the arena and returns a byte slice.
// The returned memory is valid until the arena is reset.
// Thread-safe.
func (a *Arena) Alloc(n int) []byte {
	if n <= 0 {
		return nil
	}
	// Align to 8 bytes
	n = (n + 7) & ^7
	if n > maxBlockSize {
		// Too large for arena, allocate directly
		return make([]byte, n)
	}

	a.mu.Lock()
	b := a.blocks[len(a.blocks)-1]
	cursor := atomic.AddInt64(&b.cursor, int64(n))
	if int(cursor) > len(b.mem) {
		// Current block exhausted, allocate new one
		if n > a.blockSize {
			bigBlock := &block{mem: make([]byte, n)}
			a.blocks = append(a.blocks, bigBlock)
			a.mu.Unlock()
			return bigBlock.mem[:n]
		}
		a.allocBlock()
		b = a.blocks[len(a.blocks)-1]
		atomic.StoreInt64(&b.cursor, int64(n))
		a.mu.Unlock()
		return b.mem[:n:n]
	}
	start := cursor - int64(n)
	a.mu.Unlock()
	return b.mem[start:cursor:start+int64(n)]
}

// AllocString allocates memory and copies a string into it, returning the []byte slice.
func (a *Arena) AllocString(s string) []byte {
	b := a.Alloc(len(s))
	copy(b, s)
	return b
}

// AllocBytes allocates memory and copies a byte slice into it.
func (a *Arena) AllocBytes(src []byte) []byte {
	b := a.Alloc(len(src))
	copy(b, src)
	return b
}

// Reset resets the arena, making all previously allocated memory available for reuse.
// NOT safe if any references to previously allocated memory still exist.
func (a *Arena) Reset() {
	a.mu.Lock()
	for _, b := range a.blocks {
		atomic.StoreInt64(&b.cursor, 0)
	}
	// Keep only the first block, free the rest
	if len(a.blocks) > 1 {
		a.blocks = a.blocks[:1]
		a.blocks[0] = &block{
			mem: make([]byte, a.blockSize),
		}
	}
	a.mu.Unlock()
}

// TotalAllocated returns the total bytes currently allocated from all blocks.
func (a *Arena) TotalAllocated() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	var total int64
	for _, b := range a.blocks {
		total += atomic.LoadInt64(&b.cursor)
	}
	return total
}

// TotalCapacity returns the total capacity of all blocks.
func (a *Arena) TotalCapacity() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	var total int64
	for _, b := range a.blocks {
		total += int64(len(b.mem))
	}
	return total
}

// --- Per-connection arena pool ---

var connArenas sync.Pool

func init() {
	connArenas = sync.Pool{
		New: func() interface{} {
			return New(defaultBlockSize)
		},
	}
}

// GetConnArena retrieves a per-connection arena from the pool.
func GetConnArena() *Arena {
	return connArenas.Get().(*Arena)
}

// PutConnArena returns a per-connection arena to the pool after resetting it.
func PutConnArena(a *Arena) {
	a.Reset()
	connArenas.Put(a)
}

// --- Global arena for database entities ---

var globalArena = New(256 * 1024) // 256KB initial blocks

// Global returns the global arena for database entity allocations.
func Global() *Arena {
	return globalArena
}

// --- Slab allocator for fixed-size objects ---

// SlabAllocator manages fixed-size object allocation to reduce GC churn
// from frequently created temporary objects.
type SlabAllocator struct {
	elementSize int
	slabSize    int
	freelist    *sync.Pool
}

// NewSlab creates a slab allocator for objects of the given element size.
func NewSlab(elementSize, slabSize int) *SlabAllocator {
	if elementSize <= 0 {
		elementSize = 64
	}
	if slabSize <= 0 {
		slabSize = 4096
	}
	// Round element size to 8 bytes
	elementSize = (elementSize + 7) & ^7
	elemsPerSlab := slabSize / elementSize
	if elemsPerSlab < 1 {
		elemsPerSlab = 1
	}
	actualSlabSize := elemsPerSlab * elementSize

	return &SlabAllocator{
		elementSize: elementSize,
		slabSize:    actualSlabSize,
		freelist: &sync.Pool{
			New: func() interface{} {
				return make([]byte, actualSlabSize)
			},
		},
	}
}

// Alloc returns a byte slice of the configured element size.
func (s *SlabAllocator) Alloc() []byte {
	slab := s.freelist.Get().([]byte)
	return slab[:s.elementSize]
}

// Free returns the slab to the pool for reuse.
func (s *SlabAllocator) Free(buf []byte) {
	if cap(buf) < s.slabSize {
		return
	}
	s.freelist.Put(buf[:s.slabSize])
}

// --- Memory-efficient byte buffer pool ---

type BufferPool struct {
	pool sync.Pool
}

// NewBufferPool creates a buffer pool for byte slices of varying sizes.
func NewBufferPool() *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 1024)
				return &buf
			},
		},
	}
}

// Get returns a byte buffer of at least the given size.
func (bp *BufferPool) Get(size int) []byte {
	ptr := bp.pool.Get().(*[]byte)
	buf := *ptr
	if cap(buf) < size {
		// Grow to next power of 2
		newSize := 1 << uint(bits.Len32(uint32(size-1)))
		buf = make([]byte, newSize)
		*ptr = buf
	}
	return buf[:size]
}

// Put returns a byte buffer to the pool.
func (bp *BufferPool) Put(buf []byte) {
	// Only keep reasonable sizes (64KB max)
	if cap(buf) > 65536 {
		return
	}
	bp.pool.Put(&buf)
}

var globalBufferPool = NewBufferPool()

// BufferPoolGlobal returns the global buffer pool.
func BufferPoolGlobal() *BufferPool {
	return globalBufferPool
}
