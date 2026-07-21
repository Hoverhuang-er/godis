package array

import "sync"

// Array is a concurrent-safe array structure storing byte slices with nil support.
// Each slot can hold a []byte or nil, enabling sparse array semantics.
type Array struct {
	mu  sync.RWMutex
	arr []*[]byte
}

// Make creates a new Array with optional initial items.
func Make(items ...[]byte) *Array {
	arr := &Array{}
	for _, item := range items {
		v := make([]byte, len(item))
		copy(v, item)
		elem := v
		arr.arr = append(arr.arr, &elem)
	}
	return arr
}

// Get returns the value at the given index, or nil if out of bounds or null slot.
func (a *Array) Get(index int) []byte {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if index < 0 || index >= len(a.arr) {
		return nil
	}
	slot := a.arr[index]
	if slot == nil {
		return nil
	}
	return *slot
}

// Set stores value at the given index, auto-expanding the array if needed.
func (a *Array) Set(index int, value []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if index >= len(a.arr) {
		newArr := make([]*[]byte, index+1)
		copy(newArr, a.arr)
		a.arr = newArr
	}
	v := make([]byte, len(value))
	copy(v, value)
	a.arr[index] = &v
}

// MultiSet sets multiple index-value pairs atomically.
func (a *Array) MultiSet(pairs ...struct {
	Index int
	Value []byte
}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, p := range pairs {
		if p.Index >= len(a.arr) {
			newArr := make([]*[]byte, p.Index+1)
			copy(newArr, a.arr)
			a.arr = newArr
		}
		v := make([]byte, len(p.Value))
		copy(v, p.Value)
		a.arr[p.Index] = &v
	}
}

// MultiGet returns values at the given indices.
func (a *Array) MultiGet(indices []int) [][]byte {
	a.mu.RLock()
	defer a.mu.RUnlock()
	result := make([][]byte, len(indices))
	for i, idx := range indices {
		if idx < 0 || idx >= len(a.arr) {
			result[i] = nil
		} else if a.arr[idx] == nil {
			result[i] = nil
		} else {
			result[i] = *a.arr[idx]
		}
	}
	return result
}

// Len returns the length of the array (including null slots).
func (a *Array) Len() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.arr)
}

// Count returns the number of elements matching the given value.
// If value is nil, counts all non-null elements.
func (a *Array) Count(value []byte) int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	count := 0
	valueIsNil := value == nil
	for i := range a.arr {
		if a.arr[i] == nil {
			continue
		}
		if valueIsNil {
			count++
		} else if string(*a.arr[i]) == string(value) {
			count++
		}
	}
	return count
}

// Append adds values to the end of the array.
func (a *Array) Append(values ...[]byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, v := range values {
		val := make([]byte, len(v))
		copy(val, v)
		elem := val
		a.arr = append(a.arr, &elem)
	}
}

// Insert inserts values at the given index, shifting existing elements right.
func (a *Array) Insert(index int, values ...[]byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if index < 0 || index > len(a.arr) {
		return
	}
	inserts := make([]*[]byte, len(values))
	for i, v := range values {
		val := make([]byte, len(v))
		copy(val, v)
		inserts[i] = &val
	}
	a.arr = append(a.arr[:index], append(inserts, a.arr[index:]...)...)
}

// Remove removes up to count occurrences of value. count < 0 means unlimited.
func (a *Array) Remove(value []byte, count int) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	if count == 0 {
		return 0
	}
	removed := 0
	var result []*[]byte
	valueStr := string(value)
	for _, slot := range a.arr {
		if slot == nil {
			result = append(result, nil)
		} else if string(*slot) == valueStr {
			if count > 0 && removed >= count {
				result = append(result, slot)
			} else {
				removed++
			}
		} else {
			result = append(result, slot)
		}
	}
	a.arr = result
	return removed
}

// Pop removes and returns the last n elements from the array.
func (a *Array) Pop(n int) [][]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n <= 0 {
		return nil
	}
	if n > len(a.arr) {
		n = len(a.arr)
	}
	start := len(a.arr) - n
	popped := make([][]byte, n)
	for i := 0; i < n; i++ {
		slot := a.arr[start+i]
		if slot != nil {
			popped[i] = *slot
		}
	}
	a.arr = a.arr[:start]
	return popped
}

// Trim retains elements within [start, end] inclusive.
func (a *Array) Trim(start, end int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if start < 0 || start >= len(a.arr) || end < 0 || start > end {
		a.arr = nil
		return
	}
	if end >= len(a.arr) {
		end = len(a.arr) - 1
	}
	a.arr = a.arr[start : end+1]
}

// Info returns the total length and number of non-null elements.
func (a *Array) Info() (length int, nonNullCount int) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	length = len(a.arr)
	for _, slot := range a.arr {
		if slot != nil {
			nonNullCount++
		}
	}
	return
}

// ForEach iterates over all elements, passing index and value (nil for null slots).
// Stops early if cb returns false.
func (a *Array) ForEach(cb func(index int, value []byte) bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for i, slot := range a.arr {
		if slot != nil {
			if !cb(i, *slot) {
				return
			}
		} else {
			if !cb(i, nil) {
				return
			}
		}
	}
}

// ToSlice returns a copy of the underlying array as []*[]byte.
func (a *Array) ToSlice() []*[]byte {
	a.mu.RLock()
	defer a.mu.RUnlock()
	result := make([]*[]byte, len(a.arr))
	for i, slot := range a.arr {
		if slot != nil {
			v := make([]byte, len(*slot))
			copy(v, *slot)
			result[i] = &v
		}
	}
	return result
}

// FromSlice replaces the array contents with a copy of the given slice.
func (a *Array) FromSlice(s []*[]byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.arr = make([]*[]byte, len(s))
	for i, slot := range s {
		if slot != nil {
			v := make([]byte, len(*slot))
			copy(v, *slot)
			a.arr[i] = &v
		}
	}
}
