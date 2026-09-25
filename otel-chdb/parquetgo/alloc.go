package parquetgo

import (
	"math/bits"
	"sync"
)

// poolAllocator is an arrow memory.Allocator that keeps freed buffers in
// power-of-two size classes and hands them out again, so an encoder that
// builds a batch, writes it and releases it reaches a steady state with no
// Go allocation for its column buffers. The Go allocator instead makes (and
// zeroes, and later garbage-collects) tens of MB per 10k-span batch.
//
// It holds on to the peak working set of one batch per encoder; that is
// the price of not allocating.
type poolAllocator struct {
	mu   sync.Mutex
	free [48][][]byte
}

const minClass = 6 // 64 bytes, arrow's alignment

func class(n int) int {
	if n <= 1<<minClass {
		return minClass
	}
	return bits.Len(uint(n - 1))
}

func (a *poolAllocator) Allocate(size int) []byte {
	c := class(size)
	a.mu.Lock()
	l := a.free[c]
	if n := len(l); n > 0 {
		b := l[n-1]
		a.free[c] = l[:n-1]
		a.mu.Unlock()
		b = b[:size]
		clear(b)
		return b
	}
	a.mu.Unlock()
	return make([]byte, size, 1<<c)
}

func (a *poolAllocator) Reallocate(size int, b []byte) []byte {
	if size <= cap(b) {
		old := len(b)
		b = b[:size]
		if size > old {
			clear(b[old:])
		}
		return b
	}
	nb := a.Allocate(size)
	copy(nb, b)
	a.Free(b)
	return nb
}

func (a *poolAllocator) Free(b []byte) {
	c := cap(b)
	if c < 1<<minClass || c&(c-1) != 0 {
		return // not one of ours
	}
	a.mu.Lock()
	k := bits.Len(uint(c)) - 1
	a.free[k] = append(a.free[k], b[:0])
	a.mu.Unlock()
}
