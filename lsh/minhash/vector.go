package minhash

import (
	"unsafe"

	"github.com/koykov/simd/memclr"
	"github.com/koykov/simd/memset"
)

type Vector interface {
	// Grow grows the vector to the given capacity.
	Grow(cap uint64)
	// Add adds a v to the vector.
	Add(v uint64)
	// SetMin sets the value v at the given position if v less that current value.
	SetMin(pos, v uint64)
	// Memset sets all values to the given value.
	Memset(v uint64)
	// Get returns stored value at given position.
	Get(pos uint64) uint64
	// AppendAll appends all values to the given slice.
	AppendAll(dst []uint64) []uint64
	// Len returns the number of values in the vector.
	Len() uint64
	// Reset resets the vector.
	Reset()
}

// DefaultVector represents list of uint64 values.
type DefaultVector []uint64

func (v *DefaultVector) Grow(cap_ uint64) {
	if cap(*v) < int(cap_) {
		*v = make([]uint64, cap_)
	}
	*v = (*v)[:cap_]
}

func (v *DefaultVector) Add(val uint64) {
	*v = append(*v, val)
}

func (v *DefaultVector) SetMin(pos, val uint64) {
	(*v)[pos] = min((*v)[pos], val)
}

func (v *DefaultVector) Memset(val uint64) {
	memset.Memset64(*v, val)
}

func (v *DefaultVector) Get(pos uint64) uint64 {
	return (*v)[pos]
}

func (v *DefaultVector) AppendAll(dst []uint64) []uint64 {
	dst = append(dst, *v...)
	return dst
}

func (v *DefaultVector) Len() uint64 {
	return uint64(len(*v))
}

func (v *DefaultVector) Reset() {
	memclr.ClearUnsafe(unsafe.Pointer(&(*v)[0]), len(*v)*8)
}
