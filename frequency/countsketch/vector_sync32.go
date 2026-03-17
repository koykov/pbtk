package countsketch

// Synchronous 32/64-bit vector implementations. Generics approach is too slow in general, also there is no way
// to use atomics (in concurrent vector) together with generics.

import (
	"encoding/binary"
	"io"
	"unsafe"

	"github.com/koykov/pbtk"
	"github.com/koykov/simd/memclr"
)

const (
	dumpSignature32 = 0x52DFD9651F92338D
	dumpVersion32   = 1.0
)

// 32-bit version of sync vector implementation.
type syncvec32 struct {
	buf []int32
}

func (vec *syncvec32) add(pos uint64, delta int64) error {
	vec.buf[pos] += int32(delta)
	return nil
}

func (vec *syncvec32) estimate(pos uint64) int64 {
	return int64(vec.buf[pos])
}

func (vec *syncvec32) reset() {
	memclr.ClearUnsafe(unsafe.Pointer(&vec.buf[0]), len(vec.buf)*4)
}

func (vec *syncvec32) readFrom(r io.Reader) (n int64, err error) {
	var (
		buf [16]byte
		m   int
	)
	m, err = r.Read(buf[:])
	n += int64(m)
	if err != nil {
		return
	}

	if binary.LittleEndian.Uint64(buf[0:8]) != dumpSignature32 {
		err = pbtk.ErrInvalidSignature
		return
	}
	if binary.LittleEndian.Uint64(buf[8:16]) != dumpVersion32 {
		err = pbtk.ErrVersionMismatch
		return
	}

	h := vecbufh{
		p: uintptr(unsafe.Pointer(&vec.buf[0])),
		l: len(vec.buf) * 4,
		c: len(vec.buf) * 4,
	}
	bufv := *(*[]byte)(unsafe.Pointer(&h))
	m, err = r.Read(bufv)
	n += int64(m)
	return
}

func (vec *syncvec32) writeTo(w io.Writer) (n int64, err error) {
	var (
		buf [16]byte
		m   int
	)
	binary.LittleEndian.PutUint64(buf[0:8], dumpSignature32)
	binary.LittleEndian.PutUint64(buf[8:16], dumpVersion32)
	m, err = w.Write(buf[:])
	n += int64(m)
	if err != nil {
		return
	}

	h := vecbufh{
		p: uintptr(unsafe.Pointer(&vec.buf[0])),
		l: len(vec.buf) * 4,
		c: len(vec.buf) * 4,
	}
	bufv := *(*[]byte)(unsafe.Pointer(&h))
	m, err = w.Write(bufv)
	n += int64(m)
	return
}

func newVector32(d, w uint64) *syncvec32 {
	return &syncvec32{buf: make([]int32, d*w)}
}
