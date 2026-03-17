package countsketch

import (
	"encoding/binary"
	"io"
	"unsafe"

	"github.com/koykov/pbtk"
	"github.com/koykov/simd/memclr"
)

const (
	dumpSignature64 = 0x90302A55BFF75940
	dumpVersion64   = 1.0
)

// 64-bit version if sync vector implementation.
type syncvec64 struct {
	buf []int64
}

func (vec *syncvec64) add(pos uint64, delta int64) error {
	vec.buf[pos] += delta
	return nil
}

func (vec *syncvec64) estimate(pos uint64) int64 {
	return vec.buf[pos]
}

func (vec *syncvec64) reset() {
	memclr.ClearUnsafe(unsafe.Pointer(&vec.buf[0]), len(vec.buf)*8)
}

func (vec *syncvec64) readFrom(r io.Reader) (n int64, err error) {
	var (
		buf [16]byte
		m   int
	)
	m, err = r.Read(buf[:])
	n += int64(m)
	if err != nil {
		return
	}

	if binary.LittleEndian.Uint64(buf[0:8]) != dumpSignature64 {
		err = pbtk.ErrInvalidSignature
		return
	}
	if binary.LittleEndian.Uint64(buf[8:16]) != dumpVersion64 {
		err = pbtk.ErrVersionMismatch
		return
	}

	h := vecbufh{
		p: uintptr(unsafe.Pointer(&vec.buf[0])),
		l: len(vec.buf) * 8,
		c: len(vec.buf) * 8,
	}
	bufv := *(*[]byte)(unsafe.Pointer(&h))
	m, err = r.Read(bufv)
	n += int64(m)
	return
}

func (vec *syncvec64) writeTo(w io.Writer) (n int64, err error) {
	var (
		buf [16]byte
		m   int
	)
	binary.LittleEndian.PutUint64(buf[0:8], dumpSignature64)
	binary.LittleEndian.PutUint64(buf[8:16], dumpVersion64)
	m, err = w.Write(buf[:])
	n += int64(m)
	if err != nil {
		return
	}

	h := vecbufh{
		p: uintptr(unsafe.Pointer(&vec.buf[0])),
		l: len(vec.buf) * 8,
		c: len(vec.buf) * 8,
	}
	bufv := *(*[]byte)(unsafe.Pointer(&h))
	m, err = w.Write(bufv)
	n += int64(m)
	return
}

func newVector64(d, w uint64) *syncvec64 {
	return &syncvec64{buf: make([]int64, d*w)}
}
