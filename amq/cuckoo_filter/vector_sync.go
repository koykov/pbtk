package cuckoo

import (
	"encoding/binary"
	"io"
	"math"
	"unsafe"

	"github.com/koykov/pbtk"
	"github.com/koykov/simd/memclr"
)

const (
	syncvecDumpSignature = 0x19329bb7706377b1
	syncvecDumpVersion   = 1.0
)

// Synchronized vector implementation.
type syncvec struct {
	buf []uint32
	s   uint64
}

func (vec *syncvec) add(i uint64, fp byte) error {
	for j := 0; j < bucketsz; j++ {
		if vec.buf[i]&vecmask[j] == 0 {
			vec.buf[i] |= uint32(fp) << (j * 8)
			vec.s++
			return nil
		}
	}
	return ErrFullBucket
}

func (vec *syncvec) set(i, j uint64, fp byte) error {
	vec.buf[i] |= uint32(fp) << (j * 8)
	return nil
}

func (vec *syncvec) unset(i uint64, fp byte) bool {
	for j := 0; j < bucketsz; j++ {
		if vec.buf[i]&vecmask[j] == uint32(fp)<<(j*8) {
			vec.buf[i] &= ^vecmask[j]
			vec.s--
			return true
		}
	}
	return false
}

func (vec *syncvec) fpv(i, j uint64) byte {
	return byte(vec.buf[i] & vecmask[j] >> (j * 8))
}

func (vec *syncvec) fpi(i uint64, fp byte) int {
	for j := 0; j < bucketsz; j++ {
		if vec.buf[i]&vecmask[j] == uint32(fp)<<(j*8) {
			return j
		}
	}
	return -1
}

func (vec *syncvec) capacity() uint64 {
	return uint64(len(vec.buf))
}

func (vec *syncvec) size() uint64 {
	return vec.s
}

func (vec *syncvec) reset() {
	memclr.ClearUnsafe(unsafe.Pointer(&vec.buf[0]), len(vec.buf)*bucketsz)
}

func (vec *syncvec) writeTo(w io.Writer) (n int64, err error) {
	var (
		buf [24]byte
		m   int
	)
	binary.LittleEndian.PutUint64(buf[0:8], syncvecDumpSignature)
	binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(syncvecDumpVersion))
	binary.LittleEndian.PutUint64(buf[16:24], vec.s)
	m, err = w.Write(buf[:])
	n += int64(m)
	if err != nil {
		return int64(m), err
	}

	var h struct {
		p    uintptr
		l, c int
	}
	h.p = uintptr(unsafe.Pointer(&vec.buf[0]))
	h.l, h.c = len(vec.buf)*4, cap(vec.buf)*4
	payload := *(*[]byte)(unsafe.Pointer(&h))
	m, err = w.Write(payload)
	n += int64(m)
	return
}

func (vec *syncvec) readFrom(r io.Reader) (n int64, err error) {
	var (
		buf [24]byte
		m   int
	)
	m, err = r.Read(buf[:])
	n += int64(m)
	if err != nil {
		return n, err
	}

	sign, ver, s := binary.LittleEndian.Uint64(buf[0:8]), binary.LittleEndian.Uint64(buf[8:16]),
		binary.LittleEndian.Uint64(buf[16:24])

	if sign != syncvecDumpSignature {
		return n, pbtk.ErrInvalidSignature
	}
	if ver != math.Float64bits(syncvecDumpVersion) {
		return n, pbtk.ErrVersionMismatch
	}
	vec.s = s

	var h struct {
		p    uintptr
		l, c int
	}
	h.p = uintptr(unsafe.Pointer(&vec.buf[0]))
	h.l, h.c = len(vec.buf)*4, cap(vec.buf)*4
	payloadBuf := *(*[]byte)(unsafe.Pointer(&h))

	m, err = r.Read(payloadBuf)
	n += int64(m)
	if err == io.EOF {
		err = nil
	}
	return
}

func newSyncvec(size uint64) *syncvec {
	return &syncvec{buf: make([]uint32, size)}
}
