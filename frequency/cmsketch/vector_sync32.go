package cmsketch

// Synchronous 32/64-bit vector implementations. Generics approach is too slow in general, also there is no way
// to use atomics (in concurrent vector) together with generics.

import (
	"context"
	"encoding/binary"
	"io"
	"math"
	"unsafe"

	"github.com/koykov/bitset"
	"github.com/koykov/pbtk"
	"github.com/koykov/simd/memclr"
)

const (
	dumpSignature32 = 0x86BB26BA91E98EAD
	dumpVersion32   = 1.0
)

// 32-bit version of sync vector implementation.
type syncvec32 struct {
	basevec
	buf []uint32
}

func (vec *syncvec32) add(hkey, delta uint64) error {
	lo, hi := uint32(hkey>>32), uint32(hkey)
	switch {
	case vec.flags.CheckBit(flagConservativeUpdate):
		return vec.addCU(lo, hi, delta)
	case vec.flags.CheckBit(flagLFU):
		return vec.addLFU(lo, hi, delta)
	case vec.flags.CheckBit(flagDLC):
		return vec.addDLC(lo, hi, delta)
	default:
		return vec.addClassic(lo, hi, delta)
	}
}

func (vec *syncvec32) addClassic(lo, hi uint32, delta uint64) error {
	for i := uint64(0); i < vec.d; i++ {
		vec.buf[vecpos(lo, hi, vec.w, i)] += uint32(delta)
	}
	return nil
}

func (vec *syncvec32) addCU(lo, hi uint32, delta uint64) error {
	var mn uint32 = math.MaxUint32
	for i := uint64(0); i < vec.d; i++ {
		mn = min(mn, vec.buf[vecpos(lo, hi, vec.w, i)])
	}
	for i := uint64(0); i < vec.d; i++ {
		pos := vecpos(lo, hi, vec.w, i)
		if vec.buf[pos] == mn {
			vec.buf[pos] += uint32(delta)
		}
	}
	return nil
}

func (vec *syncvec32) addLFU(lo, hi uint32, delta uint64) error {
	return vec.addClassic(lo, hi, delta)
}

func (vec *syncvec32) addDLC(lo, hi uint32, delta uint64) error {
	var (
		mn  uint32 = math.MaxUint32
		mnp uint64 = math.MaxUint64
	)
	for i := uint64(0); i < vec.d; i++ {
		pos := vecpos(lo, hi, vec.w, i)
		if val := vec.buf[pos]; val < mn {
			mn, mnp = val, pos
		}
	}
	if mnp < uint64(len(vec.buf)) {
		vec.buf[mnp] += uint32(delta)
	}
	return nil
}

func (vec *syncvec32) estimate(hkey uint64) (r uint64) {
	lo, hi := uint32(hkey>>32), uint32(hkey)
	for i := uint64(0); i < vec.d; i++ {
		if ce := uint64(vec.buf[vecpos(lo, hi, vec.w, i)]); r == 0 || r > ce {
			r = ce
		}
	}
	return
}

func (vec *syncvec32) decay(ctx context.Context, factor float64) error {
	for i := 0; i < len(vec.buf); i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			vec.buf[i] = uint32(float64(vec.buf[i]) * factor)
		}
	}
	return nil
}

func (vec *syncvec32) reset() {
	memclr.ClearUnsafe(unsafe.Pointer(&vec.buf[0]), int(vec.w*vec.d*4))
}

func (vec *syncvec32) readFrom(r io.Reader) (n int64, err error) {
	var (
		buf [24]byte
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
	vec.flags = bitset.Bitset64(binary.LittleEndian.Uint64(buf[16:24]))

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
		buf [24]byte
		m   int
	)
	binary.LittleEndian.PutUint64(buf[0:8], dumpSignature32)
	binary.LittleEndian.PutUint64(buf[8:16], dumpVersion32)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(vec.flags))
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

func newVector32(d, w uint64, flags bitset.Bitset64) *syncvec32 {
	return &syncvec32{
		basevec: basevec{d: d, w: w, flags: flags},
		buf:     make([]uint32, d*w),
	}
}
