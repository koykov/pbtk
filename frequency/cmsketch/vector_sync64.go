package cmsketch

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
	dumpSignature64 = 0x643E037364AB8CD0
	dumpVersion64   = 1.0
)

// 64-bit version if sync vector implementation.
type syncvec64 struct {
	basevec
	buf []uint64
}

func (vec *syncvec64) add(hkey, delta uint64) error {
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

func (vec *syncvec64) addClassic(lo, hi uint32, delta uint64) error {
	for i := uint64(0); i < vec.d; i++ {
		vec.buf[vecpos(lo, hi, vec.w, i)] += delta
	}
	return nil
}

func (vec *syncvec64) addCU(lo, hi uint32, delta uint64) error {
	var mn uint64 = math.MaxUint64
	for i := uint64(0); i < vec.d; i++ {
		mn = min(mn, vec.buf[vecpos(lo, hi, vec.w, i)])
	}
	for i := uint64(0); i < vec.d; i++ {
		pos := vecpos(lo, hi, vec.w, i)
		if vec.buf[pos] == mn {
			vec.buf[pos] += delta
		}
	}
	return nil
}

func (vec *syncvec64) addLFU(lo, hi uint32, delta uint64) error {
	return vec.addClassic(lo, hi, delta)
}

func (vec *syncvec64) addDLC(lo, hi uint32, delta uint64) error {
	var (
		mn  uint64 = math.MaxUint64
		mnp uint64 = math.MaxUint64
	)
	for i := uint64(0); i < vec.d; i++ {
		pos := vecpos(lo, hi, vec.w, i)
		if vec.buf[pos] < mn {
			mn, mnp = vec.buf[pos], pos
		}
	}
	if mnp < uint64(len(vec.buf)) {
		vec.buf[mnp] += delta
	}
	return nil
}

func (vec *syncvec64) estimate(hkey uint64) (r uint64) {
	lo, hi := uint32(hkey>>32), uint32(hkey)
	for i := uint64(0); i < vec.d; i++ {
		if ce := vec.buf[vecpos(lo, hi, vec.w, i)]; r == 0 || r > ce {
			r = ce
		}
	}
	return
}

func (vec *syncvec64) decay(ctx context.Context, factor float64) error {
	for i := 0; i < len(vec.buf); i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			vec.buf[i] = uint64(float64(vec.buf[i]) * factor)
		}
	}
	return nil
}

func (vec *syncvec64) reset() {
	memclr.ClearUnsafe(unsafe.Pointer(&vec.buf[0]), int(vec.w*vec.d*8))
}

func (vec *syncvec64) readFrom(r io.Reader) (n int64, err error) {
	var (
		buf [24]byte
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
	vec.flags = bitset.Bitset64(binary.LittleEndian.Uint64(buf[16:24]))

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
		buf [24]byte
		m   int
	)
	binary.LittleEndian.PutUint64(buf[0:8], dumpSignature64)
	binary.LittleEndian.PutUint64(buf[8:16], dumpVersion64)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(vec.flags))
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

func newVector64(d, w uint64, flags bitset.Bitset64) *syncvec64 {
	return &syncvec64{
		basevec: basevec{d: d, w: w, flags: flags},
		buf:     make([]uint64, d*w),
	}
}
