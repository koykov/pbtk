package xor

import (
	"encoding/binary"
	"io"
	"math"
	"math/bits"
	"sync"
	"unsafe"

	"github.com/koykov/pbtk"
	"github.com/koykov/pbtk/amq"
	"github.com/koykov/simd/memclr"
)

const (
	dumpSignature = 0x3172920594a19200
	dumpVersion   = 1.0
)

// XorBinaryFuse8 implementation.
type filter[T pbtk.Hashable] struct {
	pbtk.Base[T]
	conf *Config
	once sync.Once

	len, cap       uint64
	segc, segcl    uint64 // segment count and count length
	segl, seglmask uint64 // segment length and length mask
	vec            []uint8

	hkeys []uint64
	revo  []uint64
	revh  []uint8
	t2c   []uint8
	t2h   []uint64
	a     []uint32
	spos  []uint64
	h012  [5]uint32

	err error
}

func NewFilterWithKeys[T pbtk.Hashable](config *Config, keys []T) (amq.Filter[T], error) {
	if config == nil {
		return nil, pbtk.ErrInvalidConfig
	}
	if keys = pbtk.Deduplicate(keys); len(keys) == 0 {
		return nil, ErrEmptyKeyset
	}
	f := &filter[T]{
		conf: config.copy(),
		len:  uint64(len(keys)),
	}
	if f.once.Do(f.init); f.err != nil {
		return nil, f.err
	}
	f.hkeys = growu64(f.hkeys, uint64(len(keys)))[:0]
	for i := 0; i < len(keys); i++ {
		hkey, err := f.Hash(f.conf.Hasher, keys[i])
		if err != nil {
			return nil, err
		}
		f.hkeys = append(f.hkeys, hkey)
	}
	if err := f.hbatch(f.hkeys); err != nil {
		return nil, err
	}
	return f, nil
}

func NewFilterWithHKeys(config *Config, hkeys []uint64) (amq.Filter[uint64], error) {
	if config == nil {
		return nil, pbtk.ErrInvalidConfig
	}
	if hkeys = pbtk.Deduplicate(hkeys); len(hkeys) == 0 {
		return nil, ErrEmptyKeyset
	}
	f := &filter[uint64]{
		conf: config.copy(),
		len:  uint64(len(hkeys)),
	}
	if f.once.Do(f.init); f.err != nil {
		return nil, f.err
	}
	if err := f.hbatch(hkeys); err != nil {
		return nil, err
	}
	return f, nil
}

func NewFilterFromReader[T pbtk.Hashable](config *Config, r io.Reader) (amq.Filter[T], int64, error) {
	if config == nil {
		return nil, 0, pbtk.ErrInvalidConfig
	}
	f := &filter[T]{
		conf: config.copy(),
	}
	f.once.Do(f.tinyinit)
	n, err := f.ReadFrom(r)
	f.err = err
	return f, n, err
}

func (f *filter[T]) hbatch(hkeys []uint64) (err error) {
	f.revo = growu64(f.revo, f.len+1)
	f.revo[f.len] = 1
	f.revh = growu8(f.revh, f.len)

	f.t2c = growu8(f.t2c, f.cap)
	f.t2h = growu64(f.t2h, f.cap)
	f.a = growu32(f.a, f.cap)

	blkB := 1
	for (1 << blkB) < f.segc {
		blkB++
	}
	blk := uint64(1) << blkB

	f.spos = growu64(f.spos, blk)
	for i := uint64(0); i < blk; i++ {
		f.spos[i] = (i * f.len) >> blkB
	}

	for i := 0; i < len(hkeys); i++ {
		hkey := hkeys[i]
		segidx := hkey >> (64 - blkB)
		for f.revo[f.spos[segidx]] != 0 {
			segidx++
			segidx &= (1 << blkB) - 1
		}
		f.revo[f.spos[segidx]] = hkey
		f.spos[segidx]++
	}

	for i := uint64(0); i < f.len; i++ {
		hkey := f.revo[i]
		h0, h1, h3 := f.hash3(hkey)
		f.t2c[h0] += 4
		f.t2h[h0] ^= hkey
		f.t2c[h1] += 4
		f.t2c[h1] ^= 1
		f.t2h[h1] ^= hkey
		f.t2c[h3] += 4
		f.t2c[h3] ^= 2
		f.t2h[h3] ^= hkey
	}

	qsz := 0
	for i := uint64(0); i < f.cap; i++ {
		f.a[qsz] = uint32(i)
		if (f.t2c[i] >> 2) == 1 {
			qsz++
		}
	}

	for i := 0; qsz > 0; qsz-- {
		idx := f.a[qsz]
		if (f.t2c[idx] >> 2) == 1 {
			hash := f.t2h[idx]
			found := f.t2c[idx] & 3
			f.revh[i] = found
			f.revo[i] = hash
			i++

			i0, i1, i2 := f.hash3(hash)
			f.h012[1], f.h012[2], f.h012[3] = i1, i2, i0
			f.h012[4] = f.h012[1]

			j := f.h012[found+1]
			f.a[qsz] = j
			if (f.t2c[j] >> 2) == 2 {
				qsz++
			}
			f.t2c[j] -= 4
			f.t2c[j] ^= f.mod3(found + 1)
			f.t2h[j] ^= hash

			k := f.h012[found+2]
			f.a[qsz] = k
			if (f.t2c[k] >> 2) == 2 {
				qsz++
			}
			f.t2c[k] -= 4
			f.t2c[k] ^= f.mod3(found + 2)
			f.t2h[k] ^= hash
		}
	}

	for i := int(f.len - 1); i >= 0; i-- {
		hkey := f.revo[i]
		xor2 := hkey ^ (hkey >> 32)
		i0, i1, i2 := f.hash3(hkey)
		found := f.revh[i]
		f.h012[0], f.h012[1], f.h012[2] = i0, i1, i2
		f.h012[3], f.h012[4] = f.h012[0], f.h012[1]
		f.vec[f.h012[found]] = uint8(xor2 ^ uint64(f.vec[f.h012[found+1]]) ^ uint64(f.vec[f.h012[found+2]]))
	}

	return
}

func (f *filter[T]) Set(_ T) error {
	return f.conf.MetricsWriter.Set(ErrUnsupportedSet)
}

func (f *filter[T]) HSet(_ uint64) error {
	return f.conf.MetricsWriter.Set(ErrUnsupportedSet)
}

func (f *filter[T]) Unset(_ T) error {
	return f.conf.MetricsWriter.Unset(ErrUnsupportedUnset)
}

func (f *filter[T]) HUnset(_ uint64) error {
	return f.conf.MetricsWriter.Unset(ErrUnsupportedUnset)
}

func (f *filter[T]) Contains(key T) bool {
	if f.once.Do(f.init); f.err != nil {
		return false
	}
	hkey, err := f.Hash(f.conf.Hasher, key)
	if err != nil {
		return false
	}
	return f.hcontains(hkey)
}

func (f *filter[T]) HContains(hkey uint64) bool {
	if f.once.Do(f.init); f.err != nil {
		return false
	}
	return f.hcontains(hkey)
}

func (f *filter[T]) hcontains(hkey uint64) bool {
	f_ := uint8(hkey ^ (hkey >> 32))
	h0, h1, h2 := f.hash3(hkey)
	f_ ^= f.vec[h0] ^ f.vec[h1] ^ f.vec[h2]
	return f.conf.MetricsWriter.Contains(f_ == 0)
}

func (f *filter[T]) hash3(hkey uint64) (uint32, uint32, uint32) {
	hi, _ := bits.Mul64(hkey, f.segcl)
	h0 := uint32(hi)
	h1 := h0 + uint32(f.segl)
	h2 := h1 + uint32(f.segl)
	h1 ^= uint32(hkey>>18) & uint32(f.seglmask)
	h2 ^= uint32(hkey) & uint32(f.seglmask)
	return h0, h1, h2
}

func (f *filter[T]) mod3(x uint8) uint8 {
	if x > 2 {
		x -= 3
	}
	return x
}

func (f *filter[T]) Capacity() uint64 {
	return f.cap
}

func (f *filter[T]) Size() uint64 {
	return f.len
}

func (f *filter[T]) Reset() {
	if f.once.Do(f.init); f.err != nil || f.len == 0 {
		return
	}
	f.len, f.cap = 0, 0
	f.segc, f.segcl = 0, 0
	f.segl, f.seglmask = 0, 0
	memclr.ClearUnsafe(unsafe.Pointer(&f.vec[0]), len(f.vec))
	memclr.ClearUnsafe(unsafe.Pointer(&f.hkeys[0]), len(f.hkeys)*8)
	memclr.ClearUnsafe(unsafe.Pointer(&f.revo[0]), len(f.revo)*8)
	memclr.ClearUnsafe(unsafe.Pointer(&f.revh[0]), len(f.revh))
	memclr.ClearUnsafe(unsafe.Pointer(&f.t2c[0]), len(f.t2c))
	memclr.ClearUnsafe(unsafe.Pointer(&f.t2h[0]), len(f.t2h)*8)
	memclr.ClearUnsafe(unsafe.Pointer(&f.a[0]), len(f.a)*4)
	memclr.ClearUnsafe(unsafe.Pointer(&f.spos[0]), len(f.spos)*8)
	memclr.ClearUnsafe(unsafe.Pointer(&f.h012[0]), len(f.h012)*4)

	f.err = nil
	f.once = sync.Once{}

	f.conf.MetricsWriter.Reset()
}

func (f *filter[T]) WriteTo(w io.Writer) (n int64, err error) {
	if f.once.Do(f.init); f.err != nil {
		return 0, f.err
	}
	var (
		buf [48]byte
		m   int
	)
	binary.LittleEndian.PutUint64(buf[0:8], dumpSignature)
	binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(dumpVersion))
	binary.LittleEndian.PutUint64(buf[16:24], f.segl)
	binary.LittleEndian.PutUint64(buf[24:32], f.segcl)
	binary.LittleEndian.PutUint64(buf[32:40], f.seglmask)
	binary.LittleEndian.PutUint64(buf[40:48], f.cap)
	m, err = w.Write(buf[:])
	n += int64(m)
	if err != nil {
		return int64(m), err
	}

	m, err = w.Write(f.vec)
	n += int64(m)
	return
}

func (f *filter[T]) ReadFrom(r io.Reader) (n int64, err error) {
	if f.once.Do(f.init); f.err != nil {
		return 0, f.err
	}
	var (
		buf [48]byte
		m   int
	)
	m, err = r.Read(buf[:])
	n += int64(m)
	if err != nil {
		return n, err
	}
	if m != 48 {
		return n, io.ErrUnexpectedEOF
	}

	sign, ver, segl, segcl, seglmask, cap_ := binary.LittleEndian.Uint64(buf[0:8]), binary.LittleEndian.Uint64(buf[8:16]),
		binary.LittleEndian.Uint64(buf[16:24]), binary.LittleEndian.Uint64(buf[24:32]), binary.LittleEndian.Uint64(buf[32:40]),
		binary.LittleEndian.Uint64(buf[40:48])

	if sign != dumpSignature {
		return n, pbtk.ErrInvalidSignature
	}
	if ver != math.Float64bits(dumpVersion) {
		return n, pbtk.ErrVersionMismatch
	}
	f.segl, f.segcl, f.seglmask, f.cap = segl, segcl, seglmask, cap_

	f.vec = growu8(f.vec, f.cap)
	m, err = r.Read(f.vec)
	n += int64(m)
	if err == io.EOF {
		err = nil
	}
	if uint64(m) != f.cap {
		err = io.ErrUnexpectedEOF
	}
	return
}

func (f *filter[T]) tinyinit() {
	if f.conf.Hasher == nil {
		f.err = pbtk.ErrNoHasher
		return
	}
	if f.conf.MetricsWriter == nil {
		f.conf.MetricsWriter = amq.DummyMetricsWriter{}
	}
}

func (f *filter[T]) init() {
	f.tinyinit()

	const (
		arity         = 3
		seglThreshold = uint64(1 << 18)
	)

	var segl uint64
	if segl = optimalSegmentLength(f.len, arity); segl > seglThreshold {
		segl = seglThreshold
	}
	sf := optimalSizeFactor(f.len, arity)
	cap_ := uint64(float64(f.len) * sf)
	segc := (cap_+segl-1)/segl - (arity - 1)
	alen := (segc + arity - 1) * segl
	segc = (alen + segl - 1) / segl
	if segc > arity-1 {
		segc = segc - (arity - 1)
	} else {
		segc = 1
	}

	f.segl, f.segc = segl, segc
	f.seglmask = f.segl - 1
	f.segcl = f.segc * f.segl
	f.cap = (f.segc + arity - 1) * f.segl
	f.vec = growu8(f.vec, f.cap)
	f.conf.MetricsWriter.Capacity(f.cap)
}
