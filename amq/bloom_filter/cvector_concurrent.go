package bloom

import (
	"encoding/binary"
	"io"
	"math"
	"sync/atomic"

	"github.com/koykov/bitvector"
	"github.com/koykov/pbtk"
)

const (
	ccnvectorDumpSignature = 0x5f0f6bdc99b85fea
	ccnvectorDumpVersion   = 1.0
)

// Concurrent counting vector implementation.
type ccnvector struct {
	bitvector.Interface
	buf []uint32
	lim uint64
	s   uint64
}

func (vec *ccnvector) Set(i uint64) bool {
	for j := uint64(0); j < vec.lim; j++ {
		o := atomic.LoadUint32(&vec.buf[i/2])
		v0, v1 := uint16(o>>16), uint16(o)
		if i%2 == 0 {
			v0++
		} else {
			v1++
		}
		if atomic.CompareAndSwapUint32(&vec.buf[i/2], o, uint32(v0)<<16|uint32(v1)) {
			atomic.AddUint64(&vec.s, 1)
			return true
		}
	}
	return false
}

func (vec *ccnvector) Xor(_ uint64) bool {
	return false // senseless for CBF
}

func (vec *ccnvector) Unset(i uint64) bool {
	for j := uint64(0); j < vec.lim; j++ {
		o := atomic.LoadUint32(&vec.buf[i/2])
		v0, v1 := uint16(o>>16), uint16(o)
		if i%2 == 0 {
			v0 += math.MaxUint16
		} else {
			v1 += math.MaxUint16
		}
		if atomic.CompareAndSwapUint32(&vec.buf[i/2], o, uint32(v0)<<16|uint32(v1)) {
			atomic.AddUint64(&vec.s, math.MaxUint64)
			return true
		}
	}
	return false
}

func (vec *ccnvector) Get(i uint64) uint8 {
	c := atomic.LoadUint32(&vec.buf[i/2])
	v0, v1 := uint16(c>>16), uint16(c)
	var r bool
	if i%2 == 0 {
		r = v0 > 0
	} else {
		r = v1 > 0
	}
	if r {
		return 1
	}
	return 0
}

func (vec *ccnvector) Size() uint64 {
	return vec.s
}

func (vec *ccnvector) Capacity() uint64 {
	return uint64(len(vec.buf)) * 2
}

func (vec *ccnvector) Popcnt() uint64 {
	return 0 // useless for Bloom
}

func (vec *ccnvector) Difference(_ bitvector.Interface) (uint64, error) {
	return 0, nil // useless for Bloom
}

func (vec *ccnvector) Clone() bitvector.Interface {
	clone := &ccnvector{
		buf: make([]uint32, len(vec.buf)),
		s:   atomic.LoadUint64(&vec.s),
	}
	for i := 0; i < len(vec.buf); i++ {
		clone.buf[i] = atomic.LoadUint32(&vec.buf[i])
	}
	return clone
}

func (vec *ccnvector) Reset() {
	atomic.StoreUint64(&vec.s, 0)
	for i := 0; i < len(vec.buf); i++ {
		atomic.StoreUint32(&vec.buf[i], 0)
	}
}

func (vec *ccnvector) WriteTo(w io.Writer) (n int64, err error) {
	var (
		buf [40]byte
		m   int
	)
	binary.LittleEndian.PutUint64(buf[0:8], ccnvectorDumpSignature)
	binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(ccnvectorDumpVersion))
	binary.LittleEndian.PutUint64(buf[16:24], atomic.LoadUint64(&vec.s))
	binary.LittleEndian.PutUint64(buf[24:32], vec.lim)
	if m, err = w.Write(buf[:]); err != nil {
		return int64(m), err
	}
	n += int64(m)

	var off int
	const blocksz = 4096
	var blk [blocksz]byte
	for i := 0; i < len(vec.buf); i++ {
		v := atomic.LoadUint32(&vec.buf[i])
		binary.LittleEndian.PutUint32(blk[off:], v)
		if off += 4; off == blocksz {
			m, err = w.Write(blk[:off])
			n += int64(m)
			if err != nil {
				return
			}
			if m < blocksz {
				err = io.ErrShortWrite
				return
			}
			off = 0
		}
	}
	if off > 0 {
		m, err = w.Write(blk[:off])
		n += int64(m)
	}
	return
}

func (vec *ccnvector) ReadFrom(r io.Reader) (n int64, err error) {
	var (
		buf [40]byte
		m   int
	)
	m, err = r.Read(buf[:])
	n += int64(m)
	if err != nil {
		return n, err
	}

	sign, ver, s, lim := binary.LittleEndian.Uint64(buf[0:8]), binary.LittleEndian.Uint64(buf[8:16]),
		binary.LittleEndian.Uint64(buf[16:24]), binary.LittleEndian.Uint64(buf[24:32])

	if sign != ccnvectorDumpSignature {
		return n, pbtk.ErrInvalidSignature
	}
	if ver != math.Float64bits(ccnvectorDumpVersion) {
		return n, pbtk.ErrVersionMismatch
	}
	vec.s, vec.lim = s, lim
	vec.buf = vec.buf[:0]

	const blocksz = 4096
	var blk [blocksz]byte
	for {
		m, err = r.Read(blk[:])
		n += int64(m)
		if err != nil && err != io.EOF {
			return n, err
		}
		for i := 0; i < m; i += 4 {
			v := binary.LittleEndian.Uint32(blk[i:])
			vec.buf = append(vec.buf, v) // todo may be race here
		}
		if err == io.EOF {
			err = nil
			break
		}
	}
	return
}

func newCcnvector(size, lim uint64) *ccnvector {
	return &ccnvector{
		buf: make([]uint32, size/2+1),
		lim: lim + 1,
	}
}
