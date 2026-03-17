package loglog

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/koykov/pbtk"
	"github.com/koykov/simd/memclr"
)

const (
	syncvecDumpSignature = 0x97e74cad17b2b80
	syncvecDumpVersion   = 1.0
)

type syncvec struct {
	m   float64
	s   uint64
	buf []uint8
}

func (vec *syncvec) add(idx uint64, val uint8) error {
	o := vec.buf[idx]
	if val > o {
		vec.buf[idx] = val
		vec.s++
	}
	return nil
}

func (vec *syncvec) estimate() (raw, nz float64) {
	buf := vec.buf
	_, _, _ = buf[len(buf)-1], pow2d1[math.MaxUint8-1], zt[math.MaxUint8-1]
	for len(buf) > 8 {
		n0, n1, n2, n3, n4, n5, n6, n7 := buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]
		raw += pow2d1[n0] + pow2d1[n1] + pow2d1[n2] + pow2d1[n3] + pow2d1[n4] + pow2d1[n5] + pow2d1[n6] + pow2d1[n7]
		nz += zt[n0] + zt[n1] + zt[n2] + zt[n3] + zt[n4] + zt[n5] + zt[n6] + zt[n7]
		buf = buf[8:]
	}
	for i := 0; i < len(buf); i++ {
		n := buf[i]
		raw += pow2d1[n]
		nz += zt[n]
	}
	return
}

func (vec *syncvec) capacity() uint64 {
	return uint64(len(vec.buf))
}

func (vec *syncvec) size() uint64 {
	return vec.s
}

func (vec *syncvec) reset() {
	memclr.Clear(vec.buf)
}

func (vec *syncvec) writeTo(w io.Writer) (n int64, err error) {
	var (
		buf [32]byte
		m   int
	)
	binary.LittleEndian.PutUint64(buf[0:8], syncvecDumpSignature)
	binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(syncvecDumpVersion))
	binary.LittleEndian.PutUint64(buf[16:24], math.Float64bits(vec.m))
	binary.LittleEndian.PutUint64(buf[24:32], vec.s)
	m, err = w.Write(buf[:])
	n += int64(m)
	if err != nil {
		return int64(m), err
	}

	m, err = w.Write(vec.buf)
	n += int64(m)
	return
}

func (vec *syncvec) readFrom(r io.Reader) (n int64, err error) {
	var (
		buf [32]byte
		m   int
	)
	m, err = r.Read(buf[:])
	n += int64(m)
	if err != nil {
		return n, err
	}

	sign, ver, m_, s := binary.LittleEndian.Uint64(buf[0:8]), binary.LittleEndian.Uint64(buf[8:16]),
		binary.LittleEndian.Uint64(buf[16:24]), binary.LittleEndian.Uint64(buf[24:32])

	if sign != syncvecDumpSignature {
		return n, pbtk.ErrInvalidSignature
	}
	if ver != math.Float64bits(syncvecDumpVersion) {
		return n, pbtk.ErrVersionMismatch
	}
	vec.m, vec.s = math.Float64frombits(m_), s

	m, err = r.Read(vec.buf)
	n += int64(m)
	if err == io.EOF {
		err = nil
	}

	return
}

func newSyncvec(m float64) *syncvec {
	return &syncvec{m: m, buf: make([]byte, int(m))}
}
