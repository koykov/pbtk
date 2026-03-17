package tinylfu

import (
	"encoding/binary"
	"io"
	"math"
	"unsafe"

	"github.com/koykov/pbtk"
	"github.com/koykov/simd/memclr"
)

const (
	syncvecDumpSignature = 0x32a221b228581800
	syncvecDumpVersion   = 1.0
)

type syncvec struct {
	*basevec
}

func (vec *syncvec) set(pos, n uint64, dtime uint32) error {
	val := vec.buf[pos]
	val = vec.recalc(val, n, dtime)
	vec.buf[pos] = val
	return nil
}

func (vec *syncvec) get(pos uint64, stime, now uint32) float64 {
	val := vec.buf[pos]
	return vec.estimate(val, stime, now)
}

func (vec *syncvec) reset() {
	memclr.ClearUnsafe(unsafe.Pointer(&vec.buf[0]), len(vec.buf)*8)
}

func (vec *syncvec) readFrom(r io.Reader) (n int64, err error) {
	var (
		buf [56]byte
		m   int
	)
	m, err = r.Read(buf[:])
	n += int64(m)
	if err != nil {
		return n, err
	}

	sign, ver, dtimeMin, tau, decayMin, bufsz, exptabsz := binary.LittleEndian.Uint64(buf[0:8]),
		binary.LittleEndian.Uint64(buf[8:16]), binary.LittleEndian.Uint64(buf[16:24]),
		binary.LittleEndian.Uint64(buf[24:32]), binary.LittleEndian.Uint64(buf[32:40]),
		binary.LittleEndian.Uint64(buf[40:48]), binary.LittleEndian.Uint64(buf[48:56])

	if sign != syncvecDumpSignature {
		return n, pbtk.ErrInvalidSignature
	}
	if ver != math.Float64bits(syncvecDumpVersion) {
		return n, pbtk.ErrVersionMismatch
	}
	vec.dtimeMin, vec.tau = dtimeMin, tau
	vec.decayMin = math.Float64frombits(decayMin)

	if uint64(len(vec.buf)) < bufsz {
		vec.buf = make([]uint64, bufsz)
	}
	vec.buf = vec.buf[:bufsz]
	for i := uint64(0); i < bufsz; i++ {
		m, err = r.Read(buf[0:8])
		n += int64(m)
		if err != nil {
			return n, err
		}
		vec.buf[i] = binary.LittleEndian.Uint64(buf[0:8])
	}

	if uint64(len(vec.exptab)) < exptabsz {
		vec.exptab = make([]float64, exptabsz)
	}
	vec.exptab = vec.exptab[:exptabsz]
	for i := uint64(0); i < exptabsz; i++ {
		m, err = r.Read(buf[0:8])
		n += int64(m)
		if err == io.EOF {
			err = nil
		}
		if err != nil {
			return n, err
		}
		vec.exptab[i] = math.Float64frombits(binary.LittleEndian.Uint64(buf[0:8]))
	}
	if err == io.EOF {
		err = nil
	}

	return
}

func (vec *syncvec) writeTo(w io.Writer) (n int64, err error) {
	var (
		buf [56]byte
		m   int
	)
	binary.LittleEndian.PutUint64(buf[0:8], syncvecDumpSignature)
	binary.LittleEndian.PutUint64(buf[8:16], math.Float64bits(syncvecDumpVersion))
	binary.LittleEndian.PutUint64(buf[16:24], vec.dtimeMin)
	binary.LittleEndian.PutUint64(buf[24:32], vec.tau)
	binary.LittleEndian.PutUint64(buf[32:40], math.Float64bits(vec.decayMin))
	binary.LittleEndian.PutUint64(buf[40:48], uint64(len(vec.buf)))
	binary.LittleEndian.PutUint64(buf[48:56], vec.exptabsz)
	m, err = w.Write(buf[:])
	n += int64(m)
	if err != nil {
		return int64(m), err
	}

	for i := 0; i < len(vec.buf); i++ {
		binary.LittleEndian.PutUint64(buf[0:8], vec.buf[i])
		m, err = w.Write(buf[:8])
		n += int64(m)
		if err != nil {
			return int64(m), err
		}
	}
	for i := 0; i < len(vec.exptab); i++ {
		binary.LittleEndian.PutUint64(buf[0:8], math.Float64bits(vec.exptab[i]))
		m, err = w.Write(buf[:8])
		n += int64(m)
		if err != nil {
			return int64(m), err
		}
	}
	return
}

func newVector(sz uint64, ewma *EWMA) vector {
	vec := &syncvec{
		basevec: &basevec{
			buf:      make([]uint64, sz),
			dtimeMin: ewma.MinDeltaTime,
			tau:      ewma.Tau,
			exptabsz: ewma.ExpTableSize,
		},
	}
	vec.basevec.init()
	return vec
}
