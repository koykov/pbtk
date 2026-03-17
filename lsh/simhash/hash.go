package simhash

import (
	"sync"
	"unsafe"

	"github.com/koykov/byteseq"
	"github.com/koykov/pbtk"
	"github.com/koykov/pbtk/lsh"
	"github.com/koykov/simd/memclr"
)

const vectorsz = 64

type hash[T byteseq.Q] struct {
	conf   *Config[T]
	vector [vectorsz]int64
	token  []T
	once   sync.Once

	err error
}

func NewHasher[T byteseq.Q](conf *Config[T]) (lsh.Hasher[T], error) {
	if conf == nil {
		return nil, pbtk.ErrInvalidConfig
	}
	h := &hash[T]{conf: conf.copy()}
	if h.once.Do(h.init); h.err != nil {
		return nil, h.err
	}
	return h, nil
}

func (h *hash[T]) Add(value T) error {
	if h.once.Do(h.init); h.err != nil {
		return h.err
	}
	h.token = h.conf.Shingler.AppendShingle(h.token, value)
	for i := 0; i < len(h.token); i++ {
		hsum := h.conf.Algo.Sum64([]byte(h.token[i]))
		for j := uint64(0); j < vectorsz; j += 8 {
			h.vector[j+0] += btable[(hsum>>j+0)&1]
			h.vector[j+1] += btable[(hsum>>j+1)&1]
			h.vector[j+2] += btable[(hsum>>j+2)&1]
			h.vector[j+3] += btable[(hsum>>j+3)&1]
			h.vector[j+4] += btable[(hsum>>j+4)&1]
			h.vector[j+5] += btable[(hsum>>j+5)&1]
			h.vector[j+6] += btable[(hsum>>j+6)&1]
			h.vector[j+7] += btable[(hsum>>j+7)&1]
		}
	}
	return nil
}

func (h *hash[T]) Hash() []uint64 {
	var r [1]uint64
	return h.AppendHash(r[:0])
}

func (h *hash[T]) AppendHash(dst []uint64) []uint64 {
	var r uint64
	for i := 0; i < vectorsz; i++ {
		if h.vector[i] >= 0 {
			r = r | rtable[i]
		}
	}
	return append(dst, r)
}

func (h *hash[T]) Reset() {
	memclr.ClearUnsafe(unsafe.Pointer(&h.vector), vectorsz*8)
	h.token = h.token[:0]
	h.conf.Shingler.Reset()
}

func (h *hash[T]) init() {
	if h.conf.Algo == nil {
		h.err = pbtk.ErrNoHasher
		return
	}
	if h.conf.Shingler == nil {
		h.err = lsh.ErrNoShingler
		return
	}
}
