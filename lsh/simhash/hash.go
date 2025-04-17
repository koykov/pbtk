package simhash

import (
	"sync"
	"unsafe"

	"github.com/koykov/openrt"
	"github.com/koykov/pbtk"
	"github.com/koykov/pbtk/lsh"
)

const vecsz = 64

type hash[T pbtk.Hashable] struct {
	pbtk.Base[T]
	algo pbtk.Hasher
	vec  [vecsz]int64
	once sync.Once

	err error
}

func NewHasher[T pbtk.Hashable](algo pbtk.Hasher) (lsh.Hasher[T], error) {
	h := &hash[T]{algo: algo}
	if h.once.Do(h.init); h.err != nil {
		return nil, h.err
	}
	return h, nil
}

func (h *hash[T]) Add(value T) error {
	if h.once.Do(h.init); h.err != nil {
		return h.err
	}
	hkey, err := h.Base.Hash(h.algo, value)
	if err != nil {
		return err
	}
	return h.hadd(hkey)
}

func (h *hash[T]) HAdd(hvalue uint64) error {
	if h.once.Do(h.init); h.err != nil {
		return h.err
	}
	return h.hadd(hvalue)
}

func (h *hash[T]) hadd(hval uint64) error {
	for i := uint64(0); i < vecsz; i++ {
		v := (hval >> i) & 1
		h.vec[i] += btable[v]
	}
	return nil
}

func (h *hash[T]) Hash() []uint64 {
	var r [1]uint64
	for i := 0; i < vecsz; i++ {
		if h.vec[i] >= 0 {
			r[0] = r[0] | rtable[i]
		}
	}
	return r[:]
}

func (h *hash[T]) Reset() {
	openrt.MemclrUnsafe(unsafe.Pointer(&h.vec), vecsz*8)
}

func (h *hash[T]) init() {
	if h.algo == nil {
		h.err = pbtk.ErrNoHasher
		return
	}
}
