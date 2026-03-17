package quotient

import (
	"io"
	"sync"
	"unsafe"

	"github.com/koykov/pbtk"
	"github.com/koykov/pbtk/amq"
	"github.com/koykov/simd/memclr"
)

// Quotient filter implementation.
type filter[T pbtk.Hashable] struct {
	pbtk.Base[T]
	conf                 *Config
	once                 sync.Once
	qbits, rbits         uint64 // quotient and remainder bits
	bsz                  uint64 // bucket size (rbits+3)
	m                    uint64 // total filter size
	bmask, qmask, rmmask uint64 // bucket mask, quotient mask, remainder mask
	vec                  []uint64
	s                    uint64 // items counter

	err error
}

// NewFilter creates new filter.
func NewFilter[T pbtk.Hashable](config *Config) (amq.Filter[T], error) {
	if config == nil {
		return nil, pbtk.ErrInvalidConfig
	}
	f := &filter[T]{
		conf: config.copy(),
	}
	if f.once.Do(f.init); f.err != nil {
		return nil, f.err
	}
	return f, nil
}

// Set adds new key to the filter.
func (f *filter[T]) Set(key T) error {
	if f.once.Do(f.init); f.err != nil {
		return f.err
	}
	if f.overflow() {
		return ErrFilterOverflow
	}
	hkey, err := f.Hash(f.conf.Hasher, key)
	if err != nil {
		return err
	}
	return f.hset(hkey)
}

// HSet sets new predefined hash key to the filter.
func (f *filter[T]) HSet(hkey uint64) error {
	if f.once.Do(f.init); f.err != nil {
		return f.err
	}
	if f.overflow() {
		return ErrFilterOverflow
	}
	return f.hset(hkey)
}

func (f *filter[T]) hset(hkey uint64) error {
	q, r := f.calcQR(hkey)
	b := f.getBucket(q)
	nb := newBucket(r)
	if b.empty() {
		nb.setbit(btypeOccupied)
		f.setBucket(q, nb)
		f.s++
		return nil
	}
	if !b.checkbit(btypeOccupied) {
		b.setbit(btypeOccupied)
		f.setBucket(q, b)
	}

	lo := f.lo(q)
	i := lo
	if b.checkbit(btypeOccupied) {
		lob := f.getBucket(i)
		for {
			if rem := b.rem(); rem == r {
				return nil
			} else if rem > r {
				break
			}
			i = (i + 1) & f.qmask
			if lob = f.getBucket(i); !lob.checkbit(btypeContinuation) {
				break
			}
		}
		if i == lo {
			ob := f.getBucket(lo)
			ob.setbit(btypeContinuation)
			f.setBucket(lo, ob)
		} else {
			nb.setbit(btypeContinuation)
		}
	}
	if lo != q {
		nb.setbit(btypeShifted)
	}

	c := nb
	for {
		p := f.getBucket(i)
		pe := p.empty()
		if !pe {
			p.setbit(btypeShifted)
			if p.checkbit(btypeOccupied) {
				c.setbit(btypeOccupied)
				p.clearbit(btypeOccupied)
			}
		}
		f.setBucket(i, c)
		c = p
		i = (i + 1) & f.qmask
		if pe {
			break
		}
	}
	f.s++
	return nil
}

// Unset removes key from the filter.
func (f *filter[T]) Unset(key T) error {
	if f.once.Do(f.init); f.err != nil || f.s == 0 {
		return f.err
	}
	hkey, err := f.Hash(f.conf.Hasher, key)
	if err != nil {
		return err
	}
	return f.hunset(hkey)
}

// HUnset removes predefined hash key from the filter.
func (f *filter[T]) HUnset(hkey uint64) error {
	if f.once.Do(f.init); f.err != nil || f.s == 0 {
		return f.err
	}
	return f.hunset(hkey)
}

func (f *filter[T]) hunset(hkey uint64) error {
	q, r := f.calcQR(hkey)
	t := f.getBucket(q)
	if !t.checkbit(btypeOccupied) {
		return nil
	}

	lo := f.lo(q)
	i := lo
	var rem uint64
	for {
		b := f.getBucket(i)
		if rem = b.rem(); rem == r {
			break
		} else if rem > r {
			return nil
		}
		i = (i + 1) & f.qmask
		b = f.getBucket(i)
		if !b.checkbit(btypeContinuation) {
			break
		}
	}
	if rem != r {
		return nil
	}

	k := t
	if i != q {
		k = f.getBucket(lo)
	}
	lo0 := k.checkLo0()
	if lo0 {
		n := f.getBucket((i + 1) & f.qmask)
		if !n.checkbit(btypeContinuation) {
			t.clearbit(btypeOccupied)
			f.setBucket(q, t)
		}
	}

	del := func(i, q uint64) {
		var n bucket
		c := f.getBucket(i)
		ip := (i + 1) & f.qmask
		oi := i
		for {
			n = f.getBucket(ip)
			co := c.checkbit(btypeOccupied)
			if n.empty() || n.checkcluster() || ip == oi {
				f.setBucket(i, 0)
				return
			} else {
				un := n
				if n.checkLo0() {
					for {
						q = (q + 1) & f.qmask
						x := f.getBucket(q)
						if !x.checkbit(btypeOccupied) {
							break
						}
					}
					if co && q == i {
						n.clearbit(btypeShifted)
						un = n
					}
				}
				if co {
					un.setbit(btypeOccupied)
				} else {
					un.clearbit(btypeOccupied)
				}
				i = ip
				ip = (ip + 1) & f.qmask
				c = n
			}
		}
	}
	del(i, q)

	if lo0 {
		n := f.getBucket(i)
		un := n
		if n.checkbit(btypeContinuation) {
			un.clearbit(btypeContinuation)
		}
		if i == q && un.checkLo0() {
			un.clearbit(btypeShifted)
		}
		if !un.eqbits(n) {
			f.setBucket(i, un)
		}
	}
	f.s--
	return nil
}

// Contains checks if key is in the filter.
func (f *filter[T]) Contains(key T) bool {
	if f.once.Do(f.init); f.err != nil || f.s == 0 {
		return false
	}
	hkey, err := f.Hash(f.conf.Hasher, key)
	if err != nil {
		return false
	}
	return f.hcontains(hkey)
}

// HContains checks if predefined hash key is in the filter.
func (f *filter[T]) HContains(hkey uint64) bool {
	if f.once.Do(f.init); f.err != nil || f.s == 0 {
		return false
	}
	return f.hcontains(hkey)
}

func (f *filter[T]) hcontains(hkey uint64) bool {
	q, r := f.calcQR(hkey)
	b := f.getBucket(q)
	if !b.checkbit(btypeOccupied) {
		return false
	}

	i := f.lo(q)
	b = f.getBucket(i)
	for {
		if rem := b.rem(); rem == r {
			return true
		} else if rem > r {
			return false
		}
		i = (i + 1) & f.qmask
		b = f.getBucket(i)
		if !b.checkbit(btypeContinuation) {
			break
		}
	}
	return false
}

// Capacity returns filter capacity.
func (f *filter[T]) Capacity() uint64 {
	return uint64(len(f.vec))
}

// Size returns number of items added to the filter.
func (f *filter[T]) Size() uint64 {
	return f.s
}

// Reset flushes filter data.
func (f *filter[T]) Reset() {
	if f.once.Do(f.init); f.err != nil {
		return
	}
	memclr.ClearUnsafe(unsafe.Pointer(&f.vec[0]), len(f.vec)*8)
	f.s = 0
}

func (f *filter[T]) ReadFrom(r io.Reader) (int64, error) {
	if f.once.Do(f.init); f.err != nil {
		return 0, f.err
	}
	return 0, pbtk.ErrUnsupportedOp
}

func (f *filter[T]) WriteTo(w io.Writer) (int64, error) {
	if f.once.Do(f.init); f.err != nil {
		return 0, f.err
	}
	return 0, pbtk.ErrUnsupportedOp
}

func (f *filter[T]) init() {
	c := f.conf
	if c.ItemsNumber == 0 {
		f.err = amq.ErrNoItemsNumber
		return
	}
	if c.Hasher == nil {
		f.err = pbtk.ErrNoHasher
		return
	}
	if c.MetricsWriter == nil {
		c.MetricsWriter = amq.DummyMetricsWriter{}
	}
	if c.FPP == 0 {
		c.FPP = defaultFPP
	}
	if c.FPP < 0 || c.FPP > 1 {
		f.err = amq.ErrInvalidFPP
		return
	}
	if c.LoadFactor == 0 {
		c.LoadFactor = defaultLoadFactor
	}
	if c.LoadFactor < 0 || c.LoadFactor > 1 {
		f.err = ErrInvalidLoadFactor
		return
	}

	if f.m, f.qbits, f.rbits = optimalMQR(c.ItemsNumber, c.FPP, c.LoadFactor); f.qbits+f.qbits > 64 {
		f.err = ErrBucketOverflow
		return
	}
	f.bsz = f.rbits + 3
	f.vec = make([]uint64, f.m)
	f.mw().Capacity(f.m)

	f.qmask, f.rmmask, f.bmask = lowMask(f.qbits), lowMask(f.rbits), lowMask(f.bsz)
}

func (f *filter[T]) overflow() bool {
	return f.s >= 1<<f.qbits
}

func (f *filter[T]) calcQR(hkey uint64) (q, r uint64) {
	q, r = (hkey>>f.rbits)&f.qmask, hkey&f.rmmask
	return
}

func (f *filter[T]) getBucket(q uint64) bucket {
	i, off, bits := f.bucketIOB(q)
	v := (f.vec[i] >> off) & f.bmask
	if bits > 0 {
		i++
		v = v | (f.vec[i]&lowMask(uint64(bits)))<<(f.bsz-uint64(bits))
	}
	return bucket(v)
}

func (f *filter[T]) setBucket(q uint64, b bucket) {
	i, off, bits := f.bucketIOB(q)
	b = b & bucket(f.bmask)
	nb := f.vec[i]
	nb &= ^(f.bmask << off)
	nb |= b.raw() << off
	f.vec[i] = nb
	if bits > 0 {
		nb = f.vec[i+1]
		nb &^= lowMask(uint64(bits))
		nb |= b.raw() >> (f.bsz - uint64(bits))
		f.vec[i+1] = nb
	}
}

func (f *filter[T]) bucketIOB(q uint64) (i, off uint64, bits int64) {
	bi := f.bsz * q
	i, off = bi/64, bi%64
	bits = int64(off + f.bsz - 64)
	return
}

func (f *filter[T]) lo(q uint64) (lo uint64) {
	var b bucket
	i := q
	for {
		if b = f.getBucket(i); !b.checkbit(btypeShifted) {
			break
		}
		i = (i - 1) & f.qmask
	}
	lo = i
	for i != q {
		for {
			lo = (lo + 1) & f.qmask
			b = f.getBucket(lo)
			if !b.checkbit(btypeContinuation) {
				break
			}
		}
		for {
			i = (i + 1) & f.qmask
			b = f.getBucket(i)
			if b.checkbit(btypeOccupied) {
				break
			}
		}
	}
	return
}

func (f *filter[T]) mw() amq.MetricsWriter {
	return f.conf.MetricsWriter
}

func lowMask(v uint64) uint64 {
	return (1 << v) - 1
}
