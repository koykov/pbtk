package quotient

import (
	"io"
	"sync"

	"github.com/koykov/amq"
)

type Filter struct {
	amq.Base
	conf       *Config
	once       sync.Once
	qb, rb     uint64 // quotient and remainder bits
	bs         uint64 // bucket size (rb+3)
	m          uint64 // total filter size
	bm, qm, rm uint64 // bucket mask, quotient mask, remainder mask
	vec        []uint64
	s          uint64

	err error
}

func NewFilter(config *Config) (*Filter, error) {
	if config == nil {
		return nil, amq.ErrInvalidConfig
	}
	f := &Filter{
		conf: config.copy(),
	}
	if f.once.Do(f.init); f.err != nil {
		return nil, f.err
	}
	return f, nil
}

func (f *Filter) Set(key any) error {
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

func (f *Filter) HSet(hkey uint64) error {
	if f.once.Do(f.init); f.err != nil {
		return f.err
	}
	if f.overflow() {
		return ErrFilterOverflow
	}
	return f.hset(hkey)
}

func (f *Filter) hset(hkey uint64) error {
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
	for b.checkbit(btypeOccupied) {
		lob := f.getBucket(i)
		for {
			if rem := b.rem(); rem == r {
				return nil
			} else if rem > r {
				break
			}
			i = (i + 1) & f.qm
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
		i = (i + 1) & f.qm
		if pe {
			break
		}
	}
	f.s++
	return nil
}

func (f *Filter) Unset(key any) error {
	if f.once.Do(f.init); f.err != nil {
		return f.err
	}
	// todo implement me
	return nil
}

func (f *Filter) HUnset(hkey uint64) error {
	if f.once.Do(f.init); f.err != nil {
		return f.err
	}
	// todo implement me
	return nil
}

func (f *Filter) Contains(key any) bool {
	if f.once.Do(f.init); f.err != nil {
		return false
	}
	// todo implement me
	return false
}

func (f *Filter) HContains(hkey uint64) bool {
	if f.once.Do(f.init); f.err != nil {
		return false
	}
	// todo implement me
	return false
}

func (f *Filter) Reset() {
	if f.once.Do(f.init); f.err != nil {
		return
	}
	// todo implement me
}

func (f *Filter) ReadFrom(r io.Reader) (int64, error) {
	if f.once.Do(f.init); f.err != nil {
		return 0, f.err
	}
	// todo implement me
	return 0, nil
}

func (f *Filter) WriteTo(w io.Writer) (int64, error) {
	if f.once.Do(f.init); f.err != nil {
		return 0, f.err
	}
	// todo implement me
	return 0, nil
}

func (f *Filter) init() {
	c := f.conf
	if c.ItemsNumber == 0 {
		f.err = amq.ErrNoItemsNumber
		return
	}
	if c.Hasher == nil {
		f.err = amq.ErrNoHasher
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

	if f.m, f.qb, f.rb = optimalMQR(c.ItemsNumber, c.FPP, c.LoadFactor); f.qb+f.qb > 64 {
		f.err = ErrBucketOverflow
		return
	}
	f.bs = f.rb + 3
	f.vec = make([]uint64, f.m)
	f.mw().Capacity(f.m)

	f.qm, f.rm, f.bm = lowMask(f.qb), lowMask(f.rb), lowMask(f.bs)
}

func (f *Filter) overflow() bool {
	return f.s >= 1<<f.qb
}

func (f *Filter) calcQR(hkey uint64) (q, r uint64) {
	q, r = (hkey>>f.rb)&f.qm, hkey&f.rm
	return
}

func (f *Filter) getBucket(q uint64) bucket {
	i, off, bits := f.bucketIOB(q)
	v := (f.vec[i] >> off) & f.qm
	if bits > 0 {
		v = v | (f.vec[i]&lowMask(uint64(bits)))<<(f.bs-uint64(bits))
	}
	return bucket(v)
}

func (f *Filter) setBucket(q uint64, b bucket) {
	i, off, bits := f.bucketIOB(q)
	b = b & bucket(f.bm)
	nb := f.vec[i]
	nb &= ^(f.bm << off)
	nb |= b.raw() << off
	f.vec[i] = nb
	if bits > 0 {
		nb = f.vec[i+1]
		nb &^= lowMask(uint64(bits))
		nb |= b.raw()>>f.bs - uint64(bits)
		f.vec[i+1] = nb
	}
}

func (f *Filter) bucketIOB(q uint64) (i, off uint64, bits int64) {
	bi := f.bs * q
	i, off = bi/64, bi%64
	bits = int64(off + f.bs - 64)
	return
}

func (f *Filter) lo(q uint64) (lo uint64) {
	var b bucket
	i := q
	for {
		if b = f.getBucket(i); b.checkbit(btypeShifted) {
			break
		}
		i = (i - 1) & f.qm
	}
	lo = i
	for i != q {
		for {
			lo = (lo + 1) & f.qm
			b = f.getBucket(lo)
			if !b.checkbit(btypeContinuation) {
				break
			}
		}
		// todo check
		// for {
		// 	i = (i + 1) & f.qm
		// 	b = f.getBucket(i)
		// 	if b.checkbit(btypeOccupied) {
		// 		break
		// 	}
		// }
	}
	return
}

func (f *Filter) mw() amq.MetricsWriter {
	return f.conf.MetricsWriter
}

func lowMask(v uint64) uint64 {
	return (1 << v) - 1
}
