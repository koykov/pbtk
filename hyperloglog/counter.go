package hyperloglog

import (
	"io"
	"math"
	"math/bits"
	"sync"

	"github.com/koykov/amq"
)

type counter struct {
	amq.Base
	conf *Config
	once sync.Once
	a    float64
	m    float64
	vec  []uint8

	err error
}

func NewCounter(config *Config) (amq.Counter, error) {
	if config == nil {
		return nil, amq.ErrInvalidConfig
	}
	c := &counter{
		conf: config.copy(),
	}
	if c.once.Do(c.init); c.err != nil {
		return nil, c.err
	}
	return c, nil
}

func (c *counter) Add(key any) error {
	if c.once.Do(c.init); c.err != nil {
		return c.err
	}
	hkey, err := c.Hash(c.conf.Hasher, key)
	if err != nil {
		return err
	}
	return c.hadd(hkey)
}

func (c *counter) HAdd(hkey uint64) error {
	if c.once.Do(c.init); c.err != nil {
		return c.err
	}
	return c.hadd(hkey)
}

func (c *counter) hadd(hkey uint64) error {
	p := c.conf.Precision
	i := hkey >> (64 - p)
	r := 64 - p
	if h := hkey << p; h > 0 {
		if lz := uint64(bits.LeadingZeros64(h)) + 1; lz < r {
			r = lz
		}
	}
	c.vec[i] = uint8(r)
	// i := (hkey >> (64 - p)) & ((1 << p) - 1)
	// w := hkey<<p | 1<<(p-1)
	// lbp1 := uint8(bits.LeadingZeros64(w)) + 1
	// if mx := c.vec[i]; lbp1 > mx {
	// 	c.vec[i] = lbp1
	// }
	return nil
}

func (c *counter) Count() uint64 {
	if c.once.Do(c.init); c.err != nil || len(c.vec) == 0 {
		return 0
	}
	e, z := c.rawEstimation()

	if e < 5*c.m {
		e = e - biasfn(c.conf.Precision-4, e)
	}

	h := e
	if z < c.m {
		h = c.linearEstimation(z)
	}
	if h <= threshold[c.conf.Precision-4] {
		return uint64(h)
	}
	return uint64(e)
}

func (c *counter) rawEstimation() (raw, z float64) {
	_ = c.vec[len(c.vec)-1]
	for i := 0; i < len(c.vec); i++ {
		n := c.vec[i]
		raw += 1 / math.Pow(2, float64(n))
		if n == 0 {
			z++
		}
	}
	raw = c.a * c.m * c.m / raw
	return
}

func (c *counter) linearEstimation(z float64) float64 {
	return c.m * math.Log(c.m/(c.m-z))
}

func (c *counter) WriteTo(w io.Writer) (n int64, err error) {
	if c.once.Do(c.init); c.err != nil {
		err = c.err
		return
	}
	// todo: implement me
	return
}

func (c *counter) ReadFrom(r io.Reader) (n int64, err error) {
	if c.once.Do(c.init); c.err != nil {
		err = c.err
		return
	}
	// todo: implement me
	return
}

func (c *counter) Reset() {
	if c.once.Do(c.init); c.err != nil {
		return
	}
	// todo: implement me
}

func (c *counter) init() {
	if c.conf.Precision < 4 || c.conf.Precision > 18 {
		c.err = ErrInvalidPrecision
		return
	}
	if c.conf.Hasher == nil {
		c.err = amq.ErrNoHasher
		return
	}

	m := uint64(1) << c.conf.Precision
	c.m = float64(m)
	c.vec = make([]uint8, m)

	// alpha approximation, see https://en.wikipedia.org/wiki/HyperLogLog#Practical_considerations for details
	switch m {
	case 16:
		c.a = .673
	case 32:
		c.a = .697
	case 64:
		c.a = .709
	default:
		c.a = .7213 / (1 + 1.079/c.m)
	}
}

var threshold = [15]float64{10, 20, 40, 80, 220, 400, 900, 1800, 3100, 6500, 11500, 20000, 50000, 120000, 350000}
