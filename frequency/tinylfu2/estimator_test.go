package tinylfu

import (
	"testing"
	"time"

	"github.com/koykov/hash/xxhash"
)

const (
	testConfidence = 0.99999
	testEpsilon    = 0.00001
)

var testh = xxhash.Hasher64[[]byte]{}

func TestEstimator(t *testing.T) {
	t.Run("eshop simulation", func(t *testing.T) {
		now := time.Now()
		clock := newTestClock(now)
		conf := NewConfig(0.99, 0.01, xxhash.Hasher64[[]byte]{})
		conf.EWMA.Tau = 60
		conf.Clock = clock
		est, err := NewEstimator[string](conf)
		if err != nil {
			t.Fatal(err)
		}

		assert := func(since int, key string, expect uint64) {
			if e := est.Estimate(key); e != expect {
				t.Errorf("time since start = %d; estimation espected %d, got %d", since, expect, e)
			}
		}
		t.Run("t=0", func(t *testing.T) {
			// time since start = 0; user view iphone 15; action weight 1
			_ = est.Add("iphone 15")
			assert(0, "iphone 15", 1)
		})

		t.Run("t=30", func(t *testing.T) {
			clock.add(time.Second * 30)
			// time = 30; user add iphone 15 to card; action weight 3
			_ = est.AddN("iphone 15", 3)
			// weight increased n=3, but decay reduces it at once:
			// Δt = 30, oldEst = 1, n = 3
			// decay = e^(-30/60) ≈ 0.6065
			// rawEst = oldEst*0.6065 + n*(1-0.6065) ≈ 1.79
			// est = floor(rawEst) = 1
			assert(30, "iphone 15", 1)
		})

		t.Run("t=45", func(t *testing.T) {
			clock.add(time.Second * 15)
			// time = 45; user view samsung s24; action weight 1
			_ = est.Add("samsung s24")
			assert(45, "samsung s24", 1)
		})

		t.Run("t=60", func(t *testing.T) {
			clock.add(time.Second * 15)
			// counter decreases by decay:
			// Δt = 30, oldEst = 1
			// decay = e^(-30/60) ≈ 0.6065
			// rawEst = oldEst*0.6065 = 0.6065
			// est = floor(rawEst) = 0
			assert(0, "iphone 15", 0)
		})

		t.Run("t=120", func(t *testing.T) {
			clock.add(time.Second * 60)
			// time = 120; bulk view phone 15; action weight 50
			_ = est.AddN("iphone 15", 50)
			// weight increased n=50, but decay reduces it at once:
			// Δt = 90, oldEst = 1, n = 50
			// decay = e^(-90/60) ≈ 0.2231
			// rawEst = oldEst*0.2231 + n*(1-0.2231) ≈ 39.0666
			// est = floor(rawEst) = 39
			assert(120, "iphone 15", 39)
		})

		t.Run("t=180", func(t *testing.T) {
			clock.add(time.Second * 60)
			// counter decreases by decay:
			// Δt = 60, oldEst = 39
			// decay = e^(-60/60) ≈ 0.3679
			// rawEst = oldEst*0.3679 ≈ 14.3472
			// est = floor(rawEst) = 14
			// conclusion: iphone 15 still is popular
			assert(180, "iphone 15", 14)
			// counter decreases by decay:
			// Δt = 135, oldEst = 1
			// decay = e^(-135/60) ≈ 0.1054
			// rawEst = oldEst*0.1054 ≈ 0.1054
			// est = floor(rawEst) = 0
			// conclusion: samsung s24 is not popular
			assert(180, "samsung s24", 0)
		})
	})
}
