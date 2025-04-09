package tinylfu

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/koykov/hash/xxhash"
	"github.com/koykov/pbtk"
	"github.com/koykov/pbtk/frequency"
)

const (
	testConfidence = 0.99999
	testEpsilon    = 0.00001
)

var testh = xxhash.Hasher64[[]byte]{}

func TestEstimator(t *testing.T) {
	t.Run("eshop simulation", func(t *testing.T) {
		clock := newTestClock(time.Now())
		est, err := NewEstimator[string](NewConfig(0.99, 0.01, xxhash.Hasher64[[]byte]{}).
			WithEWMATau(60).
			WithClock(clock))
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
	t.Run("sync", func(t *testing.T) {
		epochs := []int{0, 1, 5, 10, 30, 60}
		now := time.Now()
		clock := newTestClock(now)
		est, err := NewEstimator[[]byte](NewConfig(testConfidence, testEpsilon, testh).
			WithEWMATau(60).
			WithClock(clock))
		if err != nil {
			t.Fatal(err)
		}
		pbtk.EachTestingDataset(func(_ int, ds *pbtk.TestingDataset[[]byte]) {
			t.Run(ds.Name, func(t *testing.T) {
				est.Reset()
				for i := 0; i < len(ds.All); i++ {
					var n uint64 = 1
					if i != 0 && i%1000 == 0 {
						n = 1000
					} else if i != 0 && i%100 == 0 {
						n = 100
					} else if i != 0 && i%10 == 0 {
						n = 10
					}
					_ = est.AddN(ds.All[i], n)
				}
				for _, epoch := range epochs {
					t.Run(fmt.Sprintf("t=%d", epoch), func(t *testing.T) {
						clock.set(now.Add(time.Duration(epoch) * time.Second))
						var diffv, diffc float64
						for i := 0; i < len(ds.All); i++ {
							var must uint64 = 1
							if i != 0 && i%1000 == 0 {
								must = 1000
							} else if i != 0 && i%100 == 0 {
								must = 100
							} else if i != 0 && i%10 == 0 {
								must = 10
							}
							var e float64
							e = float64(est.Estimate(ds.All[i]))
							if diff := math.Abs(e - float64(must)); diff > 0 {
								est.Estimate(ds.All[i])
								diffv += diff
								diffc++
							}
						}
						if diffc > 0 {
							t.Logf("avg diff: %f", diffv/diffc)
						}
					})
				}
			})
		})
	})
}

func BenchmarkEstimator(b *testing.B) {
	b.Run("dataset", func(b *testing.B) {
		est, err := NewEstimator[[]byte](NewConfig(testConfidence, testEpsilon, testh))
		if err != nil {
			b.Fatal(err)
		}
		frequency.BenchMe(b, frequency.NewTestAdapter(est))
	})
	b.Run("dataset parallel", func(b *testing.B) {
		est, err := NewEstimator[[]byte](NewConfig(testConfidence, testEpsilon, testh).
			WithConcurrency().WithWriteAttemptsLimit(5))
		if err != nil {
			b.Fatal(err)
		}
		frequency.BenchMeConcurrently(b, frequency.NewTestAdapter(est))
	})
}
