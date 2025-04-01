package tinylfu

import (
	"testing"

	"github.com/koykov/hash/xxhash"
	"github.com/koykov/pbtk/frequency"
)

const (
	testConfidence = 0.99999
	testEpsilon    = 0.00001
)

var testh = xxhash.Hasher64[[]byte]{}

func TestEstimator(t *testing.T) {
	t.Run("dataset", func(t *testing.T) {
		est, err := NewEstimator[[]byte](NewConfig(testConfidence, testEpsilon, testh))
		if err != nil {
			t.Fatal(err)
		}
		frequency.TestMe(t, frequency.NewTestAdapter(est))
	})
	t.Run("decay", func(t *testing.T) {
		t.Run("counter", func(t *testing.T) {
			est, _ := NewEstimator[string](NewConfig(0.99, 0.01, testh).
				WithDecayLimit(20))
			for i := 0; i < 10; i++ {
				_ = est.Add("foobar")
				_ = est.Add("qwerty")
			}
			_ = est.Add("final")
			t.Log(est.Estimate("foobar"), est.Estimate("qwerty"))
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
