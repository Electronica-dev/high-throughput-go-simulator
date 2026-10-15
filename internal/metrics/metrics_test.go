package metrics

import (
	"math/rand"
	"sync"
	"testing"
)

func TestHistogramPercentiles(t *testing.T) {
	histogram := NewHistogram()
	rng := rand.New(rand.NewSource(7))
	// 1000 uniform samples over [1ms, 10ms]: true p50=5.5ms, p95=9.55ms.
	for i := 0; i < 1000; i++ {
		histogram.Record(1_000_000 + rng.Int63n(9_000_000))
	}
	tol := func(got, want float64) bool {
		return got >= want*0.9 && got <= want*1.1
	}
	if p := histogram.Percentile(0.5); !tol(p, 5_500_000) {
		t.Fatalf("p50 %.0fns, want 5.5ms ±10%%", p)
	}
	if p := histogram.Percentile(0.95); !tol(p, 9_550_000) {
		t.Fatalf("p95 %.0fns, want 9.55ms ±10%%", p)
	}
}

func TestHistogramEmpty(t *testing.T) {
	histogram := NewHistogram()
	if p := histogram.Percentile(0.5); p != 0 {
		t.Fatalf("empty histogram percentile want 0, got %.0f", p)
	}
	if histogram.Count() != 0 {
		t.Fatalf("empty count want 0, got %d", histogram.Count())
	}
}

func TestHistogramKnownValue(t *testing.T) {
	histogram := NewHistogram()
	for i := 0; i < 100; i++ {
		histogram.Record(5_000_000) // all exactly 5ms
	}
	if p := histogram.Percentile(0.99); p < 5_000_000 || p > 6_000_000 {
		t.Fatalf("constant-value p99 want ~5ms, got %.0f", p)
	}
}

func TestRegistryShardedIncr(t *testing.T) {
	registry := New(8)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				registry.Incr("done")
			}
		}()
	}
	wg.Wait()
	if got := registry.Get("done"); got != 80000 {
		t.Fatalf("Get want 80000, got %d", got)
	}
}

func TestSnapshotIsCopy(t *testing.T) {
	registry := New(4)
	registry.Incr("x")
	snap := registry.Snapshot()
	snap["x"] = 999
	if registry.Get("x") != 1 {
		t.Fatalf("mutating snapshot must not affect registry, got %d", registry.Get("x"))
	}
}
