package metrics

import (
	"math/rand"
	"sync"
	"testing"
)

func TestHistogramPercentiles(t *testing.T) {
	h := NewHistogram()
	rng := rand.New(rand.NewSource(7))
	// 1000 uniform samples over [1ms, 10ms]: true p50=5.5ms, p95=9.55ms.
	for i := 0; i < 1000; i++ {
		h.Record(1_000_000 + rng.Int63n(9_000_000))
	}
	tol := func(got, want float64) bool {
		return got >= want*0.9 && got <= want*1.1
	}
	if p := h.Percentile(0.5); !tol(p, 5_500_000) {
		t.Fatalf("p50 %.0fns, want 5.5ms ±10%%", p)
	}
	if p := h.Percentile(0.95); !tol(p, 9_550_000) {
		t.Fatalf("p95 %.0fns, want 9.55ms ±10%%", p)
	}
}

func TestHistogramEmpty(t *testing.T) {
	h := NewHistogram()
	if p := h.Percentile(0.5); p != 0 {
		t.Fatalf("empty histogram percentile want 0, got %.0f", p)
	}
	if h.Count() != 0 {
		t.Fatalf("empty count want 0, got %d", h.Count())
	}
}

func TestHistogramKnownValue(t *testing.T) {
	h := NewHistogram()
	for i := 0; i < 100; i++ {
		h.Record(5_000_000) // all exactly 5ms
	}
	if p := h.Percentile(0.99); p < 5_000_000 || p > 6_000_000 {
		t.Fatalf("constant-value p99 want ~5ms, got %.0f", p)
	}
}

func TestRegistryShardedIncr(t *testing.T) {
	r := New(8)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i++ {
				r.Incr("done")
			}
		}()
	}
	wg.Wait()
	if got := r.Get("done"); got != 80000 {
		t.Fatalf("Get want 80000, got %d", got)
	}
}

func TestSnapshotIsCopy(t *testing.T) {
	r := New(4)
	r.Incr("x")
	snap := r.Snapshot()
	snap["x"] = 999
	if r.Get("x") != 1 {
		t.Fatalf("mutating snapshot must not affect registry, got %d", r.Get("x"))
	}
}
