// Package metrics provides lock-sharded counters and a bucketed latency
// histogram with interpolated percentiles. Everything is atomic so
// measurement never throttles the hot path.
package metrics

import (
	"math"
	"sync"
	"sync/atomic"

	mathrand "math/rand/v2"
)

// Histogram is a log-scale bucketed histogram from 1µs to 2s (1.1x growth,
// ~160 buckets). Percentiles are interpolated linearly within the crossing
// bucket; estimator precision is ±10% of the true value — the documented
// trade for O(1) Record on the hot path.
type Histogram struct {
	counts []atomic.Int64
	total  atomic.Int64
	bounds []int64 // upper bound of each bucket, ascending
	logInv float64 // 1 / ln(1.1), for O(1) bucket lookup
}

// NewHistogram builds the standard 1µs..2s histogram.
func NewHistogram() *Histogram {
	const growth = 1.1
	var bounds []int64
	ub := float64(1_000) // 1µs
	for ub < 2_000_000_000 {
		bounds = append(bounds, int64(ub))
		ub *= growth
	}
	bounds = append(bounds, math.MaxInt64/2)
	return &Histogram{
		counts: make([]atomic.Int64, len(bounds)),
		bounds: bounds,
		logInv: 1 / math.Log(growth),
	}
}

// Record adds one latency sample in nanoseconds.
func (h *Histogram) Record(ns int64) {
	i := 0
	if ns > 1_000 {
		i = int(math.Log(float64(ns)/1_000) * h.logInv) + 1
		if i >= len(h.counts) {
			i = len(h.counts) - 1
		}
	}
	h.counts[i].Add(1)
	h.total.Add(1)
}

// Count returns the number of recorded samples.
func (h *Histogram) Count() int64 { return h.total.Load() }

// Percentile returns the interpolated p-value (0..1) estimate in ns.
func (h *Histogram) Percentile(p float64) float64 {
	total := h.total.Load()
	if total == 0 {
		return 0
	}
	target := p * float64(total)
	var before float64
	for i := range h.counts {
		cnt := float64(h.counts[i].Load())
		if before+cnt >= target && cnt > 0 {
			lower := float64(0)
			if i > 0 {
				lower = float64(h.bounds[i-1])
			}
			upper := float64(h.bounds[i])
			frac := (target - before) / cnt
			return lower + frac*(upper-lower)
		}
		before += cnt
	}
	return 0
}

// Counter is a pre-registered sharded counter: Add takes no locks beyond the
// atomic slot add, so it is safe on the hottest path.
type Counter struct {
	slots []atomic.Int64
	rr    atomic.Uint64
}

// Add adds v, round-robining across shards.
func (c *Counter) Add(v int64) {
	i := int(mathrand.Int64N(int64(len(c.slots))))
	c.slots[i].Add(v)
}

// Sum returns the counter total across shards.
func (c *Counter) Sum() int64 {
	var s int64
	for i := range c.slots {
		s += c.slots[i].Load()
	}
	return s
}

// Registry holds named sharded counters. Lookups take a read lock (concurrent
// and cheap); creation takes the write lock once per name. Pointers are
// stable across creation of other names. Hot paths should Register once and
// use the returned *Counter.
type Registry struct {
	shards int
	mu     sync.RWMutex
	counts map[string]*Counter
}

// New builds a registry with the given shard count per counter.
func New(shards int) *Registry {
	return &Registry{shards: shards, counts: make(map[string]*Counter)}
}

// Register returns the named counter, creating it on first use. Call once at
// setup; keep the *Counter for lock-free Add on hot paths.
func (r *Registry) Register(name string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counts[name]; ok {
		return c
	}
	c := &Counter{slots: make([]atomic.Int64, r.shards)}
	r.counts[name] = c
	return c
}

// Incr adds 1 to the named counter (lookup path; prefer a registered *Counter
// on hot paths).
func (r *Registry) Incr(name string) { r.Add(name, 1) }

// Add adds v to the named counter (lookup path; creates the counter on
// first use — hot paths should Register once and use the *Counter directly).
func (r *Registry) Add(name string, v int64) {
	r.Register(name).Add(v)
}

func (r *Registry) lookup(name string) *Counter {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.counts[name]
}

// Get sums the named counter across shards.
func (r *Registry) Get(name string) int64 {
	c := r.lookup(name)
	if c == nil {
		return 0
	}
	return c.Sum()
}

// Snapshot returns a copy of all counter totals.
func (r *Registry) Snapshot() map[string]int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]int64, len(r.counts))
	for name, c := range r.counts {
		out[name] = c.Sum()
	}
	return out
}
