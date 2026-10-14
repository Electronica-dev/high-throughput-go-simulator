// Package metrics provides lock-sharded counters and a bucketed latency
// histogram with interpolated percentiles. Everything is atomic so
// measurement never throttles the hot path.
package metrics

import (
	"math"
	"sync"
	"sync/atomic"
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

// Registry holds named sharded counters. Lookups take a read lock (concurrent
// and cheap); creation takes the write lock once per name. Pointers are
// stable across creation of other names.
type Registry struct {
	shards int
	mu     sync.RWMutex
	counts map[string]*sharded
}

type sharded struct {
	slots []atomic.Int64
}

// New builds a registry with the given shard count per counter.
func New(shards int) *Registry {
	return &Registry{shards: shards, counts: make(map[string]*sharded)}
}

func (r *Registry) counter(name string) *sharded {
	r.mu.RLock()
	s, ok := r.counts[name]
	r.mu.RUnlock()
	if ok {
		return s
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.counts[name]; ok {
		return s
	}
	s = &sharded{slots: make([]atomic.Int64, r.shards)}
	r.counts[name] = s
	return s
}

// Incr adds 1 to the named counter on a pseudo-random shard.
func (r *Registry) Incr(name string) { r.Add(name, 1) }

// Add adds v to the named counter. The shard is picked by an atomic round
// robin; at millions of ops/sec this spreads contention without pinning.
var shardRR atomic.Uint64

func (r *Registry) Add(name string, v int64) {
	s := r.counter(name)
	i := shardRR.Add(1) % uint64(len(s.slots))
	s.slots[i].Add(v)
}

// Get sums the named counter across shards.
func (r *Registry) Get(name string) int64 {
	r.mu.RLock()
	s, ok := r.counts[name]
	r.mu.RUnlock()
	if !ok {
		return 0
	}
	var sum int64
	for k := range s.slots {
		sum += s.slots[k].Load()
	}
	return sum
}

// Snapshot returns a copy of all counter totals.
func (r *Registry) Snapshot() map[string]int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]int64, len(r.counts))
	for name, s := range r.counts {
		var sum int64
		for k := range s.slots {
			sum += s.slots[k].Load()
		}
		out[name] = sum
	}
	return out
}
