// Package wheel implements a sharded timer wheel: O(1) scheduling of
// completion events and batched expiry, sized so no entry ever wraps the ring
// (config validation guarantees latencies fit the horizon).
package wheel

import (
	"sync"
	"sync/atomic"
	"time"

	"consuma/internal/ledger"
)

// Entry is one scheduled completion event.
type Entry struct {
	Req    *ledger.Request
	FireNS int64 // absolute deadline, nanoseconds
}

// Wheel holds shards; each shard is a ring of buckets drained by one ticker.
type Wheel struct {
	shards  int
	buckets uint64
	tickNS  int64

	onFire func([]*Entry)

	nowFn func() int64

	pending atomic.Int64
	stopped atomic.Bool
	stop    chan struct{}
	wg      sync.WaitGroup

	shardWheels []*shardWheel
}

type shardWheel struct {
	buckets []([]*Entry)
	mus     []sync.Mutex
}

// New builds a wheel. buckets must be a power of two. onFire receives drained
// batches; it is called from ticker goroutines and must not block.
func New(shards, buckets int, tick time.Duration, onFire func([]*Entry)) *Wheel {
	if buckets&(buckets-1) != 0 || buckets <= 0 {
		panic("wheel: buckets must be a positive power of two")
	}
	if shards <= 0 {
		panic("wheel: shards must be > 0")
	}
	w := &Wheel{
		shards:      shards,
		buckets:     uint64(buckets),
		tickNS:      int64(tick),
		onFire:      onFire,
		nowFn:       func() int64 { return time.Now().UnixNano() },
		stop:        make(chan struct{}),
		shardWheels: make([]*shardWheel, shards),
	}
	for s := range w.shardWheels {
		sw := &shardWheel{
			buckets: make([]([]*Entry), buckets),
			mus:     make([]sync.Mutex, buckets),
		}
		w.shardWheels[s] = sw
	}
	return w
}

// SetNow injects a clock for tests; returns the wheel for chaining.
func (w *Wheel) SetNow(fn func() int64) *Wheel {
	w.nowFn = fn
	return w
}

// Start launches one ticker goroutine per shard.
func (w *Wheel) Start() {
	w.stopped.Store(false)
	for s := range w.shardWheels {
		w.wg.Add(1)
		go w.run(s)
	}
}

// Stop halts all tickers and waits for them to exit.
func (w *Wheel) Stop() {
	if w.stopped.Swap(true) {
		return
	}
	close(w.stop)
	w.wg.Wait()
}

// Pending returns the number of scheduled-but-not-fired entries.
func (w *Wheel) Pending() int64 { return w.pending.Load() }

// Schedule adds an entry to the shard that owns its deadline tick. Entries
// sharing a deadline land in the same shard and drain as one batch.
func (w *Wheel) Schedule(fireNS int64, e *Entry) {
	if w.stopped.Load() {
		return
	}
	tick := uint64(fireNS / w.tickNS)
	shard := tick % uint64(w.shards)
	idx := tick & (w.buckets - 1)
	sw := w.shardWheels[shard]
	sw.mus[idx].Lock()
	sw.buckets[idx] = append(sw.buckets[idx], e)
	sw.mus[idx].Unlock()
	w.pending.Add(1)
}

func (w *Wheel) run(shard int) {
	defer w.wg.Done()
	sw := w.shardWheels[shard]
	ticker := time.NewTicker(time.Duration(w.tickNS))
	defer ticker.Stop()
	var batch []*Entry
	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
			nowTick := uint64(w.nowFn() / w.tickNS)
			idx := (nowTick - 1) & (w.buckets - 1)
			sw.mus[idx].Lock()
			batch = append(batch[:0], sw.buckets[idx]...)
			sw.buckets[idx] = nil
			sw.mus[idx].Unlock()
			if len(batch) > 0 {
				w.pending.Add(-int64(len(batch)))
				w.onFire(batch)
				batch = batch[:0] // release for GC; onFire must not retain
			}
		}
	}
}
