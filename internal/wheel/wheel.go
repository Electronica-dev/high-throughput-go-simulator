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

// Entry is one scheduled completion event. Data carries the owner's payload
// (the engine stores the drawn provider CallResult here) so the wheel stays
// decoupled from what it schedules.
type Entry struct {
	Req    *ledger.Request
	FireNS int64 // absolute deadline, nanoseconds
	Data   any
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
	wheel := &Wheel{
		shards:      shards,
		buckets:     uint64(buckets),
		tickNS:      int64(tick),
		onFire:      onFire,
		nowFn:       func() int64 { return time.Now().UnixNano() },
		stop:        make(chan struct{}),
		shardWheels: make([]*shardWheel, shards),
	}
	for s := range wheel.shardWheels {
		sw := &shardWheel{
			buckets: make([]([]*Entry), buckets),
			mus:     make([]sync.Mutex, buckets),
		}
		wheel.shardWheels[s] = sw
	}
	return wheel
}

// SetNow injects a clock for tests; returns the wheel for chaining.
func (wheel *Wheel) SetNow(fn func() int64) *Wheel {
	wheel.nowFn = fn
	return wheel
}

// Start launches one ticker goroutine per shard.
func (wheel *Wheel) Start() {
	wheel.stopped.Store(false)
	for s := range wheel.shardWheels {
		wheel.wg.Add(1)
		go wheel.run(s)
	}
}

// Stop halts all tickers and waits for them to exit.
func (wheel *Wheel) Stop() {
	if wheel.stopped.Swap(true) {
		return
	}
	close(wheel.stop)
	wheel.wg.Wait()
}

// Pending returns the number of scheduled-but-not-fired entries.
func (wheel *Wheel) Pending() int64 { return wheel.pending.Load() }

// SetHandler replaces the fire callback (must be called before Start).
func (wheel *Wheel) SetHandler(fn func([]*Entry)) { wheel.onFire = fn }

// Done returns a channel closed when the wheel has stopped.
func (wheel *Wheel) Done() <-chan struct{} { return wheel.stop }

// Schedule adds an entry to the shard that owns its deadline tick. Entries
// sharing a deadline land in the same shard and drain as one batch.
func (wheel *Wheel) Schedule(fireNS int64, e *Entry) {
	if wheel.stopped.Load() {
		return
	}
	tick := uint64(fireNS / wheel.tickNS)
	shard := tick % uint64(wheel.shards)
	idx := tick & (wheel.buckets - 1)
	sw := wheel.shardWheels[shard]
	sw.mus[idx].Lock()
	sw.buckets[idx] = append(sw.buckets[idx], e)
	sw.mus[idx].Unlock()
	wheel.pending.Add(1)
}

func (wheel *Wheel) run(shard int) {
	defer wheel.wg.Done()
	sw := wheel.shardWheels[shard]
	ticker := time.NewTicker(time.Duration(wheel.tickNS))
	defer ticker.Stop()
	var batch []*Entry
	// Sequence-based draining with clock re-anchoring: the shard walks its
	// buckets in order (no skipping), but re-anchors to the wall clock
	// whenever it has fallen behind — after a stall it drains the backlog
	// promptly instead of replaying stale ticks one per ticker fire (which
	// would leave every scheduled entry uniformly late).
	var seq uint64
	first := true
	for {
		select {
		case <-wheel.stop:
			return
		case <-ticker.C:
			if first {
				// anchor seq to the current tick; one tick of entries may
				// already be scheduled in the current bucket, so start at
				// the bucket the clock considers "just passed"
				nowTick := uint64(wheel.nowFn() / wheel.tickNS)
				seq = nowTick
				first = false
			}
			// how far behind the wall clock are we (in ticks)?
			nowTick := uint64(wheel.nowFn() / wheel.tickNS)
			behind := int64(nowTick - seq)
			if behind > 2 {
				// stalled: drain the entire overdue span in one pass so
				// queued completions are not metered out one tick at a time
				for behind > 0 {
					seq++
					idx := (seq - 1) & (wheel.buckets - 1)
					sw.mus[idx].Lock()
					batch = append(batch[:0], sw.buckets[idx]...)
					sw.buckets[idx] = nil
					sw.mus[idx].Unlock()
					if len(batch) > 0 {
						wheel.pending.Add(-int64(len(batch)))
						wheel.onFire(batch)
						batch = batch[:0]
					}
					behind = int64(uint64(wheel.nowFn()/wheel.tickNS) - seq)
				}
				continue
			}
			seq++
			idx := (seq - 1) & (wheel.buckets - 1)
			sw.mus[idx].Lock()
			batch = append(batch[:0], sw.buckets[idx]...)
			sw.buckets[idx] = nil
			sw.mus[idx].Unlock()
			if len(batch) > 0 {
				wheel.pending.Add(-int64(len(batch)))
				wheel.onFire(batch)
				batch = batch[:0] // release for GC; onFire must not retain
			}
		}
	}
}
