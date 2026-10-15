// Package queue implements the bounded wait queue with expiry — the cold
// path for requests that overflow a model's capacity. Every queued request
// either leaves via PopBatch (admitted when capacity frees) or SweepExpired
// (waited past max wait); the queue never drops silently.
package queue

import (
	"sync"
	"time"

	"consuma/internal/ledger"
)

type queued struct {
	req      *ledger.Request
	pushedNS int64
	recovered bool // crash-recovery re-enqueue: exempt from max_wait expiry
}

// WaitQueue is a FIFO bounded queue with per-request max wait.
type WaitQueue struct {
	depth    int
	maxWait  time.Duration
	ld       *ledger.Ledger
	mu       sync.Mutex
	items    []queued
	head     int // index of next item to pop
}

// New builds a queue of the given depth and max wait.
func New(depth int, maxWait time.Duration, ld *ledger.Ledger) *WaitQueue {
	return &WaitQueue{depth: depth, maxWait: maxWait, ld: ld, items: make([]queued, 0, depth)}
}

// Push enqueues r; returns false when the queue is full (caller rejects).
func (q *WaitQueue) Push(r *ledger.Request, now time.Time) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items)-q.head >= q.depth {
		return false
	}
	if err := q.ld.Transition(r, ledger.Queued); err != nil {
		return false // already terminal or illegal state; never double-queue
	}
	q.items = append(q.items, queued{req: r, pushedNS: now.UnixNano()})
	return true
}

// Peek returns the head request without popping or transitioning; nil when
// empty. The engine peeks to test admission before popping (FIFO fairness).
func (q *WaitQueue) Peek() *ledger.Request {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head >= len(q.items) {
		return nil
	}
	return q.items[q.head].req
}

// PushQueued enqueues a request that is ALREADY in the Queued ledger state
// (the retry-deferral path transitions InFlight→Queued before calling this),
// recording pushedNS for expiry. Returns false when the queue is full.
func (q *WaitQueue) PushQueued(r *ledger.Request, now time.Time) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items)-q.head >= q.depth {
		return false
	}
	q.items = append(q.items, queued{req: r, pushedNS: now.UnixNano()})
	return true
}

// PushRecovered enqueues a crash-recovered request: same FIFO as PushQueued
// but exempt from max_wait expiry — recovery resumes work with extra
// latency (the owner decision), it does not expire it.
func (q *WaitQueue) PushRecovered(r *ledger.Request, now time.Time) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items)-q.head >= q.depth {
		return false
	}
	q.items = append(q.items, queued{req: r, pushedNS: now.UnixNano(), recovered: true})
	return true
}

// PopIfAdmissible atomically pops the head request when the admission check
// passes: peek, admit, and pop all happen under the queue lock, so a
// concurrent SweepExpired can never expire the peeked head between the
// admission decision and the pop (which would strand the popped request in
// Admitted with no wheel entry). Returns nil when the queue is empty or the
// head is not admissible.
func (q *WaitQueue) PopIfAdmissible(admit func(*ledger.Request) bool) *ledger.Request {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head >= len(q.items) {
		return nil
	}
	r := q.items[q.head].req
	if !admit(r) {
		return nil
	}
	q.head++
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	}
	q.ld.Transition(r, ledger.Admitted)
	return r
}

// PopOne pops the head request if any (FIFO), transitioning it to Admitted.
// Returns nil when the queue is empty. The engine uses this to drain the
// queue only as fast as the limiter admits, keeping FIFO fairness.
func (q *WaitQueue) PopOne() *ledger.Request {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.head >= len(q.items) {
		return nil
	}
	it := q.items[q.head]
	q.head++
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	}
	q.ld.Transition(it.req, ledger.Admitted)
	return it.req
}

// PopBatch returns queued requests in FIFO order (admitted when capacity
// frees), transitioning each to Admitted in the ledger.
func (q *WaitQueue) PopBatch(now time.Time) []*ledger.Request {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := len(q.items) - q.head
	if n == 0 {
		return nil
	}
	batch := make([]*ledger.Request, 0, n)
	for i := q.head; i < len(q.items); i++ {
		batch = append(batch, q.items[i].req)
	}
	q.items = q.items[:0] // full drain: the queue pops everything available
	q.head = 0
	for _, r := range batch {
		q.ld.Transition(r, ledger.Admitted)
	}
	return batch
}

// SweepExpired expires every request past max wait, transitioning it to
// Expired in the ledger, and returns them.
func (q *WaitQueue) SweepExpired(now time.Time) []*ledger.Request {
	q.mu.Lock()
	defer q.mu.Unlock()
	cutoff := now.UnixNano() - q.maxWait.Nanoseconds()
	var expired []*ledger.Request
	kept := q.items[q.head:]
	q.head = 0
	q.items = q.items[:0]
	for _, it := range kept {
		if it.recovered {
			q.items = append(q.items, it) // recovery resumes, never expires
			continue
		}
		if it.pushedNS <= cutoff {
			expired = append(expired, it.req)
			q.ld.Transition(it.req, ledger.Expired)
		} else {
			q.items = append(q.items, it)
		}
	}
	return expired
}

// Len returns the number of queued requests.
func (q *WaitQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items) - q.head
}
