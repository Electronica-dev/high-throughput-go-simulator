// Package queue implements the bounded wait queue with expiry — the cold
// path for requests that overflow a model's capacity. Every queued request
// either leaves via PopBatch (admitted when capacity frees) or SweepExpired
// (waited past max wait); the queue never drops silently.
package queue

import (
	"sync"
	"time"

	"htsim/internal/ledger"
)

type queued struct {
	req       *ledger.Request
	pushedNS  int64
	recovered bool // crash-recovery re-enqueue: exempt from max_wait expiry
}

// WaitQueue is a FIFO bounded queue with per-request max wait.
type WaitQueue struct {
	depth   int
	maxWait time.Duration
	ld      *ledger.Ledger
	mu      sync.Mutex
	items   []queued
	head    int // index of next item to pop
}

// New builds a queue of the given depth and max wait.
func New(depth int, maxWait time.Duration, ld *ledger.Ledger) *WaitQueue {
	return &WaitQueue{depth: depth, maxWait: maxWait, ld: ld, items: make([]queued, 0, depth)}
}

// Push enqueues r; returns false when the queue is full (caller rejects).
func (queue *WaitQueue) Push(r *ledger.Request, now time.Time) bool {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.items)-queue.head >= queue.depth {
		return false
	}
	if err := queue.ld.Transition(r, ledger.Queued); err != nil {
		return false // already terminal or illegal state; never double-queue
	}
	queue.items = append(queue.items, queued{req: r, pushedNS: now.UnixNano()})
	return true
}

// Peek returns the head request without popping or transitioning; nil when
// empty. The engine peeks to test admission before popping (FIFO fairness).
func (queue *WaitQueue) Peek() *ledger.Request {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.head >= len(queue.items) {
		return nil
	}
	return queue.items[queue.head].req
}

// PushQueued enqueues a request that is ALREADY in the Queued ledger state
// (the retry-deferral path transitions InFlight→Queued before calling this),
// recording pushedNS for expiry. Returns false when the queue is full.
func (queue *WaitQueue) PushQueued(r *ledger.Request, now time.Time) bool {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.items)-queue.head >= queue.depth {
		return false
	}
	queue.items = append(queue.items, queued{req: r, pushedNS: now.UnixNano()})
	return true
}

// PushRecovered enqueues a crash-recovered request: same FIFO as PushQueued
// but exempt from max_wait expiry — recovery resumes work with extra
// latency (the owner decision), it does not expire it.
func (queue *WaitQueue) PushRecovered(r *ledger.Request, now time.Time) bool {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.items)-queue.head >= queue.depth {
		return false
	}
	queue.items = append(queue.items, queued{req: r, pushedNS: now.UnixNano(), recovered: true})
	return true
}

// PopIfAdmissible atomically pops the head request when the admission check
// passes: peek, admit, and pop all happen under the queue lock, so a
// concurrent SweepExpired can never expire the peeked head between the
// admission decision and the pop (which would strand the popped request in
// Admitted with no wheel entry). Returns nil when the queue is empty or the
// head is not admissible.
func (queue *WaitQueue) PopIfAdmissible(admit func(*ledger.Request) bool) *ledger.Request {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.head >= len(queue.items) {
		return nil
	}
	r := queue.items[queue.head].req
	if !admit(r) {
		return nil
	}
	queue.head++
	if queue.head == len(queue.items) {
		queue.items = queue.items[:0]
		queue.head = 0
	}
	queue.ld.Transition(r, ledger.Admitted)
	return r
}

// PopOne pops the head request if any (FIFO), transitioning it to Admitted.
// Returns nil when the queue is empty. The engine uses this to drain the
// queue only as fast as the limiter admits, keeping FIFO fairness.
func (queue *WaitQueue) PopOne() *ledger.Request {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.head >= len(queue.items) {
		return nil
	}
	it := queue.items[queue.head]
	queue.head++
	if queue.head == len(queue.items) {
		queue.items = queue.items[:0]
		queue.head = 0
	}
	queue.ld.Transition(it.req, ledger.Admitted)
	return it.req
}

// PopBatch returns queued requests in FIFO order (admitted when capacity
// frees), transitioning each to Admitted in the ledger.
func (queue *WaitQueue) PopBatch(now time.Time) []*ledger.Request {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	n := len(queue.items) - queue.head
	if n == 0 {
		return nil
	}
	batch := make([]*ledger.Request, 0, n)
	for i := queue.head; i < len(queue.items); i++ {
		batch = append(batch, queue.items[i].req)
	}
	queue.items = queue.items[:0] // full drain: the queue pops everything available
	queue.head = 0
	for _, r := range batch {
		queue.ld.Transition(r, ledger.Admitted)
	}
	return batch
}

// SweepExpired expires every request past max wait, transitioning it to
// Expired in the ledger, and returns them.
func (queue *WaitQueue) SweepExpired(now time.Time) []*ledger.Request {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	cutoff := now.UnixNano() - queue.maxWait.Nanoseconds()
	var expired []*ledger.Request
	kept := queue.items[queue.head:]
	queue.head = 0
	queue.items = queue.items[:0]
	for _, it := range kept {
		if it.recovered {
			queue.items = append(queue.items, it) // recovery resumes, never expires
			continue
		}
		if it.pushedNS <= cutoff {
			expired = append(expired, it.req)
			queue.ld.Transition(it.req, ledger.Expired)
		} else {
			queue.items = append(queue.items, it)
		}
	}
	return expired
}

// Len returns the number of queued requests.
func (queue *WaitQueue) Len() int {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return len(queue.items) - queue.head
}
