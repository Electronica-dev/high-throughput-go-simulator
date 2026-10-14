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
	q.items = q.items[q.head:]
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
