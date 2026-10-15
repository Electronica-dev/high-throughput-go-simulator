package queue

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"consuma/internal/ledger"
)

func newQueue(depth int, maxWait time.Duration) (*WaitQueue, *ledger.Ledger) {
	ld := ledger.New()
	return New(depth, maxWait, ld), ld
}

func mkReq(id uint64) *ledger.Request {
	return &ledger.Request{ID: id, Model: "m", EstimateTokens: 10}
}

// TestPopIfAdmissibleAtomicAgainstSweep: pushers, a popper, and a sweeper
// race; every request must reach exactly one terminal state - the atomic
// pop-if-admissible contract is what makes that hold.
func TestPopIfAdmissibleAtomicAgainstSweep(t *testing.T) {
	ld := ledger.New()
	maxWait := time.Millisecond
	q := New(64, maxWait, ld)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	t0 := time.Unix(100000, 0)
	var clock atomic.Int64

	// pusher: adds requests while running
	wg.Add(1)
	go func() {
		defer wg.Done()
		id := uint64(1)
		for {
			select {
			case <-stop:
				return
			default:
				r := &ledger.Request{ID: id, Model: "m", EstimateTokens: 1}
				id++
				ld.Submit(r)
				if !q.Push(r, time.Unix(t0.Unix()+clock.Load(), 0)) {
					ld.Transition(r, ledger.Rejected) // queue full: reject at door
				}
			}
		}
	}()

	// sweeper: expires anything older than maxWait as the clock advances
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				clock.Add(1) // advance time so pushed requests age out
				q.SweepExpired(time.Unix(t0.Unix()+clock.Load(), 0))
			}
		}
	}()

	// popper: atomically pop-if-admissible; admitted requests complete here
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				r := q.PopIfAdmissible(func(*ledger.Request) bool { return true })
				if r != nil {
					ld.Transition(r, ledger.InFlight)
					ld.Transition(r, ledger.Completed)
				}
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	// final sweep + drain whatever remains, then every request must balance
	now := time.Unix(t0.Unix()+clock.Load()+100, 0)
	q.SweepExpired(now)
	for {
		r := q.PopIfAdmissible(func(*ledger.Request) bool { return true })
		if r == nil {
			break
		}
		ld.Transition(r, ledger.InFlight)
		ld.Transition(r, ledger.Completed)
	}
	if !ld.TerminalBalance() {
		t.Fatalf("terminal balance broken: submitted=%d completed=%d expired=%d rejected=%d queued=%d admitted=%d inflight=%d qLen=%d",
			ld.Submitted(), ld.Count(ledger.Completed), ld.Count(ledger.Expired), ld.Count(ledger.Rejected),
			ld.Count(ledger.Queued), ld.Count(ledger.Admitted), ld.Count(ledger.InFlight), q.Len())
	}
}

func TestFIFOOrder(t *testing.T) {
	q, _ := newQueue(10, time.Second)
	t0 := time.Unix(100000, 0)
	for i := 1; i <= 5; i++ {
		if !q.Push(mkReq(uint64(i)), t0) {
			t.Fatalf("push %d failed", i)
		}
	}
	batch := q.PopBatch(t0)
	if len(batch) != 5 {
		t.Fatalf("want 5, got %d", len(batch))
	}
	for i, r := range batch {
		if r.ID != uint64(i+1) {
			t.Fatalf("FIFO violated: want id %d, got %d", i+1, r.ID)
		}
	}
}

func TestFullQueueRejects(t *testing.T) {
	q, _ := newQueue(3, time.Second)
	t0 := time.Unix(100000, 0)
	for i := 1; i <= 3; i++ {
		if !q.Push(mkReq(uint64(i)), t0) {
			t.Fatalf("push %d should succeed", i)
		}
	}
	if q.Push(mkReq(4), t0) {
		t.Fatal("4th push into depth-3 queue should fail")
	}
}

func TestSweepExpires(t *testing.T) {
	q, ld := newQueue(10, 100*time.Millisecond)
	t0 := time.Unix(100000, 0)
	q.Push(mkReq(1), t0)
	expired := q.SweepExpired(t0.Add(150 * time.Millisecond))
	if len(expired) != 1 || expired[0].ID != 1 {
		t.Fatalf("want request 1 expired, got %+v", expired)
	}
	if ld.Count(ledger.Expired) != 1 {
		t.Fatalf("ledger should show 1 expired, got %d", ld.Count(ledger.Expired))
	}
	if q.Len() != 0 {
		t.Fatalf("queue should be empty after sweep, got %d", q.Len())
	}
}

func TestSweepSparesYoung(t *testing.T) {
	q, _ := newQueue(10, 100*time.Millisecond)
	t0 := time.Unix(100000, 0)
	q.Push(mkReq(1), t0)
	if got := q.SweepExpired(t0.Add(50 * time.Millisecond)); len(got) != 0 {
		t.Fatalf("young request should not expire, got %+v", got)
	}
	if q.Len() != 1 {
		t.Fatalf("request should still be queued, got len %d", q.Len())
	}
}

func TestPopRespectsCapacity(t *testing.T) {
	q, _ := newQueue(2, time.Second)
	t0 := time.Unix(100000, 0)
	q.Push(mkReq(1), t0)
	q.Push(mkReq(2), t0)
	if q.Push(mkReq(3), t0) {
		t.Fatal("push into full queue should fail")
	}
	q.PopBatch(t0)
	if !q.Push(mkReq(3), t0) {
		t.Fatal("push after pop should succeed")
	}
}

// TestRecoveredEntriesSurviveSweep: a recovered request resumes with extra
// latency, not expiry — SweepExpired must skip recovered entries even past
// max_wait, while normal entries expire on schedule.
func TestRecoveredEntriesSurviveSweep(t *testing.T) {
	q := New(100, 50*time.Millisecond, ledger.New())
	now := time.Now()
	for i := 0; i < 5; i++ {
		r := &ledger.Request{ID: uint64(i), Model: "m", BornNS: now.UnixNano()}
		if !q.Push(r, now) {
			t.Fatal("push failed")
		}
	}
	for i := 100; i < 105; i++ {
		r := &ledger.Request{ID: uint64(i), Model: "m", BornNS: now.UnixNano()}
		if !q.PushRecovered(r, now) {
			t.Fatal("PushRecovered failed")
		}
	}
	time.Sleep(60 * time.Millisecond) // past max_wait
	expired := q.SweepExpired(time.Now())
	if len(expired) != 5 {
		t.Fatalf("normal entries must expire, got %d expired", len(expired))
	}
	// all 5 recovered entries still admitted afterwards
	for i := 0; i < 5; i++ {
		r := q.PopIfAdmissible(func(req *ledger.Request) bool { return true })
		if r == nil {
			t.Fatalf("recovered entry %d must survive the sweep and admit", i)
		}
	}
}
