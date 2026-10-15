package queue

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"htsim/internal/ledger"
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
	queue := New(64, maxWait, ld)

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
				if !queue.Push(r, time.Unix(t0.Unix()+clock.Load(), 0)) {
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
				queue.SweepExpired(time.Unix(t0.Unix()+clock.Load(), 0))
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
				r := queue.PopIfAdmissible(func(*ledger.Request) bool { return true })
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
	queue.SweepExpired(now)
	for {
		r := queue.PopIfAdmissible(func(*ledger.Request) bool { return true })
		if r == nil {
			break
		}
		ld.Transition(r, ledger.InFlight)
		ld.Transition(r, ledger.Completed)
	}
	if !ld.TerminalBalance() {
		t.Fatalf("terminal balance broken: submitted=%d completed=%d expired=%d rejected=%d queued=%d admitted=%d inflight=%d qLen=%d",
			ld.Submitted(), ld.Count(ledger.Completed), ld.Count(ledger.Expired), ld.Count(ledger.Rejected),
			ld.Count(ledger.Queued), ld.Count(ledger.Admitted), ld.Count(ledger.InFlight), queue.Len())
	}
}

func TestFIFOOrder(t *testing.T) {
	queue, _ := newQueue(10, time.Second)
	t0 := time.Unix(100000, 0)
	for i := 1; i <= 5; i++ {
		if !queue.Push(mkReq(uint64(i)), t0) {
			t.Fatalf("push %d failed", i)
		}
	}
	batch := queue.PopBatch(t0)
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
	queue, _ := newQueue(3, time.Second)
	t0 := time.Unix(100000, 0)
	for i := 1; i <= 3; i++ {
		if !queue.Push(mkReq(uint64(i)), t0) {
			t.Fatalf("push %d should succeed", i)
		}
	}
	if queue.Push(mkReq(4), t0) {
		t.Fatal("4th push into depth-3 queue should fail")
	}
}

func TestSweepExpires(t *testing.T) {
	queue, ld := newQueue(10, 100*time.Millisecond)
	t0 := time.Unix(100000, 0)
	queue.Push(mkReq(1), t0)
	expired := queue.SweepExpired(t0.Add(150 * time.Millisecond))
	if len(expired) != 1 || expired[0].ID != 1 {
		t.Fatalf("want request 1 expired, got %+v", expired)
	}
	if ld.Count(ledger.Expired) != 1 {
		t.Fatalf("ledger should show 1 expired, got %d", ld.Count(ledger.Expired))
	}
	if queue.Len() != 0 {
		t.Fatalf("queue should be empty after sweep, got %d", queue.Len())
	}
}

func TestSweepSparesYoung(t *testing.T) {
	queue, _ := newQueue(10, 100*time.Millisecond)
	t0 := time.Unix(100000, 0)
	queue.Push(mkReq(1), t0)
	if got := queue.SweepExpired(t0.Add(50 * time.Millisecond)); len(got) != 0 {
		t.Fatalf("young request should not expire, got %+v", got)
	}
	if queue.Len() != 1 {
		t.Fatalf("request should still be queued, got len %d", queue.Len())
	}
}

func TestPopRespectsCapacity(t *testing.T) {
	queue, _ := newQueue(2, time.Second)
	t0 := time.Unix(100000, 0)
	queue.Push(mkReq(1), t0)
	queue.Push(mkReq(2), t0)
	if queue.Push(mkReq(3), t0) {
		t.Fatal("push into full queue should fail")
	}
	queue.PopBatch(t0)
	if !queue.Push(mkReq(3), t0) {
		t.Fatal("push after pop should succeed")
	}
}

// TestRecoveredEntriesSurviveSweep: a recovered request resumes with extra
// latency, not expiry — SweepExpired must skip recovered entries even past
// max_wait, while normal entries expire on schedule.
func TestRecoveredEntriesSurviveSweep(t *testing.T) {
	queue := New(100, 50*time.Millisecond, ledger.New())
	now := time.Now()
	for i := 0; i < 5; i++ {
		r := &ledger.Request{ID: uint64(i), Model: "m", BornNS: now.UnixNano()}
		if !queue.Push(r, now) {
			t.Fatal("push failed")
		}
	}
	for i := 100; i < 105; i++ {
		r := &ledger.Request{ID: uint64(i), Model: "m", BornNS: now.UnixNano()}
		if !queue.PushRecovered(r, now) {
			t.Fatal("PushRecovered failed")
		}
	}
	time.Sleep(60 * time.Millisecond) // past max_wait
	expired := queue.SweepExpired(time.Now())
	if len(expired) != 5 {
		t.Fatalf("normal entries must expire, got %d expired", len(expired))
	}
	// all 5 recovered entries still admitted afterwards
	for i := 0; i < 5; i++ {
		r := queue.PopIfAdmissible(func(req *ledger.Request) bool { return true })
		if r == nil {
			t.Fatalf("recovered entry %d must survive the sweep and admit", i)
		}
	}
}
