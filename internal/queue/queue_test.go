package queue

import (
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
