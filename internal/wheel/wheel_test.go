package wheel

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"consuma/internal/ledger"
)

func mkReq(id uint64) *ledger.Request {
	return &ledger.Request{ID: id, Model: "m"}
}

func newWheel(t *testing.T, shards, buckets int) (*Wheel, chan []*Entry) {
	t.Helper()
	ch := make(chan []*Entry, 4096)
	w := New(shards, buckets, time.Millisecond, func(batch []*Entry) {
		ch <- batch
	})
	w.Start()
	t.Cleanup(w.Stop)
	return w, ch
}

func TestFiresWithinTolerance(t *testing.T) {
	w, ch := newWheel(t, 2, 256)
	now := time.Now().UnixNano()
	r := mkReq(1)
	w.Schedule(now+10*int64(time.Millisecond), &Entry{Req: r, FireNS: now + 10*int64(time.Millisecond)})

	select {
	case batch := <-ch:
		if len(batch) != 1 {
			t.Fatalf("want 1 entry, got %d", len(batch))
		}
		if batch[0].Req.ID != 1 {
			t.Fatalf("wrong entry: %+v", batch[0].Req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("entry did not fire within 2s")
	}
}

func TestBatchDrain(t *testing.T) {
	w, ch := newWheel(t, 2, 256)
	now := time.Now().UnixNano()
	deadline := now + 10*int64(time.Millisecond)
	for i := 0; i < 100; i++ {
		w.Schedule(deadline, &Entry{Req: mkReq(uint64(i + 1)), FireNS: deadline})
	}
	select {
	case batch := <-ch:
		if len(batch) != 100 {
			t.Fatalf("want single batch of 100, got %d", len(batch))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("batch did not fire within 2s")
	}
	// no partial batches before or after
	select {
	case b := <-ch:
		t.Fatalf("unexpected extra batch of %d", len(b))
	case <-time.After(50 * time.Millisecond):
	}
}

func TestNoEarlyFire(t *testing.T) {
	w, ch := newWheel(t, 2, 256)
	now := time.Now().UnixNano()
	w.Schedule(now+50*int64(time.Millisecond), &Entry{Req: mkReq(1), FireNS: now + 50*int64(time.Millisecond)})
	select {
	case b := <-ch:
		t.Fatalf("fired early: %d entries", len(b))
	case <-time.After(25 * time.Millisecond):
	}
}

func TestShardDistribution(t *testing.T) {
	w, _ := newWheel(t, 4, 256)
	now := time.Now().UnixNano()
	for i := 0; i < 1000; i++ {
		// spread deadlines so multiple shards receive entries
		deadline := now + int64(i%50)*int64(time.Millisecond)
		w.Schedule(deadline, &Entry{Req: mkReq(uint64(i)), FireNS: deadline})
	}
	if got := w.Pending(); got != 1000 {
		t.Fatalf("Pending want 1000, got %d", got)
	}
}

func TestStopQuiesces(t *testing.T) {
	ch := make(chan []*Entry, 16)
	var fired atomic.Int64
	w := New(1, 64, time.Millisecond, func(batch []*Entry) {
		fired.Add(int64(len(batch)))
		ch <- batch
	})
	w.Start()
	now := time.Now().UnixNano()
	deadline := now + 5*int64(time.Millisecond)
	w.Schedule(deadline, &Entry{Req: mkReq(1), FireNS: deadline})
	select {
	case <-ch:
	case <-time.After(1 * time.Second):
		t.Fatal("entry never fired")
	}
	w.Stop()
	n := fired.Load()
	time.Sleep(20 * time.Millisecond)
	if fired.Load() != n {
		t.Fatalf("fired after Stop: %d -> %d", n, fired.Load())
	}
}

func TestPowerOfTwoBucketsRequired(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("non-power-of-two buckets should panic at construction")
		}
	}()
	New(1, 100, time.Millisecond, func([]*Entry) {})
}

var _ sync.Mutex // keep sync import if unused paths change
