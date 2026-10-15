package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"consuma/internal/config"
	"consuma/internal/ledger"
	"consuma/internal/provider"
	"consuma/internal/store"
)

func realConfig() *config.Config {
	c := testConfig(nil)
	c.Workers = 8
	return c
}

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir() + "/sys.db")
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func extReq(i int) *ledger.Request {
	return &ledger.Request{
		ID:             uint64(i),
		ExternalID:     fmt.Sprintf("ext-%d", i),
		Model:          "model-a",
		EstimateTokens: 1000,
	}
}

// TestRealModeCompletesWithPersistedState: fake caller processes every
// request; the store reflects each request's terminal state with the actual
// charge; no wheel entries at any point.
func TestRealModeCompletesWithPersistedState(t *testing.T) {
	st := openStore(t)
	fc := &fakeCaller{res: provider.CallResult{
		Latency: 0, Outcome: provider.Success, ActualTokens: 900,
	}}
	e, err := NewReal(realConfig(), fc, st)
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	defer e.Stop()

	const n = 5
	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := extReq(i)
			outcome, tokens, err := e.SubmitAndWait(r, 2*time.Second)
			if err != nil {
				t.Errorf("SubmitAndWait %d: %v", i, err)
				return
			}
			if outcome != "success" || tokens != 900 {
				t.Errorf("request %d: outcome=%s tokens=%d", i, outcome, tokens)
			}
		}(i)
	}
	wg.Wait()

	if got := e.WheelPending(); got != 0 {
		t.Fatalf("real mode must not use the wheel, pending=%d", got)
	}
	if fc.calls.Load() != n {
		t.Fatalf("provider calls want %d, got %d", n, fc.calls.Load())
	}
	for i := 1; i <= n; i++ {
		row, ok, err := st.GetRequest(fmt.Sprintf("ext-%d", i))
		if err != nil || !ok {
			t.Fatalf("row %d missing: ok=%v err=%v", i, ok, err)
		}
		if row.State != "completed" || row.Outcome != "success" || row.Tokens != 900 {
			t.Fatalf("row %d mismatch: %+v", i, row)
		}
	}
}

// TestRecoveryReenqueuesAndCompletes: rows left queued/in_flight by a crash
// are re-enqueued on Recover() and then processed to completion; the sweep
// is idempotent.
func TestRecoveryReenqueuesAndCompletes(t *testing.T) {
	st := openStore(t)
	fc := &fakeCaller{res: provider.CallResult{Outcome: provider.Success, ActualTokens: 800}}
	e, err := NewReal(realConfig(), fc, st)
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	defer e.Stop()

	// simulate crash aftermath: 2 rows needing recovery, 1 already terminal
	mustInsert := func(id, state string) {
		if err := st.InsertRequest(store.RequestRow{ID: id, BatchID: "b-1", Model: "model-a", State: state, Tokens: 1000}); err != nil {
			t.Fatal(err)
		}
	}
	mustInsert("ext-101", "queued")
	mustInsert("ext-102", "in_flight")
	mustInsert("ext-103", "completed")

	n, err := e.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n != 2 {
		t.Fatalf("Recover want 2, got %d", n)
	}
	if n2, _ := e.Recover(); n2 != 0 {
		t.Fatalf("Recover must be idempotent, second run got %d", n2)
	}

	// the re-enqueued requests complete on their own
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r1, _, _ := st.GetRequest("ext-101")
		r2, _, _ := st.GetRequest("ext-102")
		if r1.State == "completed" && r2.State == "completed" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("recovered requests did not complete in time")
}

// TestOnTerminalFiresPerRequest: the hook fires exactly once per request at
// its terminal transition (the batch manager's count source).
func TestOnTerminalFiresPerRequest(t *testing.T) {
	st := openStore(t)
	fc := &fakeCaller{res: provider.CallResult{Outcome: provider.Success, ActualTokens: 100}}
	e, err := NewReal(realConfig(), fc, st)
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	defer e.Stop()

	var fired atomic.Int32
	e.OnTerminal = func(r *ledger.Request, outcome string, tokens int64) {
		fired.Add(1)
	}
	var wg sync.WaitGroup
	for i := 1; i <= 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, _ = e.SubmitAndWait(extReq(i), 2*time.Second)
		}(i)
	}
	wg.Wait()
	if fired.Load() != 4 {
		t.Fatalf("OnTerminal want 4 fires, got %d", fired.Load())
	}
}

// TestRealModeRetryTerminatesWithBoundedAttempts: a permanently transient
// caller must terminate the request as failed after MaxRetries attempts —
// and the RPM window must see ONE admission plus at most MaxRetries-1
// ChargeRetry reservations (an unbounded retry loop floods RPM at thousands
// of reservations per second and starves every other request).
func TestRealModeRetryTerminatesWithBoundedAttempts(t *testing.T) {
	st := openStore(t)
	fc := &fakeCaller{res: provider.CallResult{Outcome: provider.TransientFailure, ActualTokens: 0}}
	cfg := realConfig()
	cfg.MaxRetries = 3
	e, err := NewReal(cfg, fc, st)
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	defer e.Stop()

	r := &ledger.Request{ExternalID: "ext-retry", Model: "model-a", EstimateTokens: 1000}
	outcome, _, err := e.SubmitAndWait(r, 5*time.Second)
	if err != nil {
		t.Fatalf("SubmitAndWait: %v", err)
	}
	if outcome != "failed" {
		t.Fatalf("outcome want failed, got %q", outcome)
	}
	if got := r.Attempts.Load(); got != 3 {
		t.Fatalf("attempts want 3, got %d", got)
	}
	if calls := fc.calls.Load(); calls != 3 {
		t.Fatalf("provider calls want 3, got %d", calls)
	}
	// bounded RPM: 1 admission + 2 ChargeRetry = 3 reservations, never a flood
	l := e.Limiter("model-a")
	if rpm := l.ObservedRPM(time.Now()); rpm > 10 {
		t.Fatalf("RPM reservations want <= 10, got %d (retry loop is unbounded)", rpm)
	}
}
