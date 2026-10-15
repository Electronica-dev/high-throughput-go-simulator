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
	engine, err := NewReal(realConfig(), fc, st)
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	defer engine.Stop()

	const n = 5
	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := extReq(i)
			outcome, tokens, err := engine.SubmitAndWait(r, 2*time.Second)
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

	if got := engine.WheelPending(); got != 0 {
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
	engine, err := NewReal(realConfig(), fc, st)
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	defer engine.Stop()

	// simulate crash aftermath: 2 rows needing recovery, 1 already terminal
	mustInsert := func(id, state string) {
		if err := st.InsertRequest(store.RequestRow{ID: id, BatchID: "b-1", Model: "model-a", State: state, Tokens: 1000}); err != nil {
			t.Fatal(err)
		}
	}
	mustInsert("ext-101", "queued")
	mustInsert("ext-102", "in_flight")
	mustInsert("ext-103", "completed")

	n, err := engine.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if n != 2 {
		t.Fatalf("Recover want 2, got %d", n)
	}
	if n2, _ := engine.Recover(); n2 != 0 {
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
	engine, err := NewReal(realConfig(), fc, st)
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	defer engine.Stop()

	var fired atomic.Int32
	engine.OnTerminal = func(r *ledger.Request, outcome string, tokens int64) {
		fired.Add(1)
	}
	var wg sync.WaitGroup
	for i := 1; i <= 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, _ = engine.SubmitAndWait(extReq(i), 2*time.Second)
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
	engine, err := NewReal(cfg, fc, st)
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	defer engine.Stop()

	r := &ledger.Request{ExternalID: "ext-retry", Model: "model-a", EstimateTokens: 1000}
	outcome, _, err := engine.SubmitAndWait(r, 5*time.Second)
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
	l := engine.Limiter("model-a")
	if rpm := l.ObservedRPM(time.Now()); rpm > 10 {
		t.Fatalf("RPM reservations want <= 10, got %d (retry loop is unbounded)", rpm)
	}
}

// TestRecoveryNeverExpiresRecoveredRequests: recovered requests resume with
// extra latency, not expiry — even when the admission rate is far slower
// than the recovered backlog (the owner decision: the client never sees the
// crash, only extra latency).
func TestRecoveryNeverExpiresRecoveredRequests(t *testing.T) {
	st := openStore(t)
	fc := &fakeCaller{res: provider.CallResult{Outcome: provider.Success, ActualTokens: 900}}
	cfg := realConfig()
	cfg.MaxWaitMS = 50 // aggressive expiry: an unfixed sweep would kill the backlog
	cfg.Models[0].RPM = 6000
	engine, err := NewReal(cfg, fc, st)
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	defer engine.Stop()

	// 100 recovered requests vs a 100/s admission rate and a 50ms queue TTL
	for i := 0; i < 100; i++ {
		if err := st.InsertRequest(store.RequestRow{
			ID: fmt.Sprintf("ext-rec-%d", i), BatchID: "b-rec", Model: "model-a", State: "in_flight", Tokens: 1000,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := engine.Recover(); err != nil || n != 100 {
		t.Fatalf("Recover: %d %v", n, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		done := 0
		for i := 0; i < 100; i++ {
			if row, ok, _ := st.GetRequest(fmt.Sprintf("ext-rec-%d", i)); ok && row.State == "completed" {
				done++
			}
		}
		if done == 100 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	expired := 0
	for i := 0; i < 100; i++ {
		if row, ok, _ := st.GetRequest(fmt.Sprintf("ext-rec-%d", i)); ok && row.State == "expired" {
			expired++
		}
	}
	t.Fatalf("recovered requests expired: %d of 100 (recovery must not be subject to max_wait)", expired)
}

// scriptedCaller returns pre-scripted results in order.
type scriptedCaller struct {
	mu    sync.Mutex
	res   []provider.CallResult
	i     int
	calls atomic.Int32
}

func (s *scriptedCaller) Call(r *ledger.Request, now time.Time) provider.CallResult {
	s.mu.Lock()
	i := s.i
	if i < len(s.res) {
		s.i++
	}
	s.mu.Unlock()
	s.calls.Add(1)
	if i >= len(s.res) {
		i = len(s.res) - 1
	}
	return s.res[i]
}

// TestRecoveredRequestRetryDeferralNeverExpires (review F3): the recovered
// exemption must survive a retry deferral — a recovered request that fails
// once is re-queued via the deferral path and must NOT become sweep-expiry
// fodder. Unfixed, the deferral rides a normal queue entry and the request
// expires ~50ms later.
func TestRecoveredRequestRetryDeferralNeverExpires(t *testing.T) {
	st := openStore(t)
	fc := &scriptedCaller{res: []provider.CallResult{
		{Outcome: provider.TransientFailure, ActualTokens: 0},
		{Outcome: provider.Success, ActualTokens: 900},
	}}
	cfg := realConfig()
	cfg.MaxWaitMS = 50
	cfg.Models[0].RPM = 1 // window full after the first admission → ChargeRetry fails → deferral
	cfg.Models[0].TPM = 1 << 40
	engine, err := NewReal(cfg, fc, st)
	if err != nil {
		t.Fatalf("NewReal: %v", err)
	}
	defer engine.Stop()

	if err := st.InsertRequest(store.RequestRow{
		ID: "ext-rec-def", BatchID: "b-rec", Model: "model-a", State: "in_flight", Tokens: 1000,
	}); err != nil {
		t.Fatal(err)
	}
	if n, err := engine.Recover(); err != nil || n != 1 {
		t.Fatalf("Recover: %d %v", n, err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		row, _, _ := st.GetRequest("ext-rec-def")
		if row.State == "expired" {
			t.Fatal("recovered request expired after a retry deferral — the exemption did not survive the re-queue")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// still not expired after 2s (the deferral waits for the rolling window;
	// completing would need the window to roll, which takes ~60s)
	row, _, _ := st.GetRequest("ext-rec-def")
	if row.State == "expired" {
		t.Fatal("recovered request expired")
	}
}
