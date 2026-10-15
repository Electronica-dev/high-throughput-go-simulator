package batch

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"consuma/internal/config"
	"consuma/internal/engine"
	"consuma/internal/ledger"
	"consuma/internal/provider"
	"consuma/internal/store"
)

// ---------- fakes ----------

type okCaller struct{ calls atomic.Int32 }

func (f *okCaller) Call(r *ledger.Request, now time.Time) provider.CallResult {
	f.calls.Add(1)
	return provider.CallResult{Outcome: provider.Success, ActualTokens: 900}
}

// blockingCaller blocks the FIRST call until released.
type blockingCaller struct {
	release chan struct{}
	calls   atomic.Int32
}

func (f *blockingCaller) Call(r *ledger.Request, now time.Time) provider.CallResult {
	if f.calls.Add(1) == 1 {
		<-f.release
	}
	return provider.CallResult{Outcome: provider.Success, ActualTokens: 900}
}

func bigConfig() *config.Config {
	return &config.Config{
		MaxWaitMS: 500, QueueDepth: 64000, WheelShards: 2, WheelBuckets: 256, WheelTickMS: 1, MaxRetries: 3, Workers: 16,
		Models: []config.ModelConfig{{
			Name: "model-a", RPM: 1000000, TPM: 1000000000, EstimateTokens: 100, ActualTokenMean: 100,
			Latency: config.LatencyConfig{Kind: "fixed", MeanMS: 1},
			Outcome: config.OutcomeConfig{SuccessPct: 100},
		}},
	}
}

// fastCallbackCfg: compressed schedule so retry tests finish in milliseconds.
func fastCallbackCfg() config.CallbackConfig {
	return config.CallbackConfig{
		InitialBackoffs:  []time.Duration{5 * time.Millisecond, 5 * time.Millisecond, 5 * time.Millisecond},
		PeriodicInterval: 5 * time.Millisecond,
		DeliveryTTL:      time.Second,
		HTTPTimeout:      200 * time.Millisecond,
	}
}

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir() + "/b.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// sinkCount is a callback sink recording every hit (first failFirst get 500).
type sinkCount struct {
	mu    sync.Mutex
	codes []int
	ch    chan struct{}
}

func newSink(failFirst int) (*sinkCount, *httptest.Server) {
	s := &sinkCount{ch: make(chan struct{}, 1000)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		code := 200
		if len(s.codes) < failFirst {
			code = 500
		}
		s.codes = append(s.codes, code)
		s.mu.Unlock()
		w.WriteHeader(code)
		s.ch <- struct{}{}
	}))
	return s, srv
}

func (s *sinkCount) hitsCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.codes)
}

// ---------- tests ----------

// TestCallbackOnlyAfterAllTerminal: the callback must not fire while any
// request is still in flight — the batch completes only when every request
// is accounted for.
func TestCallbackOnlyAfterAllTerminal(t *testing.T) {
	st := openStore(t)
	rel := make(chan struct{})
	sink, srv := newSink(0)
	defer srv.Close()

	eng, err := engine.NewReal(bigConfig(), &blockingCaller{release: rel}, st)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()
	m := New(st, eng, fastCallbackCfg())

	id, err := m.Submit("model-a", 1000, 2, srv.URL)
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// first request is blocked; the second completes but the batch must NOT
	// complete, and no callback may fire
	select {
	case <-sink.ch:
		t.Fatal("callback fired before all requests reached terminal state")
	case <-time.After(150 * time.Millisecond):
	}

	close(rel) // let the blocked request finish
	select {
	case <-sink.ch:
	case <-time.After(2 * time.Second):
		t.Fatal("callback never fired after all requests completed")
	}

	b, ok := m.Status(id)
	if !ok || b.Status != "completed" {
		t.Fatalf("batch status: ok=%v %+v", ok, b)
	}
	if b.Counts.Successful != 2 || b.Counts.Total != 2 {
		t.Fatalf("counts mismatch: %+v", b.Counts)
	}
}

// TestBatchAckUnderLoad: Submit of a 10,000-request batch must acknowledge
// in well under a second.
func TestBatchAckUnderLoad(t *testing.T) {
	st := openStore(t)
	eng, err := engine.NewReal(bigConfig(), &okCaller{}, st)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()
	m := New(st, eng, fastCallbackCfg())

	start := time.Now()
	id, err := m.Submit("model-a", 1000, 10000, "") // no callback
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > time.Second {
		t.Fatalf("batch ack took %v, want < 1s", elapsed)
	}
	_ = id
}

// TestCallbackRetriesUntilSuccess: 500, 500, then 200 — delivered, three
// attempts persisted in order.
func TestCallbackRetriesUntilSuccess(t *testing.T) {
	st := openStore(t)
	sink, srv := newSink(2)
	defer srv.Close()

	eng, err := engine.NewReal(bigConfig(), &okCaller{}, st)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()
	m := New(st, eng, fastCallbackCfg())

	id, err := m.Submit("model-a", 1000, 1, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(3 * time.Second)
	for {
		sink.mu.Lock()
		done := len(sink.codes) >= 3 && sink.codes[len(sink.codes)-1] == 200
		sink.mu.Unlock()
		if done {
			break
		}
		select {
		case <-sink.ch:
		case <-deadline:
			t.Fatalf("callback never succeeded; hits=%d", sink.hitsCount())
		}
	}
	var att []store.Attempt
	deadline2 := time.Now().Add(2 * time.Second)
	for {
		att, err = st.CallbackAttempts(id)
		if err == nil && len(att) >= 3 {
			break
		}
		if time.Now().After(deadline2) {
			t.Fatalf("want 3 persisted attempts, got %d", len(att))
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(att) != 3 {
		t.Fatalf("want 3 persisted attempts, got %d", len(att))
	}
	if att[0].StatusCode != 500 || att[1].StatusCode != 500 || att[2].StatusCode != 200 {
		t.Fatalf("attempt codes: %v/%v/%v", att[0].StatusCode, att[1].StatusCode, att[2].StatusCode)
	}
	if att[2].Outcome != "delivered" {
		t.Fatalf("final attempt outcome: %q", att[2].Outcome)
	}
}

// TestCallbackResumeAfterRestart: delivery that never succeeded within the
// TTL (sink down the whole time) is re-dispatched by a fresh Manager over
// the same database, to the same persisted URL, once the sink works.
func TestCallbackResumeAfterRestart(t *testing.T) {
	st := openStore(t)

	// sink that fails until flipped
	var failing atomic.Bool
	failing.Store(true)
	hits := make(chan struct{}, 1000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failing.Load() {
			w.WriteHeader(500)
		} else {
			w.WriteHeader(200)
		}
		hits <- struct{}{}
	}))
	defer srv.Close()

	eng1, err := engine.NewReal(bigConfig(), &okCaller{}, st)
	if err != nil {
		t.Fatal(err)
	}
	m1 := New(st, eng1, fastCallbackCfg()) // TTL 1s
	id, err := m1.Submit("model-a", 1000, 1, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	// let m1's delivery exhaust its TTL against the failing sink
	time.Sleep(1300 * time.Millisecond)
	eng1.Stop()

	undelivered, err := st.UndeliveredBatches()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, u := range undelivered {
		if u == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("batch %s should be undelivered, got %v", id, undelivered)
	}

	// restart: fresh engine + manager over the SAME store; sink now works
	failing.Store(false)
	eng2, err := engine.NewReal(bigConfig(), &okCaller{}, st)
	if err != nil {
		t.Fatal(err)
	}
	defer eng2.Stop()
	m2 := New(st, eng2, fastCallbackCfg())
	if err := m2.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	select {
	case <-hits:
	case <-time.After(3 * time.Second):
		t.Fatal("resumed delivery never succeeded")
	}
}

// TestResultsListedOncePerID: results list every request exactly once.
func TestResultsListedOncePerID(t *testing.T) {
	st := openStore(t)
	eng, err := engine.NewReal(bigConfig(), &okCaller{}, st)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()
	m := New(st, eng, fastCallbackCfg())

	id, err := m.Submit("model-a", 1000, 7, "")
	if err != nil {
		t.Fatal(err)
	}
	// wait for all terminal
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if b, ok := m.Status(id); ok && b.Status == "completed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	rows, ok := m.Results(id)
	if !ok {
		t.Fatal("results not found for a completed batch")
	}
	if len(rows) != 7 {
		t.Fatalf("want 7 result rows, got %d", len(rows))
	}
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.ID] {
			t.Fatalf("duplicate result row for %s", r.ID)
		}
		seen[r.ID] = true
		if r.State != "completed" || r.Outcome != "success" {
			t.Fatalf("row %s mismatch: %+v", r.ID, r)
		}
	}
	if b, _ := m.Status(id); b.Counts.Successful != 7 {
		t.Fatalf("batch counts: %+v", b.Counts)
	}
}

// TestRestoreContinuesInterruptedBatches: after a restart the manager must
// re-register batches that were mid-flight (status=processing) so OnTerminal
// keeps counting and the batch reaches completed with the full breakdown —
// without this, post-restart terminal events find no batchState and the
// batch freezes in processing forever.
func TestRestoreContinuesInterruptedBatches(t *testing.T) {
	st := openStore(t)

	// simulate crash aftermath: batch mid-flight, 2 requests already terminal
	if err := st.CreateBatch(store.Batch{
		ID: "b-crash", Status: "processing", CallbackURL: "", CreatedAt: time.Now(),
		Counts: store.Counts{Total: 10, Successful: 2},
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := st.InsertRequest(store.RequestRow{
			ID: fmt.Sprintf("ext-done-%d", i), BatchID: "b-crash", Model: "model-a", State: "completed", Outcome: "success", Tokens: 900,
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 8; i++ {
		if err := st.InsertRequest(store.RequestRow{
			ID: fmt.Sprintf("ext-pend-%d", i), BatchID: "b-crash", Model: "model-a", State: "in_flight", Tokens: 1000,
		}); err != nil {
			t.Fatal(err)
		}
	}

	eng, err := engine.NewReal(bigConfig(), &okCaller{}, st)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()
	m := New(st, eng, fastCallbackCfg())
	if err := m.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if n, err := eng.Recover(); err != nil || n != 8 {
		t.Fatalf("Recover: %d %v", n, err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if b, ok := m.Status("b-crash"); ok && b.Status == "completed" {
			if b.Counts.Total != 10 {
				t.Fatalf("total want 10, got %d", b.Counts.Total)
			}
			if b.Counts.Successful+b.Counts.Failed+b.Counts.Expired+b.Counts.Rejected != 10 {
				t.Fatalf("breakdown must account for all 10: %+v", b.Counts)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	b, _ := m.Status("b-crash")
	t.Fatalf("batch never completed after restore: status=%q counts=%+v", b.Status, b.Counts)
}

// ---------- F1: startup reconcile derives batch truth from request rows ----------

// TestRestoreCompletesFullyTerminalBatch: crash window (b) — every request
// row is terminal and the counts are persisted, but the SetBatchStatus
// write was lost, so the batch is stuck at processing. Restore must
// recompute from the rows, complete the batch, and dispatch the callback.
func TestRestoreCompletesFullyTerminalBatch(t *testing.T) {
	st := openStore(t)
	sink, srv := newSink(0)
	defer srv.Close()

	if err := st.CreateBatch(store.Batch{
		ID: "b-window-b", Status: "processing", CallbackURL: srv.URL, CreatedAt: time.Now(),
		Counts: store.Counts{Total: 3, Successful: 2, Failed: 1},
	}); err != nil {
		t.Fatal(err)
	}
	rows := []store.RequestRow{
		{ID: "r-1", BatchID: "b-window-b", Model: "model-a", State: "completed", Outcome: "success", Tokens: 900},
		{ID: "r-2", BatchID: "b-window-b", Model: "model-a", State: "completed", Outcome: "success", Tokens: 900},
		{ID: "r-3", BatchID: "b-window-b", Model: "model-a", State: "completed", Outcome: "failed", Tokens: 40},
	}
	if err := st.BulkInsertRequests(rows); err != nil {
		t.Fatal(err)
	}

	eng, err := engine.NewReal(bigConfig(), &okCaller{}, st)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()
	m := New(st, eng, fastCallbackCfg())
	if err := m.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	b, ok := m.Status("b-window-b")
	if !ok || b.Status != "completed" {
		t.Fatalf("batch must be completed by Restore, got status=%q", b.Status)
	}
	if b.Counts.Successful != 2 || b.Counts.Failed != 1 || b.Counts.Total != 3 {
		t.Fatalf("recomputed counts mismatch: %+v", b.Counts)
	}
	select {
	case <-sink.ch:
	case <-time.After(3 * time.Second):
		t.Fatal("callback never dispatched for the restored-completed batch")
	}
}

// TestRestoreReconcilesStaleCounters: crash window (a) — a row went terminal
// after the last counter write, so the persisted batch counts are one short.
// Restore must recompute from rows so the remaining terminal lands the
// batch on exactly total.
func TestRestoreReconcilesStaleCounters(t *testing.T) {
	st := openStore(t)

	if err := st.CreateBatch(store.Batch{
		ID: "b-window-a", Status: "processing", CallbackURL: "", CreatedAt: time.Now(),
		Counts: store.Counts{Total: 2}, // persisted counters stale: 0 terminal
	}); err != nil {
		t.Fatal(err)
	}
	rows := []store.RequestRow{
		{ID: "r-done", BatchID: "b-window-a", Model: "model-a", State: "completed", Outcome: "success", Tokens: 900},
		{ID: "r-pend", BatchID: "b-window-a", Model: "model-a", State: "in_flight", Tokens: 1000},
	}
	if err := st.BulkInsertRequests(rows); err != nil {
		t.Fatal(err)
	}

	eng, err := engine.NewReal(bigConfig(), &okCaller{}, st)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()
	m := New(st, eng, fastCallbackCfg())
	if err := m.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if n, err := eng.Recover(); err != nil || n != 1 {
		t.Fatalf("Recover: %d %v", n, err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if b, ok := m.Status("b-window-a"); ok && b.Status == "completed" {
			if b.Counts.Successful != 2 || b.Counts.Total != 2 {
				t.Fatalf("counts must land on exactly 2/2, got %+v", b.Counts)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	b, _ := m.Status("b-window-a")
	t.Fatalf("batch never completed: status=%q counts=%+v", b.Status, b.Counts)
}

// TestRestoreFailsEmptyProcessingBatch: crash window (c) — CreateBatch
// persisted but BulkInsertRequests never did: a processing batch with zero
// rows and total>0 can never progress; Restore marks it failed.
func TestRestoreFailsEmptyProcessingBatch(t *testing.T) {
	st := openStore(t)
	if err := st.CreateBatch(store.Batch{
		ID: "b-window-c", Status: "processing", CallbackURL: "", CreatedAt: time.Now(),
		Counts: store.Counts{Total: 5},
	}); err != nil {
		t.Fatal(err)
	}

	eng, err := engine.NewReal(bigConfig(), &okCaller{}, st)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Stop()
	m := New(st, eng, fastCallbackCfg())
	if err := m.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	b, ok := m.Status("b-window-c")
	if !ok || b.Status != "failed" {
		t.Fatalf("empty processing batch must be failed by Restore, got status=%q ok=%v", b.Status, ok)
	}
}
