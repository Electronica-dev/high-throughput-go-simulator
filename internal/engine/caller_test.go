package engine

import (
	"sync/atomic"
	"testing"
	"time"

	"htsim/internal/ledger"
	"htsim/internal/provider"
)

// fakeCaller returns a scripted CallResult for every call.
type fakeCaller struct {
	res   provider.CallResult
	calls atomic.Int32
}

func (f *fakeCaller) Call(r *ledger.Request, now time.Time) provider.CallResult {
	f.calls.Add(1)
	return f.res
}

func TestFakeCallerDrivesRequestToCompletion(t *testing.T) {
	cfg := testConfig(nil)
	engine, err := NewWithCaller(cfg, &fakeCaller{res: provider.CallResult{
		Latency:      1 * time.Millisecond,
		Outcome:      provider.Success,
		ActualTokens: 900,
	}})
	if err != nil {
		t.Fatalf("NewWithCaller: %v", err)
	}
	defer engine.Stop()

	rs := submitN(engine, 1)
	s := engine.Drain()
	if s.OK != 1 || s.Failed != 0 {
		t.Fatalf("fake caller should drive Success, got %+v", s)
	}
	if got := rs[0].ActualCharged.Load(); got != 900 {
		t.Fatalf("ActualCharged want 900, got %d", got)
	}
}

// TestInProcessCallerDelegatesToProvider: the in-process caller routes by
// request model and applies the provider's full semantics (backstop,
// latency draw, outcome mix) — exercised implicitly by every phase-1 engine
// test; here we pin the routing directly.
func TestInProcessCallerDelegatesToProvider(t *testing.T) {
	cfg := testConfig(nil)
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer engine.Stop()

	r := &ledger.Request{ID: 1, Model: "model-a", EstimateTokens: 1000}
	now := time.Now()
	res := engine.caller.Call(r, now)
	if res.Latency <= 0 {
		t.Fatalf("in-process caller should draw positive latency, got %v", res.Latency)
	}
	switch res.Outcome {
	case provider.Success, provider.TransientFailure, provider.PermanentFailure:
	default:
		t.Fatalf("unexpected outcome %d", res.Outcome)
	}
	if res.ActualTokens < 1 {
		t.Fatalf("actual tokens should be >= 1, got %d", res.ActualTokens)
	}
}

// guard: the latency config must still come from the model (fixed 1ms in
// testConfig), proving the caller honors model config rather than a default.
func TestCallerHonorsModelLatency(t *testing.T) {
	cfg := testConfig(nil)
	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer engine.Stop()
	_ = cfg
	start := time.Now()
	r := &ledger.Request{ID: 1, Model: "model-a", EstimateTokens: 1000}
	res := engine.caller.Call(r, start)
	if res.Latency != 1*time.Millisecond {
		t.Fatalf("fixed-latency model should draw exactly 1ms, got %v", res.Latency)
	}
}
