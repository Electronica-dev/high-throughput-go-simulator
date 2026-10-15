package limiter

import (
	"testing"
	"time"

	"htsim/internal/config"
)

func testModel() *config.ModelConfig {
	return &config.ModelConfig{
		Name:           "m",
		RPM:            10,
		TPM:            100000,
		EstimateTokens: 1000,
	}
}

func TestAdmitUnderLimit(t *testing.T) {
	limiter := New(testModel())
	now := time.Unix(100000, 0)
	for i := 0; i < 10; i++ {
		r := limiter.TryAdmit(1000, now)
		if !r.OK {
			t.Fatalf("admit %d should pass, got %+v", i+1, r)
		}
	}
}

func TestAdmitOverRate(t *testing.T) {
	limiter := New(testModel())
	now := time.Unix(100000, 0)
	for i := 0; i < 10; i++ {
		limiter.TryAdmit(1000, now)
	}
	r := limiter.TryAdmit(1000, now)
	if r.OK || !r.OverRate {
		t.Fatalf("11th admit should be OverRate, got %+v", r)
	}
}

func TestAdmitRejectsTokensOverTPMBudget(t *testing.T) {
	limiter := New(testModel())
	now := time.Unix(100000, 0)
	r := limiter.TryAdmit(200000, now) // 2x the entire TPM budget
	if r.OK || !r.OverTPMBudget {
		t.Fatalf("tokens over TPM budget should reject immediately, got %+v", r)
	}
}

// TestSustainedRateEnforcedOver60s is the core sliding-window guarantee:
// hitting the per-second pace of the limit does NOT admit 60x the limit over
// a minute. 10 RPM means 10 admissions per rolling 60s, period.
func TestSustainedRateEnforcedOver60s(t *testing.T) {
	limiter := New(testModel())
	t0 := time.Unix(100000, 0)
	for i := 0; i < 10; i++ {
		if r := limiter.TryAdmit(1000, t0); !r.OK {
			t.Fatalf("admit %d in sec 0 should pass", i+1)
		}
	}
	// every later second within the window must reject: the minute budget is spent
	for slot := int64(1); slot <= 3; slot++ {
		if r := limiter.TryAdmit(1000, t0.Add(time.Duration(slot)*time.Second)); r.OK {
			t.Fatalf("admit in sec +%d must fail: 60s budget exhausted", slot)
		}
	}
	// outside the window it works again
	if r := limiter.TryAdmit(1000, t0.Add(60*time.Second)); !r.OK {
		t.Fatalf("admit at +60s should succeed after window slide, got %+v", r)
	}
}

// TestSustainedTPMOverWindow: TPM=100000 with 1000-token requests means 100
// admissions per rolling 60s total, regardless of how seconds are split.
func TestSustainedTPMOverWindow(t *testing.T) {
	limiter := New(testModel())
	t0 := time.Unix(100000, 0)
	for slot := int64(0); slot < 5; slot++ {
		now := t0.Add(time.Duration(slot) * time.Second)
		for i := 0; i < 2; i++ {
			if r := limiter.TryAdmit(1000, now); !r.OK {
				t.Fatalf("admit %d of sec +%d should pass (rolling used=%d)", i+1, slot, limiter.ObservedRPM(now))
			}
		}
	}
	// 10 admissions done; 11th within the window must reject on TPM
	if r := limiter.TryAdmit(1000, t0.Add(5*time.Second)); r.OK {
		t.Fatal("11th admission must fail: 60s TPM budget exhausted")
	}
}

// TestObservedRPMCappedByLimit: the report's observed figure can never
// exceed the configured limit while enforcement is active.
func TestObservedRPMCappedByLimit(t *testing.T) {
	limiter := New(testModel())
	t0 := time.Unix(100000, 0)
	for slot := int64(0); slot < 3; slot++ {
		now := t0.Add(time.Duration(slot) * time.Second)
		for limiter.TryAdmit(1000, now).OK {
		}
	}
	if got := limiter.ObservedRPM(t0.Add(3 * time.Second)); got > 10 {
		t.Fatalf("ObservedRPM %d exceeds configured RPM 10", got)
	}
}

// TestRollingWindowSustainedMinute: the reviewer's starvation probe — RPM=60
// paced at 1 request/second for 120 seconds must admit every request (each
// second's admission leaves the window 60s later, freeing budget for the
// next). A broken eviction (skipped seconds, snap-to-now) blackouts the
// limiter after ~70 admits.
func TestRollingWindowSustainedMinute(t *testing.T) {
	m := testModel()
	m.RPM = 60
	m.TPM = 100000000
	limiter := New(m)
	t0 := time.Unix(100000, 0)
	for slot := int64(0); slot < 120; slot++ {
		if r := limiter.TryAdmit(1000, t0.Add(time.Duration(slot)*time.Second)); !r.OK {
			t.Fatalf("admit at +%ds rejected: rolling window is starving (used=%d)", slot, limiter.ObservedRPM(t0.Add(time.Duration(slot)*time.Second)))
		}
	}
}

// TestRefundForAgedOutReservationSkipped: a refund whose reservation already
// left the rolling window must not deflate the in-window total (that would
// mint headroom: 9 in-window reservations + an aged-out refund would admit
// 2 more instead of 1).
func TestRefundForAgedOutReservationSkipped(t *testing.T) {
	m := testModel()
	m.RPM = 1000  // isolate TPM
	m.TPM = 10000 // 10 x 1000-token reservations saturate it exactly
	limiter := New(m)
	t0 := time.Unix(100000, 0)
	// 1 admission at t0 (will age out), 9 at t0+20s (still in-window at +70s)
	if r := limiter.TryAdmit(1000, t0); !r.OK {
		t.Fatal("admit at t0 should pass")
	}
	for i := 0; i < 9; i++ {
		if r := limiter.TryAdmit(1000, t0.Add(20*time.Second)); !r.OK {
			t.Fatalf("in-window admit %d should pass", i+1)
		}
	}
	later := t0.Add(70 * time.Second) // window covers [t0+11, t0+70]
	limiter.Reconcile(1000, 0, t0.Unix(), later)
	admitted := 0
	for i := 0; i < 5; i++ {
		if limiter.TryAdmit(1000, later).OK {
			admitted++
		}
	}
	if admitted != 1 {
		t.Fatalf("aged-out refund minted headroom: %d admits, want 1", admitted)
	}
}

func TestWindowSlides(t *testing.T) {
	limiter := New(testModel())
	now := time.Unix(100000, 0)
	for i := 0; i < 10; i++ {
		limiter.TryAdmit(1000, now)
	}
	if r := limiter.TryAdmit(1000, now); r.OK {
		t.Fatal("11th admit in-window should fail")
	}
	later := now.Add(60 * time.Second)
	if r := limiter.TryAdmit(1000, later); !r.OK {
		t.Fatalf("admit 60s later should succeed after window slide, got %+v", r)
	}
}

func TestSetLimits(t *testing.T) {
	limiter := New(testModel())
	now := time.Unix(100000, 0)
	limiter.SetLimits(5, 100000)
	for i := 0; i < 5; i++ {
		if r := limiter.TryAdmit(1000, now); !r.OK {
			t.Fatalf("admit %d under reduced limit should pass", i+1)
		}
	}
	if r := limiter.TryAdmit(1000, now); r.OK {
		t.Fatal("6th admit under RPM=5 should fail")
	}
}

func TestReconcileRefunds(t *testing.T) {
	limiter := New(testModel())
	now := time.Unix(100000, 0)
	if r := limiter.TryAdmit(1000, now); !r.OK {
		t.Fatal("admit should pass")
	}
	limiter.Reconcile(1000, 800, now.Unix(), now) // provider charged 800 of 1000 reserved
	if got := limiter.ObservedTPM(now); got != 800 {
		t.Fatalf("ObservedTPM want 800 (actual charged), got %d", got)
	}
	if got := limiter.AdmittedTPM(); got != 1000 {
		t.Fatalf("AdmittedTPM want 1000 (reserved), got %d", got)
	}
}

// TestRefundNeverDrivesNegative: refunds cannot create phantom headroom —
// netting them may not push the outstanding figure below zero, and freed
// budget is usable exactly up to the limit, never beyond.
func TestRefundNeverDrivesNegative(t *testing.T) {
	m := testModel()
	m.RPM = 1000  // isolate TPM: refunds free token budget, not request budget
	m.TPM = 10000 // 10 x 1000-token requests exhaust the TPM budget exactly
	limiter := New(m)
	t0 := time.Unix(100000, 0)
	for i := 0; i < 10; i++ {
		limiter.TryAdmit(1000, t0)
	}
	for i := 0; i < 10; i++ {
		limiter.Reconcile(1000, 0, t0.Unix(), t0) // provider charged nothing: full refunds
	}
	if got := limiter.ObservedTPM(t0); got != 0 {
		t.Fatalf("ObservedTPM want 0 after full refunds, got %d", got)
	}
	admitted := 0
	for i := 0; i < 15; i++ {
		if limiter.TryAdmit(1000, t0).OK {
			admitted++
		}
	}
	if admitted != 10 {
		t.Fatalf("freed budget should allow exactly 10 more admits, got %d", admitted)
	}
	if got := limiter.ObservedTPM(t0); got != 10000 {
		t.Fatalf("ObservedTPM want 10000 after re-admission, got %d", got)
	}
}

// TestChargeRetryCountsAgainstRPM: retries consume RPM like fresh calls —
// blocked while the rolling window is full, admitted (and counted) once it
// slides.
func TestChargeRetryCountsAgainstRPM(t *testing.T) {
	limiter := New(testModel())
	t0 := time.Unix(100000, 0)
	for i := 0; i < 10; i++ {
		limiter.TryAdmit(1000, t0)
	}
	if limiter.ChargeRetry(t0) {
		t.Fatal("retry must be rejected while the rolling window is full")
	}
	// at +60s the original second is outside the [sec-59, sec] window, so a
	// retry is legitimately admitted (same semantics as TestWindowSlides)
	if !limiter.ChargeRetry(t0.Add(60 * time.Second)) {
		t.Fatal("retry should be admitted after the window slides")
	}
	if got := limiter.ObservedRPM(t0.Add(60 * time.Second)); got != 1 {
		t.Fatalf("ObservedRPM want 1 (one retry), got %d", got)
	}
}

func TestObservedWindows(t *testing.T) {
	limiter := New(testModel())
	now := time.Unix(100000, 0)
	early := now.Add(-30 * time.Second)
	for i := 0; i < 3; i++ {
		limiter.TryAdmit(1000, early)
	}
	for i := 0; i < 5; i++ {
		limiter.TryAdmit(1000, now)
	}
	if got := limiter.ObservedRPM(now); got != 8 {
		t.Fatalf("ObservedRPM want 8 (3 early + 5 now), got %d", got)
	}
	if got := limiter.ObservedTPM(now); got != 8000 {
		t.Fatalf("ObservedTPM want 8000 reserved, got %d", got)
	}
}

// TestRefundThenEvictDoesNotDoubleSubtract: a reconciled refund must be
// netted into the admission slot, so the later window eviction subtracts the
// post-refund residual — not the original estimate. Refund + eviction used to
// double-subtract (net per request = actual - estimate, negative for failed
// requests), bleeding tpmUsed toward zero and silently disabling TPM
// enforcement while ObservedTPM reported garbage.
func TestRefundThenEvictDoesNotDoubleSubtract(t *testing.T) {
	limiter := New(&config.ModelConfig{RPM: 1 << 20, TPM: 1 << 30})
	t0 := time.Unix(100000, 0)
	// admit 10 requests x 1000 tokens
	for i := 0; i < 10; i++ {
		if res := limiter.TryAdmit(1000, t0); !res.OK {
			t.Fatalf("admit %d: %+v", i, res)
		}
	}
	// reconcile every one at actual 500 (refund 1000-500=500 each)
	for i := 0; i < 10; i++ {
		limiter.Reconcile(1000, 500, t0.Unix(), t0)
	}
	if got := limiter.ObservedTPM(t0); got != 5000 {
		t.Fatalf("after reconcile want 5000 outstanding, got %d", got)
	}
	// a full window later: the old second is evicted. The eviction must
	// subtract only the reconciled residual (500 each) — the new window's
	// 10 fresh 1000-token admissions are then the only outstanding tokens.
	t1 := t0.Add(61 * time.Second)
	for i := 0; i < 10; i++ {
		if res := limiter.TryAdmit(1000, t1); !res.OK {
			t.Fatalf("admit after eviction %d: %+v", i, res)
		}
	}
	if got := limiter.ObservedTPM(t1); got != 10000 {
		t.Fatalf("after eviction want 10000 (fresh reservations only), got %d — eviction double-subtracted the refunded reservations", got)
	}
}
