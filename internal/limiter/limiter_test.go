package limiter

import (
	"testing"
	"time"

	"consuma/internal/config"
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
	l := New(testModel())
	now := time.Unix(100000, 0)
	for i := 0; i < 10; i++ {
		r := l.TryAdmit(1000, now)
		if !r.OK {
			t.Fatalf("admit %d should pass, got %+v", i+1, r)
		}
	}
}

func TestAdmitOverRate(t *testing.T) {
	l := New(testModel())
	now := time.Unix(100000, 0)
	for i := 0; i < 10; i++ {
		l.TryAdmit(1000, now)
	}
	r := l.TryAdmit(1000, now)
	if r.OK || !r.OverRate {
		t.Fatalf("11th admit should be OverRate, got %+v", r)
	}
}

func TestAdmitRejectsTokensOverTPMBudget(t *testing.T) {
	l := New(testModel())
	now := time.Unix(100000, 0)
	r := l.TryAdmit(200000, now) // 2x the entire TPM budget
	if r.OK || !r.OverTPMBudget {
		t.Fatalf("tokens over TPM budget should reject immediately, got %+v", r)
	}
}

// TestSustainedRateEnforcedOver60s is the core sliding-window guarantee:
// hitting the per-second pace of the limit does NOT admit 60x the limit over
// a minute. 10 RPM means 10 admissions per rolling 60s, period.
func TestSustainedRateEnforcedOver60s(t *testing.T) {
	l := New(testModel())
	t0 := time.Unix(100000, 0)
	for i := 0; i < 10; i++ {
		if r := l.TryAdmit(1000, t0); !r.OK {
			t.Fatalf("admit %d in sec 0 should pass", i+1)
		}
	}
	// every later second within the window must reject: the minute budget is spent
	for s := int64(1); s <= 3; s++ {
		if r := l.TryAdmit(1000, t0.Add(time.Duration(s)*time.Second)); r.OK {
			t.Fatalf("admit in sec +%d must fail: 60s budget exhausted", s)
		}
	}
	// outside the window it works again
	if r := l.TryAdmit(1000, t0.Add(60*time.Second)); !r.OK {
		t.Fatalf("admit at +60s should succeed after window slide, got %+v", r)
	}
}

// TestSustainedTPMOverWindow: TPM=100000 with 1000-token requests means 100
// admissions per rolling 60s total, regardless of how seconds are split.
func TestSustainedTPMOverWindow(t *testing.T) {
	l := New(testModel())
	t0 := time.Unix(100000, 0)
	for s := int64(0); s < 5; s++ {
		now := t0.Add(time.Duration(s) * time.Second)
		for i := 0; i < 2; i++ {
			if r := l.TryAdmit(1000, now); !r.OK {
				t.Fatalf("admit %d of sec +%d should pass (rolling used=%d)", i+1, s, l.ObservedRPM(now))
			}
		}
	}
	// 10 admissions done; 11th within the window must reject on TPM
	if r := l.TryAdmit(1000, t0.Add(5 * time.Second)); r.OK {
		t.Fatal("11th admission must fail: 60s TPM budget exhausted")
	}
}

// TestObservedRPMCappedByLimit: the report's observed figure can never
// exceed the configured limit while enforcement is active.
func TestObservedRPMCappedByLimit(t *testing.T) {
	l := New(testModel())
	t0 := time.Unix(100000, 0)
	for s := int64(0); s < 3; s++ {
		now := t0.Add(time.Duration(s) * time.Second)
		for l.TryAdmit(1000, now).OK {
		}
	}
	if got := l.ObservedRPM(t0.Add(3 * time.Second)); got > 10 {
		t.Fatalf("ObservedRPM %d exceeds configured RPM 10", got)
	}
}

func TestWindowSlides(t *testing.T) {
	l := New(testModel())
	now := time.Unix(100000, 0)
	for i := 0; i < 10; i++ {
		l.TryAdmit(1000, now)
	}
	if r := l.TryAdmit(1000, now); r.OK {
		t.Fatal("11th admit in-window should fail")
	}
	later := now.Add(60 * time.Second)
	if r := l.TryAdmit(1000, later); !r.OK {
		t.Fatalf("admit 60s later should succeed after window slide, got %+v", r)
	}
}

func TestSetLimits(t *testing.T) {
	l := New(testModel())
	now := time.Unix(100000, 0)
	l.SetLimits(5, 100000)
	for i := 0; i < 5; i++ {
		if r := l.TryAdmit(1000, now); !r.OK {
			t.Fatalf("admit %d under reduced limit should pass", i+1)
		}
	}
	if r := l.TryAdmit(1000, now); r.OK {
		t.Fatal("6th admit under RPM=5 should fail")
	}
}

func TestReconcileRefunds(t *testing.T) {
	l := New(testModel())
	now := time.Unix(100000, 0)
	if r := l.TryAdmit(1000, now); !r.OK {
		t.Fatal("admit should pass")
	}
	l.Reconcile(1000, 800, now) // provider charged 800 of 1000 reserved
	if got := l.ObservedTPM(now); got != 800 {
		t.Fatalf("ObservedTPM want 800 (actual charged), got %d", got)
	}
	if got := l.AdmittedTPM(); got != 1000 {
		t.Fatalf("AdmittedTPM want 1000 (reserved), got %d", got)
	}
}

// TestRefundNeverDrivesNegative: refunds cannot create phantom headroom —
// netting them may not push the outstanding figure below zero, and freed
// budget is usable exactly up to the limit, never beyond.
func TestRefundNeverDrivesNegative(t *testing.T) {
	m := testModel()
	m.RPM = 1000 // isolate TPM: refunds free token budget, not request budget
	m.TPM = 10000 // 10 x 1000-token requests exhaust the TPM budget exactly
	l := New(m)
	t0 := time.Unix(100000, 0)
	for i := 0; i < 10; i++ {
		l.TryAdmit(1000, t0)
	}
	for i := 0; i < 10; i++ {
		l.Reconcile(1000, 0, t0) // provider charged nothing: full refunds
	}
	if got := l.ObservedTPM(t0); got != 0 {
		t.Fatalf("ObservedTPM want 0 after full refunds, got %d", got)
	}
	admitted := 0
	for i := 0; i < 15; i++ {
		if l.TryAdmit(1000, t0).OK {
			admitted++
		}
	}
	if admitted != 10 {
		t.Fatalf("freed budget should allow exactly 10 more admits, got %d", admitted)
	}
	if got := l.ObservedTPM(t0); got != 10000 {
		t.Fatalf("ObservedTPM want 10000 after re-admission, got %d", got)
	}
}

// TestChargeRetryCountsAgainstRPM: retries consume RPM like fresh calls —
// blocked while the rolling window is full, admitted (and counted) once it
// slides.
func TestChargeRetryCountsAgainstRPM(t *testing.T) {
	l := New(testModel())
	t0 := time.Unix(100000, 0)
	for i := 0; i < 10; i++ {
		l.TryAdmit(1000, t0)
	}
	if l.ChargeRetry(t0) {
		t.Fatal("retry must be rejected while the rolling window is full")
	}
	// at +60s the original second is outside the [sec-59, sec] window, so a
	// retry is legitimately admitted (same semantics as TestWindowSlides)
	if !l.ChargeRetry(t0.Add(60 * time.Second)) {
		t.Fatal("retry should be admitted after the window slides")
	}
	if got := l.ObservedRPM(t0.Add(60 * time.Second)); got != 1 {
		t.Fatalf("ObservedRPM want 1 (one retry), got %d", got)
	}
}

func TestObservedWindows(t *testing.T) {
	l := New(testModel())
	now := time.Unix(100000, 0)
	early := now.Add(-30 * time.Second)
	for i := 0; i < 3; i++ {
		l.TryAdmit(1000, early)
	}
	for i := 0; i < 5; i++ {
		l.TryAdmit(1000, now)
	}
	if got := l.ObservedRPM(now); got != 8 {
		t.Fatalf("ObservedRPM want 8 (3 early + 5 now), got %d", got)
	}
	if got := l.ObservedTPM(now); got != 8000 {
		t.Fatalf("ObservedTPM want 8000 reserved, got %d", got)
	}
}
