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
