package provider

import (
	"math/rand"
	"testing"
	"time"

	"consuma/internal/config"
	"consuma/internal/ledger"
)

func fixedModel() *config.ModelConfig {
	return &config.ModelConfig{
		Name:            "m",
		RPM:             1000000,
		TPM:             1000000000,
		EstimateTokens:  1000,
		ActualTokenMean: 1050,
		Latency:         config.LatencyConfig{Kind: "fixed", MeanMS: 50},
		Outcome:         config.OutcomeConfig{SuccessPct: 90, TransientPct: 5, PermanentPct: 5},
	}
}

func callAt(provider *Provider, rng *rand.Rand, id uint64, now time.Time) CallResult {
	r := &ledger.Request{ID: id, Model: "m", EstimateTokens: 1000, BornNS: now.UnixNano()}
	return provider.Call(r, rng, now)
}

func TestFixedLatencyDraw(t *testing.T) {
	provider := New(fixedModel())
	rng := rand.New(rand.NewSource(42))
	now := time.Unix(100000, 0)
	for i := 0; i < 10; i++ {
		res := callAt(provider, rng, uint64(i+1), now)
		if res.Latency != 50*time.Millisecond {
			t.Fatalf("fixed latency: want 50ms, got %v", res.Latency)
		}
	}
}

func TestOutcomeDistribution(t *testing.T) {
	provider := New(fixedModel())
	rng := rand.New(rand.NewSource(42))
	now := time.Unix(100000, 0)
	counts := map[Outcome]int{}
	const n = 10000
	for i := 0; i < n; i++ {
		counts[callAt(provider, rng, uint64(i+1), now).Outcome]++
	}
	want := map[Outcome]float64{Success: 0.90, TransientFailure: 0.05, PermanentFailure: 0.05}
	for o, share := range want {
		got := float64(counts[o]) / float64(n)
		if diff := got - share; diff > 0.01 || diff < -0.01 {
			t.Fatalf("outcome %d share %.3f, want %.3f ±0.01", o, got, share)
		}
	}
}

func TestActualTokens(t *testing.T) {
	m := fixedModel()
	m.ActualTokenMean = 1200
	provider := New(m)
	rng := rand.New(rand.NewSource(42))
	now := time.Unix(100000, 0)
	var sum int64
	const n = 10000
	for i := 0; i < n; i++ {
		res := callAt(provider, rng, uint64(i+1), now)
		if res.ActualTokens < 1 {
			t.Fatalf("actual tokens must be >= 1, got %d", res.ActualTokens)
		}
		sum += res.ActualTokens
	}
	avg := float64(sum) / n
	if avg < 0.9*1200 || avg > 1.1*1200 {
		t.Fatalf("avg actual tokens %.0f, want 1200 ±10%%", avg)
	}
}

func TestProviderBackstopRejection(t *testing.T) {
	m := fixedModel()
	m.RPM = 5
	provider := New(m)
	rng := rand.New(rand.NewSource(42))
	now := time.Unix(100000, 0)
	for i := 0; i < 5; i++ {
		if res := callAt(provider, rng, uint64(i+1), now); res.Outcome == ProviderRejected {
			t.Fatalf("call %d rejected before limit", i+1)
		}
	}
	if res := callAt(provider, rng, 6, now); res.Outcome != ProviderRejected {
		t.Fatalf("6th call should be ProviderRejected, got %d", res.Outcome)
	}
}

func TestFailedCallsConsumeTokens(t *testing.T) {
	m := fixedModel()
	// force all transient failures
	m.Outcome = config.OutcomeConfig{SuccessPct: 0, TransientPct: 100, PermanentPct: 0}
	provider := New(m)
	rng := rand.New(rand.NewSource(42))
	now := time.Unix(100000, 0)
	res := callAt(provider, rng, 1, now)
	if res.Outcome != TransientFailure {
		t.Fatalf("want transient failure, got %d", res.Outcome)
	}
	if got := provider.ObservedTPM(now); got < 1000 {
		t.Fatalf("failed call should consume tokens, observed TPM = %d", got)
	}
}
