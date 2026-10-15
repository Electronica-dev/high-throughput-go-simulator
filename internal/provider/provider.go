// Package provider simulates an inference provider: configurable latency,
// outcome mix, and actual token charges, plus its own RPM/TPM limits enforced
// as a real provider would (rejection backstop) against a rolling 60-second
// window - the same enforcement semantics as the system's admission limiter.
package provider

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"htsim/internal/config"
	"htsim/internal/ledger"
)

// Outcome is the final classification of one provider call.
type Outcome uint8

const (
	Success Outcome = iota
	TransientFailure
	PermanentFailure
	ProviderRejected // provider-side limit backstop (429-equivalent)
)

// CallResult is what one provider call produced.
type CallResult struct {
	Latency      time.Duration
	Outcome      Outcome
	ActualTokens int64
}

// providerSlot mirrors the limiter's slot accounting, kept local to this
// package so the provider stays decoupled from the system's admission
// limiter (the duplication is deliberate and small).
type providerSlot struct {
	mu  sync.Mutex
	sec int64
	rpm int64
	tpm int64
}

// Provider simulates calls for one model. Enforcement reads rolling 60s
// window totals maintained incrementally over the slot ring (see limiter).
type Provider struct {
	model *config.ModelConfig
	rpm   atomic.Int64
	tpm   atomic.Int64
	slots [60]providerSlot

	rpmUsed   int64
	tpmUsed   int64
	wheelBase int64
	usedMu    sync.Mutex
}

// New builds a provider for a model.
func New(m *config.ModelConfig) *Provider {
	provider := &Provider{model: m}
	provider.rpm.Store(m.RPM)
	provider.tpm.Store(m.TPM)
	return provider
}

// SetLimits changes the provider-side limits live.
func (provider *Provider) SetLimits(rpm, tpm int64) {
	provider.rpm.Store(rpm)
	provider.tpm.Store(tpm)
}

func (provider *Provider) slotFor(sec int64) *providerSlot {
	return &provider.slots[sec%60]
}

// roll advances the rolling window base (call with usedMu held). wheelBase
// is always the oldest live second and is never snapped to the current
// second (a snap would skip evictions and starve the backstop).
func (provider *Provider) roll(sec int64) {
	if provider.wheelBase == 0 {
		provider.wheelBase = sec
		return
	}
	for provider.wheelBase < sec-59 {
		s := provider.slotFor(provider.wheelBase)
		s.mu.Lock()
		if s.sec == provider.wheelBase {
			provider.rpmUsed -= s.rpm
			provider.tpmUsed -= s.tpm
			s.rpm, s.tpm = 0, 0
		}
		s.mu.Unlock()
		provider.wheelBase++
	}
}

// Call simulates one provider call at time now.
func (provider *Provider) Call(r *ledger.Request, rng *rand.Rand, now time.Time) CallResult {
	sec := now.Unix()
	provider.usedMu.Lock()
	provider.roll(sec)
	if provider.rpmUsed+1 > provider.rpm.Load() || provider.tpmUsed+r.EstimateTokens > provider.tpm.Load() {
		provider.usedMu.Unlock()
		return CallResult{Outcome: ProviderRejected}
	}
	s := provider.slotFor(sec)
	s.mu.Lock()
	if s.sec != sec {
		s.sec = sec
		s.rpm = 0
		s.tpm = 0
	}
	s.rpm++
	s.tpm += r.EstimateTokens
	s.mu.Unlock()
	provider.rpmUsed++
	provider.tpmUsed += r.EstimateTokens
	provider.usedMu.Unlock()

	latency := provider.model.Latency.Draw(rng)
	outcome := provider.drawOutcome(rng)
	actual := provider.drawTokens(rng)
	return CallResult{Latency: latency, Outcome: outcome, ActualTokens: actual}
}

func (provider *Provider) drawOutcome(rng *rand.Rand) Outcome {
	roll := rng.Float64() * 100
	o := provider.model.Outcome
	switch {
	case roll < o.SuccessPct:
		return Success
	case roll < o.SuccessPct+o.TransientPct:
		return TransientFailure
	default:
		return PermanentFailure
	}
}

func (provider *Provider) drawTokens(rng *rand.Rand) int64 {
	mean := provider.model.ActualTokenMean
	if mean <= 0 {
		mean = float64(provider.model.EstimateTokens)
	}
	// +-20% normal noise around the mean, clamped to [1, 2*mean].
	t := mean + rng.NormFloat64()*0.2*mean
	if t < 1 {
		t = 1
	}
	if t > 2*mean {
		t = 2 * mean
	}
	return int64(math.Round(t))
}

// ObservedRPM reports provider-side rolling 60s request usage.
func (provider *Provider) ObservedRPM(now time.Time) int64 {
	sec := now.Unix()
	provider.usedMu.Lock()
	defer provider.usedMu.Unlock()
	provider.roll(sec)
	return provider.rpmUsed
}

// ObservedTPM reports provider-side rolling 60s token usage.
func (provider *Provider) ObservedTPM(now time.Time) int64 {
	sec := now.Unix()
	provider.usedMu.Lock()
	defer provider.usedMu.Unlock()
	provider.roll(sec)
	return provider.tpmUsed
}
