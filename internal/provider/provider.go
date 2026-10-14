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

	"consuma/internal/config"
	"consuma/internal/ledger"
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
	p := &Provider{model: m}
	p.rpm.Store(m.RPM)
	p.tpm.Store(m.TPM)
	return p
}

// SetLimits changes the provider-side limits live.
func (p *Provider) SetLimits(rpm, tpm int64) {
	p.rpm.Store(rpm)
	p.tpm.Store(tpm)
}

func (p *Provider) slotFor(sec int64) *providerSlot {
	return &p.slots[sec%60]
}

// roll advances the rolling window base (call with usedMu held).
func (p *Provider) roll(sec int64) {
	if p.wheelBase == 0 {
		p.wheelBase = sec
		return
	}
	target := sec - 59
	if target < p.wheelBase {
		return
	}
	for p.wheelBase <= target {
		s := p.slotFor(p.wheelBase)
		s.mu.Lock()
		if s.sec == p.wheelBase {
			p.rpmUsed -= s.rpm
			p.tpmUsed -= s.tpm
			s.rpm, s.tpm = 0, 0
		}
		s.mu.Unlock()
		p.wheelBase++
	}
}

// Call simulates one provider call at time now.
func (p *Provider) Call(r *ledger.Request, rng *rand.Rand, now time.Time) CallResult {
	sec := now.Unix()
	p.usedMu.Lock()
	p.roll(sec)
	if p.rpmUsed+1 > p.rpm.Load() || p.tpmUsed+r.EstimateTokens > p.tpm.Load() {
		p.usedMu.Unlock()
		return CallResult{Outcome: ProviderRejected}
	}
	s := p.slotFor(sec)
	s.mu.Lock()
	if s.sec != sec {
		s.sec = sec
		s.rpm = 0
		s.tpm = 0
	}
	s.rpm++
	s.tpm += r.EstimateTokens
	s.mu.Unlock()
	p.rpmUsed++
	p.tpmUsed += r.EstimateTokens
	p.usedMu.Unlock()

	latency := p.model.Latency.Draw(rng)
	outcome := p.drawOutcome(rng)
	actual := p.drawTokens(rng)
	return CallResult{Latency: latency, Outcome: outcome, ActualTokens: actual}
}

func (p *Provider) drawOutcome(rng *rand.Rand) Outcome {
	roll := rng.Float64() * 100
	o := p.model.Outcome
	switch {
	case roll < o.SuccessPct:
		return Success
	case roll < o.SuccessPct+o.TransientPct:
		return TransientFailure
	default:
		return PermanentFailure
	}
}

func (p *Provider) drawTokens(rng *rand.Rand) int64 {
	mean := p.model.ActualTokenMean
	if mean <= 0 {
		mean = float64(p.model.EstimateTokens)
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
func (p *Provider) ObservedRPM(now time.Time) int64 {
	sec := now.Unix()
	p.usedMu.Lock()
	defer p.usedMu.Unlock()
	p.roll(sec)
	return p.rpmUsed
}

// ObservedTPM reports provider-side rolling 60s token usage.
func (p *Provider) ObservedTPM(now time.Time) int64 {
	sec := now.Unix()
	p.usedMu.Lock()
	defer p.usedMu.Unlock()
	p.roll(sec)
	return p.tpmUsed
}
