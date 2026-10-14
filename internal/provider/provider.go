// Package provider simulates an inference provider: configurable latency,
// outcome mix, and actual token charges, plus its own RPM/TPM limits enforced
// as a real provider would (rejection backstop).
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

// providerSlot mirrors limiter slot accounting, kept local to this package so
// the provider stays decoupled from the system's admission limiter (the
// duplication is deliberate and small).
type providerSlot struct {
	mu  sync.Mutex
	sec int64
	rpm int64
	tpm int64
}

// Provider simulates calls for one model.
type Provider struct {
	model *config.ModelConfig
	rpm   atomic.Int64
	tpm   atomic.Int64
	slots [60]providerSlot
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

func (p *Provider) slotFor(now time.Time) *providerSlot {
	return &p.slots[now.Unix()%60]
}

func (s *providerSlot) resetIfStale(sec int64) {
	if s.sec != sec {
		s.sec = sec
		s.rpm = 0
		s.tpm = 0
	}
}

// Call simulates one provider call at time now.
func (p *Provider) Call(r *ledger.Request, rng *rand.Rand, now time.Time) CallResult {
	sec := now.Unix()
	s := p.slotFor(now)
	s.mu.Lock()
	s.resetIfStale(sec)
	if s.rpm+1 > p.rpm.Load() || s.tpm+r.EstimateTokens > p.tpm.Load() {
		s.mu.Unlock()
		return CallResult{Outcome: ProviderRejected}
	}
	s.rpm++
	s.tpm += r.EstimateTokens
	s.mu.Unlock()

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

// ObservedRPM reports provider-side request usage in the sliding window.
func (p *Provider) ObservedRPM(now time.Time) int64 {
	return p.sumSlots(now, func(s *providerSlot) int64 { return s.rpm })
}

// ObservedTPM reports provider-side token usage in the sliding window.
func (p *Provider) ObservedTPM(now time.Time) int64 {
	return p.sumSlots(now, func(s *providerSlot) int64 { return s.tpm })
}

func (p *Provider) sumSlots(now time.Time, get func(*providerSlot) int64) int64 {
	sec := now.Unix()
	var sum int64
	for i := range p.slots {
		s := &p.slots[i]
		s.mu.Lock()
		if s.sec > sec-60 && s.sec != 0 {
			sum += get(s)
		}
		s.mu.Unlock()
	}
	return sum
}
