// Package limiter implements per-model admission control: a true rolling
// 60-second RPM window and TPM token reservation with reconciliation against
// actuals. Enforcement is against the ROLLING total, not the per-second
// pace: a client sustaining the limit's per-second pace is still capped at
// the limit across any 60-second span.
package limiter

import (
	"sync"
	"sync/atomic"
	"time"

	"consuma/internal/config"
)

// AdmissionResult tells the engine what to do with a request: OK -> schedule;
// OK=false with OverTPMBudget -> reject outright (the request can never fit);
// OK=false with OverRate -> overflow to the bounded wait queue.
type AdmissionResult struct {
	OK            bool
	OverRate      bool
	OverTPMBudget bool
}

// slot is one second's worth of accounting. All mutations take the slot
// mutex so stale-slot resets can never race concurrent adds.
type slot struct {
	mu     sync.Mutex
	sec    int64
	rpm    int64 // requests admitted this second
	tpm    int64 // tokens reserved this second (estimated)
	actual int64 // tokens actually charged this second (post-reconcile)
}

// reset adopts the slot for a new second (call with slot.mu held).
func (s *slot) reset(sec int64) {
	if s.sec != sec {
		s.sec = sec
		s.rpm = 0
		s.tpm = 0
		s.actual = 0
	}
}

// Limiter is admission control for one model. Enforcement reads the rolling
// 60s window totals: rpmUsed/tpmUsed track the sum over the 60-slot ring and
// are maintained incrementally (add on admit, subtract the evicted slot on
// rollover), so TryAdmit's decision is a plain compare - no 60-slot scan on
// the hot path. The rolling totals and the slot ring are updated under
// usedMu; slot.mu protects slot fields against adopt races only.
type Limiter struct {
	rpmLimit atomic.Int64
	tpmLimit atomic.Int64

	slots [60]slot

	rpmUsed   int64 // rolling 60s admitted count (guarded by usedMu)
	tpmUsed   int64 // rolling 60s reserved tokens (guarded by usedMu)
	wheelBase int64 // unix second of the oldest live slot (guarded by usedMu)
	usedMu    sync.Mutex

	actualUsed int64 // rolling 60s actually-charged tokens
	actualMu   sync.Mutex

	admittedTPM atomic.Int64 // cumulative reserved tokens
}

// New builds a limiter from a model config.
func New(m *config.ModelConfig) *Limiter {
	l := &Limiter{}
	l.rpmLimit.Store(m.RPM)
	l.tpmLimit.Store(m.TPM)
	return l
}

// roll advances the ring's window base so that the window ending at sec
// covers [sec-59, sec]. wheelBase is ALWAYS the oldest live second: it
// advances one second per eviction and is never snapped to the current
// second (a snap would skip evictions, strand counts in the rolling totals,
// and permanently starve the limiter under sustained load). Call with
// usedMu held. Back-dated seconds (sec < wheelBase) are inside the live
// window: no eviction.
func (l *Limiter) roll(sec int64) {
	if l.wheelBase == 0 {
		l.wheelBase = sec // first use: window starts empty here
		return
	}
	for l.wheelBase < sec-59 {
		// evict second wheelBase: it just left the 60s window
		s := l.slotFor(l.wheelBase)
		s.mu.Lock()
		if s.sec == l.wheelBase {
			l.rpmUsed -= s.rpm
			l.tpmUsed -= s.tpm
			s.rpm, s.tpm, s.actual = 0, 0, 0
		}
		s.mu.Unlock()
		l.wheelBase++
	}
}

func (l *Limiter) slotFor(sec int64) *slot {
	return &l.slots[sec%60]
}

// TryAdmit checks the rolling 60s RPM/TPM usage and reserves tokens.
func (l *Limiter) TryAdmit(tokens int64, now time.Time) AdmissionResult {
	rpmLimit := l.rpmLimit.Load()
	tpmLimit := l.tpmLimit.Load()
	if tokens > tpmLimit {
		return AdmissionResult{OK: false, OverTPMBudget: true}
	}
	sec := now.Unix()
	l.usedMu.Lock()
	l.roll(sec)
	if l.rpmUsed+1 > rpmLimit || l.tpmUsed+tokens > tpmLimit {
		l.usedMu.Unlock()
		return AdmissionResult{OK: false, OverRate: true}
	}
	s := l.slotFor(sec)
	s.mu.Lock()
	s.reset(sec)
	s.rpm++
	s.tpm += tokens
	s.mu.Unlock()
	l.rpmUsed++
	l.tpmUsed += tokens
	l.usedMu.Unlock()
	l.admittedTPM.Add(tokens)
	return AdmissionResult{OK: true}
}

// Reconcile records the provider's actual charge and refunds the unused
// reservation (estimated - actual) so freed budget is admissible again.
// Refunds decrement the rolling total immediately under usedMu, so freed
// budget is usable at once and can never double-count. A refund is applied
// only if the reservation's admission second is still inside the rolling
// window (admittedSec >= wheelBase): a refund for an already-evicted
// reservation must not mint headroom. Net is clamped at zero.
func (l *Limiter) Reconcile(reserved, actual, admittedSec int64, now time.Time) {
	sec := now.Unix()
	refund := reserved - actual
	if refund < 0 {
		refund = 0 // provider charged more than reserved; no phantom budget
	}
	l.usedMu.Lock()
	l.roll(sec)
	if admittedSec >= l.wheelBase && refund > 0 {
		l.tpmUsed -= refund
		if l.tpmUsed < 0 {
			l.tpmUsed = 0 // defensive: never let refunds mint headroom
		}
	}
	l.usedMu.Unlock()
	s := l.slotFor(sec)
	s.mu.Lock()
	s.reset(sec)
	s.actual += actual
	s.mu.Unlock()
	l.actualMu.Lock()
	l.actualUsed += actual
	l.actualMu.Unlock()
}

// ChargeRetry counts one retry attempt against the rolling RPM window (and
// its estimated tokens against TPM) like a fresh admission, without passing
// through the queue/overflow path.
func (l *Limiter) ChargeRetry(now time.Time) bool {
	return l.TryAdmit(0, now).OK
}

// SetLimits swaps the configured limits live (no restart). Enforcement
// applies to new admissions immediately.
func (l *Limiter) SetLimits(rpm, tpm int64) {
	l.rpmLimit.Store(rpm)
	l.tpmLimit.Store(tpm)
}

// Limits returns the currently configured limits.
func (l *Limiter) Limits() (int64, int64) {
	return l.rpmLimit.Load(), l.tpmLimit.Load()
}

// ObservedRPM returns the rolling 60s admitted count - by construction
// capped at the configured limit while enforcement is active.
func (l *Limiter) ObservedRPM(now time.Time) int64 {
	sec := now.Unix()
	l.usedMu.Lock()
	defer l.usedMu.Unlock()
	l.roll(sec)
	return l.rpmUsed
}

// ObservedTPM returns the rolling 60s outstanding reserved tokens, net of
// reconciled refunds. Before reconciliation this equals the reservation
// (conservative upper bound); refunds lower it toward the actual charge.
func (l *Limiter) ObservedTPM(now time.Time) int64 {
	sec := now.Unix()
	l.usedMu.Lock()
	defer l.usedMu.Unlock()
	l.roll(sec)
	if l.tpmUsed < 0 {
		return 0
	}
	return l.tpmUsed
}

// ActualTPM returns the rolling 60s actually-charged tokens (post-reconcile).
func (l *Limiter) ActualTPM(now time.Time) int64 {
	l.actualMu.Lock()
	defer l.actualMu.Unlock()
	return l.actualUsed
}

// AdmittedTPM returns cumulative tokens reserved at admission.
func (l *Limiter) AdmittedTPM() int64 { return l.admittedTPM.Load() }
