// Package limiter implements per-model admission control: a sliding-window
// RPM limit and TPM token reservation with reconciliation against actuals.
package limiter

import (
	"sync"
	"sync/atomic"
	"time"

	"consuma/internal/config"
)

// AdmissionResult tells the engine what to do with a request: OK → schedule;
// OK=false with OverTPMBudget → reject outright (the request can never fit);
// OK=false with OverRate → overflow to the bounded wait queue.
type AdmissionResult struct {
	OK            bool
	OverRate      bool
	OverTPMBudget bool
}

// slot is one second's worth of accounting. All ops take the slot mutex so a
// stale-slot reset can never race a concurrent add; the critical section is
// a few int operations (~tens of ns) and only the current second's slot is
// contended.
type slot struct {
	mu     sync.Mutex
	sec    int64
	rpm    int64 // requests admitted this second
	tpm    int64 // tokens reserved this second (estimated)
	actual int64 // tokens actually charged this second (post-reconcile)
}

// Limiter is admission control for one model.
type Limiter struct {
	rpmLimit atomic.Int64
	tpmLimit atomic.Int64

	slots [60]slot

	admittedTPM atomic.Int64 // cumulative reserved tokens
}

// New builds a limiter from a model config.
func New(m *config.ModelConfig) *Limiter {
	l := &Limiter{}
	l.rpmLimit.Store(m.RPM)
	l.tpmLimit.Store(m.TPM)
	return l
}

func (l *Limiter) slotFor(now time.Time) *slot {
	return &l.slots[now.Unix()%60]
}

// resetIfStale adopts the slot for a new minute when the wall clock wraps.
func (s *slot) resetIfStale(sec int64) {
	if s.sec != sec {
		s.sec = sec
		s.rpm = 0
		s.tpm = 0
		s.actual = 0
	}
}

// TryAdmit checks the RPM window and reserves tokens against TPM.
func (l *Limiter) TryAdmit(tokens int64, now time.Time) AdmissionResult {
	if tokens > l.tpmLimit.Load() {
		return AdmissionResult{OK: false, OverTPMBudget: true}
	}
	sec := now.Unix()
	s := l.slotFor(now)
	s.mu.Lock()
	s.resetIfStale(sec)
	if s.rpm+1 > l.rpmLimit.Load() || s.tpm+tokens > l.tpmLimit.Load() {
		s.mu.Unlock()
		return AdmissionResult{OK: false, OverRate: true}
	}
	s.rpm++
	s.tpm += tokens
	s.mu.Unlock()
	l.admittedTPM.Add(tokens)
	return AdmissionResult{OK: true}
}

// Reconcile records the provider's actual charge and refunds the unused
// reservation (estimated - actual) so freed budget is admissible again.
func (l *Limiter) Reconcile(reserved, actual int64, now time.Time) {
	sec := now.Unix()
	s := l.slotFor(now)
	s.mu.Lock()
	s.resetIfStale(sec)
	s.actual += actual
	refund := reserved - actual
	if refund > 0 && s.tpm >= refund {
		s.tpm -= refund
	}
	s.mu.Unlock()
}

// SetLimits swaps the configured limits live (no restart).
func (l *Limiter) SetLimits(rpm, tpm int64) {
	l.rpmLimit.Store(rpm)
	l.tpmLimit.Store(tpm)
}

// Limits returns the currently configured limits.
func (l *Limiter) Limits() (int64, int64) {
	return l.rpmLimit.Load(), l.tpmLimit.Load()
}

// ObservedRPM sums admitted requests over the sliding 60s window ending now.
func (l *Limiter) ObservedRPM(now time.Time) int64 {
	return l.sumSlots(now, func(s *slot) int64 { return s.rpm })
}

// ObservedTPM sums outstanding reserved tokens over the sliding 60s window.
// Before reconciliation this equals the reservation (conservative upper
// bound); Reconcile refunds unused reservations so the observed figure
// settles to the actually-charged amount.
func (l *Limiter) ObservedTPM(now time.Time) int64 {
	return l.sumSlots(now, func(s *slot) int64 { return s.tpm })
}

// AdmittedTPM returns cumulative tokens reserved at admission.
func (l *Limiter) AdmittedTPM() int64 { return l.admittedTPM.Load() }

func (l *Limiter) sumSlots(now time.Time, get func(*slot) int64) int64 {
	sec := now.Unix()
	var sum int64
	for i := range l.slots {
		s := &l.slots[i]
		s.mu.Lock()
		// fresh: slot belongs to the last 60 seconds
		if s.sec > sec-60 && s.sec != 0 {
			sum += get(s)
		}
		s.mu.Unlock()
	}
	return sum
}
