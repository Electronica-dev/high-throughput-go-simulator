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
	mu       sync.Mutex
	sec      int64
	rpm      int64 // requests admitted this second
	tpm      int64 // tokens reserved this second (estimated)
	actual   int64 // tokens actually charged this second (post-reconcile)
	actualMu sync.Mutex // actual is touched from the completion path; tiny lock
}

// Limiter is admission control for one model.
type Limiter struct {
	rpmLimit atomic.Int64
	tpmLimit atomic.Int64

	slots [60]slot

	admittedTPM atomic.Int64 // cumulative reserved tokens
	refundPool  atomic.Int64 // reconciled refunds, consumed by TryAdmit
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
	refund := l.consumeRefund(tokens) // freed budget offsets this reservation
	s.mu.Lock()
	s.resetIfStale(sec)
	net := s.tpm - refund
	if s.rpm+1 > l.rpmLimit.Load() || net+tokens > l.tpmLimit.Load() {
		s.mu.Unlock()
		if refund > 0 {
			l.refundPool.Add(refund) // unused; return to the pool
		}
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
// Lock-free: refunds accumulate in an atomic pool that TryAdmit subtracts
// from the current slot's reservation total; no mutex on the completion
// hot path (Reconcile runs once per completed request).
func (l *Limiter) Reconcile(reserved, actual int64, now time.Time) {
	_ = now
	s := l.slotFor(now)
	refund := reserved - actual
	if refund > 0 {
		l.refundPool.Add(refund)
	}
	s.actualMu.Lock()
	s.actual += actual
	s.actualMu.Unlock()
}

// consumeRefund drains up to `want` tokens from the refund pool.
func (l *Limiter) consumeRefund(want int64) int64 {
	for {
		cur := l.refundPool.Load()
		if cur <= 0 {
			return 0
		}
		take := cur
		if take > want {
			take = want
		}
		if l.refundPool.CompareAndSwap(cur, cur-take) {
			return take
		}
	}
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

// ObservedTPM sums outstanding reserved tokens over the sliding 60s window,
// net of reconciled refunds still sitting in the refund pool. Before
// reconciliation this equals the reservation (conservative upper bound);
// refunds lower it toward the actually-charged amount.
func (l *Limiter) ObservedTPM(now time.Time) int64 {
	v := l.sumSlots(now, func(s *slot) int64 { return s.tpm }) - l.refundPool.Load()
	if v < 0 {
		v = 0
	}
	return v
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
