// Shared per-request finalization used by both modes: the benchmark wheel
// path (applyBatch) and the real-mode worker pool. Contains the state
// transition, retry decision (ChargeRetry / queue deferral), and the
// once-per-request terminal bookkeeping (e2e sample, TPM reconciliation,
// real-mode persistence + notifications).
package engine

import (
	"time"

	"consuma/internal/ledger"
	"consuma/internal/provider"
)

// finalizeOne applies one call result to its request. Returns the number of
// successful, failed, and completed (any terminal) transitions it performed
// (0/0/0 when the request was already terminal — a lost race).
func (e *Engine) finalizeOne(p *modelPipeline, r *ledger.Request, res provider.CallResult, now time.Time, nowNS int64) (okN, failN, compN int64) {
	r.ActualCharged.Add(res.ActualTokens)

	// terminal bookkeeping: e2e sample ONCE per request (at the terminal
	// transition — retried requests must not contribute partial samples),
	// TPM reconciliation exactly once, real-mode persistence + notifications.
	terminal := func(outcomeName string) {
		e2e := nowNS - r.BornNS
		if e2e < 0 {
			e2e = 0 // clock anomaly defense; clamp before recording
		}
		p.hist.Record(e2e)
		e.cE2ESum.Add(e2e)
		for {
			cur := e.e2eMaxNS.Load()
			if e2e <= cur {
				break
			}
			if e.e2eMaxNS.CompareAndSwap(cur, e2e) {
				break
			}
		}
		p.lim.Reconcile(r.EstimateTokens, r.ActualCharged.Load(), r.AdmittedSec, now)
		if e.mode == modeReal {
			e.realTerminal(r, outcomeName, r.ActualCharged.Load())
		}
	}

	switch res.Outcome {
	case provider.Success:
		if e.ld.Transition(r, ledger.Completed) == nil {
			terminal("success")
			return 1, 0, 1
		}
	case provider.TransientFailure, provider.ProviderRejected:
		if int(r.Attempts.Load()) < e.cfg.MaxRetries {
			// charge the retry against the rolling RPM window FIRST; if the
			// window is full, defer the retry to the wait queue instead of
			// bypassing system-side enforcement
			if p.lim.ChargeRetry(now) {
				if e.ld.Transition(r, ledger.Admitted) == nil {
					// exponential backoff: 50ms base, doubling per attempt
					backoff := time.Duration(50<<uint(r.Attempts.Load()-1)) * time.Millisecond
					if e.mode == modeReal {
						// real mode: the blocking call re-issues after the
						// backoff from a worker (no wheel in real mode)
						time.AfterFunc(backoff, func() { e.dispatch(r) })
					} else {
						res2 := e.drawCall(p, r, now) // fresh call, counts against provider limits
						e.schedule(p, r, res2, now, backoff)
					}
				}
			} else if e.ld.Transition(r, ledger.Queued) == nil {
				if !p.q.PushQueued(r, now) {
					// queue full too: retry infrastructure exhausted
					if e.ld.Transition(r, ledger.Completed) == nil {
						terminal("failed")
						return 0, 1, 1
					}
				}
			}
		} else if e.ld.Transition(r, ledger.Completed) == nil {
			terminal("failed")
			return 0, 1, 1
		}
	case provider.PermanentFailure:
		if e.ld.Transition(r, ledger.Completed) == nil {
			terminal("failed")
			return 0, 1, 1
		}
	}
	return 0, 0, 0
}
