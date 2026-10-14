// Real-mode engine wiring: worker pool instead of the timer wheel (the
// blocking HTTP provider call IS the latency), SQLite persistence of each
// request's lifecycle, the crash-recovery sweep, and per-request completion
// notifications for the synchronous API. The benchmark hot path never runs
// any of this.
package engine

import (
	"fmt"
	"sync/atomic"
	"time"

	"consuma/internal/config"
	"consuma/internal/ledger"
	"consuma/internal/store"
)

type engineMode uint8

const (
	modeBenchmark engineMode = iota
	modeReal
)

// terminalResult is what a synchronous submitter waits for.
type terminalResult struct {
	Outcome string
	Tokens  int64
}

// recoverIDSeq hands recovered requests non-colliding in-memory IDs.
var recoverIDSeq atomic.Uint64

func init() { recoverIDSeq.Store(1 << 62) }

// NewReal builds and starts a real-mode engine: given Caller (an
// httpprovider.HTTPProvider), SQLite store, worker pool of cfg.Workers.
// The wheel stays running but idle (the queue drainer exits on wheel.Done()).
func NewReal(cfg *config.Config, c Caller, st *store.Store) (*Engine, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = 64
	}
	e, err := NewWithCaller(cfg, c)
	if err != nil {
		return nil, err
	}
	e.mode = modeReal
	e.st = st
	e.workerCh = make(chan *ledger.Request, 1<<16)
	e.doneChans = make(map[string]chan terminalResult)
	e.recovering = make(map[string]bool)
	for i := 0; i < cfg.Workers; i++ {
		e.workersWg.Add(1)
		go e.workerLoop()
	}
	return e, nil
}

// dispatch hands an admitted request to the worker pool.
func (e *Engine) dispatch(r *ledger.Request) {
	e.workerCh <- r // buffered 64K; blocking beyond that means the system is 64K calls behind
}

// workerLoop consumes admitted requests and drives each through one
// (possibly retried) provider call to a terminal state.
func (e *Engine) workerLoop() {
	defer e.workersWg.Done()
	for {
		select {
		case <-e.shutdown:
			return
		case r := <-e.workerCh:
			e.processOne(r)
		}
	}
}

// processOne runs one blocking provider call for r and finalizes it.
func (e *Engine) processOne(r *ledger.Request) {
	p, ok := e.pipes[r.Model]
	if !ok {
		// model vanished between submit and pickup: rejected, nothing charged
		if e.ld.Transition(r, ledger.Rejected) == nil {
			e.cRejected.Add(1)
			e.realTerminal(r, "rejected", 0)
		}
		return
	}
	now := time.Now()
	if e.st != nil && r.ExternalID != "" {
		_ = e.st.SetRequestState(r.ExternalID, "in_flight")
	}
	// mirror the benchmark wheel path: Admitted→InFlight before the call; a
	// failure means the request went terminal elsewhere (e.g. swept) and its
	// bookkeeping already happened
	if e.ld.Transition(r, ledger.InFlight) != nil {
		return
	}
	res := e.caller.Call(r, now)
	e.cProvLatency.Add(int64(res.Latency))
	okN, failN, compN := e.finalizeOne(p, r, res, now, now.UnixNano())
	if okN > 0 {
		e.cOK.Add(1)
	}
	if failN > 0 {
		e.cFailed.Add(1)
	}
	if compN > 0 {
		e.cCompleted.Add(1)
	}
}

// realTerminal persists the final request state, fires the OnTerminal hook
// (batch manager counts), and releases any synchronous waiter. Exactly-once
// by construction: every caller is behind a successful CAS transition.
func (e *Engine) realTerminal(r *ledger.Request, outcomeName string, tokens int64) {
	dbState := "completed"
	switch outcomeName {
	case "expired":
		dbState = "expired"
	case "rejected":
		dbState = "rejected"
	}
	if e.st != nil && r.ExternalID != "" {
		_ = e.st.SetRequestFinal(r.ExternalID, dbState, outcomeName, tokens)
	}
	if h := e.OnTerminal; h != nil {
		h(r, outcomeName, tokens)
	}
	e.doneMu.Lock()
	if ch, ok := e.doneChans[r.ExternalID]; ok {
		delete(e.doneChans, r.ExternalID)
		ch <- terminalResult{Outcome: outcomeName, Tokens: tokens}
	}
	e.doneMu.Unlock()
}

// SubmitAndWait submits one request and blocks until it reaches a terminal
// state (or the timeout expires). Real mode only.
func (e *Engine) SubmitAndWait(r *ledger.Request, timeout time.Duration) (string, int64, error) {
	if e.mode != modeReal {
		return "", 0, fmt.Errorf("engine: SubmitAndWait requires real mode")
	}
	ch := make(chan terminalResult, 1)
	e.doneMu.Lock()
	e.doneChans[r.ExternalID] = ch
	e.doneMu.Unlock()

	e.Submit(r)

	select {
	case res := <-ch:
		return res.Outcome, res.Tokens, nil
	case <-time.After(timeout):
		e.doneMu.Lock()
		delete(e.doneChans, r.ExternalID)
		e.doneMu.Unlock()
		return "", 0, fmt.Errorf("engine: request %s did not finish in %v", r.ExternalID, timeout)
	}
}

// Recover re-enqueues every persisted request that a crash caught before its
// terminal state (submitted/queued/in_flight). Idempotent: a second run
// finds nothing. Returns the number re-enqueued.
func (e *Engine) Recover() (int, error) {
	if e.mode != modeReal {
		return 0, fmt.Errorf("engine: Recover requires real mode")
	}
	rows, err := e.st.RequestsNeedingRecovery()
	if err != nil {
		return 0, err
	}
	var requeued int
	for _, row := range rows {
		// in-memory dedup: a row re-enqueued by an earlier Recover in THIS
		// process is being processed; the DB row only leaves the recovery
		// set at its terminal write. A crash between still finds it — the
		// durable guarantee is unchanged.
		e.recMu.Lock()
		if e.recovering[row.ID] {
			e.recMu.Unlock()
			continue
		}
		e.recovering[row.ID] = true
		e.recMu.Unlock()
		requeued++

		if _, ok := e.pipes[row.Model]; !ok {
			_ = e.st.SetRequestFinal(row.ID, "rejected", "unknown_model", 0)
			continue
		}
		r := &ledger.Request{
			ID:             recoverIDSeq.Add(1),
			ExternalID:     row.ID,
			Model:          row.Model,
			EstimateTokens: row.Tokens,
			BornNS:         time.Now().UnixNano(),
		}
		e.ld.Submit(r)
		if err := e.ld.Transition(r, ledger.Queued); err != nil {
			return len(rows), fmt.Errorf("engine: recover transition %s: %w", row.ID, err)
		}
		_ = e.st.MarkRequestQueued(row.ID)
		p := e.pipes[row.Model]
		for !p.q.PushQueued(r, time.Now()) {
			time.Sleep(2 * time.Millisecond) // queue full: wait for drain
		}
	}
	return requeued, nil
}

// WheelPending exposes the wheel's outstanding-entry count (tests: real mode
// must always report 0).
func (e *Engine) WheelPending() int64 { return e.wheel.Pending() }
