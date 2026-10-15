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
	engine, err := NewWithCaller(cfg, c)
	if err != nil {
		return nil, err
	}
	engine.mode = modeReal
	engine.st = st
	engine.workerCh = make(chan *ledger.Request, 1<<16)
	engine.doneChans = make(map[string]chan terminalResult)
	engine.recovering = make(map[string]bool)
	for i := 0; i < cfg.Workers; i++ {
		engine.workersWg.Add(1)
		go engine.workerLoop()
	}
	return engine, nil
}

// dispatch hands an admitted request to the worker pool.
func (engine *Engine) dispatch(r *ledger.Request) {
	engine.workerCh <- r // buffered 64K; blocking beyond that means the system is 64K calls behind
}

// workerLoop consumes admitted requests and drives each through one
// (possibly retried) provider call to a terminal state.
func (engine *Engine) workerLoop() {
	defer engine.workersWg.Done()
	for {
		select {
		case <-engine.shutdown:
			return
		case r := <-engine.workerCh:
			engine.processOne(r)
		}
	}
}

// processOne runs one blocking provider call for r and finalizes it.
func (engine *Engine) processOne(r *ledger.Request) {
	p, ok := engine.pipes[r.Model]
	if !ok {
		// model vanished between submit and pickup: rejected, nothing charged
		if engine.ld.Transition(r, ledger.Rejected) == nil {
			engine.cRejected.Add(1)
			engine.realTerminal(r, "rejected", 0)
		}
		return
	}
	now := time.Now()
	if engine.st != nil && r.ExternalID != "" {
		_ = engine.st.SetRequestState(r.ExternalID, "in_flight")
	}
	// mirror the benchmark wheel path: Admitted→InFlight before the call; a
	// failure means the request went terminal elsewhere (engine.g. swept) and its
	// bookkeeping already happened
	if engine.ld.Transition(r, ledger.InFlight) != nil {
		return
	}
	// count the attempt BEFORE the call (schedule() does this on the wheel
	// path; real mode has no schedule) — without it the retry decision in
	// finalizeOne never sees attempts grow and retries forever
	r.Attempts.Add(1)
	res := engine.caller.Call(r, now)
	engine.cProvLatency.Add(int64(res.Latency))
	okN, failN, compN := engine.finalizeOne(p, r, res, now, now.UnixNano())
	if okN > 0 {
		engine.cOK.Add(1)
	}
	if failN > 0 {
		engine.cFailed.Add(1)
	}
	if compN > 0 {
		engine.cCompleted.Add(1)
	}
}

// realTerminal persists the final request state, fires the OnTerminal hook
// (batch manager counts), and releases any synchronous waiter. Exactly-once
// by construction: every caller is behind a successful CAS transition.
func (engine *Engine) realTerminal(r *ledger.Request, outcomeName string, tokens int64) {
	dbState := "completed"
	switch outcomeName {
	case "expired":
		dbState = "expired"
	case "rejected":
		dbState = "rejected"
	}
	if engine.st != nil && r.ExternalID != "" {
		_ = engine.st.SetRequestFinal(r.ExternalID, dbState, outcomeName, tokens)
	}
	if h := engine.OnTerminal; h != nil {
		h(r, outcomeName, tokens)
	}
	engine.doneMu.Lock()
	if ch, ok := engine.doneChans[r.ExternalID]; ok {
		delete(engine.doneChans, r.ExternalID)
		ch <- terminalResult{Outcome: outcomeName, Tokens: tokens}
	}
	engine.doneMu.Unlock()
}

// SubmitAndWait submits one request and blocks until it reaches a terminal
// state (or the timeout expires). Real mode only.
func (engine *Engine) SubmitAndWait(r *ledger.Request, timeout time.Duration) (string, int64, error) {
	if engine.mode != modeReal {
		return "", 0, fmt.Errorf("engine: SubmitAndWait requires real mode")
	}
	ch := make(chan terminalResult, 1)
	engine.doneMu.Lock()
	engine.doneChans[r.ExternalID] = ch
	engine.doneMu.Unlock()

	engine.Submit(r)

	select {
	case res := <-ch:
		return res.Outcome, res.Tokens, nil
	case <-time.After(timeout):
		engine.doneMu.Lock()
		delete(engine.doneChans, r.ExternalID)
		engine.doneMu.Unlock()
		return "", 0, fmt.Errorf("engine: request %s did not finish in %v", r.ExternalID, timeout)
	}
}

// Recover re-enqueues every persisted request that a crash caught before its
// terminal state (submitted/queued/in_flight). Idempotent: a second run
// finds nothing. Returns the number re-enqueued.
func (engine *Engine) Recover() (int, error) {
	if engine.mode != modeReal {
		return 0, fmt.Errorf("engine: Recover requires real mode")
	}
	rows, err := engine.st.RequestsNeedingRecovery()
	if err != nil {
		return 0, err
	}
	var requeued int
	for _, row := range rows {
		// in-memory dedup: a row re-enqueued by an earlier Recover in THIS
		// process is being processed; the DB row only leaves the recovery
		// set at its terminal write. A crash between still finds it — the
		// durable guarantee is unchanged.
		engine.recMu.Lock()
		if engine.recovering[row.ID] {
			engine.recMu.Unlock()
			continue
		}
		engine.recovering[row.ID] = true
		engine.recMu.Unlock()
		requeued++

		if _, ok := engine.pipes[row.Model]; !ok {
			_ = engine.st.SetRequestFinal(row.ID, "rejected", "unknown_model", 0)
			continue
		}
		r := &ledger.Request{
			ID:             recoverIDSeq.Add(1),
			ExternalID:     row.ID,
			BatchExtID:     row.BatchID, // OnTerminal routes counts by this
			Model:          row.Model,
			EstimateTokens: row.Tokens,
			BornNS:         time.Now().UnixNano(),
			Recovered:      true,
		}
		engine.ld.Submit(r)
		if err := engine.ld.Transition(r, ledger.Queued); err != nil {
			return len(rows), fmt.Errorf("engine: recover transition %s: %w", row.ID, err)
		}
		_ = engine.st.MarkRequestQueued(row.ID)
		p := engine.pipes[row.Model]
		for !p.q.PushRecovered(r, time.Now()) {
			time.Sleep(2 * time.Millisecond) // queue full: wait for drain
		}
	}
	return requeued, nil
}

// WheelPending exposes the wheel's outstanding-entry count (tests: real mode
// must always report 0).
func (engine *Engine) WheelPending() int64 { return engine.wheel.Pending() }
