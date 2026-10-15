// Package ledger tracks every request through its lifecycle with exactly-once
// terminal accounting. Terminal states: Completed, Expired, Rejected — every
// submitted request ends in exactly one of them.
package ledger

import (
	"fmt"
	"sync/atomic"
)

// State is a request lifecycle state.
type State uint8

const (
	Submitted State = iota
	Queued
	Admitted
	InFlight
	Completed
	Expired
	Rejected
	stateCount
)

// legal maps each state to the states it may transition to.
var legal = map[State][]State{
	Submitted: {Queued, Admitted, Rejected},
	Queued:    {Admitted, Expired},
	Admitted:  {InFlight},
	// InFlight -> Queued is the retry-deferral path: the rolling window is
	// full, so the retry goes back to the wait queue instead of bypassing
	// RPM enforcement.
	InFlight:  {Completed, Admitted, Queued},
	Completed: {},
	Expired:   {},
	Rejected:  {},
}

func isTerminal(s State) bool { return len(legal[s]) == 0 }

// Request is one simulated inference request. State is packed into the struct
// to keep allocations single-sized and cache friendly on the hot path.
type Request struct {
	ID             uint64
	Model          string
	EstimateTokens int64
	BornNS         int64
	State          atomic.Uint32 // holds State values
	Attempts       atomic.Int32
	ActualCharged  atomic.Int64 // cumulative tokens charged across attempts
	AdmittedSec    int64        // unix second of the (first) admission; set once
	ExternalID     string       // real-mode UUID (set by the batch manager); empty in benchmark mode
	BatchExtID     string       // real-mode batch UUID this request belongs to; empty in benchmark mode
	Recovered      bool         // crash-recovered request: exempt from sweep expiry (survives re-queue)
}

// Ledger aggregates per-state counts with atomic counters.
type Ledger struct {
	counts     [stateCount]atomic.Int64
	submitted  atomic.Int64
	terminated atomic.Int64 // completed + expired + rejected
}

// New returns an empty ledger.
func New() *Ledger { return &Ledger{} }

// Submit records a newly submitted request (state starts at Submitted).
func (ledger *Ledger) Submit(r *Request) {
	r.State.Store(uint32(Submitted))
	ledger.submitted.Add(1)
	ledger.counts[Submitted].Add(1)
}

// Transition moves r to `to` if the current state allows it; otherwise it
// returns an error and changes nothing. Exactly-once terminals follow from
// the compare-and-swap: only one goroutine can win any given transition.
func (ledger *Ledger) Transition(r *Request, to State) error {
	from := State(r.State.Load())
	allowed := false
	for _, s := range legal[from] {
		if s == to {
			allowed = true
			break
		}
	}
	if !allowed {
		return fmt.Errorf("ledger: illegal transition %d -> %d for request %d", from, to, r.ID)
	}
	if !r.State.CompareAndSwap(uint32(from), uint32(to)) {
		return fmt.Errorf("ledger: lost race on transition %d -> %d for request %d", from, to, r.ID)
	}
	ledger.counts[from].Add(-1)
	ledger.counts[to].Add(1)
	if isTerminal(to) {
		ledger.terminated.Add(1)
	}
	return nil
}

// Count returns the number of requests currently in state s.
func (ledger *Ledger) Count(s State) int64 { return ledger.counts[s].Load() }

// Submitted returns the total number of submitted requests.
func (ledger *Ledger) Submitted() int64 { return ledger.submitted.Load() }

// TerminalBalance reports whether every submitted request has reached a
// terminal state (submitted == completed + expired + rejected).
func (ledger *Ledger) TerminalBalance() bool {
	return ledger.submitted.Load() == ledger.terminated.Load()
}
