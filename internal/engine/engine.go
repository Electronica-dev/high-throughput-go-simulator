// Package engine wires the simulation pipeline: submissions flow through the
// per-model admission limiter, overflow waits in the bounded queue, admitted
// calls are scheduled on the timer wheel and finalized against the provider's
// drawn result. This file is the only place those packages connect.
package engine

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"consuma/internal/config"
	"consuma/internal/ledger"
	"consuma/internal/limiter"
	"consuma/internal/metrics"
	"consuma/internal/provider"
	"consuma/internal/queue"
	"consuma/internal/store"
	"consuma/internal/wheel"
)

// Summary is the end-of-run accounting for one benchmark run.
type Summary struct {
	Submitted int64
	Completed int64
	OK        int64
	Failed    int64
	Expired   int64
	Rejected  int64

	WholeRunAvgPerSec float64
	SteadyStatePerSec float64
	DrainSec          float64

	Percentiles map[string]float64 // per model: e2e p50/p95/p99 in ms
}

// modelPipeline is everything belonging to one simulated model.
type modelPipeline struct {
	cfg  *config.ModelConfig
	lim  *limiter.Limiter
	prov *provider.Provider
	q    *queue.WaitQueue
	hist *metrics.Histogram
}

// Engine connects limiter, queue, wheel, provider, ledger and metrics.
type Engine struct {
	cfg     *config.Config
	ld      *ledger.Ledger
	reg     *metrics.Registry
	wheel   *wheel.Wheel
	pipes   map[string]*modelPipeline
	caller  Caller
	startNS int64

	// real mode only (benchmark mode leaves these zero)
	mode        engineMode
	st          *store.Store
	workerCh    chan *ledger.Request
	workersWg   sync.WaitGroup
	shutdown    chan struct{}
	shutdownDo  sync.Once
	OnTerminal  func(r *ledger.Request, outcome string, tokens int64)
	doneMu      sync.Mutex
	doneChans   map[string]chan terminalResult
	recMu       sync.Mutex
	recovering  map[string]bool

	// pre-registered counters: no map/RWMutex cost on the hot path
	cSubmitted, cAdmitted, cRejected, cOK, cFailed, cCompleted,
	cExpired, cOverflow, cProvLatency, cE2ESum, cE2EMax *metrics.Counter

	// e2eMaxNS tracks the largest observed end-to-end latency (CAS max).
	e2eMaxNS atomic.Int64

	accepting atomic.Bool

	// DebugTrackRequests keeps every request in memory (test/debug only).
	// Off by default: at 1M req/sec a tracked run would hold ~1GB of
	// request objects and throttle the pipeline through GC alone.
	DebugTrackRequests bool


	steadyStart      atomic.Int64
	steadyEnd        atomic.Int64
	completedAtStart atomic.Int64
	steadyCompleted  atomic.Int64

	rngs sync.Pool

	requestsMu sync.Mutex
	allReqs    []*ledger.Request
}

// rng returns a goroutine-local random source (LatencyConfig.Draw and the
// provider draws are not safe for concurrent use on one Rand). Always pair
// with putRNG.
func (e *Engine) rng() *rand.Rand {
	if r, ok := e.rngs.Get().(*rand.Rand); ok && r != nil {
		return r
	}
	return rand.New(rand.NewSource(time.Now().UnixNano() + rand.Int63()))
}

func putRNG(e *Engine, r *rand.Rand) { e.rngs.Put(r) }

// drawCall runs one provider call via the engine's Caller.
func (e *Engine) drawCall(p *modelPipeline, r *ledger.Request, now time.Time) provider.CallResult {
	return e.caller.Call(r, now)
}

// New builds and starts a benchmark-mode engine (in-process caller).
func New(cfg *config.Config) (*Engine, error) {
	return NewWithCaller(cfg, nil)
}

// NewWithCaller builds and starts an engine using the given Caller
// (nil = benchmark mode's in-process caller).
func NewWithCaller(cfg *config.Config, c Caller) (*Engine, error) {
	horizon := time.Duration(cfg.WheelBuckets) * time.Duration(cfg.WheelTickMS) * time.Millisecond
	if err := cfg.Validate(horizon); err != nil {
		return nil, err
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	ld := ledger.New()
	reg := metrics.New(8)
	w := wheel.New(cfg.WheelShards, cfg.WheelBuckets, time.Duration(cfg.WheelTickMS)*time.Millisecond, nil)
	e := &Engine{
		cfg:      cfg,
		ld:       ld,
		reg:      reg,
		wheel:    w,
		pipes:    make(map[string]*modelPipeline, len(cfg.Models)),
		startNS:  time.Now().UnixNano(),
		mode:     modeBenchmark,
		shutdown: make(chan struct{}),
	}
	e.cSubmitted = reg.Register("submitted")
	e.cAdmitted = reg.Register("admitted")
	e.cRejected = reg.Register("rejected")
	e.cOK = reg.Register("ok")
	e.cFailed = reg.Register("failed")
	e.cCompleted = reg.Register("completed")
	e.cExpired = reg.Register("expired")
	e.cOverflow = reg.Register("overflowed")
	e.cProvLatency = reg.Register("provider_latency_ns")
	e.cE2ESum = reg.Register("e2e_sum_ns")
	e.cE2EMax = reg.Register("e2e_max_ns")
	e.accepting.Store(true)
	for i := range cfg.Models {
		m := &cfg.Models[i]
		p := &modelPipeline{
			cfg:  m,
			lim:  limiter.New(m),
			prov: provider.New(m),
			q:    queue.New(cfg.QueueDepth, time.Duration(cfg.MaxWaitMS)*time.Millisecond, ld),
			hist: metrics.NewHistogram(),
		}
		e.pipes[m.Name] = p
	}
	if c == nil {
		provs := make(map[string]*provider.Provider, len(e.pipes))
		for name, p := range e.pipes {
			provs[name] = p.prov
		}
		c = NewInProcessCaller(provs, e.rng, func(r *rand.Rand) { putRNG(e, r) })
	}
	e.caller = c
	w.SetHandler(func(batch []*wheel.Entry) { e.applyBatch(batch) })
	w.Start()
	// queue drain ticker: admit queued requests as capacity frees
	go e.queueDrainer()
	return e, nil
}

// Submit routes one request into the pipeline, persisting its row (real mode).
func (e *Engine) Submit(r *ledger.Request) {
	e.submit(r, true)
}

// SubmitUntracked routes one request whose row the caller already persisted
// (the batch manager's single bulk transaction). Real mode only.
func (e *Engine) SubmitUntracked(r *ledger.Request) {
	e.submit(r, false)
}

func (e *Engine) submit(r *ledger.Request, persist bool) {
	if !e.accepting.Load() {
		return // draining; loadgen stops submitting before Drain
	}
	e.ld.Submit(r)
	if persist && e.mode == modeReal && e.st != nil && r.ExternalID != "" {
		_ = e.st.InsertRequest(store.RequestRow{
			ID: r.ExternalID, BatchID: r.BatchExtID, Model: r.Model,
			State: "submitted", Tokens: r.EstimateTokens, AdmittedSec: 0,
		})
	}
	if e.DebugTrackRequests {
		e.requestsMu.Lock()
		e.allReqs = append(e.allReqs, r)
		e.requestsMu.Unlock()
	}
	e.cSubmitted.Add(1)

	p, ok := e.pipes[r.Model]
	if !ok {
		e.ld.Transition(r, ledger.Rejected)
		e.cRejected.Add(1)
		if e.mode == modeReal {
			e.realTerminal(r, "rejected", 0)
		}
		return
	}
	now := time.Now()
	// admission first, provider draw second: a queued or rejected request
	// must never consume provider-side capacity (the backstop measures real
	// calls, not attempted ones)
	adm := p.lim.TryAdmit(r.EstimateTokens, now)
	switch {
	case adm.OK:
		e.cAdmitted.Add(1)
		r.AdmittedSec = now.Unix()
		if e.mode == modeReal {
			if e.ld.Transition(r, ledger.Admitted) == nil {
				e.dispatch(r) // worker transitions InFlight, draws, finalizes
			}
			return
		}
		res := e.drawCall(p, r, now)
		e.schedule(p, r, res, now, 0)
	case adm.OverTPMBudget:
		e.ld.Transition(r, ledger.Rejected)
		e.cRejected.Add(1)
		if e.mode == modeReal {
			e.realTerminal(r, "rejected", 0)
		}
	default:
		// OverRate: bounded wait queue with expiry
		e.cOverflow.Add(1)
		if !p.q.Push(r, now) {
			e.ld.Transition(r, ledger.Rejected)
			e.cRejected.Add(1)
			if e.mode == modeReal {
				e.realTerminal(r, "rejected", 0)
			}
		}
	}
}

// schedule puts the drawn call on the wheel for nowNS+latency (+backoff).
func (e *Engine) schedule(p *modelPipeline, r *ledger.Request, res provider.CallResult, now time.Time, backoff time.Duration) {
	r.Attempts.Add(1)
	fireNS := now.UnixNano() + backoff.Nanoseconds() + res.Latency.Nanoseconds()
	if err := e.ld.Transition(r, ledger.Admitted); err != nil && ledger.State(r.State.Load()) != ledger.Admitted {
		return // terminal; do not resurrect
	}
	e.ld.Transition(r, ledger.InFlight)
	e.wheel.Schedule(fireNS, &wheel.Entry{
		Req:    r,
		FireNS: fireNS,
		Data:   pendingCall{res: res, pipeline: p},
	})
}

type pendingCall struct {
	res      provider.CallResult
	pipeline *modelPipeline
}

// applyBatch finalizes drained wheel entries: terminal transition, token
// reconciliation, retries with exponential backoff, metrics. Counters are
// aggregated per batch (one atomic add per batch, not per request).
func (e *Engine) applyBatch(batch []*wheel.Entry) {
	now := time.Now()
	nowNS := now.UnixNano()
	var nOK, nFailed, nCompleted, provLatencyNS int64
	for _, ent := range batch {
		pc := ent.Data.(pendingCall)
		r := ent.Req
		p := pc.pipeline
		provLatencyNS += int64(pc.res.Latency)
		okN, failN, compN := e.finalizeOne(p, r, pc.res, now, nowNS)
		nOK += okN
		nFailed += failN
		nCompleted += compN
	}
	if nOK > 0 {
		e.cOK.Add(nOK)
	}
	if nFailed > 0 {
		e.cFailed.Add(nFailed)
	}
	if nCompleted > 0 {
		e.cCompleted.Add(nCompleted)
	}
	if provLatencyNS > 0 {
		e.cProvLatency.Add(provLatencyNS)
	}
}

// queueDrainer admits queued requests as capacity frees (FIFO, atomic
// pop-if-admissible so a concurrent sweep can never strand a popped request)
// and enforces the wait-queue expiry DURING the run, not just at drain.
func (e *Engine) queueDrainer() {
	ticker := time.NewTicker(time.Duration(e.cfg.WheelTickMS) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-e.wheel.Done():
			return
		case now := <-ticker.C:
			for _, p := range e.pipes {
				expired := p.q.SweepExpired(now) // max_wait_ms enforced during the run
				for _, r := range expired {
					if e.mode == modeReal {
						e.realTerminal(r, "expired", r.ActualCharged.Load())
					}
				}
				for {
					r := p.q.PopIfAdmissible(func(req *ledger.Request) bool {
						return p.lim.TryAdmit(req.EstimateTokens, now).OK
					})
					if r == nil {
						break // head can't admit; FIFO stop
					}
					e.cAdmitted.Add(1)
					r.AdmittedSec = now.Unix()
					if e.mode == modeReal {
						e.dispatch(r)
						continue
					}
					res := e.drawCall(p, r, now)
					e.schedule(p, r, res, now, 0)
				}
			}
		}
	}
}

// SubmitBatch submits a chunk of requests (satisfies loadgen.RunTarget).
func (e *Engine) SubmitBatch(reqs []*ledger.Request) {
	for _, r := range reqs {
		e.Submit(r)
	}
}

// SetLimit changes a model's limits live (no restart).
func (e *Engine) SetLimit(model string, rpm, tpm int64) error {
	p, ok := e.pipes[model]
	if !ok {
		return fmt.Errorf("engine: unknown model %q", model)
	}
	p.lim.SetLimits(rpm, tpm)
	p.prov.SetLimits(rpm, tpm)
	return nil
}

// MarkSteady records the steady-state window (load generator's hold phase).
// Called at hold start; SteadyStatePerSec is computed at drain time as
// (completed at drain - completed at hold start) / hold seconds. Computing at
// drain (not at hold end) correctly includes the trailing in-flight
// completions, which belong to submissions made inside the window.
func (e *Engine) MarkSteady(startNS, endNS int64) {
	e.steadyStart.Store(startNS)
	e.steadyEnd.Store(endNS)
	e.completedAtStart.Store(e.cCompleted.Sum())
}

// Drain stops accepting submissions, sweeps queues, waits for all in-flight
// work to reach a terminal state, and returns the run summary.
func (e *Engine) Drain() Summary {
	drainStart := time.Now()
	e.accepting.Store(false)
	deadline := drainStart.Add(2*time.Duration(e.cfg.MaxWaitMS)*time.Millisecond + 5*time.Second)
	if e.mode == modeReal {
		// real mode: blocking HTTP calls take real seconds; the benchmark
		// deadline (2*max_wait + 5s) does not apply. Bounded at 5 min so a
		// stuck request surfaces as an error instead of hanging the drain.
		deadline = drainStart.Add(5 * time.Minute)
	}
	for time.Now().Before(deadline) {
		if e.wheel.Pending() == 0 && e.ld.TerminalBalance() {
			break
		}
		now := time.Now()
		for _, p := range e.pipes {
			p.q.SweepExpired(now)
		}
		time.Sleep(time.Duration(e.cfg.WheelTickMS) * time.Millisecond)
	}
	for _, p := range e.pipes {
		p.q.SweepExpired(time.Now())
	}
	drainSec := time.Since(drainStart).Seconds()
	return e.buildSummary(drainSec)
}

func (e *Engine) buildSummary(drainSec float64) Summary {
	s := Summary{
		Submitted: e.ld.Submitted(),
		Completed: e.ld.Count(ledger.Completed),
		Expired:   e.ld.Count(ledger.Expired),
		Rejected:  e.ld.Count(ledger.Rejected),
		OK:        e.reg.Get("ok"),
		Failed:    e.reg.Get("failed"),
		DrainSec:  drainSec,
	}
	elapsed := float64(time.Now().UnixNano()-e.startNS) / 1e9
	if elapsed > 0 {
		s.WholeRunAvgPerSec = float64(s.Completed) / elapsed
	}
	start := e.steadyStart.Load()
	end := e.steadyEnd.Load()
	if end > start {
		s.SteadyStatePerSec = float64(e.cCompleted.Sum()-e.completedAtStart.Load()) / (float64(end-start) / 1e9)
	}
	s.Percentiles = map[string]float64{}
	for name, p := range e.pipes {
		s.Percentiles[name+"/p50"] = float64(p.hist.Percentile(0.50)) / 1e6
		s.Percentiles[name+"/p95"] = float64(p.hist.Percentile(0.95)) / 1e6
		s.Percentiles[name+"/p99"] = float64(p.hist.Percentile(0.99)) / 1e6
	}
	return s
}

// Metrics exposes the registry for the report writer.
func (e *Engine) Metrics() *metrics.Registry { return e.reg }

// E2EMaxNS returns the largest observed end-to-end latency in ns (0 if none).
func (e *Engine) E2EMaxNS() int64 { return e.e2eMaxNS.Load() }

// Provider exposes a model's simulated provider (report).
func (e *Engine) Provider(model string) *provider.Provider {
	if p, ok := e.pipes[model]; ok {
		return p.prov
	}
	return nil
}

// Limiter exposes a model's admission limiter (tests, report).
func (e *Engine) Limiter(model string) *limiter.Limiter {
	if p, ok := e.pipes[model]; ok {
		return p.lim
	}
	return nil
}

// Stop halts the wheel, workers and background goroutines. In real mode,
// work caught mid-flight by the shutdown is abandoned in memory but stays
// non-terminal in SQLite and is recovered on the next start (at-least-once).
func (e *Engine) Stop() {
	e.shutdownDo.Do(func() { close(e.shutdown) })
	e.wheel.Stop()
	e.workersWg.Wait()
}

func (e *Engine) requests() []*ledger.Request {
	if !e.DebugTrackRequests {
		return nil
	}
	e.requestsMu.Lock()
	defer e.requestsMu.Unlock()
	out := make([]*ledger.Request, len(e.allReqs))
	copy(out, e.allReqs)
	return out
}


