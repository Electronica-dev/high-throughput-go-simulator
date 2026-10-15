# Benchmark Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the in-process inference simulation pipeline (load generator → limiter → wait queue → timer wheel → provider simulator → ledger) that completes ≥300K simulated requests/sec steady-state with headroom toward 1M/sec, and reports honest metrics.

**Architecture:** Single Go module, one process for benchmark mode. The hot path is: load generator paces submissions; a per-model sliding-window limiter (RPM) + token reservation (TPM) admits or queues; a bounded wait queue holds overflow with expiry; a sharded timer wheel schedules completion events at the drawn latency; a provider simulator draws latency/outcome/actual-tokens and enforces its own limits as a backstop; a ledger guarantees exactly-once terminal accounting; sharded atomic metrics feed a console+JSON report. HTTP/batch/callbacks are out of scope for this phase (design doc Non-goals) but the engine must not preclude them.

**Tech Stack:** Go 1.27, stdlib only except `gopkg.in/yaml.v3` for config. `testing` + `testing/quick` for tests; no test frameworks.

**Spec:** `docs/superpowers/specs/2026-10-13-inference-system-design.md`

## Global Constraints

- Go 1.27; module name `htsim`.
- Only external dependency allowed: `gopkg.in/yaml.v3`.
- Target machine: ASUS Zenbook, 16 cores, Windows 11; benchmarks run on AC power.
- Terminal states are exactly: `completed`, `expired`, `rejected`. `waiting` is never terminal. Every submitted request ends in exactly one terminal state; runs drain fully before reporting.
- Headline benchmark number = steady-state completed/sec (ramp-up and drain excluded); whole-run average reported alongside, labeled separately.
- Measurements and projections are reported in visibly separate sections, never mixed.
- Overload policy: bounded wait queue with configurable max wait → `expired`; reject immediately only when the queue is full.
- Two enforcement layers: system admission limiter primary; provider simulator enforces its own limits (429-equivalent rejection) as backstop.
- Retries of transient failures use exponential backoff (50ms base, doubling: 50/100/200ms) and count against RPM/TPM like fresh calls; max 3 attempts by default (`MaxRetries` in config).
- Code layout: one file per responsibility inside its package (no god files); `cmd/htsim/main.go` is the only place packages are wired together; no package imports upward into `cmd`.
- TPM: reserve estimated tokens at admission; reconcile against actuals at completion (refund unused); failed calls consume tokens.
- All randomness through an injectable `*rand.Rand` so tests are deterministic.

## Review Focus

- A request whose estimated tokens exceed the model's entire TPM budget must be rejected immediately at admission, never deadlock the queue. Pinned in Task 3 test `TestAdmitRejectsTokensOverTPMBudget`.
- Configured latency longer than the wheel horizon must fail at config validation, not silently wrap. Pinned in Task 1 test `TestConfigRejectsLatencyOverHorizon`.
- A model-mix entry naming a model absent from config must fail fast at startup with the model name in the error. Pinned in Task 9 test `TestLoadGenFailsFastOnUnknownModel`.
- 100% permanent-failure outcome mix must still balance accounting (submitted = completed + rejected + expired). Pinned in Task 8 test `TestEngineAllPermanentFailuresBalance`.
- CLI args `rate=0` or `duration=0` must produce a clean usage error, not a zero-run report. Pinned in Task 10 test `TestCLIRunsValidateArgs`.

---

### Task 1: Module scaffold and config

**Files:**
- Create: `go.mod`, `internal/config/config.go`
- Test: `internal/config/config_test.go`
- Create: `configs/benchmark.yaml` (sample config used by later tasks and Task 11)

**Interfaces:**
- Produces:
  - `type LatencyConfig struct { Kind string; MeanMS, MinMS, MaxMS, Sigma float64 }` with `Draw(rng *rand.Rand) time.Duration` (kinds: `fixed`, `uniform`, `lognormal`)
  - `type OutcomeConfig struct { SuccessPct, TransientPct, PermanentPct float64 }` (must sum to 100 ±0.01)
  - `type ModelConfig struct { Name string; RPM, TPM int64; Latency LatencyConfig; EstimateTokens int64; ActualTokenMean float64; Outcome OutcomeConfig }`
  - `type Config struct { Models []ModelConfig; MaxWaitMS int64; QueueDepth int; WheelShards, WheelBuckets int; WheelTickMS int64 }` with `Load(path string) (*Config, error)` and `Validate(horizon time.Duration) error`
  - `Model(cfg *Config, name string) *ModelConfig`

- [ ] **Step 1: Write the failing tests**

`config_test.go` parses a fixture YAML string (embed as const) asserting: models load with correct RPM/TPM values; `OutcomeConfig` summing to 99 fails validation with message containing "outcome"; `fixed` latency draws exactly MeanMS for any rng; `uniform` draws stay within [MinMS, MaxMS] over 1000 seeded draws; a model named in config is found by `Model` and a missing one returns nil. `TestConfigRejectsLatencyOverHorizon`: a model with `fixed` latency 5000ms against a wheel horizon of 4s fails `Validate` with error containing "horizon".

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/config/`
Expected: FAIL (package not implemented)

- [ ] **Step 3: Implement `config.go`**

`Load` uses yaml.v3 into `Config` with `yaml:"..."` tags; `Validate` checks: RPM/TPM > 0, outcome pcts sum, latency kinds known, every model's max drawable latency ≤ wheel horizon (`WheelBuckets*WheelTickMS`). `Draw` switches on kind; lognormal uses `rng.NormFloat64()*Sigma + MeanMS` clamped ≥ 0, exp-transformed.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/config/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add go.mod internal/config configs
git commit -m "feat: config loading and validation for models"
```

### Task 2: Request ledger and terminal accounting

**Files:**
- Create: `internal/ledger/ledger.go`
- Test: `internal/ledger/ledger_test.go`

**Interfaces:**
- Consumes: nothing (leaf).
- Produces:
  - `type State uint8` — constants `Submitted, Queued, Admitted, InFlight, Completed, Expired, Rejected`
  - `type Request struct { ID uint64; Model string; EstimateTokens int64; BornNS int64; State atomic.Uint8; Attempts atomic.Int32 }`
  - `type Ledger struct` with `New() *Ledger`, `Submit(r *Request)`, `Transition(r *Request, to State) error` (enforces the legal-transition table, panics-free: returns error on illegal), `Count(s State) int64`, `Submitted() int64`, `TerminalBalance() bool` (submitted == completed+expired+rejected)

- [ ] **Step 1: Write the failing tests**

`ledger_test.go`: `TestSubmitIncrements`; `TestLegalPath` walks Submitted→Queued→Admitted→InFlight→Completed with no error; `TestIllegalTransition` expects error transitioning Submitted→InFlight; `TestTerminalOnce` expects error on a second transition of a request already `Completed`; `TestTerminalBalance` returns true only after balancing counts; concurrent test: 8 goroutines × 1000 requests each walking the legal path, then `TerminalBalance()` is true and no transition errors occurred (collect via channel).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/ledger/`
Expected: FAIL

- [ ] **Step 3: Implement `ledger.go`**

`Transition` uses `CompareAndSwap` on `r.State` against the current state; legality table is a map[State][]State. Counts are per-state `atomic.Int64`s incremented on successful transition; terminal states also increment their terminal counter once (guaranteed by CAS semantics).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/ledger/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/ledger
git commit -m "feat: request ledger with exactly-once terminal accounting"
```

### Task 3: Sliding-window RPM limiter with TPM reservation

**Files:**
- Create: `internal/limiter/limiter.go`
- Test: `internal/limiter/limiter_test.go`

**Interfaces:**
- Consumes: nothing (leaf).
- Produces:
  - `type Limiter struct` with `New(model *config.ModelConfig) *Limiter`, `TryAdmit(tokens int64, now time.Time) AdmissionResult`, `Reconcile(actualTokens int64)`, `SetLimits(rpm, tpm int64)`, `ObservedRPM(now time.Time) int64`, `ObservedTPM(now time.Time) int64`, `AdmittedTPM() int64`
  - `type AdmissionResult struct { OK bool; OverRate, OverTPMBudget bool }` — queue-vs-reject on `OK=false` is the engine's decision (Task 8), not the limiter's

- [ ] **Step 1: Write the failing tests**

`limiter_test.go` uses fixed `now` values. `TestAdmitUnderLimit`: model RPM=10, TPM=100000, tokens=1000 → 10 admits succeed within one second. `TestAdmitOverRate`: 11th admit in the same window returns `OK=false, OverRate=true`; `TestAdmitRejectsTokensOverTPMBudget` (Review Focus): tokens=200000 against TPM=100000 → `OK=false, OverTPMBudget=true` on the first call; `TestWindowSlides`: at now+60s the same 10 requests admit again; `TestSetLimits`: reduce RPM to 5 → 6th admit in-window fails; `TestReconcileRefunds`: admit reserves 1000 estimated, `Reconcile(800)` lowers observed TPM accordingly; `TestObservedWindows` sums slots correctly.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/limiter/`
Expected: FAIL

- [ ] **Step 3: Implement `limiter.go`**

RPM: 60-slot `atomic.Int64` ring indexed `now.Unix()%60`. TPM: 60-slot ring of reserved token sums. `TryAdmit`: check `OverTPMBudget` first (tokens > TPM → immediate rejection), then reserve on current slot if under both limits. `SetLimits` swaps via `atomic.Int64` pair. `ObservedRPM/TPM` sum the 60 slots.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/limiter/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/limiter
git commit -m "feat: sliding-window RPM limiter with TPM reservation"
```

### Task 4: Timer wheel

**Files:**
- Create: `internal/wheel/wheel.go`
- Test: `internal/wheel/wheel_test.go`

**Interfaces:**
- Consumes: nothing (leaf).
- Produces:
  - `type Wheel struct` with `New(shards, buckets int, tick time.Duration, onFire func(entries []*Entry)) *Wheel`, `Schedule(fireNS int64, e *Entry)`, `Start()`, `Stop()`, `Pending() int64`
  - `type Entry struct { Req *ledger.Request; FireNS int64 }`
  - `SetNow(func() int64) *Wheel` — injectable clock for tests; default `time.Now().UnixNano()`

- [ ] **Step 1: Write the failing tests**

`wheel_test.go`: `TestFiresWithinTolerance`: tick=1ms, schedule entry at now+10ms, poll a channel fed by `onFire` with 2s timeout — fires once, entry matches; `TestBatchDrain`: 100 entries same deadline → single `onFire` call with len 100; `TestNoEarlyFire`: entry at now+50ms has not fired after 20ms; `TestShardDistribution`: 1000 schedules across 4 shards → `Pending()` returns 1000 and no shard lock contention assertion (just correctness); `TestStopQuiesces`: after `Stop`, no further `onFire` calls (sleep 20ms, count stays).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/wheel/`
Expected: FAIL

- [ ] **Step 3: Implement `wheel.go`**

Per shard: `[]sync.Mutex` over `[][]*Entry` buckets, one ticker goroutine per shard ticking at `tick`, draining bucket `(nowNS/tickNS-1) & (buckets-1)` and calling `onFire` with the drained slice (reused buffer). `Schedule` computes shard by `fireNS` hash (e.g. shard = (fireNS/tickNS) % shards so same-deadline entries batch on one shard). Power-of-two buckets asserted at construction.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/wheel/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/wheel
git commit -m "feat: sharded timer wheel with batched expiry"
```

### Task 5: Provider simulator

**Files:**
- Create: `internal/provider/provider.go`
- Test: `internal/provider/provider_test.go`

**Interfaces:**
- Consumes: `config.ModelConfig`, `ledger.Request`.
- Produces:
  - `type Outcome uint8` — `Success, TransientFailure, PermanentFailure, ProviderRejected`
  - `type CallResult struct { Latency time.Duration; Outcome Outcome; ActualTokens int64 }`
  - `type Provider struct` with `New(model *config.ModelConfig) *Provider`, `Call(r *ledger.Request, rng *rand.Rand, now time.Time) CallResult`
  - `SetLimits(rpm, tpm int64)` — provider-side backstop, same slot-ring accounting as Limiter (reimplemented locally in provider to keep packages decoupled; ~40 lines, duplication accepted for decoupling)

- [ ] **Step 1: Write the failing tests**

`provider_test.go` uses a seeded `rand.New(rand.NewSource(42))`. `TestFixedLatencyDraw`: latency config `fixed 50ms` → every `Call` returns `Latency == 50*time.Millisecond`. `TestOutcomeDistribution`: outcome config 90/5/5, 10000 calls → each outcome within ±1% of expected share. `TestActualTokens`: `ActualTokenMean=1200` → actual tokens are 1000–1500 for lognormal-ish draw (assert range, seeded). `TestProviderBackstopRejection`: model with RPM=5, `Call` 6 times within one second → 6th returns `ProviderRejected`. `TestFailedCallsConsumeTokens`: after `Call` returning `TransientFailure`, provider-side TPM observed equals estimate+draw (assert > 0).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/provider/`
Expected: FAIL

- [ ] **Step 3: Implement `provider.go`**

`Call`: check provider-side RPM/TPM slot rings → `ProviderRejected` (zero latency) if over; else draw latency via `model.Latency.Draw(rng)`, outcome via cumulative pct against `rng.Float64()*100`, actual tokens `max(1, round(NormFloat64()*0.2*mean + mean))`, record provider-side usage, return.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/provider/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/provider
git commit -m "feat: provider simulator with latency/outcome/token draws and backstop"
```

### Task 6: Bounded wait queue with expiry

**Files:**
- Create: `internal/queue/queue.go`
- Test: `internal/queue/queue_test.go`

**Interfaces:**
- Consumes: `ledger.Request`, `ledger.Ledger`.
- Produces:
  - `type WaitQueue struct` with `New(depth int, maxWait time.Duration, ld *ledger.Ledger) *WaitQueue`, `Push(r *ledger.Request, now time.Time) bool` (false → queue full, caller rejects), `PopBatch(now time.Time) []*ledger.Request` (FIFO, admitted requests ready to schedule), `SweepExpired(now time.Time) []*ledger.Request` (requests past maxWait; transitions them to `Expired` in the ledger), `Len() int`

- [ ] **Step 1: Write the failing tests**

`queue_test.go`: `TestFIFOOrder`: push 5, PopBatch returns them in order; `TestFullQueueRejects`: depth=3, 4th Push returns false; `TestSweepExpires`: maxWait=100ms, request pushed at t0, `SweepExpired(t0+150ms)` returns it and ledger shows Expired; `TestSweepSparesYoung`: request pushed at t0, sweep at t0+50ms returns empty; `TestPopRespectsCapacity`: after PopBatch, Push succeeds again.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/queue/`
Expected: FAIL

- [ ] **Step 3: Implement `queue.go`**

Ring buffer of `*ledger.Request` with head/tail atomics, mutex only on Push/PopBatch/SweepExpired (single-engine-writer design; contention acceptable at queue level because the queue is the cold path — the hot path is limiter→wheel). `Push` transitions ledger state to `Queued`; `SweepExpired` transitions to `Expired`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/queue/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/queue
git commit -m "feat: bounded wait queue with expiry"
```

### Task 7: Sharded metrics and histogram

**Files:**
- Create: `internal/metrics/metrics.go`
- Test: `internal/metrics/metrics_test.go`

**Interfaces:**
- Consumes: nothing (leaf).
- Produces:
  - `type Histogram struct` with `Record(ns int64)`, `Percentile(p float64) float64` (ns), `Count() int64`
  - `type Registry struct` with `New(shards int) *Registry`, `Incr(name string)`, `Add(name string, v int64)`, `Get(name string) int64`, `Snapshot() map[string]int64`
  - `func NewModelHistograms(models []string) map[string]*Histogram` (end-to-end latency per model)

- [ ] **Step 1: Write the failing tests**

`metrics_test.go`: `TestHistogramPercentiles`: record ns values 1000..10000 step 1000 → `Percentile(0.5)` within [5000, 5400], `Percentile(0.95)` within [9500, 9900]; `TestHistogramEmpty` → 0; `TestRegistryShardedIncr`: 8 goroutines × 10000 `Incr("done")` → `Get` == 80000; `TestSnapshot` returns a copy (mutating it doesn't affect Get).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/metrics/`
Expected: FAIL

- [ ] **Step 3: Implement `metrics.go`**

Histogram: bucket bounds 1.35× growth from 1µs to 2s (same scheme as spike), `Percentile` walks counts. Registry: per-name slice of shard-local `atomic.Int64`, index by goroutine-stable shard via `runtime_procPin`-free round-robin from caller (pass shard = counter of call site is overkill; use `atomic counter % shards` — good enough, contention already sharded).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/metrics/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/metrics
git commit -m "feat: sharded counters and bucketed latency histogram"
```

### Task 8: Engine pipeline

**Files:**
- Create: `internal/engine/engine.go`
- Test: `internal/engine/engine_test.go`

**Interfaces:**
- Consumes: `config.Config`, `ledger.Ledger`, `limiter.Limiter`, `queue.WaitQueue`, `wheel.Wheel`, `provider.Provider`, `metrics.Registry/Histogram`.
- Produces:
  - `type Engine struct` with `New(cfg *config.Config) *Engine`, `Submit(r *ledger.Request)`, `SetLimit(model string, rpm, tpm int64) error`, `Drain() Summary`, `Stop()`
  - `type Summary struct { Submitted, Completed, OK, Failed, Expired, Rejected int64; WholeRunAvgPerSec float64; SteadyStatePerSec float64; DrainSec float64; Percentiles map[string]float64 }` (populated by Task 9's collector; engine fills counts and drain time)

Engine flow per Submit: ledger Submit → model limiter `TryAdmit` → if `OK`: schedule provider call — wheel entry at `now + drawn latency`, and the wheel's `onFire` handler runs `provider.Call` *result application* (outcome → terminal transition; reconcile tokens; retry transient by re-scheduling with backoff up to 3 attempts). If `OverRate`: wait queue `Push` (queue drain: each tick the engine tries `PopBatch` for models with freed capacity). If queue full or `OverTPMBudget`: ledger transition to `Rejected`.

Retry semantics: a transient failure re-enters as a fresh provider call (counts against limiter/provider rings per Global Constraints), with **exponential backoff: 50ms base, doubling per attempt (50, 100, 200ms), max 3 attempts** (configurable via `MaxRetries` on Config, default 3), then terminal `Failed` (counts as Completed+Failed, not Expired).

- [ ] **Step 1: Write the failing tests**

`engine_test.go` builds a Config in code: one model "model-a" RPM=1000000 TPM=10^12, fixed latency 1ms, outcomes 90/5/5, QueueDepth=10, MaxWaitMS=50. `TestEngineBalancesAccounts`: submit 1000 requests, `Drain()` → Submitted == Completed+Expired+Rejected; every request terminal. `TestEngineAllPermanentFailuresBalance` (Review Focus): outcomes 0/0/100, submit 100 → OK==0, Failed==100, balance holds. `TestEngineQueueExpiry`: RPM=100 (tight), MaxWaitMS=1ms, submit 1000 → some Expired > 0, balance holds. `TestEngineRetryTransient`: outcomes 100 transient, retry cap 3 → after drain every request terminal with Attempts in [1,3], and limiter observed RPM reflects retries (observed ≥ submitted). `TestSetLimitTakesEffectLive`: SetLimit mid-run reduces admit rate (submit 500 before, 500 after, second-half admit count < first-half under RPM=300 total).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/engine/`
Expected: FAIL

- [ ] **Step 3: Implement `engine.go`**

Per-model struct {limiter, provider, queue, histogram}. Wheel handler: apply CallResult. Background goroutine per tick drains queues for models whose limiter now admits. `Drain`: stop accepting, sweep queues until `TerminalBalance()` or 2×MaxWait elapsed, return Summary counts. Keep per-request work out of locks; all counters atomic.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/engine/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/engine
git commit -m "feat: engine pipeline wiring limiter, queue, wheel, provider"
```

### Task 9: Load generator

**Files:**
- Create: `internal/loadgen/loadgen.go`
- Test: `internal/loadgen/loadgen_test.go`

**Interfaces:**
- Consumes: `config.Config`, `engine.Engine`.
- Produces:
  - `type LoadConfig struct { RatePerSec int64; Duration time.Duration; ModelMix map[string]float64; BatchSize int; EstimateTokens int64; RampUp time.Duration; RNGSeed int64 }`
  - `func Run(ctx context.Context, lc *LoadConfig, cfg *config.Config, eng *engine.Engine) error` — paces submissions (per-ms budget like the spike), honors ModelMix weights, submits in BatchSize chunks; phases: ramp-up (RampUp, linear rate increase) → hold (full rate until Duration) → stop submitting; returns when submission is complete (drain is Engine.Drain's job)

- [ ] **Step 1: Write the failing tests**

`loadgen_test.go`: `TestPacingHitsTarget`: RatePerSec=50000, Duration=1s, RampUp=0 against a counting fake engine (interface extracted: `type Submitter interface { Submit(*ledger.Request) }` — Engine implements it; test uses fake) → submitted within [45000, 55000]. `TestRampUpExcludes`: RampUp=500ms, Duration=1s → first 100ms submits ≪ rate/10 (assert < 30% of target). `TestModelMixRespected`: mix {"a":0.8,"b":0.2}, 10000 submissions → model-a share in [0.75, 0.85]. `TestBatchSizeChunks`: BatchSize=10 → submissions arrive in groups of 10 (fake records chunk boundaries). `TestLoadGenFailsFastOnUnknownModel` (Review Focus): mix referencing "nope" → error contains "nope".

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/loadgen/`
Expected: FAIL

- [ ] **Step 3: Implement `loadgen.go`**

Ticker-driven per-ms budget loop (spike pattern). Steady-state boundary: records `holdStartNS`/`holdEndNS` into the engine's summary via a `MarkSteady(startNS, endNS int64)` method on Engine (added in this task to `engine.Engine`: `func (e *Engine) MarkSteady(startNS, endNS int64)`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/loadgen/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/loadgen
git commit -m "feat: paced load generator with ramp/hold phases and model mix"
```

### Task 10: CLI — three subcommands

**Files:**
- Create: `cmd/htsim/main.go`, `cmd/htsim/benchmark.go`, `cmd/htsim/limits.go`
- Test: `cmd/htsim/main_test.go`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `htsim benchmark -config configs/benchmark.yaml -rate 300000 -duration 60s -models model-a:1.0 -batch 1 -tokens 1000` — in-process run: loadgen → Engine.Drain → report to stdout + `-report out.json`
  - `htsim limits -config configs/benchmark.yaml -model model-a -rpm 5000 -tpm 100000000` — updates the config file live (atomic write) so a running benchmark's file-watcher picks it up; this is the Task 8 `SetLimit` surface for the CLI phase
  - shared flag validation helper `validateRunArgs(rate int64, duration time.Duration) error`

`benchmark.go` wires: config load → Engine → LoadGen.Run → Engine.Drain → Report (Task 11's collector writes; this task writes a stub `printSummary`). Config file-watching: a goroutine with 500ms polling mtime; on change, reload and call `Engine.SetLimit` for each model.

- [ ] **Step 1: Write the failing tests**

`main_test.go`: `TestCLIRunsValidateArgs` (Review Focus): rate=0 → error "rate must be > 0"; duration=0 → error "duration must be > 0". `TestConfigFileWatchAppliesLimits`: temp config file, start watcher against a real Engine, rewrite the file with reduced RPM, poll until `Engine` model limiter reports the new limit (≤2s). `TestBenchmarkEndToEnd`: rate=1000, duration=200ms, single model → process-run via `runBenchmark(args)` (extracted, not os.Exit) returns a Summary with Submitted>0 and terminal balance.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/htsim/`
Expected: FAIL

- [ ] **Step 3: Implement `main.go`, `benchmark.go`, `limits.go`**

Stdlib `flag` per subcommand; `main` dispatches on `os.Args[1]`. `limits.go` rewrites YAML via yaml.v3 marshal of the reloaded config (preserving unknown fields is not required this phase; note it in the flag help).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./cmd/htsim/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cmd/htsim
git commit -m "feat: CLI subcommands benchmark and live limits"
```

### Task 11: Report writer

**Files:**
- Create: `internal/report/report.go`
- Modify: `cmd/htsim/benchmark.go` (replace stub `printSummary` with `report.Write`)
- Test: `internal/report/report_test.go`

**Interfaces:**
- Consumes: `engine.Summary`, `metrics.Registry`, per-model histograms, limiter `ObservedRPM/TPM`, config.
- Produces:
  - `type Report struct` (JSON-tagged): `Run{Rate, Duration, RampUp}`, `Counts{Submitted, Completed, OK, Failed, Expired, Rejected}`, `Rates{SteadyStatePerSec, WholeRunAvgPerSec, DrainSec}`, `Latency{P50MS, P95MS, P99MS}` per model, `Limits{Model, ConfiguredRPM, ObservedRPM, ConfiguredTPM, ObservedTPM, AdmittedTPM}` per model, `Assumptions []string`, `Projections []string` (separate section)
  - `func WriteJSON(w io.Writer, r *Report) error`, `func WriteText(w io.Writer, r *Report) error`

- [ ] **Step 1: Write the failing tests**

`report_test.go`: `TestJSONStructure`: marshal a Report, unmarshal into a map, assert keys `measured` and `projections` exist at top level and `projections` is a list of strings (never numbers); `TestTextSeparatesSections`: WriteText output contains "MEASURED" and "PROJECTIONS" as distinct sections; `TestPercentilesPresent`: Report built from a Summary with counts has P50/P95/P99 keys per model.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/report/`
Expected: FAIL

- [ ] **Step 3: Implement `report.go`**

JSON shape: `{"measured": {...}, "projections": ["..."]}` — Assumptions go under `projections` with a leading "assumption:" prefix; the README caveat (HTTP ceiling ~100–200K/sec) is always included as the first projection string. WriteText renders the same data with section headers.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/report/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/report cmd/htsim/benchmark.go
git commit -m "feat: measured-vs-projections report writer"
```

### Task 12: Benchmark validation runs

**Files:**
- Create: `configs/benchmark-300k.yaml`, `configs/benchmark-1m.yaml` (models with high limits — e.g. RPM 500M, TPM 10T, lognormal latency p50 50ms, outcomes 93/5/2)
- Create: `docs/benchmark/2026-10-13-run-results.md`
- Modify: `README.md` (create: run instructions + the quantified in-process caveat section per spec)

**Interfaces:**
- Consumes: `htsim benchmark` CLI.
- Produces: recorded evidence files (`-report` JSON outputs committed under `docs/benchmark/`).

- [ ] **Step 1: Write the two configs**

benchmark-300k.yaml: single model, rate 300K, duration 60s. benchmark-1m.yaml: rate 1M, duration 30s.

- [ ] **Step 2: Run the required benchmark**

Run: `go build -o bin/htsim.exe ./cmd/htsim && ./bin/htsim benchmark -config configs/benchmark-300k.yaml -rate 300000 -duration 60s -report docs/benchmark/run-300k.json`
Expected: steady-state ≥ 300,000/sec; terminal balance; p99 < 100ms. If steady-state misses, profile (`go test -bench` on engine, pprof CPU profile) and fix the top contention point before re-running.

- [ ] **Step 3: Run the stretch benchmark**

Run: `./bin/htsim benchmark -config configs/benchmark-1m.yaml -rate 1000000 -duration 30s -report docs/benchmark/run-1m.json`
Expected: stretch within reach (≥900K/sec steady-state acceptable for phase 1; record the number).

- [ ] **Step 4: Record results**

`docs/benchmark/2026-10-13-run-results.md`: machine, power state, Go version, both JSON reports' key numbers (steady-state, whole-run avg, percentiles, terminal counts), explicitly labeled MEASURED; bottlenecks observed.

- [ ] **Step 5: Write README, 10B/min planning note, and commit**

README.md: what this is, how to run each command, config reference, the in-process-vs-HTTP caveat with both the measured numbers and the ~100–200K/sec HTTP projection labeled as a projection (spec requirement), pointer to benchmark results.

`docs/planning-toward-10b.md`: short design note (spec's builder item) on extending toward 10 billion requests/minute — sharding across processes/machines, zero-copy handoff between shards, the HTTP boundary remaining the low-rate control plane, and what the single-process wheel/limiter design inherits unchanged.

```bash
git add configs docs README.md
git commit -m "feat: benchmark validation runs, results, README with scope caveat"
```

---

## Execution Notes

- Tasks 1–7 are leaf packages with no cross-dependencies except config → all others; Tasks 2,3,4,5,6,7 can be built in any order after Task 1.
- Task 8 depends on 1–7; Task 9 on 8; Task 10 on 9; Task 11 on 10; Task 12 last.
- The spike code in `C:\Users\sammy\AppData\Local\hermes\cache\scratch\htsim-spike` is reference-only; do not copy it wholesale (per-wheel design differs: sharded, injectable clock, ledger integration).
