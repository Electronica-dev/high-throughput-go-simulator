# Consuma Real-Mode System Implementation Plan (Phase 2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the phase-1 simulation core into the real-mode system: HTTP API (single + async batch requests with callbacks), SQLite durability with crash recovery, a standalone simulator service, and the three validation scenarios as repeatable evidence commands.

**Architecture:** One binary, new subcommands. `consuma serve` (system: HTTP API wrapping the phase-1 engine in real mode + SQLite + callback dispatcher), `consuma simulator` (standalone provider service), `consuma scenario1|2|3` (runners). The engine gains a `Caller` abstraction: benchmark mode keeps the in-process wheel-scheduled simulator unchanged; real mode uses an HTTP provider whose blocking call IS the provider latency. SQLite (`modernc.org/sqlite`, WAL) persists batches/requests/callback attempts; a startup recovery sweep re-enqueues `queued`/`in_flight` requests; provider calls carry idempotency keys and the simulator deduplicates via a seen-keys table.

**Tech Stack:** Go 1.27; deps: `gopkg.in/yaml.v3`, `modernc.org/sqlite` (NEW), stdlib `net/http` + `database/sql`. No web framework.

**Spec:** `docs/superpowers/specs/2026-10-14-consuma-real-mode-design.md`

## Global Constraints

- Phase-1 behavior locked: the in-process benchmark path (draw → wheel → finalize) must be byte-for-byte semantically unchanged; all phase-1 tests stay green; benchmark throughput not regressed (spot-check 300K/sec after the refactor).
- SQLite is real-mode only: no SQLite call on the benchmark hot path.
- Batch ack: `POST /batches` returns 202 + ID in < 1s (admission is async; rejections land in the batch, never a 4xx at submit).
- Callback fires only after every request in the batch is terminal; payload = `{batch_id, status, counts, results_url}`; delivery schedule configurable: 3 attempts at 500ms/1s/2s backoff, then periodic retries every 5s until a 5min TTL (all knobs in `configs/system.yaml`).
- Crash recovery: on startup, re-enqueue every `queued`/`in_flight` request; provider calls carry idempotency keys; the simulator deduplicates via a seen-keys table (at-least-once, dedup — nothing lost, client never sees the crash).
- Batch status: `pending | processing | completed | failed`; `completed` requires only that every request is accounted (counts carry the mix); `failed` = invalid input or unrecoverable record loss only.
- Server-generated UUID request IDs; results document lists every ID exactly once.
- Simulator limits are a scenario parameter (headroom, default 10× system limits); `POST /limits/{model}` changes only the system's limiter.
- Terminal requests reconcile their TPM reservation exactly once (cumulative `ActualCharged`, existing phase-1 semantics).

## Review Focus

- **Crash mid-call:** a request `in_flight` at crash is re-enqueued on restart and its provider call re-issued; the simulator's seen-keys table returns the ORIGINAL outcome for the duplicate (not a second execution). Pinned in Task 4 test `TestSeenKeyReturnsOriginalOutcome` and Task 5 test `TestRecoveryReenqueuesInFlight`.
- **Callback never early:** a batch with one fast and one slow request must not fire the callback until both are terminal. Pinned in Task 7 test `TestCallbackOnlyAfterAllTerminal`.
- **Ack under load:** submitting a 10,000-request batch while the service is processing another batch still acks in < 1s. Pinned in Task 6 test `TestBatchAckUnderLoad`.
- **Callback delivery recovers:** rejecting the first two deliveries (httptest 500s) must not lose the callback — third attempt succeeds, all attempts persisted. Pinned in Task 7 test `TestCallbackRetriesUntilSuccess`.
- **Recovery idempotency:** running the recovery sweep twice must not double-enqueue (request appears once in its batch results). Pinned in Task 5 test `TestRecoverySweepIdempotent`.

---

### Task 1: Engine `Caller` abstraction (refactor, zero behavior change)

**Files:**
- Create: `internal/engine/caller.go`
- Modify: `internal/engine/engine.go` (pipes hold `Caller`; `drawCall` delegates)
- Test: existing phase-1 suite must stay green; add `internal/engine/caller_test.go`

**Interfaces:**
- Produces:
  - `type Caller interface { Call(r *ledger.Request, now time.Time) provider.CallResult }`
  - `func InProcessCaller(p *provider.Provider, rng func() *rand.Rand) Caller` — wraps today's draw path (wheel-scheduled benchmark mode)
  - Engine constructor variant: `NewWithCaller(cfg *config.Config, c Caller) (*Engine, error)`; `New(cfg)` delegates to it with `InProcessCaller`

- [ ] **Step 1: Refactor behind the existing suite** — write `caller_test.go`: a fake Caller returning a fixed CallResult drives one request to `Completed` with the fake's outcome; the in-process caller is exercised by the existing engine tests (they must pass unmodified).
- [ ] **Step 2: Run `go test ./internal/engine/ ./internal/...`** — Expected: all phase-1 tests PASS (pure refactor).
- [ ] **Step 3: Commit** `refactor: engine Caller abstraction (in-process caller unchanged)`

### Task 2: SQLite store

**Files:**
- Create: `internal/store/store.go`, `internal/store/schema.sql`
- Test: `internal/store/store_test.go`

**Interfaces:**
- Produces:
  - `type Store struct` with `Open(path string) (*Store, error)` (WAL, foreign_keys), `Close()`
  - `CreateBatch(b Batch) error`; `GetBatch(id string) (Batch, bool, error)`; `UpdateBatchCounts(id string, c Counts) error`; `SetBatchStatus(id, status string) error`
  - `InsertRequest(r RequestRow) error`; `UpdateRequestTerminal(id string, outcome string, tokens int64) error`; `RequestsNeedingRecovery() ([]RequestRow, error)` (state IN queued/in_flight); `MarkRequestQueued(id string) error`
  - `RecordCallbackAttempt(batchID string, at time.Time, statusCode int, outcome string) error`; `CallbackAttempts(batchID string) ([]Attempt, error)`; `UndeliveredBatches() ([]string, error)`
  - `SeenKey(k string) (result string, bool, error)`; `PutSeenKey(k, result string) error`
  - Types: `Batch{ID, Status, CallbackURL, Counts, CreatedAt}`, `Counts{Total, Successful, Failed, Expired, Rejected}`, `RequestRow{ID, BatchID, Model, State, Outcome, Tokens, AdmittedSec}`, `Attempt{...}`

- [ ] **Step 1: RED** — `store_test.go` with `t.TempDir()` DBs: batch round-trip; request insert → terminal update; `RequestsNeedingRecovery` returns only queued/in_flight rows; callback attempts append and list in order; seen-key put/get; `UndeliveredBatches` lists batches with status completed but no successful attempt. Run: `go test ./internal/store/` — Expected: FAIL (package missing).
- [ ] **Step 2: GREEN** — implement schema + store; single `database/sql` handle; all writes parameterized.
- [ ] **Step 3: Run suite** — Expected: PASS. **Step 4: Commit** `feat: sqlite store (batches, requests, callback attempts, seen keys)`

### Task 3: HTTP provider client

**Files:**
- Create: `internal/httpprovider/httpprovider.go`
- Test: `internal/httpprovider/httpprovider_test.go`

**Interfaces:**
- Produces: `type HTTPProvider struct` with `New(url string, client *http.Client) *HTTPProvider` and `Call(r *ledger.Request, now time.Time) provider.CallResult` (satisfies engine.Caller). Behavior: POST `{url}/call` body `{request_id, model, estimated_tokens}`; 200 → parse `{outcome, actual_tokens, latency_ms}` into CallResult; 429 → `ProviderRejected`; timeout/5xx/network → `TransientFailure` (zero latency — the retry path owns backoff). The request ID is the idempotency key sent as both body field and `Idempotency-Key` header.

- [ ] **Step 1: RED** — httptest server cases: 200 happy path (outcome/tokens mapped); 429 → ProviderRejected; 500 then 200 → second call TransientFailure then Success; server asserting the `Idempotency-Key` header equals request ID; timeout (server sleeps past client timeout) → TransientFailure. Run → FAIL.
- [ ] **Step 2: GREEN** — implement; one shared `http.Client` with per-call timeout from config.
- [ ] **Step 3: Run suite; commit** `feat: http provider client with idempotency key`

### Task 4: Simulator service

**Files:**
- Create: `internal/simserver/simserver.go`, `cmd/consuma/simulator.go` (subcommand `simulator`)
- Modify: `cmd/consuma/main.go` (dispatch)
- Test: `internal/simserver/simserver_test.go`

**Interfaces:**
- Consumes: `config` (same model schema), rolling-window accounting (same slot-ring shape as internal/provider, duplicated locally per the phase-1 decoupling ruling), a seen-key store (reuse `internal/store` against its own SQLite file).
- Produces: `type Server struct` with `New(cfg *config.Config, store *store.Store) *Server` and `Handler() http.Handler` exposing `POST /call`: validate body → check rolling limits → 429 or charge → sleep `drawn latency` (real sleep; this IS the simulated latency) → outcome/token draws → persist seen-key AFTER execution → respond `{outcome, actual_tokens, latency_ms}`. Seen-key hit → return stored response without re-executing. Subcommand flags: `-config`, `-addr`.

- [ ] **Step 1: RED** — `simserver_test.go` with a temp-dir SQLite: happy call returns drawn outcome and honors latency (short 5ms config, measure ≥5ms); 429 after limit breach (RPM=2, three calls); **seen-key dedup**: two calls same request ID → identical outcome, provider executed once (assert via a counter injected into the drawer); limits are scenario-parameterized (headroom from config, not hardcoded).
- [ ] **Step 2: GREEN** — implement; wire `simulator` subcommand.
- [ ] **Step 3: Run suite; commit** `feat: standalone simulator service with rolling limits and idempotency`

### Task 5: Real-mode engine wiring + recovery sweep

**Files:**
- Create: `internal/engine/real.go` (real-mode pipeline), `internal/engine/recover.go`
- Modify: `internal/engine/engine.go` (mode field)
- Test: `internal/engine/real_test.go`

**Interfaces:**
- Produces:
  - `func NewReal(cfg *config.Config, c Caller, st *store.Store) (*Engine, error)` — real mode: same limiter/queue/ledger/metrics; admitted requests handed to a worker pool (size from config, default 64) that runs `c.Call` (blocking) then the shared finalize path (same terminal helper semantics: e2e once, reconcile once, retry with ChargeRetry/queue-deferral).
  - `func (e *Engine) Recover() (requeued int, err error)` — sweep `RequestsNeedingRecovery`, transition each to `Queued`, mark `queued`; idempotent (second run requeues nothing — rows are now `queued`, filtered by the same query).
- Behavior notes: real mode has NO timer-wheel entries (the blocking call IS the latency); the queue drainer + limiter + wait-queue expiry are unchanged; retries keep phase-1 semantics (ChargeRetry first, queue-deferral on failure).

- [ ] **Step 1: RED** — `real_test.go` with a fake Caller (scripted outcomes) + temp SQLite: batch of N requests processed to completion with counts persisted; **recovery**: insert rows in queued/in_flight states, run `Recover()`, assert requeued count and that a subsequent fake-Caller run completes them; **idempotent sweep**: run `Recover()` twice, second returns 0; **no wheel entries**: assert `Pending() == 0` throughout a real-mode run.
- [ ] **Step 2: GREEN** — implement worker pool + finalize (reuse applyBatch's terminal helper logic via a shared `finalizeOne` extracted from `applyBatch`; the benchmark wheel path calls the same helper per entry).
- [ ] **Step 3: Run FULL suite** (phase-1 must stay green — the refactor must not change wheel-path behavior). **Step 4: Commit** `feat: real-mode engine (worker pool, recovery sweep, idempotent)`

### Task 6: System HTTP API

**Files:**
- Create: `internal/apiserver/apiserver.go` (routes + handlers), `cmd/consuma/serve.go` (subcommand `serve`)
- Modify: `cmd/consuma/main.go`
- Test: `internal/apiserver/apiserver_test.go`

**Interfaces:**
- Consumes: engine (real mode), store, batch manager (Task 7 — handlers call narrow interfaces; tests use fakes until Task 7 lands).
- Produces (JSON shapes; error → `{"error": "..."}` + 4xx/5xx):
  - `POST /requests` `{model, estimated_tokens}` → 200 `{request_id, model, outcome, tokens_charged, latency_ms}` (synchronous; connection waits through queue + provider call)
  - `POST /batches` `{model | per-request models, count, estimated_tokens, callback_url}` → 202 `{batch_id, request_ids}` in < 1s
  - `GET /batches/{id}` → `{batch_id, status, counts{total,successful,failed,expired,rejected}, results_url, callback_state}`
  - `GET /batches/{id}/results` → `{batch_id, requests: [{request_id, outcome, tokens_charged, latency_ms}]}`
  - `POST /limits/{model}` `{rpm, tpm}` → 200 (system limiter only)
  - Graceful shutdown: SIGINT/SIGTERM → stop accepting, drain in-flight, flush SQLite, exit (spec builder item)
- Subcommand: `serve -config configs/system.yaml`.

- [ ] **Step 1: RED** — `apiserver_test.go` with fake engine/store: single request 200 shape; batch 202 < 1s (measure) with UUID ids, count matching; GET batch 404 unknown id; GET results lists every id exactly once; limits endpoint applies to the engine limiter (assert via `Limiter(model).Limits()`); malformed JSON → 400 with error body.
- [ ] **Step 2: GREEN** — implement handlers + subcommand.
- [ ] **Step 3: Run suite; commit** `feat: system HTTP API (requests, batches, status, results, limits)`

### Task 7: Batch manager + callback dispatcher

**Files:**
- Create: `internal/batch/manager.go`, `internal/batch/callback.go`
- Modify: `internal/apiserver` (wire real manager), `internal/engine` (terminal hook: engine notifies the manager via a callback interface `OnTerminal(requestID, outcome)`; real mode only)
- Test: `internal/batch/batch_test.go`

**Interfaces:**
- Produces: `type Manager struct` with `New(st *store.Store, engine *engine.Engine, cfg CallbackConfig) *Manager`; `Submit(count int, model string, tokens int64, callbackURL string) (batchID string, ids []string, err error)` (persists batch + requests, enqueues to engine, returns < 1s); `Status(id) (Batch, bool)`; `Results(id) ([]ResultRow, bool)`; `OnTerminal` updates counts in SQLite, and when total terminal == count → sets batch `completed` → dispatches callback.
- Callback dispatcher: per-batch goroutine; attempts at 500ms/1s/2s then every `PeriodicInterval` (default 5s) until `DeliveryTTL` (default 5m); each attempt persisted; success (2xx) stops delivery; delivery resumed on restart from `UndeliveredBatches`.

- [ ] **Step 1: RED** — `batch_test.go` with temp SQLite + httptest callback sink: callback fires only after ALL requests terminal (`TestCallbackOnlyAfterAllTerminal` — one request's Caller blocks until released); ack < 1s for a 10,000-request batch (`TestBatchAckUnderLoad`); `TestCallbackRetriesUntilSuccess` (sink 500s twice then 200 → delivered, 3 attempts persisted); `TestCallbackResumeAfterRestart` (new Manager over same DB resumes undelivered); results listed once per id.
- [ ] **Step 2: GREEN** — implement manager + dispatcher; wire into apiserver + serve subcommand.
- [ ] **Step 3: Run FULL suite; commit** `feat: batch manager and callback dispatcher with persisted delivery state`

### Task 8: Load generator real mode

**Files:**
- Modify: `internal/loadgen/loadgen.go` (Submitter abstraction gains an HTTP target), `cmd/consuma/benchmark.go` (`-target` flag)
- Test: `internal/loadgen/loadgen_http_test.go`

**Interfaces:**
- Produces: `type HTTPTarget struct` with `New(url string, client *http.Client) *HTTPTarget` satisfying `RunTarget` (SubmitBatch → `POST /batches` with the chunk's model mix; MarkSteady → no-op in real mode — pacing is client-side; response IDs recorded). Loadgen `-target` flag routes RunTarget selection; run report gains per-batch polling for batch completion evidence (poll interval 100ms until completed, then `GET /batches/{id}/results`).

- [ ] **Step 1: RED** — httptest fake API server: SubmitBatch posts one batch per chunk with correct count/model mix and parses 202 ids; conservation test (rate × duration submissions = batches × batch sizes).
- [ ] **Step 2: GREEN** — implement; `-target` flag on benchmark subcommand.
- [ ] **Step 3: Run suite; commit** `feat: load generator real mode (HTTP target)`

### Task 9: Scenario runners

**Files:**
- Create: `internal/scenarios/scenarios.go` (shared runner helpers), `cmd/consuma/scenario1.go`, `scenario2.go`, `scenario3.go`
- Create: `configs/scenario1.yaml`, `configs/scenario2.yaml`, `configs/scenario3.yaml` (system + simulator models, headroom parameter)
- Test: `internal/scenarios/scenarios_test.go` (against in-process fakes — full-system runs are Task 10's validation)

**Interfaces:**
- Produces: `consuma scenario1|2|3 -system URL -simulator URL -out dir` — each: drives traffic via the load generator's real mode, applies limit changes via `POST /limits/{model}` where required, samples the system's observed windows over time, polls batches, and writes `{out}/scenario-N.json` + `-summary.txt`. Assertions per the spec's pass criteria:
  - S1: ≥90% of allowed capacity completed after warm-up (measured on limiter rolling windows, warm-up excluded); no observed 60s window over either limit; every request accounted.
  - S2: A 30K→5K→30K RPM mid-run; B untouched; A adjusts without restart (no process restart between phases); report includes configured-vs-observed time series.
  - S3: 10K requests, two models, transient+permanent failure mix; callback sink rejects first two deliveries; ack < 1s; callback only after all terminal; delivery succeeds after recovery; callback summary == API batch status; every id exactly once.

- [ ] **Step 1: RED** — scenario tests with fakes assert the runner's collection/assertion logic (utilization computation from window samples; window-over-limit detector; id-uniqueness check).
- [ ] **Step 2: GREEN** — implement the three runners + configs.
- [ ] **Step 3: Run suite; commit** `feat: scenario runners 1-3 with evidence output`

### Task 10: End-to-end validation + docs

**Files:**
- Create: `docs/benchmark/2026-10-14-scenarios.md` (evidence write-up)
- Modify: `README.md` (serve/simulator/scenario instructions, API reference, durability + crash-recovery section), `docs/planning-toward-10b.md` (SQLite paragraph)

- [ ] **Step 1: Build + start services** — `bin/consuma.exe simulator -config configs/scenario1.yaml -addr 127.0.0.1:9001` and `bin/consuma.exe serve -config configs/system.yaml -addr 127.0.0.1:9000`.
- [ ] **Step 2: Run scenario 1** (≥5 min at >capacity rate) — Expected: evidence JSON with utilization ≥ 90%, zero over-limit windows, full accounting.
- [ ] **Step 3: Run scenario 2** — Expected: A adjusts down/up live, B unaffected, time-series in evidence.
- [ ] **Step 4: Run scenario 3** (callback sink with first-two-reject behavior) — Expected: ack < 1s, callback after all terminal, delivery recovered, summary matches API, ids unique.
- [ ] **Step 5: Crash-recovery demo evidence** — start a batch, kill `serve` mid-run, restart, poll to `completed`: batch completes with no lost requests; record timings in the evidence doc.
- [ ] **Step 6: Benchmark regression check** — phase-1 `benchmark` at 300K for 30s: still ≥ 300K/sec steady-state (SQLite absent from that path).
- [ ] **Step 7: Docs + commit** `feat: validation scenarios 1-3 evidence, README real-mode docs`

## Execution Notes

- Dependency order: 1 → (2, 3) → 4 → 5 → 6 → 7 → 8 → 9 → 10. Tasks 2 and 3 are independent; Task 5 needs 1+3; Task 7 needs 2+5; Task 9 needs all.
- The phase-1 suite is the regression gate for every task: `go test ./...` after each task, benchmark spot-check after Tasks 1 and 5 (the two that touch the engine).
- The spike code in the phase-1 workspace is gone (cleaned); phase-1 reference numbers live in `docs/benchmark/2026-10-13-run-results.md`.
