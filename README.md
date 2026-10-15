# Consuma — high-throughput inference under provider limits

A simulated inference-processing system: a paced load generator feeds requests
through per-model RPM/TPM admission control, a bounded wait queue, and a
provider simulator whose calls complete on a sharded timer wheel. A request
counts as **completed only when its simulated provider call reaches a final
success/failure state** — API acceptance alone is not processing.

Measured on a 16-core laptop (AC power, Go 1.27):

- **300,000 completed requests/second** (required benchmark, 60s run — exact)
- **1,000,000 completed requests/second** (stretch benchmark, 30s run — exact)
- exact terminal accounting in both runs (submitted = completed + expired +
  rejected, nothing unaccounted), p50 e2e latency 52.6ms against a configured
  50ms median.

Full numbers, method, and the fix log: `docs/benchmark/2026-10-13-run-results.md`.

## Scope of the benchmark numbers — read this first

The benchmark runs **in one process**: load generator, admission limiter, and
provider simulator connected by in-memory queues and a timer wheel. The
bottleneck at these rates is the HTTP stack, not the simulation logic — a
per-request hop over localhost HTTP caps throughput at roughly **100–200K
requests/second** (tuned round-trip ~5–10µs CPU per side plus queuing), below
the required 300K/sec bar regardless of language. That HTTP ceiling is a
projection, not a measurement; the in-process numbers above are measurements.
The HTTP API surface (single + batch requests, callbacks, live limit changes)
is the next phase and is out of scope for the benchmark claims. A **real-mode
system** (HTTP API, standalone simulator over the network, SQLite durability,
crash recovery) is available under `serve` / `simulator` — see "Real mode"
below; its rates are bounded by the HTTP stack, not the simulation logic.

## Layout

```
cmd/consuma/        CLI: benchmark + limits subcommands (the only wiring point)
internal/config/    YAML config: models, limits, latency, outcomes, wheel sizing
internal/ledger/    request lifecycle, exactly-once terminal accounting
internal/limiter/   sliding-window RPM + TPM reservation/reconciliation
internal/queue/     bounded wait queue with expiry (overload policy)
internal/wheel/     sharded timer wheel (batched expiry, O(1) scheduling)
internal/provider/  provider simulator: latency/outcome/token draws + backstop
internal/engine/    connects the packages above; retries with exp. backoff
internal/loadgen/   paced load generation (ramp/hold, model mix, batching)
internal/metrics/   lock-free counters + interpolated latency histograms
internal/report/    measured-vs-projections report (JSON + text)
internal/api/       system HTTP surface (requests, batches, limits, metrics)
internal/batch/     batch manager + callback dispatcher (persisted delivery)
internal/store/     SQLite durability (WAL): batches, requests, attempts
internal/httpprovider/ real-mode provider client (idempotency keys)
internal/simserver/ standalone simulator service (rolling limits, seen-keys)
internal/scenarios/ validation runners 1-3 (sampling, assertions, evidence)
```

## Run

```bash
go build -o bin/consuma.exe ./cmd/consuma

# required benchmark: 300K completed requests/second, 60s
bin/consuma.exe benchmark -config configs/benchmark-300k.yaml -rate 300000 -duration 60s -batch 50 -report docs/benchmark/run-300k.json

# stretch benchmark: 1M completed requests/second, 30s
bin/consuma.exe benchmark -config configs/benchmark-1m.yaml -rate 1000000 -duration 30s -batch 50 -report docs/benchmark/run-1m.json

# quick smoke run
bin/consuma.exe benchmark -config configs/benchmark.yaml -rate 10000 -duration 5s
```

Run on AC power. `CONSUMA_GOGC=400` optionally raises the GC target (no
measurable effect at these rates; reported runs used defaults).

### Flags

| Flag | Meaning |
|---|---|
| `-config` | YAML config path (models, limits, wheel sizing) |
| `-rate` | target submitted requests/second |
| `-duration` | run duration (e.g. `60s`) |
| `-models` | model mix `name:share,...` (default: first model only) |
| `-batch` | requests per submission batch |
| `-tokens` | estimated tokens per request |
| `-report` | write the JSON report to this path |
| `-target` | real mode: drive a running system over HTTP instead of the in-process engine |

## Real mode

`serve` runs the system as a service; `simulator` runs the provider simulator
as a separate process (its rolling limits and outcome mix are its own config —
a scenario parameter, sized with headroom over the system's limits so the
simulator never masks what the system enforces):

```bash
bin/consuma.exe simulator -config configs/scenario1.yaml -addr 127.0.0.1:9001
bin/consuma.exe serve -config configs/system.yaml -addr 127.0.0.1:9000 -simulator http://127.0.0.1:9001 -db consuma.db
```

### HTTP API

| Endpoint | Meaning |
|---|---|
| `POST /requests` | one request, synchronous — returns `outcome` and **`tokens_charged` = the provider's actual charge** (the admission reservation used the estimate; the difference is reconciled back at the terminal transition) |
| `POST /batches` | async batch — `202 {batch_id}` immediately (bulk-persisted, well under 1s for 10K); process asynchronously |
| `GET /batches/{id}` | status + per-outcome breakdown (`processing` → `completed` once every request is terminal) |
| `GET /batches/{id}/results` | every request id exactly once, with state, outcome, and actual tokens |
| `GET /limits/{model}` · `POST /limits/{model}` | read / change a model's RPM+TPM live — no restart; enforcement applies to new admissions immediately |
| `GET /metrics/{model}` | observed vs configured rolling windows (what the scenario runners sample) |
| `GET /health` | liveness |

### Bruno collection

A ready-to-run [Bruno](https://www.usebruno.com) collection lives at
[`bruno/`](bruno/) — open that folder in Bruno (Open Collection), pick the
`Local` environment, and every endpoint above is a one-click request. The
`submit batch` request stores the returned `batch_id` in the environment
automatically, so `batch status` and `batch results` work right after it.
Request bodies match the API field-for-field (`model`, `estimated_tokens`,
`count`, `callback_url`, `rpm`, `tpm`).

Request ids are **server-generated UUIDs** (`ExternalID`); the id is the
idempotency key sent to the provider.

### Durability and crash recovery

Every accepted request and every batch row is persisted in SQLite (WAL) at
acceptance; terminal outcomes are persisted as they happen. If `serve` dies:

- restart against the same `-db` path — the startup recovery sweep re-enqueues
  every non-terminal request (submitted/queued/in_flight); recovered requests
  are **exempt from max-wait expiry** (recovery resumes work; it does not
  expire it) and batches that were mid-flight continue to `completed`
- **nothing is lost and nothing is double-counted**: the request UUID rides
  to the provider as `Idempotency-Key`; the simulator memoizes successful
  executions in its own seen-keys table and replays the original result for a
  re-issued call, so a crash between "provider executed" and "system recorded"
  cannot double-charge
- the client never sees the crash — the batch stays `processing` and
  completes; expired (max-wait overflow during downtime) remains a terminal
  outcome and is accounted like any other

Measured in `docs/validation/2026-10-15-scenarios.md`: 60K-request batch,
`serve` killed ~8s in (34,395 accepted-but-unadmitted + 412 in-flight),
restarted — "recovered 35205 in-flight requests", batch resumed and completed
with exact accounting (40,126 ok / 856 failed / 19,018 expired / 0 rejected /
60,000 total).

### Callbacks

A batch may carry a `callback_url`. When every request is terminal, the
system POSTs the outcome breakdown to it: 3 attempts at 500ms/1s/2s, then
every 5s until a 5-minute TTL (all configurable under `callbacks:` in
`configs/system.yaml`). Each attempt is persisted; delivery that never
succeeds is resumed on the next restart (`UndeliveredBatches`). The delivered
summary equals the API's batch status.

### Validation scenarios

```bash
bin/consuma.exe scenario1 -out docs/validation -duration 300   # sustained load at the limit: utilization, zero over-limit windows, full accounting
bin/consuma.exe scenario2 -out docs/validation                  # live 30K→5K→30K RPM change, second model untouched, configured-vs-observed time series
bin/consuma.exe scenario3 -out docs/validation                  # durable 10K batches, reject-first callback sink, delivery recovery, id uniqueness
```

All three PASS; evidence: `docs/validation/2026-10-15-scenarios.md`.

## Configure models / change limits

Edit the YAML (`configs/*.yaml`): each model has `rpm`, `tpm`,
`estimate_tokens`, `actual_token_mean`, a `latency` distribution
(`fixed` / `uniform` / `lognormal`), and an `outcome` mix
(`success_pct` / `transient_pct` / `permanent_pct`, summing to 100).
Global knobs: `max_wait_ms` (wait-queue expiry), `queue_depth`,
`wheel_shards`, `wheel_buckets`, `wheel_tick_ms`, `max_retries`.

Change limits **while a benchmark is running** from another terminal:

```bash
bin/consuma.exe limits -config configs/benchmark-300k.yaml -model model-a -rpm 5000 -tpm 1000000000
```

The running benchmark watches the config file (500ms mtime poll) and applies
changes live — no restart. Note: `limits` rewrites the file via yaml marshal,
which drops comments.

## What the report contains

Console summary + optional JSON (`-report`): counts (submitted/completed/
successful/failed/expired/rejected), steady-state and whole-run rates,
per-model p50/p95/p99 end-to-end latency, configured-vs-observed RPM/TPM
(system-side and provider-side), and a **PROJECTIONS section kept strictly
separate from MEASURED numbers** (scope caveat + simulation assumptions).
Details: `docs/benchmark/2026-10-13-run-results.md`.

## Design decisions and tradeoffs

- **Timer wheel over goroutine-per-timer.** At 1M/sec with 50ms latency,
  ~50K requests are in flight; goroutine-per-timer costs 1M spawns/wakeups
  per second. The wheel batches completions: O(1) schedule, one batched
  expiry pass per shard per tick. Tradeoff: tick-granularity latency noise
  (1ms tick → ±1ms), bounded by config validation (latency must fit the
  wheel horizon).
- **In-process benchmark pipeline.** See the scope caveat above.
- **Two enforcement layers.** The system's admission limiter is primary; the
  provider simulator also enforces its own limits (429-equivalent backstop),
  so "observed RPM/TPM" is provider-side measured. Both enforce **true
  rolling 60-second windows** (not per-second pace), maintained
  incrementally over a 60-slot second-ring.
- **TPM as reservation + reconciliation.** Estimated tokens are reserved at
  admission (limits can never be exceeded); the provider's actual charge is
  accumulated per request across attempts and reconciled exactly once at the
  terminal transition, freeing unused reservation. Retries charge the
  rolling RPM window like fresh calls.
- **Overload = bounded wait queue with expiry.** Queued requests wait up to
  `max_wait_ms`, then end `expired`; reject-on-arrival only when the queue
  is full. Every request ends in exactly one terminal state.
- **Retries.** Transient failures retry with exponential backoff until
  3 attempts total (initial + up to 2 retries; 50ms then 100ms), each retry
  counting against RPM/TPM like a fresh call; per-attempt and per-request results are both visible in counters.

## Scaling note

How the design extends toward 10 billion requests/minute:
`docs/planning-toward-10b.md` (projection).
