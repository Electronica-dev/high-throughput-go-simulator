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
is the next phase and is out of scope for the benchmark claims.

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
```

## Run

```bash
go build -o bin/consuma.exe ./cmd/consuma

# required benchmark: 300K completed requests/second, 60s
bin/consuma.exe benchmark -config configs/benchmark-300k.yaml \
    -rate 300000 -duration 60s -batch 50 -report docs/benchmark/run-300k.json

# stretch benchmark: 1M completed requests/second, 30s
bin/consuma.exe benchmark -config configs/benchmark-1m.yaml \
    -rate 1000000 -duration 30s -batch 50 -report docs/benchmark/run-1m.json

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
- **Retries.** Transient failures retry with exponential backoff
  (50/100/200ms, max 3), each retry counting against RPM/TPM like a fresh
  call; per-attempt and per-request results are both visible in counters.

## Scaling note

How the design extends toward 10 billion requests/minute:
`docs/planning-toward-10b.md` (projection).
