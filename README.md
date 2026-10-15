# Consuma — high-throughput inference under provider limits

A simulated inference-processing system: a paced load generator feeds requests
through per-model RPM/TPM admission control, a bounded wait queue, and a
provider simulator whose calls complete on a sharded timer wheel. A request
counts as **completed only when its simulated provider call reaches a final
success/failure state** — API acceptance alone is not processing.

Headline numbers, measured on a 16-core laptop (Go 1.27, AC power):

- **300,000 completed requests/second** — required benchmark (60s run, exact)
- **1,000,000 completed requests/second** — stretch benchmark (30s run, exact)
- exact terminal accounting in both runs; validation scenarios 1–3 all PASS

Full numbers: `docs/benchmark/2026-10-13-run-results.md`; scenario evidence:
`docs/validation/`.

---

## 1 · Run the service, provider simulator, and load generator

Build once:

```bash
go build -o bin/consuma.exe ./cmd/consuma
```

**The system, as a service** (HTTP API, SQLite durability, crash recovery):

```bash
bin/consuma.exe serve -config configs/system.yaml -addr 127.0.0.1:9000 \
    -simulator http://127.0.0.1:9001 -db consuma.db
```

**The provider simulator**, a separate process with its own config — its
limits and outcome mix are a scenario parameter, sized with headroom over the
system's limits so it never masks what the system enforces:

```bash
bin/consuma.exe simulator -config configs/simulator.yaml -addr 127.0.0.1:9001
```

**The load generator.** Without `-target` it drives the in-process benchmark
pipeline (the 300K/1M numbers); with `-target` it drives the running service
over HTTP:

```bash
# required benchmark, in-process
bin/consuma.exe benchmark -config configs/benchmark-300k.yaml -rate 300000 -duration 60s -batch 50 -report docs/benchmark/run-300k.json

# stretch benchmark, in-process
bin/consuma.exe benchmark -config configs/benchmark-1m.yaml -rate 1000000 -duration 30s -batch 50 -report docs/benchmark/run-1m.json

# real mode: drive the live service (model mix included)
bin/consuma.exe benchmark -target http://127.0.0.1:9000 -rate 20000 -duration 60s -batch 50 -models model-a:0.6,model-b:0.4
```

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

Run on AC power. `CONSUMA_GOGC=400` optionally raises the GC target (no
measurable effect; reported runs used defaults).

### HTTP API of `serve`

| Endpoint | Meaning |
|---|---|
| `POST /requests` | one request, synchronous — returns `outcome` and `tokens_charged` (the provider's **actual** charge; the estimate reservation is reconciled back) |
| `POST /batches` | async batch — `202 {batch_id}` immediately (persisted, well under 1s for 10K) |
| `GET /batches/{id}` | status + per-outcome breakdown (`processing` → `completed` when every request is terminal) |
| `GET /batches/{id}/results` | every request id exactly once — camelCase keys: `requestId`, `state`, `outcome`, `tokens` |
| `GET /limits/{model}` · `POST /limits/{model}` | read / change RPM+TPM live — no restart, immediate for new admissions |
| `GET /metrics/{model}` | observed vs configured rolling windows |
| `GET /health` | liveness |

A ready-to-run [Bruno](https://www.usebruno.com) collection for every endpoint
above lives at [`bruno/`](bruno/) — open the folder, pick the `Local`
environment; `submit batch` captures the `batch_id` automatically. Request ids
are **server-generated UUIDs** used as the provider idempotency key.

### Crash recovery

Every accepted request is persisted at acceptance. If `serve` dies, restart
against the same `-db`: the recovery sweep re-enqueues every non-terminal
request (exempt from max-wait expiry — a crash costs latency, never lost
work), mid-flight batches continue to `completed`, and the request UUID rides
to the provider as an idempotency key so replays can't double-charge.
Measured: 60K-request batch, `serve` killed ~8s in, restarted — "recovered
35205 in-flight requests", exact accounting (40,126 ok / 856 failed /
19,018 expired / 60,000 total). See `docs/validation/2026-10-15-scenarios.md`.

### Callbacks

A batch may carry a `callback_url`; the system POSTs the outcome breakdown to
it once every request is terminal — 3 attempts at 500ms/1s/2s, then every 5s
to a 5-minute TTL (all under `callbacks:` in `configs/system.yaml`). Each
attempt is persisted; undelivered callbacks resume on restart.

## 2 · Configure models and change limits

Models live in `configs/*.yaml` — one block per model:

```yaml
models:
  - name: model-a
    rpm: 30000                # requests/minute limit
    tpm: 60000000             # tokens/minute limit
    estimate_tokens: 2000     # admission reservation
    actual_token_mean: 2000   # what the provider actually charges (~±20%)
    latency:
      kind: lognormal         # fixed | uniform | lognormal
      mean_ms: 50
      sigma: 0.3
    outcome:                  # sums to 100
      success_pct: 93
      transient_pct: 5       # retried with exponential backoff
      permanent_pct: 2
```

Global knobs: `max_wait_ms` (wait-queue expiry), `queue_depth`, `max_retries`,
`wheel_*` (timer wheel sizing), `callbacks:` (delivery schedule + TTL).

**Change limits live, three ways:**

```bash
# over HTTP — no restart, enforcement is immediate for new admissions
curl -X POST http://127.0.0.1:9000/limits/model-a -d '{"rpm": 5000, "tpm": 60000000}'

# the `limits` CLI for the in-process benchmark (the benchmark watches the
# config file and applies changes within ~500ms)
bin/consuma.exe limits -config configs/benchmark-300k.yaml -model model-a -rpm 5000 -tpm 1000000000
```

Or edit the YAML and restart. Note: the `limits` CLI rewrites the file via
yaml marshal, which drops comments.

## 3 · Validation scenarios

```bash
bin/consuma.exe scenario1 -out docs/validation -duration 300   # sustained load at the limit
bin/consuma.exe scenario2 -out docs/validation                  # live 30K→5K→30K RPM change, both models
bin/consuma.exe scenario3 -out docs/validation                  # async batches + reject-first callback sink
```

- **Scenario 1** — 5 minutes at ~94% of the 30K RPM / 3M TPM budget: ≥90%
  utilization after warm-up (measured 96.9%), zero over-limit windows,
  every request accounted.
- **Scenario 2** — model A pinned at 30K RPM/60M TPM, model B at 20K/40M
  (both under load in every phase); A cut to 5K RPM live (no restart) and
  restored; B keeps processing (~95% of its own limit) throughout.
- **Scenario 3** — two 10K durable batches, ack < 1s, callback sent only
  after every request is terminal, sink rejects the first two deliveries and
  the retry schedule recovers, callback summary equals the API status, every
  request id exactly once.

All three PASS. Each runner prints its steps as they happen (batch acks with
ids, limit changes, delivery attempts, verification verdicts); S1/S2 also
render **SVG charts** — observed throughput against the configured limit over
time — into the `-out` dir. Evidence JSONs + summaries + charts live in
`docs/validation/`; the write-up is `docs/validation/2026-10-15-scenarios.md`.

## 4 · Design decisions and tradeoffs

- **Timer wheel over goroutine-per-timer.** At 1M/sec with 50ms latency,
  ~50K requests are in flight; goroutine-per-timer would spawn/wake 1M/sec.
  The wheel batches completions: O(1) schedule, one batched expiry pass per
  shard per tick. Tradeoff: ±1ms tick-granularity latency noise, bounded by
  config validation.
- **True rolling 60-second windows, enforced incrementally.** A per-second
  check would admit 60× the RPM budget; both the system's limiter and the
  simulator's enforce real 60s totals over a 60-slot second-ring.
- **TPM as reservation + reconciliation.** Estimated tokens are reserved at
  admission (limits can never be exceeded); the provider's actual charge
  reconciles the reservation exactly once at the terminal transition.
  Retries charge RPM like fresh calls but never re-charge TPM.
- **Overload = bounded wait queue with expiry.** Queued requests wait up to
  `max_wait_ms`, then end `expired`; reject on arrival only when the queue
  is full. Every request ends in exactly one terminal state.
- **Retries.** Transient failures retry with exponential backoff (50ms
  doubling, max 3 attempts total), each attempt counting against RPM/TPM.
- **Two enforcement layers.** The system's limiter is primary; the simulator
  enforces its own limits as a 429-equivalent backstop, so provider-side
  "observed" numbers are measured, not asserted.

More: `docs/decisions.md` (every judgment call, with cost-if-wrong),
`docs/gotchas.md` (things that bit us, in plain language),
`docs/planning-toward-10b.md` (how the design extends toward 10B requests/
minute — projection, kept separate from measurements).

---

## What the benchmark numbers measure

The 300K/1M headline runs **in one process**: load generator, admission
limiter, and simulator connected by in-memory queues and the timer wheel —
that is what "simulation benchmark" measures (the PDF's required/stretch
bars), and it is exact conservation: submitted = completed + expired +
rejected. A per-request hop over HTTP would cap any implementation at
roughly 100–200K/sec on this hardware (a projection, not a measurement);
the real-mode path exists to demonstrate the API surface, durability, and
scenarios — its rates are bounded by HTTP, not the simulation logic.

## Repo map

```
cmd/consuma/        CLI: serve | simulator | benchmark | limits | scenario1-3
internal/config/    YAML config (models, limits, latency, outcomes, wheel)
internal/ledger/    request lifecycle, exactly-once terminal accounting
internal/limiter/   sliding-window RPM + TPM reservation/reconciliation
internal/queue/     bounded wait queue with expiry (overload policy)
internal/wheel/     sharded timer wheel (batched expiry, O(1) scheduling)
internal/provider/  in-process simulator: latency/outcome/token draws
internal/engine/    connects the packages; retries, real-mode workers
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
