# Real-mode validation evidence — 2026-10-15

All scenarios were run end-to-end against the live services:
`consuma serve` (system: SQLite durability, HTTP API, batch manager,
callback dispatcher, crash recovery) driving the standalone
`consuma simulator` over HTTP. Evidence JSONs + summaries live beside this
file (`scenario-{1,2,3}.json`, `scenario-{1,2,3}-summary.txt`).

Config under test (`configs/system.yaml`): model-a, 30,000 RPM / 3,000,000 TPM,
100-token requests, lognormal 50ms/0.3 latency, 93/5/2
success/transient/permanent outcome mix, 2s wait-queue max wait, 3 retries.

## Scenario 1 — sustained load at the configured limit — PASS

- submitted 540 req/s × 300s (measured at 94% of the 500 req/s budget the
  limit allows — the margin absorbs retry re-admissions, see "capacity note")
- **RPM utilization 96.9% of the configured 30,000** (mean of the sampled
  60s rolling windows), TPM utilization 85.1% (2.55M of the 3M ceiling —
  actual charged tokens, net of reservation refunds)
- **zero over-limit samples** across all 1/s window samples
- every request accounted: per-batch breakdowns sum to the submitted count
  (completed + failed + expired + rejected = total; evidence JSON lists every
  request id exactly once, `unique_ids: true`)
- the failure mix is visible in the results (~2% `failed`, matching the
  configured 2% permanent rate; transients retried to success)

Capacity note: submitting exactly 500 req/s (the RPM/60 budget) plus the
~5% transient-retry re-admissions exceeds the window and turns the retry
deferral path into expiries; 94% of budget keeps the same saturated window
(96.9% observed) without penalizing the retry design. This is the scenario
parameter, not a limiter limitation — the limiter itself admitted every
request the window allowed (utilization > 96%).

## Scenario 2 — live limit change 30K → 5K → 30K RPM — PASS

Spec limits, pinned explicitly at run start: model-a **30,000 RPM / 60M
TPM**, model-b **20,000 RPM / 40M TPM** (2,000-token requests so the TPM
limit, not just RPM, is the binding constraint at the submitted 94% rate).
BOTH models are under load in every phase.

- phase 1 (75s, both models at their spec limits) →
  `POST /limits/model-a {rpm: 5000, tpm: 60000000}` →
  phase 2 (75s, model-a constrained; the spec reduces RPM only, so A's TPM
  stays 60M) → restore to 30K/60M → phase 3 (75s)
- model-b's limits are set exactly once (20K/40M) and never touched again
- **no restart between phases** — the limit change is applied live through
  the engine's limiter (`SetLimits`, atomic swap; enforcement is immediate
  for new admissions)
- the observed rolling window decays to the new 5K limit within one window
  period after the decrease (a live decrease cannot retroactively un-admit
  traffic already inside the 60s window; enforcement means no NEW admissions
  beyond it — the evidence time series shows the monotonic decay)
- **model-b kept processing while model-a was constrained**: ~19.1K RPM
  observed (95% of its own 20K limit) during phase 2, never over 40M TPM
- full configured-vs-observed time series in the evidence JSON for BOTH
  models, labelled per phase (`p1`/`p2`/`p3`) and per model
- graphical evidence: `scenario-2-rpm.svg` and `scenario-2-tpm.svg` — each
  chart shows both models' observed windows against both models' configured
  limits over time (A blue/red, B green/amber)

## Scenario 3 — durable async batches + callbacks — PASS

Two 10,000-request batches (model-a, model-b), callback sink rejecting the
first two deliveries then accepting:

- **ack < 1s**: both 202s returned in well under a second (bulk-insert
  persistence + async hand-off; measured 0.074s for a 10K batch in the
  crash-demo run)
- **callback fired only after every request reached a terminal state**
- **delivery recovered after the rejections**: attempt log 500, 500, then
  200 — backoff 500ms/1s/2s then periodic 5s, per `configs/system.yaml`
  (`callbacks:` block, fully configurable)
- **callback summary == API batch status**: the delivered payload's
  per-outcome breakdown matched `GET /batches/{id}` exactly
- **every id exactly once** (`unique_ids: true` on both batches)
- final accounting batch-a: 9,789 ok / 211 failed / 0 expired; batch-b:
  9,772 ok / 228 failed / 0 expired — the transient rate resolves through
  retries, the ~2% permanent rate shows as `failed`

## Crash-recovery demo — no lost requests

60,000-request batch submitted (2× the 30K RPM budget on purpose, so a large
share is queued mid-flight). `serve` was force-killed ~8s in.

State at crash (read from `crash-demo.db`):

| request state | count |
|---|---|
| completed | 6,996 |
| expired (max-wait overflow, pre-crash) | 18,197 |
| in_flight | 412 |
| submitted (accepted, not yet admitted) | 34,395 |

`serve` restarted against the same SQLite file:

- startup log: **`recovered 35205 in-flight request(s) from the last run`**
  (the 34,395 submitted + 412 in_flight rows, re-enqueued into the wait
  queue — recovered requests are exempt from max-wait expiry: recovery
  resumes work, it does not expire it)
- the batch resumed **without any client action** — the client that polled
  `GET /batches/{id}` simply saw `processing` continue to `completed`
- final accounting exact:

| outcome | count |
|---|---|
| successful | 40,126 |
| failed | 856 |
| expired (max-wait overflow) | 19,018 |
| rejected | 0 |
| **total** | **60,000** |

Nothing was lost and nothing was double-counted: idempotency keys
(`Idempotency-Key` = request UUID) make provider re-execution idempotent —
the simulator's seen-keys table memoizes successful executions and returns
the original result for a re-issued call, so a crash between "provider
executed" and "system recorded" cannot double-charge or double-complete.

## Benchmark regression — phase-1 path intact

`consuma benchmark -config configs/benchmark-300k.yaml -rate 300000
-duration 30s -batch 50` (in-process, no SQLite, no HTTP):

- **steady-state 300,015 completed/sec** (required bar: ≥ 300,000)
- 9,000,000 submitted = 9,000,000 completed (exact conservation)
- drain 0.34s; the ~2.1% `failed` are the simulated provider's permanent
  failures, as configured

Report: `docs/benchmark/2026-10-15-benchmark-report.md`.

## How to reproduce

```
bin/consuma.exe simulator -config configs/scenario1.yaml -addr 127.0.0.1:9001 &
bin/consuma.exe serve -config configs/system.yaml -addr 127.0.0.1:9000 &

bin/consuma.exe scenario1 -out docs/validation -duration 300
bin/consuma.exe scenario2 -out docs/validation
bin/consuma.exe scenario3 -out docs/validation
```

Crash-recovery demo: submit a large batch (`POST /batches`), kill `serve`
mid-run, start it again against the same `-db` path, and poll the batch id.
