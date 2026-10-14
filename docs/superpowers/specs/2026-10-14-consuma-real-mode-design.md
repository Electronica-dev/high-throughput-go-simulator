# Consuma — Real-Mode System: Design (Phase 2)

Date: 2026-10-14
Status: draft for builder check
Source: `consuma ai project.pdf` (Consuma Technologies assignment)
Phase 1: `docs/superpowers/specs/2026-10-13-consuma-inference-system-design.md` (committed, benchmark core on `main`)

## Intent and why

Phase 1 built the in-process simulation core (limiter, wait queue, timer
wheel, provider simulator, ledger, metrics) and proved 300K/1M completed
requests/second with exact terminal accounting. Phase 2 turns it into the
**real-mode system**: an HTTP service that accepts single and batch requests,
processes them asynchronously with completion callbacks, persists state in
SQLite, and exposes the three validation scenarios from the assignment as
repeatable evidence.

The assignment's requirement that motivates every piece: a request counts as
processed only when the provider call reaches a final state; batches must
acknowledge within one second and call back after all requests reach final
state; the callback summary must match what the API returns.

## Goals

- HTTP API on the system: single requests (synchronous result), async
  batches (202 + batch ID), batch status, batch results, live limit change.
- Async batch lifecycle: ack < 1s; callback fired only after every request
  reaches a terminal state; callback payload = batch ID + status + counts +
  results URL (owner decision: results fetched, not inline); callback
  delivery retried with backoff and attempts persisted; client can query
  batch status independently at any time.
- Durability via SQLite: batches, per-request states, and results survive
  restart; in-flight/queued requests at crash are lost (documented
  simulation artifact, not hidden).
- Provider simulator as a standalone CLI service (`consuma simulator`):
  charges its own rolling RPM/TPM windows, answers after its configured
  latency, returns 429 on its own limit breaches — the real-mode backstop.
- The system calls the simulator over HTTP in real mode (a real network
  round trip per provider call; the latency is the simulator's response
  delay).
- Validation scenarios 1–3 as repeatable runner commands producing evidence
  JSON:
  - Scenario 1: model at 50K RPM / 100M TPM, traffic above capacity for
    ≥5 minutes; ≥90% of allowed capacity completed after warm-up; no
    observed 60s window over either limit; every request accounted.
  - Scenario 2: model A 30K RPM / 60M TPM, model B 20K RPM / 40M TPM; drop
    A to 5K RPM mid-run, then restore; B unaffected; A adjusts without
    restart; report shows configured vs observed over time.
  - Scenario 3: 10K requests across two models with a callback URL;
    simulate transient + permanent failures; reject the first two callback
    deliveries; ack within 1s; callback only after all final; delivery
    succeeds after destination recovers; callback summary matches API; every
    request ID exactly once in the final batch result.
- The phase-1 benchmark claims remain intact: benchmark mode (in-process,
  SQLite off) is untouched and still the 300K/1M path.

## Non-goals (this phase)

- Persistence beyond SQLite-in-a-file (no server database, no replication).
- Surviving crashes with in-flight work (in-flight/queued at crash = lost;
  batches persist as failed/summary per the states recorded before crash).
- Authentication, TLS, multi-tenancy.
- Horizontal scale-out (single process per service; the 10B/min planning
  note covers the path).
- Benchmark-mode changes beyond wiring the provider interface (no behavior
  or throughput regression allowed).

## Anti-goals (failure even if it works)

- Benchmark throughput regressing because of phase-2 plumbing on its path.
- A callback fired while any request in the batch is non-terminal.
- Batch ack slower than one second (the load generator is not involved; ack
  is admission + persistence + queue, all cheap).
- Callback summary diverging from `GET /batches/{id}` (both read one store).
- Silent loss: every accepted request must be traceable to a terminal state
  or a documented crash-loss boundary.

## Constraints

- Same machine (ASUS Zenbook, 16 cores, Windows 11), same Go 1.27 module
  (`consuma`).
- Dependency policy: the benchmark core allows only `gopkg.in/yaml.v3`.
  Phase 2 adds `modernc.org/sqlite` (pure Go, no cgo — the only SQLite
  driver buildable on this host) and uses the standard library
  `net/http` for all HTTP. No web framework.
- Scenario traffic is HTTP-mode (well below the HTTP ceiling; scenario 1
  runs at ≤ ~833 RPS sustained against the service).
- Scenario 1's ≥90% capacity utilization is measured against the observed
  60s windows reported by the system (limiter rolling totals), warm-up
  excluded.

## Decided architecture (owner-approved)

- **Two new binaries in the same module:** `consuma system` (HTTP service,
  real mode) and `consuma simulator` (standalone provider simulator
  service). The existing `consuma benchmark` and `consuma limits` remain.
- **Provider abstraction:** the engine takes a provider interface. Two
  implementations: the existing in-process wheel-scheduled simulator
  (benchmark mode — unchanged, still the 300K/1M path) and an HTTP provider
  (real mode: one real round trip per call to the simulator service). The
  system's rolling RPM/TPM limiter and wait queue apply identically in both
  modes; the simulator additionally enforces its own limits over HTTP (429
  backstop becomes a real response).
- **API surface (owner-approved):**
  - `POST /requests` — single request, synchronous result (the connection
    waits out queue wait + provider latency). Response: request id, model,
    outcome, tokens charged, e2e latency.
  - `POST /batches` — async; body: model mix per request or batch-level
    model, request count, estimated tokens, callback URL. Returns 202 +
    batch ID in < 1 second. Requests admitted with the same overload policy
    (bounded wait queue with expiry).
  - `GET /batches/{id}` — status (`pending|processing|completed`), total /
    successful / failed / expired / rejected counts, results URL, callback
    delivery state.
  - `GET /batches/{id}/results` — the results document (per-request id,
    outcome, tokens charged, latency).
  - `POST /limits/{model}` — live RPM/TPM change, no restart (replaces the
    benchmark's file-watch mechanism on the service path; the watch remains
    on the benchmark CLI for the Loom demo).
- **Callbacks:** when the last request of a batch reaches a terminal state,
  the system POSTs `{batch_id, status, counts, results_url}` to the
  registered callback URL. Delivery: 3 attempts with backoff (50/100/200ms),
  then periodic retries (e.g. every 2s up to a TTL); every attempt (time,
  status code, outcome) persisted. Scenario 3 rejects the first two
  deliveries and expects recovery.

## Owner decisions (recorded 2026-10-14, review rounds 1-2)

- **Batch status:** `pending | processing | completed | failed`.
  `completed` means every request in the batch is accounted for — the
  callback and the results document carry the full breakdown
  (`successful / failed / expired / rejected` counts plus per-request
  terminal states). `failed` is reserved for invalid input or unrecoverable
  loss of the record of which requests existed. A request rejected at
  admission (wait queue full at submit time) is still part of the acked
  202 batch and appears in the rejected count — admission is asynchronous
  by design.
- **Crash recovery (owner-decided, replaces the earlier "mark processing
  as failed" default):** per-request state is persisted durably (SQLite,
  WAL mode — see Durability). On restart, a recovery sweep re-enqueues
  every request found `queued` or `in_flight`; the batch stays
  `processing` throughout and eventually reaches `completed` — the client
  never sees the crash, only extra latency. Idempotency keys per request
  are required, because an in-flight request may or may not have reached
  the provider before the crash. Batches whose callback was never
  delivered **resume delivery** after restart (persisted delivery state;
  "reasonable effort to deliver" covers restarts).
- **Simulator limits:** the simulator is a separate fixed entity —
  `POST /limits/{model}` changes only the system's limiter and cannot
  reach the simulator. The simulator's headroom over the system's limits
  (default 10×) is a **scenario parameter**, so provider 429s stay backstop
  events without eroding scenario 1's ≥90% utilization, and a scenario can
  tighten the headroom to exercise the backstop deliberately.
- **Request IDs are server-generated** (UUIDs, assigned at batch submit
  time); the client never supplies them. The submit response and the
  results document carry the full ID list, so scenario 3's "every request
  id appears exactly once" is verified by the client against the server's
  own IDs. The load generator's real mode reads IDs from the submit
  response.
- **Durability:** SQLite file per service (`system.db`), WAL mode, via
  `modernc.org/sqlite`. Schema: batches (id, status, counts, callback URL,
  callback state), requests (id, batch id nullable, model, state, outcome,
  tokens charged, timestamps), callback attempts, limits audit. Writes at
  submission and terminal transitions. Benchmark mode: SQLite disabled, hot
  path untouched.
- **Load generator:** gains a real mode (`-target http://host:port`) that
  drives the HTTP API — used by the scenario runners and by a human
  reviewer.

## Scenario runner decisions (owner-decided)

- Each scenario is a runner command (`consuma scenario1|scenario2|scenario3`)
  that: starts/uses the services, drives the traffic, changes limits via the
  API where the scenario requires it, collects the system's own observed
  windows plus client-side measurements, and writes an evidence JSON +
  human-readable summary. Pass criteria are asserted by the runner against
  the assignment's stated thresholds (e.g. ≥90% utilization, no 60s window
  over limits, ack < 1s, callback after all final, ids exactly once).
- Scenario 1's "no observed 60-second period exceeds either limit" is
  checked against the system's rolling windows (the same figures the limiter
  enforced against), sampled over time by the runner.

## What's left to the builder

- Exact HTTP JSON shapes, error codes, and validation messages.
- SQLite schema DDL, WAL pragmas, and write batching policy (terminal
  writes may micro-batch under load, but ack path must stay < 1s).
- HTTP provider client (connection pooling, timeout = per-call deadline;
  timeouts count as transient failures and retry like any other).
- Callback dispatcher (per-batch goroutine or shared worker pool), retry
  schedule, delivery-state persistence.
- Graceful shutdown: stop accepting, drain, flush SQLite, exit.
- Scenario runner implementation details (traffic shaping, timing checks,
  evidence JSON schema).
- Configuration: `configs/system.yaml` (listen address, simulator URL,
  SQLite path, callback retry policy, TTL), `configs/simulator.yaml`
  (models with limits/latency/outcomes — same schema as today).
- The 10B/min planning note gains one paragraph on where SQLite sits in the
  scaled-out picture (per-shard DB or dropped for in-flight-only).

## Open questions for the owner

None remaining. Owner decisions are recorded under "Owner decisions" above
(batch status semantics, crash recovery with re-enqueue and callback resume,
simulator headroom as a scenario parameter, server-generated request IDs)
and in the intent section (batch-ID + fetch results; 202 + ID for batches;
synchronous single-request results; SQLite durability).
