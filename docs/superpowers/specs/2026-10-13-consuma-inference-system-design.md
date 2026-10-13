# Consuma — High-Throughput Inference System: Design

Date: 2026-10-13
Status: reviewed (builder check passed), pending owner approval
Source: `consuma ai project.pdf` (Consuma Technologies assignment)

## Intent and why

Build an inference-processing system plus a provider simulator and load
generator that together demonstrate processing simulated inference requests
at very high rates while respecting per-model RPM (requests/minute) and TPM
(tokens/minute) limits. A request counts as **completed only when the
simulated provider call reaches a final success or failure state** — accepting
a request at an API is not processing it.

The immediate focus of this design (per the project owner) is the
**simulation benchmark**: hit the required 300,000 simulated inference
requests/second and lay a credible foundation for the 1,000,000/sec stretch
benchmark, on the owner's local machine. Real LLM calls are never made at
benchmark rates; simulated models may be given higher limits (explicitly
allowed by the assignment).

## Goals

- Complete ≥300,000 simulated inference requests/second (required benchmark),
  measured on the target machine.
- Foundation architecture that reaches ~1,000,000/sec without a redesign
  (stretch benchmark).
- Honest "completed" semantics: every request traverses admission (rate
  limiting) → simulated provider latency → final success/failure state.
- Track every request to a terminal accounting state — the system never
  loses a request.
- Respect configured per-model RPM and TPM at all times; support changing
  limits live (no restart).
- End-of-run report: submitted, completed, successful, failed; achieved
  completion rate; p50/p95/p99 latency; configured vs observed RPM/TPM per
  model; batch completion and callback timing (callback machinery deferred,
  see Non-goals).
- Report measurements separately from projections, with simulation
  assumptions stated.

## Non-goals (for this phase, by owner decision)

- Validation scenarios 1–3 (capacity reach, changing limits with two models,
  async batch callbacks) — deferred to a later phase; the design must not
  preclude them.
- Deliverables packaging (GitHub repo polish, Loom video, benchmark report
  document) — deferred.
- Real provider calls, HTTP client behavior tuning, anything paid.

## Anti-goals (what would make this a failure even if it works)

- Claiming throughput by counting API acceptances as completions.
- Measured numbers that mix with projections without labeling.
- A design that only works when limits are set absurdly high and cannot
  enforce realistic ones (50K RPM / 100M TPM must be enforceable).
- Code the owner cannot understand and explain (interview/defense risk).

## Constraints

- Target machine: ASUS Zenbook, Intel Core Ultra 7 255H (16 cores), 32 GB
  RAM, Windows 11. Benchmarks run locally, laptop on AC power.
- Language: **Go** (owner-decided after discussion; owner has no Go/Rust
  experience — Go chosen for readability and review-ability, with AI-assisted
  development. Python was evaluated and rejected on performance grounds).
- Simulator and load generator are CLI tools for this phase.
- Go 1.27 (installed via winget on the target machine).

## Decided architecture (owner-approved)

### Processing simulation

A "provider call" is a **state machine**, not a network call. Per request:

1. `submitted` — load generator emits a request (model, payload id,
   estimated tokens).
2. `admitted` — the per-model rate limiter checks a sliding RPM window and
   TPM token accounting; on success the request is scheduled, otherwise it
   enters the wait queue per the overload policy below.
3. `in-flight` — the provider simulator schedules a completion event after
   the model's configured latency (timer-wheel entry; no sleeps, no sockets
   in the hot path).
4. `final` — success or failure drawn from the model's configured outcome
   mix; metrics updated; the request is accounted.

At 1M req/sec with 50 ms simulated latency, ~50,000 requests are in flight
at any instant — a timer-wheel workload, not a goroutine-per-timer workload.

### Overload policy (owner-decided)

When demand exceeds a model's capacity, unadmitted requests enter a **bounded
wait queue with expiry**: they wait for capacity up to a configurable max
wait time, then end as `expired`. Only when the wait queue itself is full is
a request rejected immediately as `rejected`. Every submitted request ends in
exactly one terminal state — `completed`, `expired`, or `rejected` — and the
report accounts for all of them. (`waiting` is an in-run phase, never a
terminal state, because runs always drain; see benchmark accounting below.)

### Benchmark accounting (owner-decided)

- Headline number = **steady-state completed requests/second**: warm-up ramp
  and final drain are excluded from the headline; the whole-run average is
  reported alongside it, labeled separately.
- Runs always **drain fully**: the load generator stops submitting, all
  in-flight and queued requests reach a terminal state before the report is
  produced, and drain time is reported separately. No request is ever left
  unaccounted at end of run.
- The spike's whole-run average of 289,543/sec (below) includes ramp;
  steady-state per-second peaks reached ~299K/sec in the same run.

### Benchmark topology (the key decision)

- **Benchmark mode:** load generator, rate limiter, and provider simulator
  run **in one process** (internal pipeline, in-memory). This is what makes
  300K–1M/sec achievable: the bottleneck at these rates is the HTTP stack,
  not the simulation logic.
- **Real mode:** the system exposes a real HTTP API (accept single and batch
  requests) and the simulator runs as a standalone CLI service the system
  calls over HTTP — used for the low-rate validation scenarios and batch
  callbacks in a later phase.
- The load generator and simulator remain standalone CLI binaries that can
  also target the HTTP service.

**README caveat (required, with quantified backing):** the benchmark numbers
are achieved with the in-process pipeline; crossing a process boundary per
request over localhost HTTP caps throughput at roughly 100–200K requests/sec
(tuned round-trip cost ~5–10 µs CPU each side plus queuing), i.e. below the
required 300K bar regardless of language. This limitation is stated in the
README alongside the measured in-process numbers, so the reviewer sees both
the scope of the claim and the honest ceiling of the HTTP mode. Measurements
(spike results below) and this projection must be labeled separately.

### Spike evidence (measured, throwaway prototype, target machine)

Single process, in-memory: 1-second-granularity RPM/TPM accounting, shared
timer wheel (4 ms tick, per-bucket mutex), atomic counters, batched
completion. Model limits set high (allowed for benchmarks); 50 ms simulated
latency; 2% permanent-failure mix; laptop on AC power.

| Target rate | Whole-run avg completed/sec | Steady-state peak/sec | In-flight est. | Notes |
|---|---|---|---|---|
| 300,000/sec | **289,543/sec** (10 s run) | ~299,000/sec | ~15,000 | 2,895,431 of 2,913,691 submitted completed |
| 1,000,000/sec | **988,723/sec** (10 s run) | ~996,000/sec | ~50,000 | 9,887,234 of 9,939,574 submitted completed |

Latency readouts were inflated to ~67 ms (configured 50 ms) by the spike's
coarse tick and wheel contention — both are artifacts the real
implementation removes (sharded wheels, per-core sharding, batched pops).
The required bar was brushed with spike-grade code; the real implementation
targets ≥1.3x steady-state headroom above it.

## Policy decisions settled during review (builder's default, owner may override)

These were flagged by the builder check and settled as sensible defaults;
each is a call to be confirmed while reading, not an owner decision:

- **Two enforcement layers.** The simulator behaves like a real provider: it
  enforces its own configured RPM/TPM and can reject with a 429-equivalent.
  The system's admission limiter is the primary mechanism for staying within
  limits; provider-side rejection is the backstop and makes "observed
  RPM/TPM" a provider-side measurement in the report.
- **TPM accounting.** Tokens are reserved on *estimated* counts at admission;
  on completion the simulator draws actual tokens from a configurable
  distribution and the difference is reconciled (unused reservation
  refunded). Failed calls consume their tokens. The report distinguishes
  admitted-TPM from observed-TPM.
- **Retries.** Transient provider failures are retried with capped
  exponential backoff (default 3 attempts); each retry counts against RPM
  and TPM like a fresh call. The report gives both per-attempt and
  per-request success/failure counts.
- **Latency definition.** Headline p50/p95/p99 are end-to-end
  (submission → final state), which includes queue wait under overload.
  Provider-call latency is reported separately so simulation drift is
  visible.
- **Live limit reduction.** A reduced limit applies to *new admissions*
  immediately; already in-flight requests complete normally, so observed
  throughput may transiently exceed the new limit until the current window
  drains (documented in the report). Queued requests are throttled by the
  new limit, not aborted.

## What's left to the builder

- Repo layout, Go module structure, CLI flag surface for the three tools
  (system, simulator, load generator).
- Config format (suggestion: YAML or TOML file + flags for models with
  rpm/tpm/latency/outcome-mix/fee-free fields) and live limit-change
  mechanism (file watch or control command).
- Sliding-window implementation choice (slot-ring vs histogram buckets) and
  TPM accounting details.
- Timer-wheel design (sharded per core, batched expiry), object pooling to
  keep GC pressure low at 1M/sec.
- Latency histogram (streaming quantile or bucketed) for p50/p95/p99.
- Request state ledger with exactly-once terminal accounting
  (completed/expired/rejected) and the bounded wait queue with expiry.
- Report writer (console + JSON) separating measured vs projected numbers.
- Structure the code so the later HTTP API, batch/callback machinery, and
  validation scenarios bolt on without reworking the hot path.
- A short design note (README section or `docs/`) on extending toward
  10 billion requests/minute (planning target from the assignment).

## Open questions for the owner

None remaining. Overload policy and benchmark accounting were answered by
the owner (bounded wait queue with expiry; steady-state headline with full
drain). All other review findings were settled as the calls listed above.
