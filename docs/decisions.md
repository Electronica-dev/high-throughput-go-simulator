# Design & Implementation Decisions

A durable record of the judgment calls made while building the benchmark
core — the things a reviewer (or future you) would want explained. Derived
from the implementation ledger; commit history on `main` carries the full
fix trail.

## Owner decisions

- **Overload policy:** bounded wait queue with expiry. Requests wait for
  capacity up to `max_wait_ms`, then end `expired`; reject on arrival only
  when the queue itself is full. Every request ends in exactly one terminal
  state (`completed` / `expired` / `rejected`).
- **Benchmark accounting:** headline = steady-state completed requests/sec;
  whole-run average reported alongside; runs always drain fully.
- **Language:** Go — owner has no Go/Rust experience; Go chosen for
  readability and defensibility (interview/Loom), with AI-assisted
  development. Python was evaluated and rejected on measured performance
  grounds (spike evidence in the spec).

## Builder rulings (made during implementation, with cost-if-wrong)

1. **Branch-in-place instead of a separate git worktree.** The repo is
   brand-new with nothing to protect. Cost if wrong: negligible.
2. **Config `Load` validates internally** (fail-fast at startup, using the
   config's own wheel horizon); callers can re-validate with another
   horizon. Cost: none.
3. **`Reconcile(reserved, actual, admittedSec, now)`.** The limiter needs
   the reserved amount and the admission second to refund correctly.
4. **Observed TPM = rolling reservations net of refunds** (conservative
   upper bound before reconciliation; settles toward the actual charge).
   Refunds for reservations already outside the window are skipped — they
   would otherwise mint headroom (reviewer-verified: 9 in-window + 1
   aged-out refund admitted 2 instead of 1).
5. **Rolling-window enforcement (review round 1/2).** Limits are enforced
   against a true rolling 60-second total, not the per-second pace — a
   per-second check admits 60× the configured RPM under sustained load.
   `wheelBase` (oldest live second) is never snapped to the current second:
   the first cut did snap it, which skipped evictions and starved the
   limiter after ~70 admits (reviewer's empirical probe; pinned by
   `TestRollingWindowSustainedMinute`).
6. **Atomic pop-if-admissible.** Peek→admit→pop happens under one queue
   lock; a concurrent expiry sweep could otherwise strand a popped request
   in `Admitted` forever (dropped request). Pinned by a push/pop/sweep race
   test asserting terminal balance.
7. **Provider drawn after admission.** Queued or rejected requests never
   consume provider-side capacity; "provider observed usage" measures real
   calls only.
8. **Provider 429 (backstop rejection) is handled by the retry path**
   (exponential backoff, counts against limits) rather than terminal
   failure. Cost if wrong: under sustained backstop saturation requests can
   exhaust retries and fail instead of queueing.
9. **Retry accounting.** Retries charge the rolling RPM window like fresh
   calls (`ChargeRetry`); if the window is full the retry defers back to
   the wait queue (`InFlight→Queued` legal edge) rather than bypassing
   enforcement. TPM is reserved once at first admission and reconciled
   exactly once at the terminal transition using the cumulative actual
   charge (per-attempt reconcile double-counted refunds).
10. **E2E latency recorded once per request, at the terminal transition.**
    Recording per wheel fire let retried requests contribute partial
    samples, biasing percentiles low (review round 2).
11. **Timer wheel: sequence-based draining with clock re-anchoring.** Each
    shard walks buckets in order (a wall-clock index strands buckets when a
    ticker stalls) and drains any overdue span in one pass when >2 ticks
    behind (a metered replay left every entry uniformly ~230ms late).
    Tradeoff accepted: post-stall completion bursts; honest, because the
    requests were genuinely due.
12. **Exact-conservation pacing.** Per-tick budget truncation silently lost
    ~1% of submissions at 300K/sec; fixed with a nanosecond carry
    accumulator plus an end-of-run flush of the worker's remaining
    hold-window budget. The headline is the achieved rate, no boundary
    residue.
13. **Load generator parallelism.** One pacing goroutine saturates around
    280K submits/sec; workers split the rate (one per ~50K/sec), each with
    its own 1ms ticker and remainder-distributed budget.
14. **Request store removed from the hot path** (`DebugTrackRequests`
    flag): tracking 17M request objects cost ~1GB GC pressure. Tests that
    need per-request inspection set the flag explicitly.
15. **Metrics: pre-registered lock-free counters** (no per-request registry
    lookup), completion counters aggregated per wheel batch. Latency
    histogram: 1.1× log buckets with interpolated percentiles (±10%
    documented precision); the plan's original test windows were
    mathematically unachievable by any rank-based estimator on 10 sparse
    samples.
16. **Drain deadline = 2× max_wait + 5s grace** (the plan's 2× alone was
    too tight for sweep cadence). Drains complete in ~0.35s in practice.
17. **Round-1 reviewer's "drain-time inflation" theory was rejected** with
    a conservation argument (completions lag submissions by mean latency;
    hold-end counting *under*counts). The reviewer's conclusion — the
    headline was below the bar — was nonetheless correct, via a different
    mechanism (pacer boundary loss, ruling 12). Rejected theories get
    recorded too.
18. **Scenario load is paced at 94% of the limit's capacity** (RPM/60 ×
    0.94). Submitting at exactly 100% leaves no headroom for retry
    re-admissions: transient failures (~5%) re-enter through the same
    rolling window, pin it at the limit, and the retry deferral path
    degenerates into expiries. At 94% the window stays saturated but
    unpinned — S1 observes 96.9% utilization including retry re-admissions,
    zero over-limit samples, zero unexpected expiries. This is a scenario
    parameter (headroom for the retry design), not a limiter limitation.
    Why the graph still reads below 100%: see docs/gotchas.md
    ("the throughput graph always reads a little low").

## Deferred minors (known, accepted, unfixed)

- `ProviderRejected` and `TransientFailure` share the retry path; a
  distinct counter would separate backstop pressure from transient faults
  in reports.
- Lognormal draws can exceed the 6σ validation cap (~1e-9/call): such a
  draw lands a wheel revolution ahead and fires early. Clamp at the horizon
  if it ever matters.
- `WheelTickMS`, `QueueDepth`, `MaxWaitMS` are unvalidated (zero values
  would panic or silently always-reject); repo-controlled configs make this
  low-risk.
- `cpu_bench_test.go` has a hardcoded profile path (debug artifact).
- `Drain` hitting its deadline returns the summary without a
  `drained_complete` flag — an unbalanced report is not visibly marked.
- `cmd/htsim/main.go` has a test-only helper (`newEngineFromConfig`).
- The wheel's `Schedule` silently drops entries after `Stop` (unreachable
  in the benchmark flow, but unguarded).
- The race detector could not run on the Windows host (no cgo/gcc); run one
  `go test -race ./...` on a CGO-capable machine before external review.
