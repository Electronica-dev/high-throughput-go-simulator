# Benchmark report

Measured **2026-10-15**; raw run reports (`run-300k-final.json`,
`run-1m-final.json`) sit next to this file, and every number below is copied
from them. Projections are labeled as such and kept in their own section.

## Test environment

| Item | Value |
|---|---|
| Machine | ASUS Zenbook laptop, Intel Core Ultra 7 255H (16 cores), 32 GB RAM |
| OS | Windows 11 |
| Toolchain | Go 1.27, default GC settings |
| Power | AC (laptop throttles on battery) |
| Topology | single local process: load generator + admission limiter + provider simulator, in-memory queues + timer wheel |
| Network | none in the hot path (simulated provider calls) |
| Date | 2026-10-15 |

## Configuration

**Simulated model** (`configs/benchmark-300k.yaml`; the assignment allows
raising the simulated limits for the high-throughput benchmarks):

| Field | Value | Meaning |
|---|---|---|
| `rpm` / `tpm` | 500,000,000 / 10T | deliberately far above traffic, so the system's own limiter is what paces |
| `estimate_tokens` | 1,000 | reservation taken at admission |
| `actual_token_mean` | 1,050 | what the provider actually charges (±20% draw) |
| `latency` | lognormal, mean 50ms | per-call provider latency |
| `outcome` | 93% success / 5% transient / 2% permanent | failure mix |

**System tuning** (`max_wait_ms: 5000`, `queue_depth: 65536`,
`max_retries: 3`, wheel: 8 shards × 4096 buckets × 1ms tick).

**Run parameters:**

| Run | Command (abridged) |
|---|---|
| Required | `benchmark -config configs/benchmark-300k.yaml -rate 300000 -duration 60s -batch 50 -report docs/benchmark/run-300k-final.json` |
| Stretch | `benchmark -config configs/benchmark-1m.yaml -rate 1000000 -duration 30s -batch 50 -report docs/benchmark/run-1m-final.json` |

`-batch 50` means the pacer submits 50 requests per tick slot (6,000
submissions/sec at the required rate); pacing is split across worker
goroutines, one per ~50K/sec, each with a nanosecond carry accumulator.

## Achieved throughput

| Run | Target | **Steady-state achieved** | Whole-run avg | Submitted | Completed | Expired | Rejected | Drain |
|---|---|---|---|---|---|---|---|---|
| Required (60s) | 300,000/sec | **300,011/sec** | 298,268/sec | 18,000,000 | 18,000,000 | 0 | 0 | 0.35s |
| Stretch (30s) | 1,000,000/sec | **1,000,064/sec** | 988,405/sec | 30,000,000 | 30,000,000 | 0 | 0 | 0.35s |

- Both targets are met **exactly** in steady state (300,011 vs 300,000;
  1,000,064 vs 1,000,000); the whole-run average is ramp-inclusive.
- Conservation is exact in both runs: submitted = completed (+0 expired,
  +0 rejected). Nothing unaccounted.
- "Completed" means the simulated provider call reached a final success or
  failure state — API acceptance does not count (per the assignment).

## Latency

End-to-end per request (submission → final provider state), per model:

| Run | p50 | p95 | p99 |
|---|---|---|---|
| Required (300K/sec) | 52.5ms | 121.3ms | 179.6ms |
| Stretch (1M/sec) | 52.3ms | 121.5ms | 179.5ms |

- p50 sits within ~5% of the configured 50ms provider median.
- **The distribution is identical at 3.3× the load** — the p95/p99 tail is
  the configured lognormal provider latency (σ 0.3), not system queueing.
  Queueing latency at 1M/sec is, for practical purposes, unmeasurable in
  the tail.

## Failures

| Run | Successful | Failed | Failed % | Expected % |
|---|---|---|---|---|
| Required | 17,619,141 (97.89%) | 380,859 | 2.12% | ~2.1% |
| Stretch | 29,364,753 (97.88%) | 635,247 | 2.12% | ~2.1% |

- The failed rate matches the simulated provider's **2% permanent-failure
  mix** — failures are workload, not system breakdowns.
- The 5% transient failures are retried with exponential backoff (50ms /
  100ms / 200ms, max 3 attempts), each attempt charged against RPM/TPM like
  a fresh call; successes converge to ~97.9% of submissions, exactly the
  observed figure.
- Zero expired and zero rejected: the wait queue never overflowed at these
  rates, and the system never rejected on arrival.
- Failure behavior under stress is evidenced separately in the validation
  scenarios: S1 (zero expired, zero over-limit windows in 5 minutes),
  S3 (callback delivery recovers after rejections), and the crash-recovery
  demo (60K-request batch, SIGKILL mid-flight, exact accounting after
  restart) — all in `docs/validation/`.

## Bottlenecks

**What the number is.** The simulation benchmarks run in one process
(in-memory queues + timer wheel) — a request must complete on the simulated provider,
and at these rates no HTTP hop is possible: a per-request localhost
round-trip costs ~5–10µs CPU per side plus queuing, capping **any**
implementation near 100–200K requests/second on this hardware. That HTTP
ceiling is a **projection**, not a measurement, and is kept out of the
measured numbers. The real-mode system (`serve`/`simulator` over HTTP,
SQLite durability) exists to demonstrate the API surface, limits, batches,
callbacks, and crash recovery — its rates are HTTP-bound by design.

**What was the bottleneck during development (measured, then fixed):**

1. **The token bookkeeping was eating 12% of the CPU.** Every finished
   request had to settle its token refund through the same shared counters,
   so hot requests kept stepping on each other. We reworked that accounting
   so requests no longer queue behind each other — a big part of how 278K
   became 300K/sec.
2. **After a stall, the timer wheel needed 8.3 seconds to catch up** on
   roughly a million requests that were already due — it replayed them
   gently, so everything fired late. It now catches up in one pass: 0.34s
   (which is also why the runs above drain in a flat 0.35s).
3. **About 1% of submissions were silently lost.** The load pacer counted
   its budget in whole time slices and threw away the leftover fraction
   each time. It now carries the exact remainder over and pays off whatever
   is owed at the end of the run — every submitted request is accounted
   for, which is what makes the headline number trustworthy.

**What limits the current number:** the pacer (one goroutine saturates near
280K submissions/sec, hence the worker split), the 1ms wheel tick, and the
default GC target. At 1M/sec the latency distribution is unchanged from 300K/sec, so
the completion path has headroom left; the pacer, not the engine, is the
next wall.

**Simulation assumptions**: provider latency is drawn from the configured lognormal
distribution rather than real network calls; actual token charge is drawn
around the configured mean (±20%); outcomes follow the configured 93/5/2
mix; the provider's execution is a scheduled final state on the wheel, not
real LLM inference.

## Measured vs projected

Everything above under Achieved throughput / Latency / Failures is
**measured** from the two JSON run reports. The only projections in this
report are the HTTP ceiling estimate (~100–200K/sec) and the 10-billion/
minute scaling analysis (`docs/planning-toward-10b.md`). The two are never
mixed: the in-process pipeline is the measured benchmark environment; the
HTTP path and the 10B path are labeled projections.
