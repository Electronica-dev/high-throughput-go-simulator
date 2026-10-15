# Planning: extending toward 10 billion requests/minute

10B requests/minute ≈ 166.7M requests/second — roughly 170× the measured
single-process stretch capacity (~1M/sec on a 16-core laptop). This note is a
projection of the design's extension path, not a measurement.

## What scales unchanged

The core abstractions carry over as-is per process:

- the request state machine and ledger (exactly-once terminal accounting),
- the sliding-window RPM/TPM limiter with reservation + reconciliation,
- the bounded wait queue with expiry,
- the timer wheel (horizon, sharding, batched expiry),
- the provider simulator semantics.

## What must change

1. **Shard by model × key across processes and machines.** A single process
   saturates around 1M/sec (GC pauses, timer-wheel tick cost, memory
   bandwidth). 170 processes each running today's engine at 1M/sec reaches
   the target; in practice plan 2–3× headroom → ~400–500 engine shards.
   Requests carry a routing key (e.g. request-id hash) so all lifecycle
   events for one request stay on one shard — no cross-shard coordination in
   the hot path.
2. **Load generation must be distributed.** One load-generator process cannot
   emit 167M req/sec. Each engine shard gets a co-located load worker with
   its own exact-conservation pacer (the current per-worker split scales to
   this shape unchanged).
3. **Limits accounting becomes per-shard-local with a global ceiling.** Each
   shard enforces its local slice of a model's RPM/TPM (global_limit /
   shard_count, with a coordinator rebalancing slices as shards join/leave).
   The two-layer enforcement (system admission + provider backstop) carries
   over; the provider simulator becomes a per-shard in-process component or
   a co-located service.
4. **Reports aggregate.** Each shard writes its JSON report; a merger
   sums counters and merges per-model latency histograms (bucket-compatible
   by construction) into one global report. Percentiles remain honest
   because histograms merge exactly.
5. **Durability becomes the per-shard WAL, not a single SQLite file.** The
   current phase-2 design persists to one SQLite database (WAL) for real
   mode (single requests, batches, callbacks). At 167M req/sec a single
   SQLite file cannot keep up — the durable log must be sharded too:
   one WAL per engine shard (or a log-structured store like RocksDB per
   shard), batched group-commits, and asynchronous fsync batches. The
   recovery sweep then runs per shard over its own log; the at-least-once
   + idempotency-key contract carries over unchanged.
6. **Where the HTTP boundary stays.** HTTP remains the low-rate control and
   client surface (single requests, batch submission, callbacks, limit
   changes). The benchmark hot path stays in-process per shard — the same
   scope caveat as the single-machine results applies at every scale.

## Cost model (projection)

- ~500 shards × 16-core-equivalents ≈ 8,000 cores for the simulation itself;
- timers: 167M in flight at 50ms latency × 50ms → ~8.3M entries in flight
  system-wide per 1M/sec shard-run; wheels are sized per shard, so per-shard
  in-flight stays at today's ~50K — no redesign pressure;
- memory: ~170B/request live set per shard ≈ 60MB/shard — trivial;
- the binding constraints are pacing honesty and exact terminal accounting
  across shards during rebalancing (drain protocol: stop submits, sweep
  queues, drain wheels per shard, then merge — same rule as today).
