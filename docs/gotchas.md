# Gotchas — things that bit us while building Consuma

Every non-obvious trap we ran into, in plain language, with the fix. The
deep technical version of most of these lives in `docs/decisions.md` and the
commit history; this file is the "why did it do THAT" survival guide.

## Counting and limits

- **A per-second check would allow 60× the limit.** Checking "can I admit
  one more this second?" against a per-second budget looks right but lets
  you run at the limit every second — 60× the real RPM budget. Limits are
  enforced against a true rolling 60-second total.
- **The limiter starved itself after ~70 admits.** The first version
  "cleaned up" old window slots by snapping the window start to the current
  second, which silently skipped evictions and stranded used-up budget.
  Fix: the window start moves one second at a time, never snaps.
- **A request could vanish mid-flight.** Peek → admit → pop weren't
  atomic: an expiry sweep running in between could leave a request marked
  admitted but owned by no one. Fixed by making the three steps one atomic
  operation.
- **Retries were secretly corrupting the latency stats.** Each retry
  attempt recorded a latency sample, so retried requests contributed
  partial (short) measurements and biased the percentiles. Latency is now
  recorded exactly once, when the request reaches its final state.
- **The TPM graph showed noise instead of throughput** (flat lines, literal
  zeros, numbers with no relation to the traffic). Two accounting steps
  were both subtracting the same reservation: the refund when the provider
  responded, and the window cleanup a minute later. The token total bled
  toward zero, which also meant **TPM enforcement was silently switched
  off** — the RPM limit was the only thing actually holding traffic back.
  Fixed by netting the refund into the slot the reservation lives in, so
  the money is subtracted exactly once. (This one was spotted by eyeballing
  the scenario-2 TPM chart — the numbers were too pretty to be true.)
- **The throughput graph always reads a little low — on purpose.** Three
  stacked reasons: (1) scenarios submit at 94% of capacity by design, so
  retries have somewhere to go; (2) the TPM line shows what the provider
  actually charged, and failed requests are charged zero, so it sits below
  "estimate × requests"; (3) the 60-second window takes a minute to fill
  after any change, and the newest one-second bucket is on average half
  full when sampled. None of these are measurement bugs.

## Retries and failures

- **Every provider call returned HTTP 400, forever.** The system sent the
  request's numeric ID where the simulator expected a string, so the
  simulator rejected every single call — and the retry machinery dutifully
  retried them all. Fixed by sending the request's UUID.
- **The retry counter was never incremented in real (HTTP) mode.** Result: an
  infinite retry storm (~7,000 retries/sec) that starved the entire system
  — the failure of one feature looked like total system collapse. Fixed by
  counting the attempt before making the call.
- **The simulator had a perfect memory — for failures.** It remembered a
  request ID's assigned outcome, so a retried request got the same failure
  replayed forever and could never succeed. Fixed: remember successes for
  idempotency, let failures re-draw.

## HTTP and connections

- **Go's default HTTP client pools almost nothing** (2 idle connections
  per host). At hundreds of requests per second the system was constantly
  tearing down and re-dialing connections, which produced random slow calls
  that looked exactly like provider failures. Scenario 1's utilization went
  from 82% to 99% from this one fix — the limiter was never the problem.
- **SQLite said "database is locked" under concurrent writers.** The
  busy-timeout and WAL settings were applied to only one of the pooled
  connections. Fixed by putting the settings in the connection string so
  every connection gets them.

## Crash recovery

- **"Requests in flight at crash are lost" — they aren't, but the first
  answer was wrong.** The whole point of SQLite durability is that nothing
  is lost: on restart every accepted request is re-enqueued (at-least-once),
  and the provider's idempotency key means a replayed request returns the
  original result instead of doing the work twice. The client only sees
  extra latency.
- **Recovery ran in the wrong order and quietly lost completions.** The
  server recovered in-flight requests *before* restoring the batch
  registry, so finished requests reported into batches that didn't exist
  yet. The database was perfect; the batch just sat at "processing" forever
  with a few thousand requests unaccounted. Fixed by reordering startup:
  restore batch state first, then recover, then resume.
- **Recovered requests expired before they could restart.** A crashed batch
  re-enters at a trickle (the drainer admits slowly after a restart), and
  the normal expiry timer killed requests while they waited. Fix: recovered
  requests are exempt from the expiry sweep — a crash costs the client
  latency, never lost work.

## The simulator is part of the test rig

- **The simulator's limits must be ABOVE the system's limits.** Scenario 2
  originally gave the simulator a 30M TPM ceiling while the system under
  test was pinned at 60M — the "provider" would have started rejecting
  before the system's own limiter ever did, and the test would have been
  measuring the wrong bottleneck. Simulator limits are now a fixed multiple
  of the system's.
- **Scenario 2's model B originally had no traffic at all.** "Model B keeps
  processing while model A is constrained" can't be demonstrated against a
  model that's receiving zero requests. Both models are now under load in
  every phase, and the pass criteria actually check B kept moving.
- **Scenario limits are pinned from the spec, not derived.** The TPM limit
  was originally computed as RPM × tokens-per-request, which produced
  numbers that had nothing to do with the assignment's 60M/40M TPM. The
  spec's numbers are typed in explicitly.
- **A live limit decrease can't un-admit traffic.** Requests already inside
  the rolling window stay there; the observed window *decays* to the new
  limit over the next minute. The scenario's pass logic skips that drain
  period — enforcement means "no NEW admissions beyond the limit", not
  "the window shrinks instantly".
