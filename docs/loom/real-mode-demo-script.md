# Loom script — Consuma real-mode demo (~12 min)

Recording notes: terminal is Windows (git-bash + PowerShell). Two terminals
side by side: left = `serve` (the system), right = everything else.
Recording ~12 min at 1080p, mono narration. Rehearse the two curl one-liners
once; paste, don't type.

---

## 0. Cold open (0:00–0:45)

**Screen:** README section header "Real mode" visible, scrolled so the HTTP
API table is in view.

> "Consuma ships two numbers — 300 thousand and a million completed requests
> per second — both measured in one process, over in-memory queues. That
> number is real, and its scope caveat is on the README, deliberately. But a
> system you can't call is a report, not a product. This video is the real
> mode: an HTTP API, a separate provider simulator process, live limit
> changes, durable async batches with callbacks, and crash recovery that
> provably loses nothing."

## 1. The two processes (0:45–2:00)

**Screen:** fresh terminals. Type (both lines already in history):

```
bin/consuma.exe simulator -config configs/scenario1.yaml -addr 127.0.0.1:9001 &
bin/consuma.exe serve -config configs/system.yaml -addr 127.0.0.1:9000 \
    -simulator http://127.0.0.1:9001 -db consuma.db
```

> "Two processes. The simulator — a standalone provider with its own rolling
> limits, its own outcome mix, and idempotency-key memoization. The serve
> process — the actual system: admission limiter, wait queue, worker pool,
> SQLite durability in write-ahead-log mode, and a batch manager with a
> callback dispatcher."

**Show** the startup logs: listen address, simulator target, DB path.

## 2. A single request (2:00–3:00)

**Screen:** right terminal. `curl` a single request:

```
curl -s http://127.0.0.1:9000/requests -d '{"model":"model-a","tokens":100}' | jq
```

> "One request, synchronous. Outcome, latency — and the two token numbers
> that matter. `tokens_reserved` is what admission charged against the TPM
> window: an estimate. `tokens_charged` is what the provider actually
> billed. If the provider burned less than we reserved, the difference is
> reconciled back to the window at the terminal transition. The reservation
> can never be exceeded; the reconciliation is what makes the TPM number
> honest."

**Show** `tokens_charged` vs 100.

## 3. Live limit change (3:00–4:15)

**Screen:** left terminal shows `serve` logs. Right terminal: submit a
moderate background batch in the background:

```
bin/consuma.exe benchmark -config configs/system.yaml -target http://127.0.0.1:9000 \
    -rate 20000 -duration 60s -batch 50 &
```

then, ~15s in:

```
curl -s http://127.0.0.1:9000/limits/model-a -d '{"rpm": 5000, "tpm": 30000000}' | jq
```

> "Twenty thousand requests a second coming in — the limit is thirty
> thousand RPM, so most queue. Watch the metrics endpoint — actually,
> watch the scenario-2 runner, which samples it every second." [show
> `docs/validation/scenario-2.json` time series or rerun `scenario2`]
> "Now we cut the limit to five thousand RPM — a POST, not a config edit, no
> restart. The `serve` log shows the limit swap. The observed rolling window
> decays to the new ceiling over one window period — a decrease can't
> un-admit traffic already inside the 60-second window, that's what a
> rolling window means — but no new admission beyond five thousand per
> minute gets through. Then we restore thirty thousand and the queue drains.
> The other model, model-b, never moved — the change is per-model."

## 4. Async batch + callbacks (4:15–6:00)

**Screen:** right terminal, callback sink running (a tiny listener on
127.0.0.1:9200 configured to reject the first two deliveries):

```
curl -s http://127.0.0.1:9000/batches -d '{"model":"model-a","count":10000,"callback_url":"http://127.0.0.1:9200/cb"}' | jq
```

**Immediately note the clock.**

> "Ten thousand requests. That POST returned a batch id in seventy-four
> milliseconds — 202, not a synchronous wait. The requests were
> bulk-inserted into SQLite and handed to the async worker pool; the caller
> never blocks on the queue. When every one of the ten thousand is terminal
> — that's the only trigger — the system calls back."

**Show** the sink log: two rejections (500), then a 200.

> "The callback sink was told to reject the first two deliveries. The system
> retried at 500 milliseconds, then a second — configured schedule — and the
> third attempt succeeded. Every attempt is persisted, including the
> failures, in the callback-attempts table. If the server dies before a
> delivery succeeds, the next restart resumes it. And the delivered summary
> is byte-identical to `GET /batches/{id}` — the client can trust either."

## 5. Crash recovery (6:00–8:30)

**Screen:** fresh DB (`-db crash-demo.db`). Submit a 60,000-request batch
(twice the 30K RPM budget, on purpose):

```
curl -s http://127.0.0.1:9000/batches -d '{"model":"model-a","count":60000}' | jq
```

**Wait ~8s.** Then, in the left terminal, hard-kill serve:

```
powershell -Command "Get-NetTCPConnection -LocalPort 9000 -State Listen | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }"
```

**Show** the batch endpoint failing (connection refused) for a second.

> "Killed. Not a graceful shutdown — SIGKILL. No flush, no cleanup. Every
> request the system had accepted is in SQLite at that moment, because
> acceptance IS the durable write. About eight seconds in, the state was:
> seven thousand completed, eighteen thousand expired — the queue-overflow
> expiry — and thirty-five thousand still in flight, either waiting or
> being processed. That 35,205 is exactly the number the restart recovers."

**Restart serve** (same `-db` path). **Show** the line:

```
recovered 35205 in-flight request(s) from the last run
```

> "Restart against the same database. The startup recovery sweep finds every
> non-terminal request — submitted, queued, in-flight — and re-enqueues it.
> Recovered requests are exempt from the max-wait expiry: recovery resumes
> work, it doesn't expire it. And the client saw none of this. The batch id
> I never stopped polling? It went `processing` → `completed` like a normal
> run."

**Show** the final accounting:

> "Final accounting. Forty thousand one hundred twenty-six successful,
> 856 failed — that's the simulator's configured 2% permanent failures —
> nineteen thousand expired, all from the pre-crash queue overflow, zero
> rejected. Sum: exactly sixty thousand. Nothing lost, nothing counted
> twice. And nothing charged twice either: the request UUID rides to the
> provider as the Idempotency-Key. The simulator memoizes successful
> executions — a retry after a crash replays the original result rather
> than billing again. At-least-once delivery, exactly-once charging."

## 6. The scenarios (8:30–10:00)

**Screen:** the three evidence summaries, one at a time:

```
cat docs/validation/scenario-1-summary.txt   # utilization 96.5% / 0 over-limit
cat docs/validation/scenario-2-summary.txt
cat docs/validation/scenario-3-summary.txt
```

> "Three scenarios, each PASS. One: sustained load at the configured limit —
> 96.5% RPM utilization, zero over-limit windows, full accounting. Two: the
> live limit change you just saw, with the configured-vs-observed time
> series as evidence. Three: the async batches with the reject-first
> callback sink, delivery recovery, and every request id exactly once.
> All three write a JSON you can audit — every id is in it."

## 7. Benchmark regression (10:00–10:45)

**Screen:** the regression report header:

> "And the number this whole refactor had to protect: 300,015 completed
> requests per second, steady-state, 9 million in, 9 million out — exact
> conservation. The benchmark hot path is in-process and was never touched
> by the real-mode work; that's the point of the Caller abstraction. The
> wheel didn't slow down because SQLite exists elsewhere."

## 8. Close (10:45–11:30)

**Screen:** back to the README.

> "That's real mode. Same state machine, same limiter, same accounting —
> now reachable over HTTP, durable across crashes, and honest about what
> the provider actually charged. The scope caveat stays: these are rates a
> single machine can serve; the 10-billion-per-minute path is a separate
> projection. Docs and evidence are in the repo."

---

## Timing cheatsheet (recorded takes)

| # | Section | Take | Watch for |
|---|---|---|---|
| 1 | Startup | one | both processes up before narration |
| 2 | Single request | one | tokens_charged visible in jq output |
| 3 | Limit change | two (one per direction) | queue decay on the time series |
| 4 | Batch + callbacks | one | ack ms on the clock, sink 500/500/200 |
| 5 | Crash recovery | one, uncut | recovery line + accounting sum |
| 6-8 | Evidence + close | one | nothing to stage |
