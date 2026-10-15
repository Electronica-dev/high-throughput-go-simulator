# Loom script — Consuma real-mode demo (5 minutes)

Terminal: Windows (git-bash + PowerShell). Two terminals side by side:
left = `serve` logs, right = commands. Rehearse the curl one-liners; paste,
don't type. All commands below already ran green.

---

## 0. Cold open (0:00–0:25)

**Screen:** README "Real mode" section.

> "Consuma measures 300 thousand — and a million — completed requests per
> second, in-process. But a system you can't call is a report, not a
> product. This is the real mode: HTTP API, separate simulator process,
> SQLite durability, callbacks, and crash recovery that loses nothing."

## 1. Two processes + a single request (0:25–1:20)

**Screen:** right terminal.

```
bin/consuma.exe simulator -config configs/scenario1.yaml -addr 127.0.0.1:9001 &
bin/consuma.exe serve -config configs/system.yaml -addr 127.0.0.1:9000 \
    -simulator http://127.0.0.1:9001 -db consuma.db
curl -s http://127.0.0.1:9000/requests -d '{"model":"model-a","estimated_tokens":100}' | jq
```

> "Simulator: a standalone provider with its own rolling limits. Serve: the
> system — limiter, wait queue, workers, SQLite, batch manager. One
> synchronous request: outcome, latency, and the two token numbers —
> tokens_reserved is the estimate admission used, tokens_charged is what the
> provider actually billed; the difference reconciles back to the window.
> Limits are never exceeded, and the charge is honest."

## 2. Live limit change (1:20–2:20)

**Screen:** right terminal, background load + one POST; then the chart.

```
bin/consuma.exe benchmark -target http://127.0.0.1:9000 -rate 20000 -duration 60s -batch 50 &
curl -s http://127.0.0.1:9000/limits/model-a -d '{"rpm": 5000, "tpm": 3000000}' | jq
```

**Then show** docs/validation/scenario-2-rpm.svg in a browser.

> "Twenty thousand a second against a thirty-thousand-RPM limit — most
> queue. Now cut the limit to five thousand: a POST, no restart. The
> rolling window decays to the new ceiling over one window period — a
> decrease can't un-admit traffic already inside the window — but nothing
> new gets past it. This is the scenario-2 evidence chart: observed
> throughput against the configured limit, over time, phase by phase."

## 3. Async batch + reject-first callback (2:20–3:30)

**Screen:** right terminal (the reject-first sink is built into scenario 3).

```
bin/consuma.exe scenario3 -out docs/validation
```

> "Ten thousand durable requests per model, acknowledged in seventy-four
> milliseconds — the batch id comes back immediately; SQLite has it before
> the 202 returns. The callback fires only when every request is terminal.
> Our sink rejects the first two deliveries — watch the runner log: attempt
> one rejected, attempt two rejected, then the configured retry schedule
> recovers it. And the runner verifies the delivered summary equals
> GET /batches/{id}, with every request id exactly once."

**Show** the runner's [s3 ...] step lines scrolling.

## 4. Crash recovery (3:30–4:30)

**Screen:** right terminal, fresh DB; submit 60K; kill serve; restart.

```
curl -s http://127.0.0.1:9000/batches -d '{"model":"model-a","count":60000}' | jq
# ~8s later, kill serve (left terminal), then restart against the same -db:
bin/consuma.exe serve -config configs/system.yaml -addr 127.0.0.1:9000 \
    -simulator http://127.0.0.1:9001 -db consuma.db
```

**Show** "recovered 35205 in-flight request(s) from the last run".

> "SIGKILL, not a graceful shutdown. Restart against the same database, and
> the recovery sweep re-enqueues every accepted request — thirty-five
> thousand of them. The client polls the same batch id and sees
> processing -> completed like nothing happened. Final accounting: forty
> thousand one hundred twenty-six ok, 856 failed, nineteen thousand expired
> from the pre-crash queue overflow, zero rejected — sums to sixty thousand
> exactly. Nothing lost, nothing charged twice — the request UUID is the
> provider's idempotency key, so a replayed call returns the original
> result."

## 5. Close (4:30–5:00)

**Screen:** the summary files + the regression report header.

```
cat docs/validation/scenario-1-summary.txt   # 96.5% utilization, 0 over-limit
cat docs/validation/scenario-3-summary.txt
```

> "Three scenarios, all PASS, each writing auditable evidence — JSON,
> summary, and the throughput-over-time charts you saw. And the number this
> refactor had to protect: 300,015 per second steady, 9 million in, 9
> million out, exact conservation. Docs, evidence, and the Bruno API
> collection are in the repo."

---

## Timing table

| # | Section | Budget | Watch for |
|---|---|---|---|
| 1 | Two processes + single request | 55s | tokens_charged vs reserved in jq output |
| 2 | Live limit change | 60s | chart: observed decays under the new limit |
| 3 | Async batch + callback | 70s | [s3 ack ...] and sink reject/reject/accept lines |
| 4 | Crash recovery | 60s | recovery line + accounting sum |
| 5 | Close | 30s | summaries + regression numbers |

If over time: drop section 5's second cat and close on section 4's
accounting line.
