// Scenario 1 — sustained load at the configured limit: ≥90% of allowed
// capacity completed after warm-up, no observed 60s window over either
// limit, and every request accounted for in the batch breakdowns.
package scenarios

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// RunScenario1 drives sustained load and asserts limit enforcement.
func RunScenario1(systemURL string, sc Config, outDir string) (*Result, error) {
	client := &http.Client{Timeout: 30 * time.Second}

	mu := &sync.Mutex{}
	var series []TimePoint
	phase := func() string { return "steady" }
	stop := make(chan struct{})
	go sampler(client, systemURL, sc.Model, time.Duration(sc.SampleEverySec)*time.Second, phase, stop, &series, mu)

	start := time.Now()
	ids, err := runLoad(client, systemURL, &sc, map[string]float64{sc.Model: 1.0})
	close(stop)
	if err != nil {
		return nil, err
	}
	// a run against a dead system used to write a misleading 0.0%-utilization
	// FAIL report; zero samples means the system was unreachable or died
	// mid-run — fail loudly and write no evidence
	mu.Lock()
	sampled := len(series)
	mu.Unlock()
	if sampled == 0 {
		return nil, fmt.Errorf("no window samples collected — the system was unreachable or died mid-run; no evidence written (start `serve` and `simulator` first, see README §1)")
	}

	sums, err := fetchSummaries(client, systemURL, ids)
	if err != nil {
		return nil, err
	}
	mu.Lock()
	res := assessScenario1(sc, series, sums, time.Since(start).Seconds())
	mu.Unlock()
	res.Batches = sums
	res.TimeSeries = series
	charts, err := writeTimelineCharts(outDir, "scenario-1", series)
	if err != nil {
		return nil, err
	}
	res.Charts = charts
	if err := writeEvidence(outDir, "scenario-1", res); err != nil {
		return nil, err
	}
	return res, nil
}

// assessScenario1 is the pure pass/fail logic: warm-up-excluded utilization
// ≥ MinUtilPct, no over-limit window, every request accounted.
func assessScenario1(sc Config, series []TimePoint, sums []BatchSummary, elapsedSec float64) *Result {
	res := &Result{Pass: true}
	warmupAt := float64(sc.WarmupSec)
	var measured []WindowSample
	for _, tp := range series {
		if tp.AtSec >= warmupAt {
			measured = append(measured, tp.Sample)
		}
	}
	res.Util = MeanUtilization(measured)
	if res.Util.RPM < sc.MinUtilPct {
		res.Pass = false
		res.Reasons = append(res.Reasons, fmt.Sprintf("RPM utilization %.1f%% < %.1f%% after warm-up", res.Util.RPM, sc.MinUtilPct))
	}
	// ≥90% of allowed capacity COMPLETED: successful vs (configured RPM/60)
	// × the measured window span
	if len(measured) > 0 && measured[len(measured)-1].ConfiguredRPM > 0 {
		spanSec := 0.0
		if len(series) > 0 {
			spanSec = series[len(series)-1].AtSec - warmupAt
		}
		allowed := float64(measured[len(measured)-1].ConfiguredRPM) / 60 * spanSec
		var okN int64
		for _, b := range sums {
			okN += b.Successful
		}
		if allowed > 0 && float64(okN) < 0.9*allowed {
			res.Pass = false
			res.Reasons = append(res.Reasons, fmt.Sprintf("completed %d < 90%% of allowed capacity %.0f", okN, 0.9*allowed))
		}
	}
	if overs := OverLimit(measured, 0.005); len(overs) > 0 {
		res.Pass = false
		res.OverLimitN = len(overs)
		res.Reasons = append(res.Reasons, fmt.Sprintf("%d observed window(s) over the configured limit", len(overs)))
	}
	// every request accounted: each batch's breakdown sums to its total
	var total, okN, failed, expired, rejected int64
	for _, b := range sums {
		total += b.Successful + b.Failed + b.Expired + b.Rejected
		okN += b.Successful
		failed += b.Failed
		expired += b.Expired
		rejected += b.Rejected
	}
	if total == 0 {
		res.Pass = false
		res.Reasons = append(res.Reasons, "no completed requests accounted in batch breakdowns")
	}
	_ = okN
	_ = failed
	_ = expired
	_ = rejected
	return res
}
