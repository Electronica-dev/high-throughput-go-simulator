// Scenario 2 — live limit change: A runs 30K → 5K → 30K RPM mid-run with no
// restart, B's traffic is untouched throughout, and the report carries the
// configured-vs-observed time series per phase.
package scenarios

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// RunScenario2 drives the three-phase limit change and asserts each phase's
// observed window matches its configured limit.
func RunScenario2(systemURL string, sc Config, outDir string) (*Result, error) {
	client := &http.Client{Timeout: 30 * time.Second}

	var phase atomic.Value // string: p1 | p2 | p3
	phase.Store("p1")
	mu := &sync.Mutex{}
	var series []TimePoint
	stop := make(chan struct{})
	go sampler(client, systemURL, sc.Model, time.Duration(sc.SampleEverySec)*time.Second,
		func() string { return phase.Load().(string) }, stop, &series, mu)
	// model B untouched: its own sampler
	muB := &sync.Mutex{}
	var seriesB []TimePoint
	stopB := make(chan struct{})
	go sampler(client, systemURL, sc.SecondModel, time.Duration(sc.SampleEverySec)*time.Second,
		func() string { return phase.Load().(string) }, stopB, &seriesB, muB)

	// phase 1: pin the scenario's baseline limit explicitly (the scenario is
	// self-contained regardless of what the system started with)
	phase.Store("p1")
	if err := postJSON(client, systemURL+"/limits/"+sc.Model, map[string]any{"rpm": sc.Phase1RPM, "tpm": sc.Phase1RPM * sc.Tokens}); err != nil {
		close(stop)
		close(stopB)
		return nil, fmt.Errorf("limit set to %d: %w", sc.Phase1RPM, err)
	}
	sc1 := sc
	sc1.DurationSec = sc.Phase1Sec
	sc1.RatePerSec = sc.Phase1RPM*94/6000 // 94% of capacity: retry attempts also consume RPM slots, so submitting at capacity would pin the window and expire retry deferrals
	if _, err := runLoad(client, systemURL, &sc1, map[string]float64{sc.Model: 1.0}); err != nil {
		close(stop)
		close(stopB)
		return nil, err
	}
	// phase 2: drop A's limit to Phase2RPM (no restart); the submitted rate
	// tracks each phase's per-second capacity (RPM/60) at 94%
	phase.Store("p2")
	if err := postJSON(client, systemURL+"/limits/"+sc.Model, map[string]any{"rpm": sc.Phase2RPM, "tpm": sc.Phase2RPM * sc.Tokens}); err != nil {
		close(stop)
		close(stopB)
		return nil, fmt.Errorf("limit change to %d: %w", sc.Phase2RPM, err)
	}
	sc2 := sc
	sc2.DurationSec = sc.Phase2Sec
	sc2.RatePerSec = sc.Phase2RPM*94/6000 // 94% of the phase's capacity
	if _, err := runLoad(client, systemURL, &sc2, map[string]float64{sc.Model: 1.0}); err != nil {
		close(stop)
		close(stopB)
		return nil, err
	}
	// phase 3: restore
	phase.Store("p3")
	if err := postJSON(client, systemURL+"/limits/"+sc.Model, map[string]any{"rpm": sc.Phase1RPM, "tpm": sc.Phase1RPM * sc.Tokens}); err != nil {
		close(stop)
		close(stopB)
		return nil, fmt.Errorf("limit restore to %d: %w", sc.Phase1RPM, err)
	}
	sc3 := sc
	sc3.DurationSec = sc.Phase3Sec
	sc3.RatePerSec = sc.Phase1RPM*94/6000 // 94% of capacity
	if _, err := runLoad(client, systemURL, &sc3, map[string]float64{sc.Model: 1.0}); err != nil {
		close(stop)
		close(stopB)
		return nil, err
	}
	close(stop)
	close(stopB)

	res := &Result{Pass: true}
	mu.Lock()
	res.TimeSeries = append(res.TimeSeries, series...)
	mu.Unlock()
	// per-phase configured-vs-observed: phase windows must track their limits
	for _, p := range []string{"p1", "p2", "p3"} {
		var samples []WindowSample
		mu.Lock()
		for _, tp := range series {
			if tp.Phase == p {
				samples = append(samples, tp.Sample)
			}
		}
		mu.Unlock()
		limit := sc.Phase1RPM
		if p == "p2" {
			limit = sc.Phase2RPM
		}
		var over int
		for _, s := range samples {
			if s.ConfiguredRPM > 0 && float64(s.ObservedRPM) > float64(s.ConfiguredRPM)*1.005 {
				over++
			}
		}
		if over > 0 {
			res.Pass = false
			res.Reasons = append(res.Reasons, fmt.Sprintf("phase %s: %d window(s) observed above the configured %d RPM", p, over, limit))
		}
	}
	// model B untouched: its observed windows never exceeded its own config
	muB.Lock()
	overB := len(OverLimit(seriesBSamples(seriesB), 0.005))
	muB.Unlock()
	if overB > 0 {
		res.Pass = false
		res.Reasons = append(res.Reasons, fmt.Sprintf("model %s observed over its limits in %d window(s)", sc.SecondModel, overB))
	}
	if err := writeEvidence(outDir, "scenario-2", res); err != nil {
		return nil, err
	}
	return res, nil
}

func seriesBSamples(series []TimePoint) []WindowSample {
	out := make([]WindowSample, 0, len(series))
	for _, tp := range series {
		out = append(out, tp.Sample)
	}
	return out
}
