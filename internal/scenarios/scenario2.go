// Scenario 2 — live limit change per the spec: model A runs at 30,000 RPM /
// 60M TPM, model B at 20,000 RPM / 40M TPM; A is reduced to 5,000 RPM
// mid-run (TPM unchanged) and restored, with no restart; B is pinned once
// and keeps processing throughout. Both models are under load every phase,
// and the report carries each model's configured-vs-observed time series.
package scenarios

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// RunScenario2 drives the three-phase limit change and asserts each phase's
// observed windows match the configured limits for BOTH models.
func RunScenario2(systemURL string, sc Config, outDir string) (*Result, error) {
	client := &http.Client{Timeout: 30 * time.Second}

	var phase atomic.Value // string: p1 | p2 | p3
	phase.Store("p1")
	samplerStart := time.Now()
	mu := &sync.Mutex{}
	var series []TimePoint
	stop := make(chan struct{})
	go sampler(client, systemURL, sc.Model, time.Duration(sc.SampleEverySec)*time.Second,
		func() string { return phase.Load().(string) }, stop, &series, mu)
	// model B: its own sampler — it must keep processing while A is constrained
	muB := &sync.Mutex{}
	var seriesB []TimePoint
	stopB := make(chan struct{})
	go sampler(client, systemURL, sc.SecondModel, time.Duration(sc.SampleEverySec)*time.Second,
		func() string { return phase.Load().(string) }, stopB, &seriesB, muB)

	fail := func(err error) (*Result, error) {
		close(stop)
		close(stopB)
		return nil, err
	}
	runStart := time.Now()
	stepf := func(format string, args ...any) {
		fmt.Printf("[s2 +%.1fs] %s\n", time.Since(runStart).Seconds(), fmt.Sprintf(format, args...))
	}
	// load for one phase: BOTH models at 94% of their current per-second
	// capacity (retry attempts also consume limit slots, so submitting at
	// capacity would pin the windows and expire retry deferrals), weighted
	// by each model's share of the current total capacity.
	phaseLoad := func(modelARPM int64) (*Config, map[string]float64) {
		totalRPM := modelARPM + sc.SecondRPM
		lc := sc
		lc.RatePerSec = totalRPM * 94 / 6000
		mix := map[string]float64{
			sc.Model:       float64(modelARPM) / float64(totalRPM),
			sc.SecondModel: float64(sc.SecondRPM) / float64(totalRPM),
		}
		return &lc, mix
	}

	// baseline: pin BOTH models' spec limits explicitly (the scenario is
	// self-contained regardless of what the system started with). B's limits
	// are set exactly once here and never touched again.
	if err := postJSON(client, systemURL+"/limits/"+sc.Model, map[string]any{"rpm": sc.Phase1RPM, "tpm": sc.Phase1TPM}); err != nil {
		return fail(fmt.Errorf("limit set to %d/%d: %w", sc.Phase1RPM, sc.Phase1TPM, err))
	}
	if err := postJSON(client, systemURL+"/limits/"+sc.SecondModel, map[string]any{"rpm": sc.SecondRPM, "tpm": sc.SecondTPM}); err != nil {
		return fail(fmt.Errorf("limit set to %d/%d: %w", sc.SecondRPM, sc.SecondTPM, err))
	}
	stepf("phase p1: limits pinned per spec — %s %d RPM/%d TPM, %s %d RPM/%d TPM (no restart); both models under load",
		sc.Model, sc.Phase1RPM, sc.Phase1TPM, sc.SecondModel, sc.SecondRPM, sc.SecondTPM)
	lc1, mix1 := phaseLoad(sc.Phase1RPM)
	lc1.DurationSec = sc.Phase1Sec
	if _, err := runLoad(client, systemURL, lc1, mix1); err != nil {
		return fail(err)
	}
	// phase 2: drop A to Phase2RPM (no restart); the spec reduces RPM only —
	// A's TPM stays at Phase1TPM. B's load and limits are untouched.
	phase.Store("p2")
	p2StartSec := time.Since(samplerStart).Seconds()
	if err := postJSON(client, systemURL+"/limits/"+sc.Model, map[string]any{"rpm": sc.Phase2RPM, "tpm": sc.Phase1TPM}); err != nil {
		return fail(fmt.Errorf("limit change to %d: %w", sc.Phase2RPM, err))
	}
	stepf("phase p2: %s limit changed LIVE to %d RPM (TPM stays %d; no restart) — expect the observed window to decay to the new ceiling within one 60s window; %s keeps processing",
		sc.Model, sc.Phase2RPM, sc.Phase1TPM, sc.SecondModel)
	lc2, mix2 := phaseLoad(sc.Phase2RPM)
	lc2.DurationSec = sc.Phase2Sec
	if _, err := runLoad(client, systemURL, lc2, mix2); err != nil {
		return fail(err)
	}
	// phase 3: restore
	phase.Store("p3")
	if err := postJSON(client, systemURL+"/limits/"+sc.Model, map[string]any{"rpm": sc.Phase1RPM, "tpm": sc.Phase1TPM}); err != nil {
		return fail(fmt.Errorf("limit restore to %d/%d: %w", sc.Phase1RPM, sc.Phase1TPM, err))
	}
	stepf("phase p3: %s limit restored to %d RPM/%d TPM; %s sampled throughout (limits never changed)",
		sc.Model, sc.Phase1RPM, sc.Phase1TPM, sc.SecondModel)
	lc3, mix3 := phaseLoad(sc.Phase1RPM)
	lc3.DurationSec = sc.Phase3Sec
	if _, err := runLoad(client, systemURL, lc3, mix3); err != nil {
		return fail(err)
	}
	close(stop)
	close(stopB)
	// zero samples means the system was unreachable or died mid-run — fail
	// loudly and write no evidence (a 0.0%-utilization FAIL report is
	// misleading); each model's sampler is checked separately
	mu.Lock()
	sampledA := len(series)
	mu.Unlock()
	muB.Lock()
	sampledB := len(seriesB)
	muB.Unlock()
	if sampledA == 0 || sampledB == 0 {
		return nil, fmt.Errorf("no window samples collected (model-a: %d, model-b: %d) — the system was unreachable or died mid-run; no evidence written (start `serve` and `simulator` first, see README §1)",
			sampledA, sampledB)
	}

	res := &Result{Pass: true}
	// per-phase configured-vs-observed for model A: phase windows must track
	// BOTH of A's limits (rpm during every phase; tpm is unchanged all run)
	for _, p := range []string{"p1", "p2", "p3"} {
		limitRPM := sc.Phase1RPM
		if p == "p2" {
			limitRPM = sc.Phase2RPM
		}
		var overRPM, overTPM int
		for _, tp := range series {
			if tp.Phase != p {
				continue
			}
			// a live limit DECREASE cannot retroactively un-admit traffic
			// already inside the rolling 60s window: the observed window
			// decays to the new limit within one window period. Enforcement
			// means no NEW admissions beyond the limit — skip the drain
			// period after the change (61s covers it).
			if p == "p2" && tp.AtSec < p2StartSec+61 {
				continue
			}
			sink := tp.Sample
			if sink.ConfiguredRPM > 0 && float64(sink.ObservedRPM) > float64(sink.ConfiguredRPM)*1.005 {
				overRPM++
			}
			if sink.ConfiguredTPM > 0 && float64(sink.ObservedTPM) > float64(sink.ConfiguredTPM)*1.005 {
				overTPM++
			}
		}
		if overRPM > 0 {
			res.Pass = false
			res.Reasons = append(res.Reasons, fmt.Sprintf("phase %s: %d window(s) observed above the configured %d RPM", p, overRPM, limitRPM))
		}
		if overTPM > 0 {
			res.Pass = false
			res.Reasons = append(res.Reasons, fmt.Sprintf("phase %s: %d window(s) observed above the configured %d TPM", p, overTPM, sc.Phase1TPM))
		}
	}
	// model B: never over its own limits, and still processing while A was
	// constrained (observed rpm in p2 well above zero)
	muB.Lock()
	overB := len(OverLimit(seriesBSamples(seriesB), 0.005))
	var p2Windows int
	var p2ObservedRPMSum int64
	for _, tp := range seriesB {
		if tp.Phase != "p2" || tp.AtSec < p2StartSec+61 {
			continue
		}
		p2Windows++
		p2ObservedRPMSum += tp.Sample.ObservedRPM
	}
	muB.Unlock()
	if overB > 0 {
		res.Pass = false
		res.Reasons = append(res.Reasons, fmt.Sprintf("model %s observed over its limits in %d window(s)", sc.SecondModel, overB))
	}
	// B kept processing while A was constrained — checked only when the
	// post-drain p2 windows exist (a 60s decay period leaves none in
	// short-lived test runs)
	if p2Windows > 0 && p2ObservedRPMSum/int64(p2Windows) < sc.SecondRPM/2 {
		res.Pass = false
		res.Reasons = append(res.Reasons, fmt.Sprintf("model %s did not keep processing while %s was constrained (mean p2 observed %d RPM over %d window(s))",
			sc.SecondModel, sc.Model, p2ObservedRPMSum/int64(p2Windows), p2Windows))
	}

	mu.Lock()
	res.TimeSeries = append(append([]TimePoint{}, series...), seriesB...)
	mu.Unlock()
	charts, err := writeTimelineCharts(outDir, "scenario-2", res.TimeSeries)
	if err != nil {
		return nil, err
	}
	res.Charts = charts
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
