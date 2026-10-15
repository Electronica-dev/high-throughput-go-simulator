// Package scenarios drives the spec's three validation scenarios against a
// running system, samples observed windows over time, and writes per-scenario
// evidence (JSON + summary) to an output directory. The pass/fail assertions
// are pure functions so they test independently of any live system.
package scenarios

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"consuma/internal/config"
	"consuma/internal/loadgen"
)

// WindowSample mirrors api.WindowSample (one model's rolling-window snapshot).
type WindowSample struct {
	ObservedRPM   int64 `json:"observed_rpm"`
	ObservedTPM   int64 `json:"observed_tpm"`
	ConfiguredRPM int64 `json:"configured_rpm"`
	ConfiguredTPM int64 `json:"configured_tpm"`
}

// ResultRow is one batch result row (id + terminal state + outcome).
type ResultRow struct {
	ID      string `json:"ID"`
	State   string `json:"State"`
	Outcome string `json:"Outcome"`
	Tokens  int64  `json:"Tokens"`
}

// Utilization is the mean observed window as a percentage of its limit.
type Utilization struct {
	RPM float64
	TPM float64
}

// MeanUtilization computes utilization across samples: mean(observed)/mean(configured) × 100.
func MeanUtilization(samples []WindowSample) Utilization {
	if len(samples) == 0 {
		return Utilization{}
	}
	var obsRPM, cfgRPM, obsTPM, cfgTPM int64
	for _, s := range samples {
		obsRPM += s.ObservedRPM
		cfgRPM += s.ConfiguredRPM
		obsTPM += s.ObservedTPM
		cfgTPM += s.ConfiguredTPM
	}
	n := int64(len(samples))
	u := Utilization{}
	if cfgRPM > 0 {
		u.RPM = float64(obsRPM) / float64(cfgRPM) * 100
	}
	if cfgTPM > 0 {
		u.TPM = float64(obsTPM) / float64(cfgTPM) * 100
	}
	_ = n
	return u
}

// OverLimit returns samples whose observed window exceeds its configured
// limit beyond tolerance (a fraction of the limit, absorbing sampling/
// scheduler jitter — an observed 30100 against a 30000 limit at 0.5%
// tolerance still passes).
func OverLimit(samples []WindowSample, tolerance float64) []int {
	var over []int
	for i, s := range samples {
		if s.ConfiguredRPM > 0 && float64(s.ObservedRPM) > float64(s.ConfiguredRPM)*(1+tolerance) {
			over = append(over, i)
			continue
		}
		if s.ConfiguredTPM > 0 && float64(s.ObservedTPM) > float64(s.ConfiguredTPM)*(1+tolerance) {
			over = append(over, i)
		}
	}
	return over
}

// IDsUnique reports whether every result row's id appears exactly once.
func IDsUnique(rows []ResultRow) bool {
	seen := make(map[string]bool, len(rows))
	for _, r := range rows {
		if seen[r.ID] {
			return false
		}
		seen[r.ID] = true
	}
	return true
}

// Config is one scenario run's parameters (from configs/scenarioN.yaml).
type Config struct {
	RatePerSec     int64         `yaml:"rate_per_sec"`
	DurationSec    int           `yaml:"duration_sec"`
	BatchSize      int           `yaml:"batch_size"`
	Tokens         int64         `yaml:"tokens"`
	Model          string        `yaml:"model"`
	SecondModel    string        `yaml:"second_model"`
	WarmupSec      int           `yaml:"warmup_sec"`
	SampleEverySec int           `yaml:"sample_every_sec"`
	Headroom       float64       `yaml:"headroom"` // simulator limits = system × headroom
	MinUtilPct     float64       `yaml:"min_util_pct"`
	Phase1RPM      int64         `yaml:"phase1_rpm"`
	Phase2RPM      int64         `yaml:"phase2_rpm"`
	Phase1Sec      int           `yaml:"phase1_sec"`
	Phase2Sec      int           `yaml:"phase2_sec"`
	Phase3Sec      int           `yaml:"phase3_sec"`
	BatchCount     int64         `yaml:"batch_count"`
	CallbackFails  int           `yaml:"callback_fails"`
}

// Result is one scenario's pass/fail evidence.
type Result struct {
	Pass       bool           `json:"pass"`
	Reasons    []string       `json:"reasons,omitempty"`
	Util       Utilization    `json:"utilization_pct"`
	OverLimitN int            `json:"over_limit_samples"`
	TimeSeries []TimePoint    `json:"time_series,omitempty"`
	Batches    []BatchSummary `json:"batches,omitempty"`
}

// TimePoint is one configured-vs-observed sample with its phase label.
type TimePoint struct {
	AtSec  float64      `json:"at_sec"`
	Phase  string       `json:"phase"`
	Sample WindowSample `json:"sample"`
}

// BatchSummary is one batch's completion evidence.
type BatchSummary struct {
	ID         string      `json:"id"`
	Status     string      `json:"status"`
	Successful int64       `json:"successful"`
	Failed     int64       `json:"failed"`
	Expired    int64       `json:"expired"`
	Rejected   int64       `json:"rejected"`
	Results    []ResultRow `json:"results,omitempty"`
	UniqueIDs  bool        `json:"unique_ids"`
}

// ---------- HTTP helpers ----------

func getJSON[T any](client *http.Client, url string, out *T) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("GET %s: status %d body %s", url, resp.StatusCode, body)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func postJSON(client *http.Client, url string, in any) error {
	body, _ := json.Marshal(in)
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST %s: status %d body %s", url, resp.StatusCode, body)
	}
	return nil
}

// batchStatus is GET /batches/{id}'s shape.
type batchStatus struct {
	BatchID    string `json:"batch_id"`
	Status     string `json:"status"`
	Successful int64  `json:"successful"`
	Failed     int64  `json:"failed"`
	Expired    int64  `json:"expired"`
	Rejected   int64  `json:"rejected"`
	Total      int64  `json:"total"`
}

// sampler samples a model's windows every interval, labelling each point
// with its phase, until the stop channel closes.
func sampler(client *http.Client, systemURL, model string, every time.Duration, phase func() string, stop <-chan struct{}, out *[]TimePoint, mu *sync.Mutex) {
	t := time.NewTicker(every)
	defer t.Stop()
	start := time.Now()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			var ws WindowSample
			if err := getJSON(client, systemURL+"/metrics/"+model, &ws); err != nil {
				continue // sample lost; the report shows gaps
			}
			mu.Lock()
			*out = append(*out, TimePoint{AtSec: time.Since(start).Seconds(), Phase: phase(), Sample: ws})
			mu.Unlock()
		}
	}
}

// runLoad drives the load generator's real mode and awaits every batch.
func runLoad(client *http.Client, systemURL string, sc *Config, mix map[string]float64) ([]string, error) {
	// a minimal model registry so loadgen's mix validation passes
	models := make([]config.ModelConfig, 0, 2)
	for name := range mix {
		models = append(models, config.ModelConfig{Name: name, RPM: 1 << 40, TPM: 1 << 50})
	}
	cfg := &config.Config{Models: models}
	tgt := loadgen.NewHTTPTarget(systemURL, client)
	lc := &loadgen.LoadConfig{
		RatePerSec:     sc.RatePerSec,
		Duration:       time.Duration(sc.DurationSec) * time.Second,
		ModelMix:       mix,
		BatchSize:      sc.BatchSize,
		EstimateTokens: sc.Tokens,
	}
	if err := loadgen.Run(context.Background(), lc, cfg, tgt); err != nil {
		return nil, err
	}
	if _, err := tgt.AwaitCompletions(10*time.Minute, 200*time.Millisecond); err != nil {
		return tgt.BatchIDs(), err
	}
	return tgt.BatchIDs(), nil
}

// fetchSummaries collects every batch's completion evidence.
func fetchSummaries(client *http.Client, systemURL string, ids []string) ([]BatchSummary, error) {
	out := make([]BatchSummary, 0, len(ids))
	for _, id := range ids {
		var bs batchStatus
		if err := getJSON(client, systemURL+"/batches/"+id, &bs); err != nil {
			return out, err
		}
		sum := BatchSummary{
			ID: id, Status: bs.Status,
			Successful: bs.Successful, Failed: bs.Failed, Expired: bs.Expired, Rejected: bs.Rejected,
			UniqueIDs: true,
		}
		var rows []ResultRow
		if err := getJSON(client, systemURL+"/batches/"+id+"/results", &rows); err == nil {
			sum.Results = rows
			sum.UniqueIDs = IDsUnique(rows)
		}
		out = append(out, sum)
	}
	return out, nil
}

// writeEvidence writes the JSON + summary files.
func writeEvidence(outDir, name string, res *Result) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, name+".json"), raw, 0o644); err != nil {
		return err
	}
	var b bytes.Buffer
	fmt.Fprintf(&b, "scenario %s: PASS=%v\n", name, res.Pass)
	for _, r := range res.Reasons {
		fmt.Fprintf(&b, "  - %s\n", r)
	}
	fmt.Fprintf(&b, "utilization: rpm=%.1f%% tpm=%.1f%%\n", res.Util.RPM, res.Util.TPM)
	fmt.Fprintf(&b, "over-limit samples: %d\n", res.OverLimitN)
	return os.WriteFile(filepath.Join(outDir, name+"-summary.txt"), b.Bytes(), 0o644)
}
