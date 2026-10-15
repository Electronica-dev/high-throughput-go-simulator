// realbench.go: real-mode load generation (`benchmark -target URL`): the
// load generator drives a running `htsim serve` over HTTP; batch ids from
// each 202 are polled to completion as the run's completion evidence.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"htsim/internal/loadgen"
)

// realRunReport is the real-mode run summary printed to stdout.
type realRunReport struct {
	Target           string   `json:"target"`
	RatePerSec       int64    `json:"rate_per_sec"`
	DurationSec      float64  `json:"duration_sec"`
	BatchesSubmitted int      `json:"batches_submitted"`
	BatchesCompleted int      `json:"batches_completed"`
	RequestsExpected int64    `json:"requests_expected"`
	SubmissionErrors []string `json:"submission_errors,omitempty"`
}

// runRealBenchmark drives the load generator against a running system and
// reports batch-completion evidence.
func runRealBenchmark(ra runArgs) error {
	if err := validateRunArgs(ra.Rate, ra.Duration); err != nil {
		return err
	}
	cfg, err := loadConfigFile(ra.Config)
	if err != nil {
		return err
	}
	mix := map[string]float64{}
	if ra.ModelMix == "" {
		mix[cfg.Models[0].Name] = 1.0
	} else {
		for _, part := range strings.Split(ra.ModelMix, ",") {
			kv := strings.SplitN(strings.TrimSpace(part), ":", 2)
			if len(kv) != 2 {
				return fmt.Errorf("bad model mix entry %q (want name:share)", part)
			}
			share, err := strconv.ParseFloat(kv[1], 64)
			if err != nil {
				return fmt.Errorf("bad share in %q: %v", part, err)
			}
			mix[kv[0]] = share
		}
	}

	tgt := loadgen.NewHTTPTarget(ra.Target, nil)
	lc := &loadgen.LoadConfig{
		RatePerSec:     ra.Rate,
		Duration:       ra.Duration,
		ModelMix:       mix,
		BatchSize:      ra.Batch,
		EstimateTokens: ra.Tokens,
	}
	if err := loadgen.Run(context.Background(), lc, cfg, tgt); err != nil {
		return err
	}

	// completion evidence: poll every batch to completed
	completed, err := tgt.AwaitCompletions(5*time.Minute, 100*time.Millisecond)
	if err != nil {
		return err
	}

	rep := realRunReport{
		Target:           ra.Target,
		RatePerSec:       ra.Rate,
		DurationSec:      ra.Duration.Seconds(),
		BatchesSubmitted: len(tgt.BatchIDs()),
		BatchesCompleted: completed,
		RequestsExpected: ra.Rate * int64(ra.Duration.Seconds()),
	}
	for _, e := range tgt.Errors() {
		rep.SubmissionErrors = append(rep.SubmissionErrors, e.Error())
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}
