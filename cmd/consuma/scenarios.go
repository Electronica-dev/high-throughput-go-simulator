// scenarios.go: the `consuma scenario1|2|3` subcommands — validation
// scenario runners against a running system + simulator (evidence written
// to -out).
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"consuma/internal/scenarios"
)

func parseScenarioArgs(argv []string, name string) (string, string, scenarios.Config, string, string, error) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	system := fs.String("system", "http://127.0.0.1:9000", "system URL")
	simulator := fs.String("simulator", "http://127.0.0.1:9001", "simulator URL")
	out := fs.String("out", "docs/validation", "evidence output directory")
	callback := fs.String("callback", "", "external callback sink URL (S3: empty = managed reject-first sink)")
	var sc scenarios.Config
	fs.Int64Var(&sc.RatePerSec, "rate", 0, "submitted requests/second (0 = scenario default)")
	fs.IntVar(&sc.DurationSec, "duration", 0, "run duration seconds (0 = scenario default)")
	fs.IntVar(&sc.BatchSize, "batch", 0, "requests per batch (0 = scenario default)")
	fs.Int64Var(&sc.Tokens, "tokens", 100, "estimated tokens per request")
	fs.StringVar(&sc.Model, "model", "", "primary model (0 = scenario default)")
	fs.StringVar(&sc.SecondModel, "second-model", "", "secondary model (S2/S3)")
	fs.IntVar(&sc.WarmupSec, "warmup", 0, "warm-up seconds excluded from S1 utilization")
	fs.IntVar(&sc.SampleEverySec, "sample", 1, "window sampling interval seconds")
	fs.Float64Var(&sc.Headroom, "headroom", 10, "simulator headroom multiplier")
	fs.Float64Var(&sc.MinUtilPct, "min-util", 90, "S1 minimum utilization percent")
	if err := fs.Parse(argv); err != nil {
		return "", "", sc, "", "", err
	}
	return *system, *simulator, sc, *out, *callback, nil
}

// scenarioDefaults fills unset fields from the scenario's baseline params.
func scenarioDefaults(sc *scenarios.Config, rate int64, dur, batch int, model, second string) {
	if sc.RatePerSec == 0 {
		sc.RatePerSec = rate
	}
	if sc.DurationSec == 0 {
		sc.DurationSec = dur
	}
	if sc.BatchSize == 0 {
		sc.BatchSize = batch
	}
	if sc.Tokens == 0 {
		sc.Tokens = 100
	}
	if sc.Model == "" {
		sc.Model = model
	}
	if sc.SecondModel == "" {
		sc.SecondModel = second
	}
	if sc.SampleEverySec == 0 {
		sc.SampleEverySec = 1
	}
	if sc.Headroom == 0 {
		sc.Headroom = 10
	}
	if sc.MinUtilPct == 0 {
		sc.MinUtilPct = 90
	}
}

// mixFromModel parses "name:share" pairs; single model default.
func mixFromModel(model, second string, share float64) map[string]float64 {
	mix := map[string]float64{model: share}
	if second != "" {
		mix[second] = 1 - share
	}
	return mix
}

func runScenarioN(n string, argv []string, run func(systemURL, callbackURL string, sc scenarios.Config, out string) (*scenarios.Result, error), defaults func(*scenarios.Config)) {
	system, _, sc, out, callback, err := parseScenarioArgs(argv, "scenario"+n)
	if err != nil {
		os.Exit(2)
	}
	defaults(&sc)
	res, err := run(system, callback, sc, out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Printf("scenario %s: PASS=%v (evidence in %s)\n", n, res.Pass, out)
	for _, c := range res.Charts {
		fmt.Println("  chart:", c)
	}
	for _, r := range res.Reasons {
		fmt.Println("  -", r)
	}
	if !res.Pass {
		os.Exit(1)
	}
}

// unused-parameter guard for the parse helpers below (kept for symmetry with
// the other subcommands' flag style).
var _ = strconv.Itoa
var _ = strings.Split
