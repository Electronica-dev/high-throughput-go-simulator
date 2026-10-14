// benchmark.go runs the in-process benchmark: engine + load generator + drain
// + report. The HTTP surface is a later phase; everything here is in one
// process (see README for the quantified scope caveat).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"consuma/internal/config"
	"consuma/internal/engine"
	"consuma/internal/loadgen"

	"gopkg.in/yaml.v3"
)

// runArgs is the parsed flag set for one benchmark run.
type runArgs struct {
	Rate     int64
	Duration time.Duration
	Config   string
	ModelMix string
	Batch    int
	Tokens   int64
	Report   string
}

// parseRunFlags builds runArgs from a flag set.
func parseRunFlags(fs *flag.FlagSet) *runArgs {
	ra := &runArgs{}
	fs.Int64Var(&ra.Rate, "rate", 0, "target submitted requests/second")
	fs.DurationVar(&ra.Duration, "duration", 0, "run duration (e.g. 60s)")
	fs.StringVar(&ra.Config, "config", "configs/benchmark.yaml", "model/limit config file")
	fs.StringVar(&ra.ModelMix, "models", "", "model mix as name:share,... (default: single model evenly)")
	fs.IntVar(&ra.Batch, "batch", 1, "requests per submission batch")
	fs.Int64Var(&ra.Tokens, "tokens", 1000, "approximate token size per request")
	fs.StringVar(&ra.Report, "report", "", "write JSON report to this path")
	return ra
}

func cmdBenchmark(argv []string) {
	fs := flag.NewFlagSet("benchmark", flag.ExitOnError)
	ra := parseRunFlags(fs)
	if err := fs.Parse(argv); err != nil {
		os.Exit(2)
	}
	s, err := runBenchmark(*ra)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	printSummary(s)
}

// runBenchmark executes one full benchmark run and returns the summary.
func runBenchmark(ra runArgs) (*engine.Summary, error) {
	if err := validateRunArgs(ra.Rate, ra.Duration); err != nil {
		return nil, err
	}
	cfg, err := loadConfigFile(ra.Config)
	if err != nil {
		return nil, err
	}
	mix := map[string]float64{}
	if ra.ModelMix == "" {
		mix[cfg.Models[0].Name] = 1.0
	} else {
		for _, part := range strings.Split(ra.ModelMix, ",") {
			kv := strings.SplitN(strings.TrimSpace(part), ":", 2)
			if len(kv) != 2 {
				return nil, fmt.Errorf("bad model mix entry %q (want name:share)", part)
			}
			share, err := strconv.ParseFloat(kv[1], 64)
			if err != nil {
				return nil, fmt.Errorf("bad share in %q: %v", part, err)
			}
			mix[kv[0]] = share
		}
	}

	eng, err := engine.New(cfg)
	if err != nil {
		return nil, err
	}
	defer eng.Stop()

	// live limit changes: watch the config file while the run is going
	stop := make(chan struct{})
	doneWatch := make(chan struct{})
	go func() {
		defer close(doneWatch)
		watchConfig(ra.Config, eng, stop)
	}()

	lc := &loadgen.LoadConfig{
		RatePerSec:     ra.Rate,
		Duration:       ra.Duration,
		ModelMix:       mix,
		BatchSize:      ra.Batch,
		EstimateTokens: ra.Tokens,
	}
	runErr := loadgen.Run(context.Background(), lc, cfg, eng)
	close(stop)
	<-doneWatch

	s := eng.Drain()
	if ra.Report != "" {
		writeReportJSON(ra.Report, &s, eng, cfg)
	}
	return &s, runErr
}

// printSummary is the Task 11 stub; replaced by the report writer.
func printSummary(s *engine.Summary) {
	fmt.Printf("submitted=%d completed=%d ok=%d failed=%d expired=%d rejected=%d\n",
		s.Submitted, s.Completed, s.OK, s.Failed, s.Expired, s.Rejected)
	fmt.Printf("steady-state: %.0f/sec  whole-run avg: %.0f/sec  drain: %.2fs\n",
		s.SteadyStatePerSec, s.WholeRunAvgPerSec, s.DrainSec)
}

// writeReportJSON is wired to internal/report in Task 11.
func writeReportJSON(path string, s *engine.Summary, eng *engine.Engine, cfg *config.Config) {
	fmt.Fprintf(os.Stderr, "report writing arrives in Task 11; asked for %s\n", path)
}

// loadConfigFile and marshalConfig are the cmd-side config I/O helpers.
func loadConfigFile(path string) (*config.Config, error) {
	return config.Load(path)
}

func marshalConfig(cfg *config.Config) ([]byte, error) {
	return yaml.Marshal(cfg)
}
