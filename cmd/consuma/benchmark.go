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

	"runtime/debug"

	"consuma/internal/config"
	"consuma/internal/engine"
	"consuma/internal/loadgen"
	"consuma/internal/report"

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
	Target   string
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
	fs.StringVar(&ra.Target, "target", "", "real mode: system URL (e.g. http://127.0.0.1:9000); in-process benchmark when empty")
	return ra
}

func cmdBenchmark(argv []string) {
	fs := flag.NewFlagSet("benchmark", flag.ExitOnError)
	ra := parseRunFlags(fs)
	if err := fs.Parse(argv); err != nil {
		os.Exit(2)
	}
	if ra.Target != "" {
		if err := runRealBenchmark(*ra); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}
	summary, err := runBenchmark(*ra)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	printSummary(summary)
}

// runBenchmark executes one full benchmark run and returns the summary.
func runBenchmark(ra runArgs) (*engine.Summary, error) {
	if err := validateRunArgs(ra.Rate, ra.Duration); err != nil {
		return nil, err
	}
	// Optional GC tuning for benchmark runs (e.g. CONSUMA_GOGC=400 reduces
	// GC pause frequency; the live set is tiny so the heap cost is small).
	// Documented in the README; results note whether it was set.
	if v := os.Getenv("CONSUMA_GOGC"); v != "" {
		if p, err := strconv.Atoi(v); err == nil && p > 0 {
			debug.SetGCPercent(p)
		}
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

	summary := eng.Drain()
	rep := report.Build(summary, eng, cfg).WithRun(ra.Rate, ra.Duration)
	if ra.Report != "" {
		f, err := os.Create(ra.Report)
		if err != nil {
			return nil, fmt.Errorf("open report %s: %w", ra.Report, err)
		}
		defer f.Close()
		if err := report.WriteJSON(f, rep); err != nil {
			return nil, fmt.Errorf("write report %s: %w", ra.Report, err)
		}
	}
	return &summary, runErr
}

// printSummary renders the human-readable run report to stdout.
func printSummary(summary *engine.Summary) {
	fmt.Printf("submitted=%d completed=%d ok=%d failed=%d expired=%d rejected=%d\n",
		summary.Submitted, summary.Completed, summary.OK, summary.Failed, summary.Expired, summary.Rejected)
	fmt.Printf("steady-state: %.0f/sec  whole-run avg: %.0f/sec  drain: %.2fs\n",
		summary.SteadyStatePerSec, summary.WholeRunAvgPerSec, summary.DrainSec)
}

// loadConfigFile and marshalConfig are the cmd-side config I/O helpers.
func loadConfigFile(path string) (*config.Config, error) {
	return config.Load(path)
}

func marshalConfig(cfg *config.Config) ([]byte, error) {
	return yaml.Marshal(cfg)
}
