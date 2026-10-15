package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeConfig(t *testing.T, dir string, rpm int64) string {
	t.Helper()
	content := `
max_wait_ms: 50
queue_depth: 16
wheel_shards: 2
wheel_buckets: 256
wheel_tick_ms: 1
max_retries: 3
models:
  - name: model-a
    rpm: 1000000
    tpm: 1000000000
    estimate_tokens: 100
    actual_token_mean: 100
    latency:
      kind: fixed
      mean_ms: 1
    outcome:
      success_pct: 90
      transient_pct: 5
      permanent_pct: 5
`
	path := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCLIRunsValidateArgs(t *testing.T) {
	if err := validateRunArgs(0, time.Second); err == nil || err.Error() != "rate must be > 0" {
		t.Fatalf("want rate error, got %v", err)
	}
	if err := validateRunArgs(1000, 0); err == nil || err.Error() != "duration must be > 0" {
		t.Fatalf("want duration error, got %v", err)
	}
	if err := validateRunArgs(1000, time.Second); err != nil {
		t.Fatalf("valid args rejected: %v", err)
	}
}

func TestConfigFileWatchAppliesLimits(t *testing.T) {
	dir := t.TempDir()
	path := writeConfig(t, dir, 1000000)
	eng, err := newEngineFromConfig(path)
	if err != nil {
		t.Fatalf("newEngineFromConfig: %v", err)
	}
	defer eng.Stop()

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		watchConfig(path, eng, stop)
	}()

	// rewrite the config with a reduced RPM, like `htsim limits` does
	if err := applyLimitToFile(path, "model-a", 12345, 1000000000); err != nil {
		t.Fatalf("applyLimitToFile: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rpm, _ := eng.Limiter("model-a").Limits()
		if rpm == 12345 {
			close(stop)
			<-done
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(stop)
	<-done
	t.Fatal("watcher did not apply reduced limit within 2s")
}

func TestBenchmarkEndToEnd(t *testing.T) {
	dir := t.TempDir()
	path := writeConfig(t, dir, 1000000)
	summary, err := runBenchmark(runArgs{
		Rate:     1000,
		Duration: 200 * time.Millisecond,
		Config:   path,
		ModelMix: "model-a:1.0",
		Batch:    1,
		Tokens:   100,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if summary.Submitted == 0 {
		t.Fatal("no requests submitted")
	}
	if summary.Submitted != summary.Completed+summary.Expired+summary.Rejected {
		t.Fatalf("unbalanced: %+v", summary)
	}
}
