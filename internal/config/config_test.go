package config

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const fixtureYAML = `
max_wait_ms: 5000
queue_depth: 4096
wheel_shards: 8
wheel_buckets: 4096
wheel_tick_ms: 1
models:
  - name: model-a
    rpm: 30000
    tpm: 60000000
    estimate_tokens: 1000
    actual_token_mean: 1050
    latency:
      kind: fixed
      mean_ms: 50
    outcome:
      success_pct: 93
      transient_pct: 5
      permanent_pct: 2
  - name: model-b
    rpm: 20000
    tpm: 40000000
    estimate_tokens: 800
    actual_token_mean: 850
    latency:
      kind: uniform
      min_ms: 20
      max_ms: 80
    outcome:
      success_pct: 95
      transient_pct: 3
      permanent_pct: 2
`

func writeFixture(t *testing.T, yml string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cfg.yaml")
	if err := os.WriteFile(path, []byte(yml), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadModels(t *testing.T) {
	cfg, err := Load(writeFixture(t, fixtureYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Models) != 2 {
		t.Fatalf("want 2 models, got %d", len(cfg.Models))
	}
	a := cfg.Models[0]
	if a.Name != "model-a" || a.RPM != 30000 || a.TPM != 60000000 {
		t.Fatalf("model-a mismatch: %+v", a)
	}
	if a.EstimateTokens != 1000 || a.ActualTokenMean != 1050 {
		t.Fatalf("token fields mismatch: %+v", a)
	}
	if cfg.QueueDepth != 4096 || cfg.WheelShards != 8 || cfg.WheelBuckets != 4096 || cfg.WheelTickMS != 1 {
		t.Fatalf("wheel/queue config mismatch: %+v", cfg)
	}
}

func TestValidateOK(t *testing.T) {
	cfg, err := Load(writeFixture(t, fixtureYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	horizon := time.Duration(cfg.WheelBuckets) * time.Duration(cfg.WheelTickMS) * time.Millisecond
	if err := cfg.Validate(horizon); err != nil {
		t.Fatalf("Validate on fixture: %v", err)
	}
}

func TestOutcomeSumValidation(t *testing.T) {
	bad := `
models:
  - name: bad
    rpm: 100
    tpm: 1000
    estimate_tokens: 10
    latency:
      kind: fixed
      mean_ms: 1
    outcome:
      success_pct: 93
      transient_pct: 5
      permanent_pct: 1
`
	_, err := Load(writeFixture(t, bad))
	if err == nil {
		t.Fatal("want error for outcome pct summing to 99")
	}
	if !contains(err.Error(), "outcome") {
		t.Fatalf("error should mention outcome, got: %v", err)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

func TestFixedLatencyDraw(t *testing.T) {
	lc := LatencyConfig{Kind: "fixed", MeanMS: 50}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 10; i++ {
		if got := lc.Draw(rng); got != 50*time.Millisecond {
			t.Fatalf("fixed draw: want 50ms, got %v", got)
		}
	}
}

func TestUniformBounds(t *testing.T) {
	lc := LatencyConfig{Kind: "uniform", MinMS: 20, MaxMS: 80}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 1000; i++ {
		d := lc.Draw(rng)
		ms := float64(d) / float64(time.Millisecond)
		if ms < 20.0 || ms > 80.0 {
			t.Fatalf("uniform draw out of bounds at i=%d: %v", i, d)
		}
	}
}

func TestModelLookup(t *testing.T) {
	cfg, err := Load(writeFixture(t, fixtureYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if m := Model(cfg, "model-b"); m == nil || m.RPM != 20000 {
		t.Fatalf("Model(model-b) mismatch: %+v", m)
	}
	if m := Model(cfg, "nope"); m != nil {
		t.Fatalf("Model(nope) should be nil, got %+v", m)
	}
}

func TestConfigRejectsLatencyOverHorizon(t *testing.T) {
	yml := `
wheel_buckets: 8192
wheel_tick_ms: 1
models:
  - name: slow
    rpm: 100
    tpm: 1000
    estimate_tokens: 10
    latency:
      kind: fixed
      mean_ms: 5000
    outcome:
      success_pct: 100
`
	cfg, err := Load(writeFixture(t, yml))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	err = cfg.Validate(4 * time.Second)
	if err == nil {
		t.Fatal("want error for latency beyond wheel horizon")
	}
	if !contains(err.Error(), "horizon") {
		t.Fatalf("error should mention horizon, got: %v", err)
	}
}
