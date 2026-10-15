package report

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"htsim/internal/config"
	"htsim/internal/engine"
	"htsim/internal/ledger"
)

func sampleReport(t *testing.T) *Report {
	t.Helper()
	cfg := &config.Config{
		MaxWaitMS: 50, QueueDepth: 16, WheelShards: 2, WheelBuckets: 256, WheelTickMS: 1, MaxRetries: 3,
		Models: []config.ModelConfig{{
			Name: "model-a", RPM: 30000, TPM: 60000000, EstimateTokens: 100, ActualTokenMean: 100,
			Latency: config.LatencyConfig{Kind: "fixed", MeanMS: 1},
			Outcome: config.OutcomeConfig{SuccessPct: 100},
		}}}
	eng, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	defer eng.Stop()
	for i := 1; i <= 100; i++ {
		eng.SubmitBatch([]*ledger.Request{{ID: uint64(i), Model: "model-a", EstimateTokens: 100}})
	}
	s := eng.Drain()
	return Build(s, eng, cfg)
}

func TestJSONStructure(t *testing.T) {
	report := sampleReport(t)
	var buf bytes.Buffer
	if err := WriteJSON(&buf, report); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("json: %v", err)
	}
	if _, ok := out["measured"]; !ok {
		t.Fatal("top-level 'measured' key missing")
	}
	proj, ok := out["projections"].([]any)
	if !ok || len(proj) == 0 {
		t.Fatal("'projections' must be a non-empty list")
	}
	for _, p := range proj {
		if _, ok := p.(string); !ok {
			t.Fatalf("projections entries must be strings, got %T", p)
		}
	}
}

func TestTextSeparatesSections(t *testing.T) {
	report := sampleReport(t)
	var buf bytes.Buffer
	if err := WriteText(&buf, report); err != nil {
		t.Fatalf("WriteText: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "MEASURED") || !strings.Contains(out, "PROJECTIONS") {
		t.Fatalf("text report must separate MEASURED and PROJECTIONS sections:\n%s", out)
	}
}

func TestPercentilesPresent(t *testing.T) {
	report := sampleReport(t)
	l, ok := report.Measured.Latency["model-a"]
	if !ok {
		t.Fatalf("no latency entry for model-a: %v", report.Measured.Latency)
	}
	if l.P50MS <= 0 || l.P95MS <= 0 || l.P99MS <= 0 {
		t.Fatalf("want populated p50/p95/p99, got %+v", l)
	}
}
