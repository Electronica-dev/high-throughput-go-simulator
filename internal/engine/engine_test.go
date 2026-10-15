package engine

import (
	"testing"
	"time"

	"htsim/internal/config"
	"htsim/internal/ledger"
)

func baseModel(name string) config.ModelConfig {
	return config.ModelConfig{
		Name:            name,
		RPM:             1000000,
		TPM:             1000000000000,
		EstimateTokens:  1000,
		ActualTokenMean: 1000,
		Latency:         config.LatencyConfig{Kind: "fixed", MeanMS: 1},
		Outcome:         config.OutcomeConfig{SuccessPct: 90, TransientPct: 5, PermanentPct: 5},
	}
}

func testConfig(mutate func(*config.Config)) *config.Config {
	cfg := &config.Config{
		Models:       []config.ModelConfig{baseModel("model-a")},
		MaxWaitMS:    50,
		QueueDepth:   10,
		WheelShards:  2,
		WheelBuckets: 256,
		WheelTickMS:  1,
		MaxRetries:   3,
	}
	if mutate != nil {
		mutate(cfg)
	}
	return cfg
}

func submitN(engine *Engine, n int) []*ledger.Request {
	rs := make([]*ledger.Request, n)
	for i := 0; i < n; i++ {
		rs[i] = &ledger.Request{ID: uint64(i + 1), Model: "model-a", EstimateTokens: 1000}
		engine.Submit(rs[i])
	}
	return rs
}

func allTerminal(engine *Engine, rs []*ledger.Request) bool {
	for _, r := range rs {
		switch ledger.State(r.State.Load()) {
		case ledger.Completed, ledger.Expired, ledger.Rejected:
		default:
			return false
		}
	}
	return true
}

func TestEngineBalancesAccounts(t *testing.T) {
	engine, err := New(testConfig(nil))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer engine.Stop()
	rs := submitN(engine, 1000)
	s := engine.Drain()
	if s.Submitted != 1000 {
		t.Fatalf("Submitted want 1000, got %d", s.Submitted)
	}
	if s.Submitted != s.Completed+s.Expired+s.Rejected {
		t.Fatalf("unbalanced: %+v", s)
	}
	if !allTerminal(engine, rs) {
		t.Fatal("not every request reached a terminal state")
	}
	if s.DrainSec > 2.0 {
		t.Fatalf("drain took too long: %.2fs", s.DrainSec)
	}
}

func TestEngineAllPermanentFailuresBalance(t *testing.T) {
	engine, err := New(testConfig(func(c *config.Config) {
		c.Models[0].Outcome = config.OutcomeConfig{SuccessPct: 0, TransientPct: 0, PermanentPct: 100}
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer engine.Stop()
	submitN(engine, 100)
	s := engine.Drain()
	if s.OK != 0 || s.Failed != 100 {
		t.Fatalf("want OK=0 Failed=100, got OK=%d Failed=%d", s.OK, s.Failed)
	}
	if s.Submitted != s.Completed+s.Expired+s.Rejected {
		t.Fatalf("unbalanced: %+v", s)
	}
}

func TestEngineQueueExpiry(t *testing.T) {
	engine, err := New(testConfig(func(c *config.Config) {
		c.Models[0].RPM = 100
		c.MaxWaitMS = 1
		c.QueueDepth = 10
	}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer engine.Stop()
	submitN(engine, 1000)
	s := engine.Drain()
	if s.Expired <= 0 {
		t.Fatalf("want some expired requests, got %+v", s)
	}
	if s.Submitted != s.Completed+s.Expired+s.Rejected {
		t.Fatalf("unbalanced: %+v", s)
	}
}

func TestEngineRetryTransient(t *testing.T) {
	engine, err := New(testConfig(func(c *config.Config) {
		c.Models[0].Outcome = config.OutcomeConfig{SuccessPct: 0, TransientPct: 100, PermanentPct: 0}
	}))
	engine.DebugTrackRequests = true
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer engine.Stop()
	submitN(engine, 20)
	s := engine.Drain()
	if s.Failed != 20 {
		t.Fatalf("all-transient with 3 retries should end Failed=20, got %+v", s)
	}
	lim := engine.Limiter("model-a")
	if lim == nil {
		t.Fatal("no limiter for model-a")
	}
	if got := lim.ObservedRPM(time.Now()); got < 20 {
		t.Fatalf("retries must count against RPM: observed %d < 20", got)
	}
	for _, r := range engine.requests() {
		a := int(r.Attempts.Load())
		if a < 1 || a > 3 {
			t.Fatalf("attempts out of range [1,3]: %d", a)
		}
	}
}

func TestSetLimitTakesEffectLive(t *testing.T) {
	engine, err := New(testConfig(nil))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer engine.Stop()
	submitN(engine, 500) // high limits: all admitted
	first := engine.Metrics().Get("admitted")
	if first < 500 {
		t.Fatalf("first phase should admit all, got %d", first)
	}
	if err := engine.SetLimit("model-a", 200, 1000000000000); err != nil {
		t.Fatalf("SetLimit: %v", err)
	}
	for i := 500; i < 1000; i++ {
		engine.Submit(&ledger.Request{ID: uint64(i + 1), Model: "model-a", EstimateTokens: 1000})
	}
	second := engine.Metrics().Get("admitted") - first
	if second >= first {
		t.Fatalf("reduced limit must lower admits: first=%d second=%d", first, second)
	}
}

func TestSetLimitUnknownModel(t *testing.T) {
	engine, err := New(testConfig(nil))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer engine.Stop()
	if err := engine.SetLimit("nope", 100, 100); err == nil {
		t.Fatal("SetLimit on unknown model should error")
	}
}
