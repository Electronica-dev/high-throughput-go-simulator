package engine

import (
	"os"
	"runtime/pprof"
	"testing"

	"consuma/internal/config"
	"consuma/internal/ledger"
)

func BenchmarkEngineHot(b *testing.B) {
	cfg := &config.Config{
		MaxWaitMS: 50, QueueDepth: 65536, WheelShards: 8, WheelBuckets: 4096, WheelTickMS: 1, MaxRetries: 3,
		Models: []config.ModelConfig{{
			Name: "m", RPM: 500000000, TPM: 10000000000000, EstimateTokens: 1000, ActualTokenMean: 1050,
			Latency: config.LatencyConfig{Kind: "lognormal", MeanMS: 50, Sigma: 0.3},
			Outcome: config.OutcomeConfig{SuccessPct: 93, TransientPct: 5, PermanentPct: 2},
		}},
	}
	engine, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Stop()
	f, _ := os.Create("C:/Users/sammy/AppData/Local/hermes/cache/scratch/engine-cpu.prof")
	pprof.StartCPUProfile(f)
	defer func() {
		pprof.StopCPUProfile()
		f.Close()
	}()
	b.ResetTimer()
	s := engine.Drain // avoid unused warning shape
	_ = s
	for i := 0; i < b.N; i++ {
		engine.Submit(&ledger.Request{ID: uint64(i + 1), Model: "m", EstimateTokens: 1000, BornNS: 0})
	}
	engine.Drain()
}
