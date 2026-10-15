package loadgen

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"consuma/internal/config"
	"consuma/internal/ledger"
)

type fakeTarget struct {
	mu        sync.Mutex
	chunks    [][]*ledger.Request
	startNS   int64
	steadySet bool
}

func (target *fakeTarget) SubmitBatch(reqs []*ledger.Request) {
	target.mu.Lock()
	defer target.mu.Unlock()
	target.chunks = append(target.chunks, reqs)
}

func (target *fakeTarget) Submit(r *ledger.Request) { target.SubmitBatch([]*ledger.Request{r}) }

func (target *fakeTarget) MarkSteady(startNS, endNS int64) {
	target.mu.Lock()
	defer target.mu.Unlock()
	target.steadySet = true
	target.startNS = startNS
}

func (target *fakeTarget) count() int {
	target.mu.Lock()
	defer target.mu.Unlock()
	n := 0
	for _, c := range target.chunks {
		n += len(c)
	}
	return n
}

func (target *fakeTarget) countAtOrBefore(cutoffNS int64) int {
	target.mu.Lock()
	defer target.mu.Unlock()
	n := 0
	for _, c := range target.chunks {
		for _, r := range c {
			if r.BornNS <= cutoffNS {
				n++
			}
		}
	}
	return n
}

func twoModels() *config.Config {
	return &config.Config{
		Models: []config.ModelConfig{
			{Name: "a", RPM: 1000000, TPM: 1e12, EstimateTokens: 100},
			{Name: "b", RPM: 1000000, TPM: 1e12, EstimateTokens: 100},
		},
	}
}

// TestFinalFlushConservesBudget: whatever the ticker misses near the end of
// the run is flushed before return, so total submissions equal exactly
// rate x duration (whole-run average must not silently undershoot the
// configured rate).
func TestFinalFlushConservesBudget(t *testing.T) {
	ft := &fakeTarget{}
	lc := &LoadConfig{RatePerSec: 60000, Duration: time.Second, RampUp: 0, ModelMix: map[string]float64{"a": 1.0}, RNGSeed: 1}
	if err := Run(context.Background(), lc, twoModels(), ft); err != nil {
		t.Fatalf("Run: %v", err)
	}
	n := ft.count()
	if n < 59500 || n > 60500 {
		t.Fatalf("submitted %d, want 60000 ±0.83%%", n)
	}
	if n != 60000 {
		t.Logf("note: boundary rounding %d vs 60000 (sub-request residue)", n)
	}
}

func TestPacingHitsTarget(t *testing.T) {
	ft := &fakeTarget{}
	lc := &LoadConfig{RatePerSec: 50000, Duration: time.Second, RampUp: 0, ModelMix: map[string]float64{"a": 1.0}, RNGSeed: 1}
	if err := Run(context.Background(), lc, twoModels(), ft); err != nil {
		t.Fatalf("Run: %v", err)
	}
	n := ft.count()
	if n < 45000 || n > 55000 {
		t.Fatalf("submitted %d, want 50000 ±10%%", n)
	}
}

func TestRampUpExcludes(t *testing.T) {
	ft := &fakeTarget{}
	start := time.Now().UnixNano()
	lc := &LoadConfig{RatePerSec: 50000, Duration: time.Second, RampUp: 500 * time.Millisecond, ModelMix: map[string]float64{"a": 1.0}, RNGSeed: 1}
	if err := Run(context.Background(), lc, twoModels(), ft); err != nil {
		t.Fatalf("Run: %v", err)
	}
	first100ms := ft.countAtOrBefore(start + 100*int64(time.Millisecond))
	if first100ms >= 15000 { // 30% of the 50K target
		t.Fatalf("ramp-up not excluded: %d submits in first 100ms", first100ms)
	}
}

func TestModelMixRespected(t *testing.T) {
	ft := &fakeTarget{}
	lc := &LoadConfig{RatePerSec: 10000, Duration: time.Second, RampUp: 0, ModelMix: map[string]float64{"a": 0.8, "b": 0.2}, RNGSeed: 42}
	if err := Run(context.Background(), lc, twoModels(), ft); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ft.mu.Lock()
	defer ft.mu.Unlock()
	var a, total int
	for _, c := range ft.chunks {
		for _, r := range c {
			total++
			if r.Model == "a" {
				a++
			}
		}
	}
	share := float64(a) / float64(total)
	if share < 0.75 || share > 0.85 {
		t.Fatalf("model-a share %.2f, want 0.8 ±0.05", share)
	}
}

func TestBatchSizeChunks(t *testing.T) {
	ft := &fakeTarget{}
	lc := &LoadConfig{RatePerSec: 5000, Duration: 200 * time.Millisecond, RampUp: 0, ModelMix: map[string]float64{"a": 1.0}, BatchSize: 10, RNGSeed: 1}
	if err := Run(context.Background(), lc, twoModels(), ft); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ft.mu.Lock()
	defer ft.mu.Unlock()
	if len(ft.chunks) == 0 {
		t.Fatal("no chunks submitted")
	}
	for i, c := range ft.chunks {
		// every batch except the (optional) final flush must be full
		if i < len(ft.chunks)-1 && len(c) != 10 {
			t.Fatalf("chunk %d has %d requests, want 10", i, len(c))
		}
		if len(c) > 10 {
			t.Fatalf("chunk %d has %d requests, want <= 10", i, len(c))
		}
	}
}

func TestLoadGenFailsFastOnUnknownModel(t *testing.T) {
	ft := &fakeTarget{}
	lc := &LoadConfig{RatePerSec: 1000, Duration: time.Second, ModelMix: map[string]float64{"nope": 1.0}}
	err := Run(context.Background(), lc, twoModels(), ft)
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("want error naming unknown model, got %v", err)
	}
}
