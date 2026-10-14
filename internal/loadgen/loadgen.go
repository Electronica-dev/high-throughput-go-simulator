// Package loadgen paces request submission at a target rate with ramp-up,
// hold, and stop phases. Steady-state boundaries are marked on the engine so
// the report's headline number excludes ramp-up.
package loadgen

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"consuma/internal/config"
	"consuma/internal/ledger"
)

// RunTarget is what the load generator feeds. Engine implements it; tests
// use fakes.
type RunTarget interface {
	SubmitBatch(reqs []*ledger.Request)
	MarkSteady(startNS, endNS int64)
}

// LoadConfig is one load generation run.
type LoadConfig struct {
	RatePerSec     int64
	Duration       time.Duration
	ModelMix       map[string]float64 // model name -> share; must sum to ~1
	BatchSize      int
	EstimateTokens int64
	RampUp         time.Duration
	RNGSeed        int64
}

var reqID atomic.Uint64

// Run paces submissions until the duration elapses or ctx is cancelled.
func Run(ctx context.Context, lc *LoadConfig, cfg *config.Config, target RunTarget) error {
	if lc.RatePerSec <= 0 {
		return fmt.Errorf("loadgen: rate must be > 0, got %d", lc.RatePerSec)
	}
	if lc.Duration <= 0 {
		return fmt.Errorf("loadgen: duration must be > 0")
	}
	// fail fast on unknown models before any traffic
	for name := range lc.ModelMix {
		if config.Model(cfg, name) == nil {
			return fmt.Errorf("loadgen: model mix references unknown model %q", name)
		}
	}
	batchSize := lc.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	tokens := lc.EstimateTokens
	if tokens <= 0 {
		tokens = 1000
	}

	rng := rand.New(rand.NewSource(lc.RNGSeed))
	// cumulative weights for model selection
	cum := make([]float64, 0, len(lc.ModelMix))
	names := make([]string, 0, len(lc.ModelMix))
	var total float64
	for n, w := range lc.ModelMix {
		total += w
		names = append(names, n)
	}
	// deterministic order for cumulative walk: sort names
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[j] < names[i] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	var acc float64
	for _, n := range names {
		acc += lc.ModelMix[n] / total
		cum = append(cum, acc)
	}
	pick := func() string {
		roll := rng.Float64()
		for i, c := range cum {
			if roll < c {
				return names[i]
			}
		}
		return names[len(names)-1]
	}

	startNS := time.Now().UnixNano()
	rampNS := lc.RampUp.Nanoseconds()
	durNS := lc.Duration.Nanoseconds()
	target.MarkSteady(startNS+rampNS, startNS+durNS)

	perMS := lc.RatePerSec / 1000
	remainder := lc.RatePerSec % 1000
	pending := make([]*ledger.Request, 0, batchSize)
	flush := func() {
		if len(pending) > 0 {
			// copy: the receiver may retain the slice while we reuse the array
			batch := make([]*ledger.Request, len(pending))
			copy(batch, pending)
			target.SubmitBatch(batch)
			pending = pending[:0]
		}
	}
	newReq := func() *ledger.Request {
		return &ledger.Request{
			ID:             reqID.Add(1),
			Model:          pick(),
			EstimateTokens: tokens,
			BornNS:         time.Now().UnixNano(),
		}
	}

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	var elapsedMS int64
	for {
		select {
		case <-ctx.Done():
			flush()
			return ctx.Err()
		case <-ticker.C:
			elapsedMS++
			nowNS := time.Now().UnixNano()
			if nowNS-startNS >= durNS {
				flush()
				return nil
			}
			budget := perMS
			// distribute remainder ticks across the second
			if elapsedMS%1000 < remainder {
				budget++
			}
			// ramp-up: linear scale from 0 to full rate
			if rampNS > 0 && nowNS-startNS < rampNS {
				frac := float64(nowNS-startNS) / float64(rampNS)
				budget = int64(float64(budget) * frac)
			}
			for i := int64(0); i < budget; i++ {
				pending = append(pending, newReq())
				if len(pending) >= batchSize {
					flush()
				}
			}
		}
	}
}
