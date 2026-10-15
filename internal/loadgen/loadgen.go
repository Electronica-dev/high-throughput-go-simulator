// Package loadgen paces request submission at a target rate with ramp-up,
// hold, and stop phases. Steady-state boundaries are marked on the engine so
// the report's headline number excludes ramp-up.
package loadgen

import (
	"sync"
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
	var pickMu sync.Mutex
	pick := func() string {
		pickMu.Lock()
		roll := rng.Float64()
		pickMu.Unlock()
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
	// MarkSteady fires when the hold phase actually starts (not upfront), so
	// the engine's completedAtStart snapshot excludes ramp-phase completions.

	// Pacing: one goroutine per worker, each owning 1/workers of the rate
	// with its own 1ms ticker (the single-ticker shape saturates around
	// 280K submits/sec; the per-worker split scales with cores).
	workers := int(lc.RatePerSec / 50000)
	if workers < 1 {
		workers = 1
	}
	if workers > 32 {
		workers = 32
	}
	perWorker := lc.RatePerSec / int64(workers)
	extra := lc.RatePerSec - perWorker*int64(workers) // remainder to first workers
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	var steadyOnce sync.Once
	for wk := 0; wk < workers; wk++ {
		pw := perWorker
		if wk < int(extra) {
			pw++
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := pace(ctx, lc, pw, startNS, rampNS, durNS, batchSize, tokens, pick, target, &steadyOnce); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// pace runs one worker's pacing loop at perWorker req/sec. holdMarked fires
// MarkSteady exactly once (via steadyOnce) when the hold phase begins.
func pace(ctx context.Context, lc *LoadConfig, perWorker int64, startNS, rampNS, durNS int64, batchSize int, tokens int64, pick func() string, target RunTarget, steadyOnce *sync.Once) error {
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
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	// Catch-up pacing: the budget derives from elapsed wall time with a
	// nanosecond accumulator, so no fractional request is lost to per-tick
	// truncation and ticks missed to GC pauses are recovered exactly.
	var lastNS int64
	var carryNS int64
	var sentHold int64 // submissions since ramp end (for the final flush)
	holdMarked := false
	for {
		select {
		case <-ctx.Done():
			flush()
			return ctx.Err()
		case <-ticker.C:
			nowNS := time.Now().UnixNano()
			if lastNS == 0 {
				lastNS = nowNS
			}
			if nowNS-startNS >= durNS {
				// final flush: deliver this worker's remaining hold-window
				// budget so total submissions conserve rate x duration —
				// the pacer must not silently undershoot its target at the
				// boundary (the 0.004% headline miss the review caught)
				if lc.RampUp == 0 {
					// promise the full fractional-second budget (rate ×
					// duration exact): a 500/sec × 200ms run owes exactly 100
					promised := int64(float64(perWorker) * float64(durNS) / float64(time.Second))
					remaining := promised - sentHold
					for i := int64(0); i < remaining; i++ {
						pending = append(pending, &ledger.Request{
							ID:             reqID.Add(1),
							Model:          pick(),
							EstimateTokens: tokens,
							BornNS:         time.Now().UnixNano(),
						})
						if len(pending) >= batchSize {
							flush()
						}
					}
				}
				flush()
				return nil
			}
			if !holdMarked && nowNS-startNS >= rampNS {
				holdMarked = true
				steadyOnce.Do(func() {
					target.MarkSteady(nowNS, startNS+durNS)
				})
			}
			dt := nowNS - lastNS
			lastNS = nowNS
			carryNS += perWorker * dt
			budget := carryNS / int64(time.Second)
			carryNS %= int64(time.Second)
			// ramp-up: linear scale from 0 to full rate
			if rampNS > 0 && nowNS-startNS < rampNS {
				frac := float64(nowNS-startNS) / float64(rampNS)
				budget = int64(float64(budget) * frac)
			}
			for i := int64(0); i < budget; i++ {
				pending = append(pending, &ledger.Request{
					ID:             reqID.Add(1),
					Model:          pick(),
					EstimateTokens: tokens,
					BornNS:         time.Now().UnixNano(),
				})
				sentHold++
				if len(pending) >= batchSize {
					flush()
				}
			}
		}
	}
}
