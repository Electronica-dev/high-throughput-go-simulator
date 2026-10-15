// Caller abstracts "make one provider call": benchmark mode uses the
// in-process wheel-scheduled simulator; real mode uses an HTTP round trip
// to the standalone simulator service. Everything downstream (limiter,
// finalize, retries, accounting) is mode-agnostic.
package engine

import (
	"math/rand"
	"sync"
	"time"

	"htsim/internal/ledger"
	"htsim/internal/provider"
)

// Caller performs one simulated provider call for a request.
type Caller interface {
	Call(r *ledger.Request, now time.Time) provider.CallResult
}

// InProcessCaller routes calls to the per-model in-process provider
// (benchmark mode). RNG sources come from the engine's pool: provider draws
// are not safe for concurrent use on a single rand.Rand.
type InProcessCaller struct {
	mu     sync.RWMutex
	provs  map[string]*provider.Provider
	getRNG func() *rand.Rand
	putRNG func(*rand.Rand)
}

// NewInProcessCaller builds the benchmark-mode caller.
func NewInProcessCaller(provs map[string]*provider.Provider, getRNG func() *rand.Rand, putRNG func(*rand.Rand)) Caller {
	return &InProcessCaller{provs: provs, getRNG: getRNG, putRNG: putRNG}
}

// Call routes by request model and applies the provider's full semantics
// (backstop rejection, latency draw, outcome mix, token draw).
func (c *InProcessCaller) Call(r *ledger.Request, now time.Time) provider.CallResult {
	c.mu.RLock()
	p, ok := c.provs[r.Model]
	c.mu.RUnlock()
	if !ok {
		return provider.CallResult{Outcome: provider.ProviderRejected}
	}
	rng := c.getRNG()
	defer c.putRNG(rng)
	return p.Call(r, rng, now)
}
