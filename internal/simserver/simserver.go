// Package simserver is the standalone provider simulator service: POST /call
// charges the model's rolling RPM/TPM windows, sleeps the configured latency
// (real sleep — this delay IS the simulated provider latency), draws the
// outcome and actual tokens, and deduplicates re-issued calls via a
// persisted seen-keys table (at-least-once crash recovery).
package simserver

import (
	"encoding/json"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"htsim/internal/config"
	"htsim/internal/store"
)

type simSlot struct {
	mu  sync.Mutex
	sec int64
	rpm int64
	tpm int64
}

type modelState struct {
	cfg       *config.ModelConfig
	slots     [60]simSlot
	rpmUsed   int64
	tpmUsed   int64
	wheelBase int64
	usedMu    sync.Mutex
}

func (model *modelState) slotFor(sec int64) *simSlot { return &model.slots[sec%60] }

// roll evicts seconds that left the 60s rolling window (call with usedMu).
func (model *modelState) roll(sec int64) {
	if model.wheelBase == 0 {
		model.wheelBase = sec
		return
	}
	for model.wheelBase < sec-59 {
		server := model.slotFor(model.wheelBase)
		server.mu.Lock()
		if server.sec == model.wheelBase {
			model.rpmUsed -= server.rpm
			model.tpmUsed -= server.tpm
			server.rpm, server.tpm = 0, 0
		}
		server.mu.Unlock()
		model.wheelBase++
	}
}

type callRequest struct {
	RequestID       string `json:"request_id"`
	Model           string `json:"model"`
	EstimatedTokens int64  `json:"estimated_tokens"`
}

type callResponse struct {
	Outcome      string `json:"outcome"`
	ActualTokens int64  `json:"actual_tokens"`
	LatencyMS    int64  `json:"latency_ms"`
}

// Server is the simulator service.
type Server struct {
	cfg      *config.Config
	store    *store.Store
	models   map[string]*modelState
	executed atomic.Int64 // calls actually executed (dedup-verified in tests)
	rng      *rand.Rand
	rngMu    sync.Mutex
}

// New builds a simulator server from config.
func New(cfg *config.Config, st *store.Store) *Server {
	server := &Server{cfg: cfg, store: st, models: make(map[string]*modelState), rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
	for i := range cfg.Models {
		model := &cfg.Models[i]
		server.models[model.Name] = &modelState{cfg: model}
	}
	return server
}

// Handler returns the service's HTTP handler.
func (server *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /call", server.handleCall)
	return mux
}

func (server *Server) handleCall(w http.ResponseWriter, r *http.Request) {
	var cr callRequest
	if err := json.NewDecoder(r.Body).Decode(&cr); err != nil || cr.RequestID == "" || cr.Model == "" {
		http.Error(w, `{"error":"invalid body"}`, http.StatusBadRequest)
		return
	}
	model, ok := server.models[cr.Model]
	if !ok {
		http.Error(w, `{"error":"unknown model"}`, http.StatusBadRequest)
		return
	}

	// idempotency: a re-issued call returns the original outcome without
	// re-executing (crash recovery is at-least-once with dedup)
	if res, ok, _ := server.store.SeenKey(cr.RequestID); ok {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(res))
		return
	}

	sec := time.Now().Unix()
	model.usedMu.Lock()
	model.roll(sec)
	if model.rpmUsed+1 > model.cfg.RPM || model.tpmUsed+cr.EstimatedTokens > model.cfg.TPM {
		model.usedMu.Unlock()
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	slot := model.slotFor(sec)
	slot.mu.Lock()
	if slot.sec != sec {
		slot.sec = sec
		slot.rpm = 0
		slot.tpm = 0
	}
	slot.rpm++
	slot.tpm += cr.EstimatedTokens
	slot.mu.Unlock()
	model.rpmUsed++
	model.tpmUsed += cr.EstimatedTokens
	model.usedMu.Unlock()

	server.rngMu.Lock()
	latency := model.cfg.Latency.Draw(server.rng)
	server.rngMu.Unlock()
	outcome := server.drawOutcome(model)
	tokens := server.drawTokens(model)

	time.Sleep(latency) // the response delay IS the simulated provider latency

	resp := callResponse{
		Outcome:      outcomeName(outcome),
		ActualTokens: tokens,
		LatencyMS:    latency.Milliseconds(),
	}
	body, _ := json.Marshal(resp)
	// persist the seen-key ONLY for successful executions: the idempotency
	// key protects successful work from double execution (crash between
	// execute and persist is an at-least-once redelivery). A transient or
	// permanent failure did no provider work — a retry must re-execute with
	// a fresh draw, not replay the failure forever.
	if outcome.SuccessPct == 100 {
		_ = server.store.PutSeenKey(cr.RequestID, string(body))
	}
	server.executed.Add(1)
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}

func (server *Server) drawOutcome(model *modelState) config.OutcomeConfig {
	server.rngMu.Lock()
	roll := server.rng.Float64() * 100
	server.rngMu.Unlock()
	o := model.cfg.Outcome
	switch {
	case roll < o.SuccessPct:
		return config.OutcomeConfig{SuccessPct: 100}
	case roll < o.SuccessPct+o.TransientPct:
		return config.OutcomeConfig{TransientPct: 100}
	default:
		return config.OutcomeConfig{PermanentPct: 100}
	}
}

func (server *Server) drawTokens(model *modelState) int64 {
	mean := model.cfg.ActualTokenMean
	if mean <= 0 {
		mean = float64(model.cfg.EstimateTokens)
	}
	server.rngMu.Lock()
	t := mean + server.rng.NormFloat64()*0.2*mean
	server.rngMu.Unlock()
	if t < 1 {
		t = 1
	}
	if t > 2*mean {
		t = 2 * mean
	}
	return int64(math.Round(t))
}

func outcomeName(o config.OutcomeConfig) string {
	switch {
	case o.SuccessPct == 100:
		return "success"
	case o.TransientPct == 100:
		return "transient_failure"
	default:
		return "permanent_failure"
	}
}
