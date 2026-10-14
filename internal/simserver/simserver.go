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

	"consuma/internal/config"
	"consuma/internal/store"
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

func (m *modelState) slotFor(sec int64) *simSlot { return &m.slots[sec%60] }

// roll evicts seconds that left the 60s rolling window (call with usedMu).
func (m *modelState) roll(sec int64) {
	if m.wheelBase == 0 {
		m.wheelBase = sec
		return
	}
	for m.wheelBase < sec-59 {
		s := m.slotFor(m.wheelBase)
		s.mu.Lock()
		if s.sec == m.wheelBase {
			m.rpmUsed -= s.rpm
			m.tpmUsed -= s.tpm
			s.rpm, s.tpm = 0, 0
		}
		s.mu.Unlock()
		m.wheelBase++
	}
}

type callRequest struct {
	RequestID      string `json:"request_id"`
	Model          string `json:"model"`
	EstimatedTokens int64 `json:"estimated_tokens"`
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
	s := &Server{cfg: cfg, store: st, models: make(map[string]*modelState), rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
	for i := range cfg.Models {
		m := &cfg.Models[i]
		s.models[m.Name] = &modelState{cfg: m}
	}
	return s
}

// Handler returns the service's HTTP handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /call", s.handleCall)
	return mux
}

func (s *Server) handleCall(w http.ResponseWriter, r *http.Request) {
	var cr callRequest
	if err := json.NewDecoder(r.Body).Decode(&cr); err != nil || cr.RequestID == "" || cr.Model == "" {
		http.Error(w, `{"error":"invalid body"}`, http.StatusBadRequest)
		return
	}
	m, ok := s.models[cr.Model]
	if !ok {
		http.Error(w, `{"error":"unknown model"}`, http.StatusBadRequest)
		return
	}

	// idempotency: a re-issued call returns the original outcome without
	// re-executing (crash recovery is at-least-once with dedup)
	if res, ok, _ := s.store.SeenKey(cr.RequestID); ok {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(res))
		return
	}

	sec := time.Now().Unix()
	m.usedMu.Lock()
	m.roll(sec)
	if m.rpmUsed+1 > m.cfg.RPM || m.tpmUsed+cr.EstimatedTokens > m.cfg.TPM {
		m.usedMu.Unlock()
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	slot := m.slotFor(sec)
	slot.mu.Lock()
	if slot.sec != sec {
		slot.sec = sec
		slot.rpm = 0
		slot.tpm = 0
	}
	slot.rpm++
	slot.tpm += cr.EstimatedTokens
	slot.mu.Unlock()
	m.rpmUsed++
	m.tpmUsed += cr.EstimatedTokens
	m.usedMu.Unlock()

	s.rngMu.Lock()
	latency := m.cfg.Latency.Draw(s.rng)
	s.rngMu.Unlock()
	outcome := s.drawOutcome(m)
	tokens := s.drawTokens(m)

	time.Sleep(latency) // the response delay IS the simulated provider latency

	resp := callResponse{
		Outcome:      outcomeName(outcome),
		ActualTokens: tokens,
		LatencyMS:    latency.Milliseconds(),
	}
	body, _ := json.Marshal(resp)
	// persist seen-key AFTER execution (crash between execute and persist is
	// an at-least-once redelivery — acceptable and documented)
	_ = s.store.PutSeenKey(cr.RequestID, string(body))
	s.executed.Add(1)
	w.Header().Set("Content-Type", "application/json")
	w.Write(body)
}


func (s *Server) drawOutcome(m *modelState) config.OutcomeConfig {
	s.rngMu.Lock()
	roll := s.rng.Float64() * 100
	s.rngMu.Unlock()
	o := m.cfg.Outcome
	switch {
	case roll < o.SuccessPct:
		return config.OutcomeConfig{SuccessPct: 100}
	case roll < o.SuccessPct+o.TransientPct:
		return config.OutcomeConfig{TransientPct: 100}
	default:
		return config.OutcomeConfig{PermanentPct: 100}
	}
}

func (s *Server) drawTokens(m *modelState) int64 {
	mean := m.cfg.ActualTokenMean
	if mean <= 0 {
		mean = float64(m.cfg.EstimateTokens)
	}
	s.rngMu.Lock()
	t := mean + s.rng.NormFloat64()*0.2*mean
	s.rngMu.Unlock()
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

