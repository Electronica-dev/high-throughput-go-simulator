// Package api is the system's HTTP surface: synchronous single requests,
// 202-acknowledged async batches, and live per-model limit changes. The
// dependency surface is an interface so tests run against fakes and the
// real wiring (engine + batch manager) satisfies it unchanged.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"htsim/internal/batch"

	"htsim/internal/ledger"
)

// errUnknownModel signals an unconfigured model (maps to 400).
var errUnknownModel = errors.New("unknown model")

// Deps is what the API needs from the system behind it.
type Deps interface {
	// SubmitAndWait drives one synchronous request to its terminal state.
	SubmitAndWait(r *ledger.Request, timeout time.Duration) (outcome string, tokens int64, err error)
	// Submit accepts one request without waiting (async).
	Submit(r *ledger.Request)
	// SetLimit changes a model's limits live.
	SetLimit(model string, rpm, tpm int64) error
	// Limits reports a model's current limits.
	Limits(model string) (rpm, tpm int64, err error)
	// CreateBatch registers and starts an async batch; returns its UUID.
	CreateBatch(model string, tokens, count int64, callbackURL string) (string, error)
	// ObservedWindows snapshots a model's rolling windows (scenario sampling).
	ObservedWindows(model string) (WindowSample, error)
	// BatchStatus reports a batch's persisted record.
	BatchStatus(id string) (BatchStatus, bool)
	// BatchResults lists a batch's per-request outcomes.
	BatchResults(id string) ([]ResultRow, bool)
}

// BatchStatus is one batch's persisted completion record.
type BatchStatus struct {
	ID         string `json:"batch_id"`
	Status     string `json:"status"`
	Successful int64  `json:"successful"`
	Failed     int64  `json:"failed"`
	Expired    int64  `json:"expired"`
	Rejected   int64  `json:"rejected"`
	Total      int64  `json:"total"`
}

// WindowSample is one snapshot of a model's rolling windows (60s windows).
type WindowSample struct {
	ObservedRPM   int64 `json:"observed_rpm"`
	ObservedTPM   int64 `json:"observed_tpm"`
	ConfiguredRPM int64 `json:"configured_rpm"`
	ConfiguredTPM int64 `json:"configured_tpm"`
}

// ResultRow aliases the batch package's per-request outcome row (the API
// serializes it verbatim).
type ResultRow = batch.ResultRow

// Server is the HTTP API.
type Server struct {
	d      Deps
	known  map[string]bool
	syncTO time.Duration
}

// New builds the API server over the given models.
func New(d Deps, models []string) *Server {
	known := make(map[string]bool, len(models))
	for _, m := range models {
		known[m] = true
	}
	return &Server{d: d, known: known, syncTO: 60 * time.Second}
}

// Handler returns the API's HTTP handler.
func (server *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("POST /requests", server.handleRequest)
	mux.HandleFunc("POST /batches", server.handleBatch)
	mux.HandleFunc("GET /batches/{id}", server.handleBatchStatus)
	mux.HandleFunc("GET /batches/{id}/results", server.handleBatchResults)
	mux.HandleFunc("GET /limits/{model}", server.handleGetLimits)
	mux.HandleFunc("POST /limits/{model}", server.handleSetLimits)
	mux.HandleFunc("GET /metrics/{model}", server.handleMetrics)
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

type requestIn struct {
	Model           string `json:"model"`
	EstimatedTokens int64  `json:"estimated_tokens"`
}

// handleRequest runs one synchronous request: the response carries the
// provider's ACTUAL charge (not the estimate) and the final outcome.
func (server *Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	var in requestIn
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Model == "" || in.EstimatedTokens <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body: need model and positive estimated_tokens"})
		return
	}
	if !server.known[in.Model] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("unknown model %q", in.Model)})
		return
	}
	req := &ledger.Request{
		ExternalID:     uuid.NewString(),
		Model:          in.Model,
		EstimateTokens: in.EstimatedTokens,
		BornNS:         time.Now().UnixNano(),
	}
	outcome, tokens, err := server.d.SubmitAndWait(req, server.syncTO)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"request_id":     req.ExternalID,
		"outcome":        outcome,
		"tokens_charged": tokens,
	})
}

type batchIn struct {
	Model           string `json:"model"`
	EstimatedTokens int64  `json:"estimated_tokens"`
	Count           int64  `json:"count"`
	CallbackURL     string `json:"callback_url"`
}

// handleBatch acknowledges an async batch: 202 + server-generated UUID; the
// client later fetches status or receives the callback.
func (server *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	var in batchIn
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Model == "" || in.EstimatedTokens <= 0 || in.Count <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body: need model, positive estimated_tokens and count"})
		return
	}
	if !server.known[in.Model] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("unknown model %q", in.Model)})
		return
	}
	id, err := server.d.CreateBatch(in.Model, in.EstimatedTokens, in.Count, in.CallbackURL)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"batch_id": id,
		"count":    in.Count,
	})
}

func (server *Server) handleGetLimits(w http.ResponseWriter, r *http.Request) {
	model := r.PathValue("model")
	rpm, tpm, err := server.d.Limits(model)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"model": model, "rpm": rpm, "tpm": tpm})
}

func (server *Server) handleSetLimits(w http.ResponseWriter, r *http.Request) {
	model := r.PathValue("model")
	if !server.known[model] {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown model"})
		return
	}
	var in struct {
		RPM int64 `json:"rpm"`
		TPM int64 `json:"tpm"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.RPM <= 0 || in.TPM <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body: need positive rpm and tpm"})
		return
	}
	if err := server.d.SetLimit(model, in.RPM, in.TPM); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"model": model, "rpm": in.RPM, "tpm": in.TPM})
}

// handleMetrics snapshots a model's rolling windows (scenario runners sample
// this over time for the configured-vs-observed evidence).
func (server *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	model := r.PathValue("model")
	sample, err := server.d.ObservedWindows(model)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, sample)
}

// handleBatchResults lists a batch's per-request outcomes.
func (server *Server) handleBatchResults(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rows, ok := server.d.BatchResults(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown batch"})
		return
	}
	writeJSON(w, http.StatusOK, rows)
}

// handleBatchStatus reports a batch's persisted completion record (the load
// generator and scenario runners poll this for completion evidence).
func (server *Server) handleBatchStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	bs, ok := server.d.BatchStatus(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown batch"})
		return
	}
	writeJSON(w, http.StatusOK, bs)
}
