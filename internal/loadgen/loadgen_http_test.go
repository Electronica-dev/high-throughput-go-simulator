package loadgen

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"consuma/internal/config"
)

// fakeAPI is a minimal system API: counts POST /batches, sums submitted
// counts, and serves GET /batches/{id} as completed for polling.
type fakeAPI struct {
	mu        sync.Mutex
	batches   map[string]int64 // batch id -> submitted count
	nextID    atomic.Int32
	totalReqs atomic.Int64
	posts     atomic.Int32
}

func (f *fakeAPI) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /batches", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Model           string `json:"model"`
			EstimatedTokens int64  `json:"estimated_tokens"`
			Count           int64  `json:"count"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Count <= 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		id := fmt.Sprintf("b-%d", f.nextID.Add(1))
		f.mu.Lock()
		f.batches[id] = in.Count
		f.mu.Unlock()
		f.totalReqs.Add(in.Count)
		f.posts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]any{"batch_id": id, "count": in.Count})
	})
	mux.HandleFunc("GET /batches/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"batch_id": id, "status": "completed",
			"successful": 0, "failed": 0, "expired": 0, "rejected": 0, "total": 0,
		})
	})
	return mux
}

func TestHTTPTargetSubmitsBatchesAndConservesCount(t *testing.T) {
	api := &fakeAPI{batches: map[string]int64{}}
	srv := httptest.NewServer(api.handler())
	defer srv.Close()

	cfg := &config.Config{
		MaxWaitMS: 50, QueueDepth: 16, WheelShards: 2, WheelBuckets: 256, WheelTickMS: 1, MaxRetries: 3,
		Models: []config.ModelConfig{{Name: "model-a", RPM: 1000000, TPM: 1000000000, EstimateTokens: 100, ActualTokenMean: 100}},
	}
	lc := &LoadConfig{
		RatePerSec: 500, Duration: 200 * time.Millisecond,
		ModelMix: map[string]float64{"model-a": 1.0},
		BatchSize: 10, EstimateTokens: 100, RNGSeed: 42,
	}
	tgt := NewHTTPTarget(srv.URL, nil)
	if err := Run(context.Background(), lc, cfg, tgt); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if api.totalReqs.Load() != 100 {
		t.Fatalf("conservation: want 100 requests across batches, got %d", api.totalReqs.Load())
	}
	if api.posts.Load() == 0 || api.posts.Load() > 25 {
		t.Fatalf("chunk posting unreasonable: %d posts", api.posts.Load())
	}
	ids := tgt.BatchIDs()
	if len(ids) != int(api.posts.Load()) {
		t.Fatalf("recorded %d ids vs %d posts", len(ids), api.posts.Load())
	}
	for _, id := range ids {
		if id == "" {
			t.Fatal("empty batch id recorded")
		}
	}
}

func TestHTTPTargetMixedModelsGroupPerModel(t *testing.T) {
	api := &fakeAPI{batches: map[string]int64{}}
	srv := httptest.NewServer(api.handler())
	defer srv.Close()

	cfg := &config.Config{
		MaxWaitMS: 50, QueueDepth: 16, WheelShards: 2, WheelBuckets: 256, WheelTickMS: 1, MaxRetries: 3,
		Models: []config.ModelConfig{
			{Name: "model-a", RPM: 1000000, TPM: 1000000000, EstimateTokens: 100, ActualTokenMean: 100},
			{Name: "model-b", RPM: 1000000, TPM: 1000000000, EstimateTokens: 100, ActualTokenMean: 100},
		},
	}
	lc := &LoadConfig{
		RatePerSec: 500, Duration: 200 * time.Millisecond,
		ModelMix: map[string]float64{"model-a": 0.5, "model-b": 0.5},
		BatchSize: 10, EstimateTokens: 100, RNGSeed: 7,
	}
	tgt := NewHTTPTarget(srv.URL, nil)
	if err := Run(context.Background(), lc, cfg, tgt); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// a chunk with both models must produce one POST per model group; every
	// POST must be single-model (the API takes one model per batch)
	if api.posts.Load() < 2 {
		t.Fatalf("mixed models should post at least once per model, got %d", api.posts.Load())
	}
}

func TestHTTPTargetAwaitCompletions(t *testing.T) {
	api := &fakeAPI{batches: map[string]int64{}}
	srv := httptest.NewServer(api.handler())
	defer srv.Close()

	cfg := &config.Config{
		MaxWaitMS: 50, QueueDepth: 16, WheelShards: 2, WheelBuckets: 256, WheelTickMS: 1, MaxRetries: 3,
		Models: []config.ModelConfig{{Name: "model-a", RPM: 1000000, TPM: 1000000000, EstimateTokens: 100, ActualTokenMean: 100}},
	}
	lc := &LoadConfig{
		RatePerSec: 500, Duration: 100 * time.Millisecond,
		ModelMix: map[string]float64{"model-a": 1.0},
		BatchSize: 10, EstimateTokens: 100, RNGSeed: 1,
	}
	tgt := NewHTTPTarget(srv.URL, nil)
	if err := Run(context.Background(), lc, cfg, tgt); err != nil {
		t.Fatal(err)
	}
	completed, err := tgt.AwaitCompletions(2*time.Second, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("AwaitCompletions: %v", err)
	}
	if completed != len(tgt.BatchIDs()) {
		t.Fatalf("completed %d of %d batches", completed, len(tgt.BatchIDs()))
	}
}
