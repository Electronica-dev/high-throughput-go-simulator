package simserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"htsim/internal/config"
	"htsim/internal/store"
)

func testConfig() *config.Config {
	return &config.Config{
		MaxWaitMS: 50, QueueDepth: 16, WheelShards: 2, WheelBuckets: 256, WheelTickMS: 1, MaxRetries: 3,
		Models: []config.ModelConfig{{
			Name: "m", RPM: 2, TPM: 1000000, EstimateTokens: 100, ActualTokenMean: 100,
			Latency: config.LatencyConfig{Kind: "fixed", MeanMS: 5},
			Outcome: config.OutcomeConfig{SuccessPct: 100},
		}},
	}
}

func newServerT(t *testing.T, cfg *config.Config) (*Server, *httptest.Server) {
	return newServer(t, cfg)
}

func newServer(t *testing.T, cfg *config.Config) (*Server, *httptest.Server) {
	t.Helper()
	st, err := store.Open(t.TempDir() + "/sim.db")
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	server := New(cfg, st)
	srv := httptest.NewServer(server.Handler())
	t.Cleanup(srv.Close)
	return server, srv
}

func call(t *testing.T, url, id string) (int, map[string]any) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"request_id": id, "model": "m", "estimated_tokens": 100})
	resp, err := http.Post(url+"/call", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /call: %v", err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		// 429 has an empty/error body; tolerate
		out = map[string]any{}
	}
	return resp.StatusCode, out
}

func TestCallHonorsLatency(t *testing.T) {
	_, srv := newServer(t, testConfig())
	start := time.Now()
	code, out := call(t, srv.URL, "r-1")
	elapsed := time.Since(start)
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	if out["outcome"] != "success" {
		t.Fatalf("outcome %v", out["outcome"])
	}
	if elapsed < 5*time.Millisecond {
		t.Fatalf("call returned in %v, want >= 5ms simulated latency", elapsed)
	}
}

func TestLimitBreachReturns429(t *testing.T) {
	_, srv := newServer(t, testConfig()) // RPM=2
	call(t, srv.URL, "r-1")
	call(t, srv.URL, "r-2")
	code, _ := call(t, srv.URL, "r-3")
	if code != http.StatusTooManyRequests {
		t.Fatalf("third call want 429, got %d", code)
	}
}

func TestSeenKeyDeduplicates(t *testing.T) {
	server, srv := newServer(t, testConfig())
	_, out1 := call(t, srv.URL, "r-dup")
	code, out2 := call(t, srv.URL, "r-dup")
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	if out1["outcome"] != out2["outcome"] {
		t.Fatalf("dedup must return the original outcome: %v vs %v", out1, out2)
	}
	if server.executed.Load() != 1 {
		t.Fatalf("provider should execute once, executed %d", server.executed.Load())
	}
}

func TestSeenKeyPersistsAcrossServerRestart(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/sim.db"
	cfg := testConfig()
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	s1 := New(cfg, st)
	srv1 := httptest.NewServer(s1.Handler())
	_, out1 := call(t, srv1.URL, "r-k")
	srv1.Close()
	st.Close()

	st2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	s2 := New(cfg, st2)
	srv2 := httptest.NewServer(s2.Handler())
	defer srv2.Close()
	code, out2 := call(t, srv2.URL, "r-k")
	if code != 200 || out1["outcome"] != out2["outcome"] {
		t.Fatalf("seen key must survive restart: %v vs %v", out1, out2)
	}
	if s2.executed.Load() != 0 {
		t.Fatalf("restarted server must not re-execute, executed %d", s2.executed.Load())
	}
}

func TestRequestBodyValidation(t *testing.T) {
	_, srv := newServer(t, testConfig())
	resp, err := http.Post(srv.URL+"/call", "application/json", strings.NewReader(`{`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("malformed body want 400, got %d", resp.StatusCode)
	}
}

// TestSeenKeyDoesNotMemoizeFailures: idempotency keys protect successful
// work from double execution — a retry after a transient failure must
// RE-EXECUTE (fresh draw), not replay the failure.
func TestSeenKeyDoesNotMemoizeFailures(t *testing.T) {
	cfg := testConfig()
	cfg.Models[0].Outcome = config.OutcomeConfig{TransientPct: 100} // every execution fails
	server, srv := newServerT(t, cfg)
	_, out1 := call(t, srv.URL, "r-retry")
	code, out2 := call(t, srv.URL, "r-retry")
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	if out1["outcome"] != "transient_failure" || out2["outcome"] != "transient_failure" {
		t.Fatalf("outcomes: %v %v", out1["outcome"], out2["outcome"])
	}
	if server.executed.Load() != 2 {
		t.Fatalf("transient failure must re-execute on retry, executed %d", server.executed.Load())
	}
}
