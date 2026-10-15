package scenarios

import (
	"strings"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ---------- utilization computation ----------

func TestUtilizationFromWindowSamples(t *testing.T) {
	samples := []WindowSample{
		{ObservedRPM: 27000, ConfiguredRPM: 30000, ObservedTPM: 450000, ConfiguredTPM: 500000},
		{ObservedRPM: 28500, ConfiguredRPM: 30000, ObservedTPM: 470000, ConfiguredTPM: 500000},
	}
	u := MeanUtilization(samples)
	// mean observed RPM (27750) / mean configured (30000) = 92.5%
	if u.RPM < 92.49 || u.RPM > 92.51 {
		t.Fatalf("RPM utilization want 92.5, got %v", u.RPM)
	}
	if u.TPM < 91.99 || u.TPM > 92.01 {
		t.Fatalf("TPM utilization want 92.0, got %v", u.TPM)
	}
}

func TestUtilizationEmptySamples(t *testing.T) {
	u := MeanUtilization(nil)
	if u.RPM != 0 || u.TPM != 0 {
		t.Fatalf("empty samples want zeros, got %+v", u)
	}
}

// ---------- window-over-limit detector ----------

func TestOverLimitDetection(t *testing.T) {
	samples := []WindowSample{
		{ObservedRPM: 29000, ConfiguredRPM: 30000, ObservedTPM: 490000, ConfiguredTPM: 500000}, // ok
		{ObservedRPM: 30500, ConfiguredRPM: 30000, ObservedTPM: 495000, ConfiguredTPM: 500000}, // RPM over
		{ObservedRPM: 29000, ConfiguredRPM: 30000, ObservedTPM: 500500, ConfiguredTPM: 500000}, // TPM over
	}
	overs := OverLimit(samples, 0) // no tolerance
	if len(overs) != 2 {
		t.Fatalf("want 2 over-limit samples, got %d: %+v", len(overs), overs)
	}
	// tolerance (fraction of the limit) absorbs small scheduler jitter:
	// 0.5% of 30000 = 150, so 30500 is still over but 30100 would pass
	if overs := OverLimit([]WindowSample{{ObservedRPM: 30100, ConfiguredRPM: 30000, ConfiguredTPM: 500000, ObservedTPM: 400000}}, 0.005); len(overs) != 0 {
		t.Fatalf("30100 within 0.5%% tolerance must pass, got %+v", overs)
	}
}

// ---------- id-uniqueness check ----------

func TestIDsUnique(t *testing.T) {
	rows := []ResultRow{
		{ID: "r-1"}, {ID: "r-2"}, {ID: "r-3"},
	}
	if !IDsUnique(rows) {
		t.Fatal("unique ids must pass")
	}
	rows = append(rows, ResultRow{ID: "r-2"})
	if IDsUnique(rows) {
		t.Fatal("duplicate id must fail")
	}
}

// ---------- S1 runner flow against a fake system ----------

// fakeSystem is a minimal system API: counts batches, reports full observed
// windows, completes batches instantly.
type fakeSystem struct {
	limitPosts []int64 // rpm values posted to /limits/model-a
	batches    map[string]int64
	nextID     int
}

func (sys *fakeSystem) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /batches", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Count int64 `json:"count"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		sys.batches[itoa(sys.nextID)] = in.Count
		sys.nextID++
		w.WriteHeader(202)
		json.NewEncoder(w).Encode(map[string]any{"batch_id": itoa(sys.nextID - 1)})
	})
	mux.HandleFunc("GET /batches/{id}", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"batch_id": r.PathValue("id"), "status": "completed",
			"successful": 9000, "failed": 500, "expired": 300, "rejected": 200, "total": 10000,
		})
	})
	mux.HandleFunc("GET /batches/{id}/results", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		json.NewEncoder(w).Encode([]ResultRow{
			{ID: id + "-r-1", State: "completed", Outcome: "success", Tokens: 100},
			{ID: id + "-r-2", State: "completed", Outcome: "failed", Tokens: 40},
		})
	})
	mux.HandleFunc("POST /limits/{model}", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			RPM int64 `json:"rpm"`
			TPM int64 `json:"tpm"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		sys.limitPosts = append(sys.limitPosts, in.RPM)
		w.WriteHeader(200)
	})
	mux.HandleFunc("GET /metrics/{model}", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(WindowSample{ObservedRPM: 29000, ObservedTPM: 480000, ConfiguredRPM: 30000, ConfiguredTPM: 500000})
	})
	return mux
}

func itoa(n int) string { return fmt.Sprintf("b-%d", n) }

func TestScenario1RunnerFlow(t *testing.T) {
	sys := &fakeSystem{batches: map[string]int64{}}
	srv := httptest.NewServer(sys.handler())
	defer srv.Close()

	sc := Config{
		RatePerSec: 500, DurationSec: 1, BatchSize: 25, Tokens: 100,
		Model: "model-a", WarmupSec: 0,
		SampleEverySec: 1, Headroom: 10,
	}
	out := t.TempDir()
	res, err := RunScenario1(srv.URL, sc, out)
	if err != nil {
		t.Fatalf("RunScenario1: %v", err)
	}
	if !res.Pass {
		t.Fatalf("scenario should pass: %v", res.Reasons)
	}
	// evidence files exist and parse
	raw, err := os.ReadFile(filepath.Join(out, "scenario-1.json"))
	if err != nil {
		t.Fatalf("evidence: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("evidence json: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "scenario-1-summary.txt")); err != nil {
		t.Fatalf("summary: %v", err)
	}
}

// ---------- S2 phase machine against a fake system ----------

func TestScenario2LimitPhases(t *testing.T) {
	sys := &fakeSystem{batches: map[string]int64{}}
	srv := httptest.NewServer(sys.handler())
	defer srv.Close()

	sc := Config{
		RatePerSec: 300, DurationSec: 3, BatchSize: 25, Tokens: 100,
		Model: "model-a", SecondModel: "model-b",
		Phase1RPM: 30000, Phase1TPM: 60000000, Phase2RPM: 5000,
		SecondRPM: 20000, SecondTPM: 40000000,
		Phase1Sec: 1, Phase2Sec: 1, Phase3Sec: 1,
		SampleEverySec: 1, Headroom: 10,
	}
	out := t.TempDir()
	res, err := RunScenario2(srv.URL, sc, out)
	if err != nil {
		t.Fatalf("RunScenario2: %v", err)
	}
	// the runner must have pinned B (20K) once, then A 30K -> 5K -> 30K
	if len(sys.limitPosts) != 4 || sys.limitPosts[0] != 30000 || sys.limitPosts[1] != 20000 || sys.limitPosts[2] != 5000 || sys.limitPosts[3] != 30000 {
		t.Fatalf("limit phase posts: %v", sys.limitPosts)
	}
	// the observed-vs-configured time series must have been sampled
	if len(res.TimeSeries) == 0 {
		t.Fatal("time series must be recorded")
	}
}

// ---------- S3 runner: durable batches + delivery semantics ----------

func TestScenario3DurableBatchFlow(t *testing.T) {
	sys := &fakeSystem{batches: map[string]int64{}}
	srv := httptest.NewServer(sys.handler())
	defer srv.Close()

	sc := Config{
		RatePerSec: 0, BatchSize: 0, Tokens: 100,
		BatchCount: 10000, Model: "model-a", SecondModel: "model-b",
		SampleEverySec: 1, Headroom: 10,
	}
	out := t.TempDir()
	res, err := RunScenario3(srv.URL, srv.URL, sc, out)
	if err != nil {
		t.Fatalf("RunScenario3: %v", err)
	}
	if !res.Pass {
		t.Fatalf("scenario should pass: %v", res.Reasons)
	}
}

// ---------- callback sink: reject-first, then accept ----------

func TestCallbackSinkRejectsThenAccepts(t *testing.T) {
	sink := NewCallbackSink(2)
	defer sink.Close()
	if sink.URL() == "" {
		t.Fatal("sink did not start")
	}
	body := func(code int) {
		resp, err := http.Post(sink.URL(), "application/json",
			bytes.NewReader([]byte(`{"batch_id":"b-1","status":"completed","successful":9000,"failed":500,"expired":300,"rejected":200}`)))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != code {
			t.Fatalf("want %d, got %d", code, resp.StatusCode)
		}
	}
	body(500)
	body(500)
	body(200) // accepted
	if !sink.AwaitDelivery(time.Second) {
		t.Fatal("delivery should be signalled after the accepted attempt")
	}
	p := sink.LastPayload()
	if p.BatchID != "b-1" || p.Successful != 9000 || p.Rejected != 200 {
		t.Fatalf("payload mismatch: %+v", p)
	}
	if sink.Hits() != 3 {
		t.Fatalf("hits want 3, got %d", sink.Hits())
	}
}

// TestScenario1DeadSystemErrors: a run against a dead system used to write
// a misleading 0.0%-utilization FAIL report; it must return an actionable
// error and write NO evidence.
func TestScenario1DeadSystemErrors(t *testing.T) {
	dead := "http://127.0.0.1:1" // nothing listens here
	sc := Config{
		RatePerSec: 500, DurationSec: 1, BatchSize: 25, Tokens: 100,
		Model: "model-a", WarmupSec: 0,
		SampleEverySec: 1, Headroom: 10,
	}
	out := t.TempDir()
	res, err := RunScenario1(dead, sc, out)
	if err == nil {
		t.Fatalf("dead system must return an error, got res=%+v", res)
	}
	if !strings.Contains(err.Error(), "no window samples") {
		t.Fatalf("error should say why: %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(out, "scenario-1.json")); !os.IsNotExist(statErr) {
		t.Fatal("no evidence may be written for a dead-system run")
	}
}
