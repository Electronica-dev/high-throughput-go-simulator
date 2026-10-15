package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"htsim/internal/ledger"
)

// fakeDeps implements the server's dependency surface.
type fakeDeps struct {
	mu        sync.Mutex
	sample    WindowSample
	waitRes   string
	waitTok   int64
	waitErr   error
	setErr    error
	limitsRPM int64
	limitsTPM int64
	submitted int
	known     map[string]bool
	batchID   string
	batchErr  error
	lastReq   *ledger.Request
}

func (deps *fakeDeps) SubmitAndWait(r *ledger.Request, timeout time.Duration) (string, int64, error) {
	deps.mu.Lock()
	deps.submitted++
	deps.lastReq = r
	deps.mu.Unlock()
	return deps.waitRes, deps.waitTok, deps.waitErr
}

func (deps *fakeDeps) Submit(r *ledger.Request) {}

func (deps *fakeDeps) SetLimit(model string, rpm, tpm int64) error { return deps.setErr }

func (deps *fakeDeps) Limits(model string) (int64, int64, error) {
	if !deps.known[model] {
		return 0, 0, errUnknownModel
	}
	return deps.limitsRPM, deps.limitsTPM, nil
}

func (deps *fakeDeps) CreateBatch(model string, tokens, count int64, callbackURL string) (string, error) {
	if !deps.known[model] {
		return "", errUnknownModel
	}
	if deps.batchErr != nil {
		return "", deps.batchErr
	}
	return deps.batchID, nil
}

func apiSrv(t *testing.T, d *fakeDeps) *httptest.Server {
	t.Helper()
	server := New(d, []string{"model-a"})
	srv := httptest.NewServer(server.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func postJSON(t *testing.T, url, body string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestHealth(t *testing.T) {
	d := &fakeDeps{}
	srv := apiSrv(t, d)
	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("health want 200, got %d", resp.StatusCode)
	}
}

func TestSyncRequestReturnsActualCharged(t *testing.T) {
	d := &fakeDeps{waitRes: "success", waitTok: 933, known: map[string]bool{"model-a": true}}
	srv := apiSrv(t, d)
	code, out := postJSON(t, srv.URL+"/requests", `{"model":"model-a","estimated_tokens":1000}`)
	if code != 200 {
		t.Fatalf("want 200, got %d: %v", code, out)
	}
	if out["outcome"] != "success" {
		t.Fatalf("outcome: %v", out["outcome"])
	}
	if out["tokens_charged"] != float64(933) {
		t.Fatalf("tokens_charged must be the ACTUAL charge, got %v", out["tokens_charged"])
	}
	if id, _ := out["request_id"].(string); id == "" {
		t.Fatal("response must carry a server-generated request_id")
	}
	if d.lastReq == nil || d.lastReq.ExternalID == "" {
		t.Fatal("dependency must receive the request with its UUID")
	}
}

func TestSyncRequestUnknownModel(t *testing.T) {
	d := &fakeDeps{known: map[string]bool{"model-a": true}}
	srv := apiSrv(t, d)
	code, _ := postJSON(t, srv.URL+"/requests", `{"model":"nope","estimated_tokens":100}`)
	if code != 400 {
		t.Fatalf("unknown model want 400, got %d", code)
	}
}

func TestSyncRequestBadBody(t *testing.T) {
	d := &fakeDeps{}
	srv := apiSrv(t, d)
	code, _ := postJSON(t, srv.URL+"/requests", `{`)
	if code != 400 {
		t.Fatalf("malformed body want 400, got %d", code)
	}
}

func TestBatchReturns202WithID(t *testing.T) {
	d := &fakeDeps{batchID: "b-uuid-1", known: map[string]bool{"model-a": true}}
	srv := apiSrv(t, d)
	code, out := postJSON(t, srv.URL+"/batches", `{"model":"model-a","estimated_tokens":1000,"count":50,"callback_url":"http://c/cb"}`)
	if code != 202 {
		t.Fatalf("want 202, got %d: %v", code, out)
	}
	if out["batch_id"] != "b-uuid-1" {
		t.Fatalf("batch_id: %v", out["batch_id"])
	}
}

func TestGetLimits(t *testing.T) {
	d := &fakeDeps{limitsRPM: 30000, limitsTPM: 500000, known: map[string]bool{"model-a": true}}
	srv := apiSrv(t, d)
	resp, err := http.Get(srv.URL + "/limits/model-a")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if resp.StatusCode != 200 || got["rpm"] != float64(30000) || got["tpm"] != float64(500000) {
		t.Fatalf("GET limits: %d %v", resp.StatusCode, got)
	}
	resp2, _ := http.Get(srv.URL + "/limits/nope")
	if resp2.StatusCode != 404 {
		t.Fatalf("unknown model limits want 404, got %d", resp2.StatusCode)
	}
	resp2.Body.Close()
}

func TestPostLimitChange(t *testing.T) {
	d := &fakeDeps{known: map[string]bool{"model-a": true}}
	srv := apiSrv(t, d)
	code, _ := postJSON(t, srv.URL+"/limits/model-a", `{"rpm":100,"tpm":2000}`)
	if code != 200 {
		t.Fatalf("limit change want 200, got %d", code)
	}
	code, _ = postJSON(t, srv.URL+"/limits/nope", `{"rpm":100,"tpm":2000}`)
	if code != 404 {
		t.Fatalf("unknown model want 404, got %d", code)
	}
	code, _ = postJSON(t, srv.URL+"/limits/model-a", `{"rpm":"x"}`)
	if code != 400 {
		t.Fatalf("malformed want 400, got %d", code)
	}
}

func TestBatchUnknownModel(t *testing.T) {
	d := &fakeDeps{known: map[string]bool{"model-a": true}}
	srv := apiSrv(t, d)
	code, _ := postJSON(t, srv.URL+"/batches", `{"model":"nope","estimated_tokens":100,"count":5}`)
	if code != 400 {
		t.Fatalf("unknown model want 400, got %d", code)
	}
}

// ---------- T9: observed-window metrics + batch results endpoints ----------

func (deps *fakeDeps) ObservedWindows(model string) (WindowSample, error) {
	if !deps.known[model] {
		return WindowSample{}, errUnknownModel
	}
	return deps.sample, nil
}

func (deps *fakeDeps) BatchStatus(id string) (BatchStatus, bool) {
	if id != deps.batchID {
		return BatchStatus{}, false
	}
	return BatchStatus{ID: id, Status: "completed", Successful: 7, Failed: 1, Expired: 1, Rejected: 1, Total: 10}, true
}

func (deps *fakeDeps) BatchResults(id string) ([]ResultRow, bool) {
	if id != deps.batchID {
		return nil, false
	}
	return []ResultRow{
		{ID: "r-1", State: "completed", Outcome: "success", Tokens: 900},
		{ID: "r-2", State: "completed", Outcome: "success", Tokens: 910},
	}, true
}

func TestObservedWindowsEndpoint(t *testing.T) {
	d := &fakeDeps{known: map[string]bool{"model-a": true}}
	d.sample = WindowSample{ObservedRPM: 29000, ObservedTPM: 480000, ConfiguredRPM: 30000, ConfiguredTPM: 500000}
	srv := apiSrv(t, d)
	resp, err := http.Get(srv.URL + "/metrics/model-a")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got WindowSample
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || got.ObservedRPM != 29000 || got.ConfiguredRPM != 30000 {
		t.Fatalf("GET /metrics: %d %+v", resp.StatusCode, got)
	}
	resp2, _ := http.Get(srv.URL + "/metrics/nope")
	if resp2.StatusCode != 404 {
		t.Fatalf("unknown model want 404, got %d", resp2.StatusCode)
	}
	resp2.Body.Close()
}

func TestBatchResultsEndpoint(t *testing.T) {
	d := &fakeDeps{known: map[string]bool{"model-a": true}, batchID: "b-9"}
	srv := apiSrv(t, d)
	resp, err := http.Get(srv.URL + "/batches/b-9/results")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var rows []ResultRow
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || len(rows) != 2 || rows[0].ID != "r-1" || rows[1].Tokens != 910 {
		t.Fatalf("GET results: %d %+v", resp.StatusCode, rows)
	}
	resp2, _ := http.Get(srv.URL + "/batches/nope/results")
	if resp2.StatusCode != 404 {
		t.Fatalf("unknown batch want 404, got %d", resp2.StatusCode)
	}
	resp2.Body.Close()
}

func TestBatchStatusEndpoint(t *testing.T) {
	d := &fakeDeps{known: map[string]bool{"model-a": true}, batchID: "b-9"}
	srv := apiSrv(t, d)
	resp, err := http.Get(srv.URL + "/batches/b-9")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got BatchStatus
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || got.Status != "completed" || got.Successful != 7 {
		t.Fatalf("GET /batches/b-9: %d %+v", resp.StatusCode, got)
	}
	resp2, _ := http.Get(srv.URL + "/batches/nope")
	if resp2.StatusCode != 404 {
		t.Fatalf("unknown batch want 404, got %d", resp2.StatusCode)
	}
	resp2.Body.Close()
}
