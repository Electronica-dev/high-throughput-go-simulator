package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"consuma/internal/ledger"
)

// fakeDeps implements the server's dependency surface.
type fakeDeps struct {
	mu        sync.Mutex
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

func (f *fakeDeps) SubmitAndWait(r *ledger.Request, timeout time.Duration) (string, int64, error) {
	f.mu.Lock()
	f.submitted++
	f.lastReq = r
	f.mu.Unlock()
	return f.waitRes, f.waitTok, f.waitErr
}

func (f *fakeDeps) Submit(r *ledger.Request) {}

func (f *fakeDeps) SetLimit(model string, rpm, tpm int64) error { return f.setErr }

func (f *fakeDeps) Limits(model string) (int64, int64, error) {
	if !f.known[model] {
		return 0, 0, errUnknownModel
	}
	return f.limitsRPM, f.limitsTPM, nil
}

func (f *fakeDeps) CreateBatch(model string, tokens, count int64, callbackURL string) (string, error) {
	if !f.known[model] {
		return "", errUnknownModel
	}
	if f.batchErr != nil {
		return "", f.batchErr
	}
	return f.batchID, nil
}

func apiSrv(t *testing.T, d *fakeDeps) *httptest.Server {
	t.Helper()
	s := New(d, []string{"model-a"})
	srv := httptest.NewServer(s.Handler())
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


