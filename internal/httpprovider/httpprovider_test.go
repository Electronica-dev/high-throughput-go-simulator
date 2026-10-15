package httpprovider

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"consuma/internal/ledger"
	"consuma/internal/provider"
)

func mkReq(id string) *ledger.Request {
	return &ledger.Request{ID: 1, ExternalID: id, Model: "m", EstimateTokens: 1000}
}

func TestHappyPathMapsResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"outcome":"success","actual_tokens":933,"latency_ms":42}`))
	}))
	defer srv.Close()

	p := New(srv.URL, srv.Client())
	res := p.Call(mkReq("r-1"), time.Now())
	if res.Outcome != provider.Success || res.ActualTokens != 933 {
		t.Fatalf("mapped result mismatch: %+v", res)
	}
	if res.Latency != 42*time.Millisecond {
		t.Fatalf("latency mapping mismatch: %v", res.Latency)
	}
}

func Test429IsProviderRejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	p := New(srv.URL, srv.Client())
	if res := p.Call(mkReq("r-1"), time.Now()); res.Outcome != provider.ProviderRejected {
		t.Fatalf("429 should map to ProviderRejected, got %d", res.Outcome)
	}
}

func Test5xxIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	p := New(srv.URL, srv.Client())
	if res := p.Call(mkReq("r-1"), time.Now()); res.Outcome != provider.TransientFailure {
		t.Fatalf("5xx should map to TransientFailure, got %d", res.Outcome)
	}
}

func TestIdempotencyKeyHeader(t *testing.T) {
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		w.Write([]byte(`{"outcome":"success","actual_tokens":10}`))
	}))
	defer srv.Close()
	p := New(srv.URL, srv.Client())
	p.Call(mkReq("r-abc"), time.Now())
	if gotKey != "r-abc" {
		t.Fatalf("Idempotency-Key want r-abc, got %q", gotKey)
	}
}

func TestTimeoutIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Write([]byte(`{"outcome":"success"}`))
	}))
	defer srv.Close()
	p := New(srv.URL, &http.Client{Timeout: 20 * time.Millisecond})
	if res := p.Call(mkReq("r-1"), time.Now()); res.Outcome != provider.TransientFailure {
		t.Fatalf("timeout should map to TransientFailure, got %d", res.Outcome)
	}
}

// TestRequestBodyUsesExternalID: the simulator's request_id is the
// idempotency key — it must be the request's ExternalID (a JSON string), not
// the internal uint64 id (whose JSON encoding is a number and fails the
// simulator's body validation).
func TestRequestBodyUsesExternalID(t *testing.T) {
	var gotReqID string
	var gotModel string
	var gotTokens float64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			RequestID       string  `json:"request_id"`
			Model           string  `json:"model"`
			EstimatedTokens float64 `json:"estimated_tokens"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		gotReqID, gotModel, gotTokens = in.RequestID, in.Model, in.EstimatedTokens
		w.Write([]byte(`{"outcome":"success","actual_tokens":10}`))
	}))
	defer srv.Close()

	p := New(srv.URL, srv.Client())
	p.Call(&ledger.Request{ID: 7, ExternalID: "ext-uuid-9", Model: "m", EstimateTokens: 1000}, time.Now())
	if gotReqID != "ext-uuid-9" {
		t.Fatalf("request_id must be the ExternalID string, got %q", gotReqID)
	}
	if gotModel != "m" || gotTokens != 1000 {
		t.Fatalf("model/tokens mismatch: %q %v", gotModel, gotTokens)
	}
}

// TestNewBuildsPooledTransport: at hundreds of calls/sec the default
// transport (MaxIdleConnsPerHost=2) churns connections — resets surface as
// spurious transient failures and amplify retries. The provider must ship a
// pooled transport sized for its worker pool.
func TestNewBuildsPooledTransport(t *testing.T) {
	p := New("http://127.0.0.1:9001", nil)
	tr, ok := p.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("client.Transport must be *http.Transport, got %T", p.client.Transport)
	}
	if tr.MaxIdleConnsPerHost < 64 {
		t.Fatalf("MaxIdleConnsPerHost want >= 64, got %d", tr.MaxIdleConnsPerHost)
	}
	if tr.MaxIdleConns < 256 {
		t.Fatalf("MaxIdleConns want >= 256, got %d", tr.MaxIdleConns)
	}
}
