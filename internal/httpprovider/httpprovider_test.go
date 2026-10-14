package httpprovider

import (
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
