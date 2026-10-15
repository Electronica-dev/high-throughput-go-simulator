// Package httpprovider calls the standalone simulator service over HTTP:
// one real round trip per provider call (the simulator's response delay IS
// the simulated latency). It satisfies engine.Caller for real mode.
package httpprovider

import (
	"bytes"
	"fmt"
	"encoding/json"
	"net/http"
	"time"

	"consuma/internal/ledger"
	"consuma/internal/provider"
)

// HTTPProvider posts calls to the simulator's /call endpoint.
type HTTPProvider struct {
	url    string // simulator base URL
	client *http.Client
}

// New builds a provider client against the simulator at url.
func New(url string, client *http.Client) *HTTPProvider {
	if client == nil {
		// pooled transport: at hundreds of calls/sec the default transport
		// (MaxIdleConnsPerHost=2) churns TCP connections, and connection
		// resets surface as spurious transient failures that amplify the
		// retry load. Size the pool for the engine's worker count and above.
		tr := &http.Transport{
			MaxIdleConns:        512,
			MaxIdleConnsPerHost: 128,
			MaxConnsPerHost:     0, // unbounded active conns; workers bound concurrency
			IdleConnTimeout:     90 * time.Second,
		}
		client = &http.Client{Timeout: 30 * time.Second, Transport: tr}
	}
	return &HTTPProvider{url: url, client: client}
}

// CallResult is aliased from provider for interface satisfaction.
type CallResult = provider.CallResult

type callResponse struct {
	Outcome     string `json:"outcome"`
	ActualTokens int64 `json:"actual_tokens"`
	LatencyMS   int64  `json:"latency_ms"`
}

// Call performs one provider call. The request ID is the idempotency key
// (body field + Idempotency-Key header). Mapping: 200 → parsed outcome;
// 429 → ProviderRejected; 5xx/timeout/network error → TransientFailure.
func (h *HTTPProvider) Call(r *ledger.Request, now time.Time) provider.CallResult {
	body, _ := json.Marshal(map[string]any{
		"request_id":      r.ExternalID,
		"model":           r.Model,
		"estimated_tokens": r.EstimateTokens,
	})
	req, err := http.NewRequest(http.MethodPost, h.url+"/call", bytes.NewReader(body))
	if err != nil {
		return provider.CallResult{Outcome: provider.TransientFailure}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", r.ExternalID)

	resp, err := h.client.Do(req)
	if err != nil {
		return provider.CallResult{Outcome: provider.TransientFailure}
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return provider.CallResult{Outcome: provider.ProviderRejected}
	case resp.StatusCode >= 500:
		return provider.CallResult{Outcome: provider.TransientFailure}
	case resp.StatusCode != http.StatusOK:
		return provider.CallResult{Outcome: provider.TransientFailure}
	}

	var cr callResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return provider.CallResult{Outcome: provider.TransientFailure}
	}
	outcome, err := parseOutcome(cr.Outcome)
	if err != nil {
		return provider.CallResult{Outcome: provider.TransientFailure}
	}
	return provider.CallResult{
		Latency:      time.Duration(cr.LatencyMS) * time.Millisecond,
		Outcome:      outcome,
		ActualTokens: cr.ActualTokens,
	}
}

func parseOutcome(s string) (provider.Outcome, error) {
	switch s {
	case "success":
		return provider.Success, nil
	case "transient_failure":
		return provider.TransientFailure, nil
	case "permanent_failure":
		return provider.PermanentFailure, nil
	case "provider_rejected":
		return provider.ProviderRejected, nil
	default:
		return 0, fmt.Errorf("unknown outcome %q", s)
	}
}
