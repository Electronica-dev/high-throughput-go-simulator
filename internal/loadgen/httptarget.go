// HTTP run target: real-mode load generation. Submissions go to the system's
// POST /batches (one POST per model group per chunk — the API takes one model
// per batch); 202 batch ids are recorded; after the run the ids are polled
// until completed as the batch-completion evidence. MarkSteady is a no-op in
// real mode: pacing is client-side, and the server has no engine clock to
// mark.
package loadgen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"consuma/internal/ledger"
)

// HTTPTarget satisfies RunTarget against a running `consuma serve`.
type HTTPTarget struct {
	url    string
	client *http.Client

	mu   sync.Mutex
	ids  []string
	errs []error
}

// NewHTTPTarget builds the target against the system at url.
func NewHTTPTarget(url string, client *http.Client) *HTTPTarget {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &HTTPTarget{url: url, client: client}
}

// SubmitBatch groups the chunk by model and posts one batch per group.
func (target *HTTPTarget) SubmitBatch(reqs []*ledger.Request) {
	if len(reqs) == 0 {
		return
	}
	// group by model (chunks usually single-model; mixed chunks split)
	order := make([]string, 0, 2)
	counts := make(map[string]int64)
	for _, r := range reqs {
		if _, ok := counts[r.Model]; !ok {
			order = append(order, r.Model)
		}
		counts[r.Model]++
	}
	for _, model := range order {
		target.post(model, reqs[0].EstimateTokens, counts[model])
	}
}

func (target *HTTPTarget) post(model string, tokens, count int64) {
	body, _ := json.Marshal(map[string]any{
		"model":            model,
		"estimated_tokens": tokens,
		"count":            count,
	})
	resp, err := target.client.Post(target.url+"/batches", "application/json", bytes.NewReader(body))
	if err != nil {
		target.mu.Lock()
		target.errs = append(target.errs, fmt.Errorf("POST /batches: %w", err))
		target.mu.Unlock()
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		target.mu.Lock()
		target.errs = append(target.errs, fmt.Errorf("POST /batches: status %d", resp.StatusCode))
		target.mu.Unlock()
		return
	}
	var out struct {
		BatchID string `json:"batch_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.BatchID == "" {
		target.mu.Lock()
		target.errs = append(target.errs, fmt.Errorf("POST /batches: bad 202 body"))
		target.mu.Unlock()
		return
	}
	target.mu.Lock()
	target.ids = append(target.ids, out.BatchID)
	target.mu.Unlock()
}

// MarkSteady is a no-op in real mode (client-side pacing).
func (target *HTTPTarget) MarkSteady(startNS, endNS int64) {}

// BatchIDs returns the recorded 202 batch ids, in submission order.
func (target *HTTPTarget) BatchIDs() []string {
	target.mu.Lock()
	defer target.mu.Unlock()
	out := make([]string, len(target.ids))
	copy(out, target.ids)
	return out
}

// Errors returns submission errors seen so far.
func (target *HTTPTarget) Errors() []error {
	target.mu.Lock()
	defer target.mu.Unlock()
	out := make([]error, len(target.errs))
	copy(out, target.errs)
	return out
}

// AwaitCompletions polls every recorded batch until its status reads
// completed (or the timeout expires) — the batch-completion evidence for the
// run report. Returns the number of batches observed completed.
func (target *HTTPTarget) AwaitCompletions(timeout, pollEvery time.Duration) (int, error) {
	ids := target.BatchIDs()
	if len(ids) == 0 {
		return 0, nil
	}
	deadline := time.Now().Add(timeout)
	completed := make(map[string]bool, len(ids))
	for len(completed) < len(ids) {
		for _, id := range ids {
			if completed[id] {
				continue
			}
			done, status, err := target.status(id)
			if err != nil {
				return len(completed), fmt.Errorf("poll %s: %w", id, err)
			}
			if done && status == "completed" {
				completed[id] = true
			}
		}
		if len(completed) == len(ids) || time.Now().After(deadline) {
			break
		}
		time.Sleep(pollEvery)
	}
	return len(completed), nil
}

// status fetches one batch's status; done=true when the HTTP fetch succeeded.
func (target *HTTPTarget) status(id string) (bool, string, error) {
	resp, err := target.client.Get(target.url + "/batches/" + id)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, "", fmt.Errorf("GET /batches/%s: status %d", id, resp.StatusCode)
	}
	var out struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, "", err
	}
	return true, out.Status, nil
}
