// Callback delivery: per-batch dispatcher goroutine with the persisted
// attempt schedule — immediate first attempt, then the configurable initial
// backoffs (500ms/1s/2s), then a periodic retry (5s) until the delivery TTL
// (5m). Every attempt is persisted; the first 2xx stops delivery.
package batch

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"
)

type callbackPayload struct {
	BatchID    string      `json:"batch_id"`
	Status     string      `json:"status"`
	Successful int64       `json:"successful"`
	Failed     int64       `json:"failed"`
	Expired    int64       `json:"expired"`
	Rejected   int64       `json:"rejected"`
	Results    []ResultRow `json:"results,omitempty"`
}

// deliver drives one batch's callback until delivered or the TTL expires.
// Runs in its own goroutine; multiple batches deliver concurrently.
func (manager *Manager) deliver(batchID, url string) {
	deadline := time.Now().Add(manager.cfg.DeliveryTTL)
	waits := manager.cfg.InitialBackoffs
	for i := 0; ; i++ {
		if manager.attempt(batchID, url) {
			return
		}
		if time.Now().After(deadline) {
			return // TTL exhausted; Resume() re-dispatches after a restart
		}
		var wait time.Duration
		if i < len(waits) {
			wait = waits[i]
		} else {
			wait = manager.cfg.PeriodicInterval
		}
		time.Sleep(wait)
	}
}

// attempt makes one delivery try; returns true iff delivered (2xx).
func (manager *Manager) attempt(batchID, url string) bool {
	b, ok, _ := manager.st.GetBatch(batchID)
	if !ok {
		return true // batch vanished; nothing to deliver
	}
	payload := callbackPayload{
		BatchID:    batchID,
		Status:     b.Status,
		Successful: b.Counts.Successful,
		Failed:     b.Counts.Failed,
		Expired:    b.Counts.Expired,
		Rejected:   b.Counts.Rejected,
	}
	if results, ok := manager.Results(batchID); ok {
		payload.Results = results
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		manager.record(batchID, 0, "rejected")
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := manager.client.Do(req)
	if err != nil {
		manager.record(batchID, 0, "rejected") // network error
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		manager.record(batchID, resp.StatusCode, "delivered")
		return true
	}
	manager.record(batchID, resp.StatusCode, "rejected")
	return false
}

func (manager *Manager) record(batchID string, code int, outcome string) {
	_ = manager.st.RecordCallbackAttempt(batchID, time.Now(), code, outcome)
}
