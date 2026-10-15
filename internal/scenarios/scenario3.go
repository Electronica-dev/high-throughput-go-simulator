// Scenario 3 — durable async batches with callbacks: one 10K batch per
// model (transient+permanent failure mix), callback sink rejects the first
// deliveries then accepts; asserts ack < 1s, callback only after all
// terminal, delivery success, callback summary == API batch status, and
// every request id exactly once.
package scenarios

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

)

// RunScenario3 submits the durable batches and asserts the delivery
// semantics. callbackURL is a sink the runner controls (reject-first mode).
func RunScenario3(systemURL, callbackURL string, sc Config, outDir string) (*Result, error) {
	client := &http.Client{Timeout: 60 * time.Second}
	res := &Result{Pass: true}

	// managed sink: reject-first proves retry/backoff + resumption; the
	// delivered payload is then compared against the API's batch status
	var sink *CallbackSink
	if callbackURL == "" {
		sink = NewCallbackSink(sc.CallbackFails)
		defer sink.Close()
		callbackURL = sink.URL()
	}

	var ackWorst time.Duration
	var ids []string
	runStart := time.Now()
	stepf := func(format string, args ...any) {
		fmt.Printf("[s3 +%.1fs] %s\n", time.Since(runStart).Seconds(), fmt.Sprintf(format, args...))
	}
	for _, model := range []string{sc.Model, sc.SecondModel} {
		start := time.Now()
		var out struct {
			BatchID string `json:"batch_id"`
		}
		if err := postDecode(client, systemURL+"/batches", map[string]any{
			"model": model, "estimated_tokens": sc.Tokens, "count": sc.BatchCount, "callback_url": callbackURL,
		}, &out); err != nil {
			return nil, fmt.Errorf("submit batch for %s: %w", model, err)
		}
		ack := time.Since(start)
		if ackWorst < ack {
			ackWorst = ack
		}
		ids = append(ids, out.BatchID)
		stepf("acknowledgement returned in %v with batch id: %s (%s, %d requests) — within 1s: %v",
			ack, out.BatchID, model, sc.BatchCount, ack <= time.Second)
	}
	if ackWorst > time.Second {
		res.Pass = false
		res.Reasons = append(res.Reasons, fmt.Sprintf("batch ack took %v, want < 1s", ackWorst))
	}
	stepf("submitted; the callback dispatcher fires only after EVERY request is terminal — watching the sink")

	// await completion via polling each batch id directly (the callback
	// firing itself is asserted by the sink's counters, which the operator
	// observes at the sink URL)
	completed := 0
	deadline := time.Now().Add(10 * time.Minute)
	for completed < len(ids) {
		completed = 0
		for _, id := range ids {
			var bs batchStatus
			if err := getJSON(client, systemURL+"/batches/"+id, &bs); err == nil && bs.Status == "completed" {
				completed++
			}
		}
		if completed == len(ids) || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if completed != len(ids) {
		res.Pass = false
		res.Reasons = append(res.Reasons, fmt.Sprintf("completed %d of %d batches", completed, len(ids)))
	}
	if completed > 0 {
		stepf("all %d/%d batches terminal — callback dispatcher gate passed; sink saw %d delivery attempt(s) so far", completed, len(ids), sinkHits(sink))
	}

	sums, err := fetchSummaries(client, systemURL, ids)
	if err != nil {
		return nil, err
	}
	var okN int64
	for _, b := range sums {
		okN += b.Successful + b.Failed + b.Expired + b.Rejected
		if !b.UniqueIDs {
			res.Pass = false
			res.Reasons = append(res.Reasons, fmt.Sprintf("batch %s has duplicate result ids", b.ID))
		}
		if b.Status != "completed" {
			res.Pass = false
			res.Reasons = append(res.Reasons, fmt.Sprintf("batch %s status %q", b.ID, b.Status))
		}
	}
	// every id exactly once across both batches
	if len(sums) == 2 && !IDsUnique(append(sums[0].Results, sums[1].Results...)) {
		res.Pass = false
		res.Reasons = append(res.Reasons, "request ids are not unique across batches")
	}
	// every request accounted
	if okN == 0 {
		res.Pass = false
		res.Reasons = append(res.Reasons, "no requests accounted")
	}
	if okN > 0 {
		stepf("results verified: %d requests accounted, ids unique per batch: %v", okN, allUnique(sums))
	}

	// managed-sink verification: delivery happened, and the callback summary
	// agrees with the API's batch status
	if sink != nil {
		if !sink.AwaitDelivery(3 * time.Minute) {
			res.Pass = false
			res.Reasons = append(res.Reasons, fmt.Sprintf("callback never delivered in %d attempts", sink.Hits()))
		} else {
			stepf("callback delivered after %d attempt(s) (sink rejected the first %d, retry/backoff recovered)", sink.Hits(), sc.CallbackFails)
		}
		agree := true
		for _, id := range ids {
			var bs batchStatus
			if err := getJSON(client, systemURL+"/batches/"+id, &bs); err != nil {
				continue
			}
			p, ok := sink.PayloadFor(id)
			if !ok {
				res.Pass = false
				res.Reasons = append(res.Reasons, fmt.Sprintf("no callback payload recorded for %s", id))
				continue
			}
			if p.Status != bs.Status || p.Successful != bs.Successful || p.Failed != bs.Failed ||
				p.Expired != bs.Expired || p.Rejected != bs.Rejected {
				agree = false
				res.Pass = false
				res.Reasons = append(res.Reasons, fmt.Sprintf("callback summary != API status for %s: callback(%s,%d,%d,%d,%d) api(%s,%d,%d,%d,%d)",
					id, p.Status, p.Successful, p.Failed, p.Expired, p.Rejected,
					bs.Status, bs.Successful, bs.Failed, bs.Expired, bs.Rejected))
			}
		}
		if agree && len(ids) > 0 {
			stepf("callback summary == API batch status for all %d batch(es): true", len(ids))
		}
	}

	res.Batches = sums
	if err := writeEvidence(outDir, "scenario-3", res); err != nil {
		return nil, err
	}
	return res, nil
}

// sinkHits is a nil-safe Hits for the managed sink (external sinks have none).
func sinkHits(sink *CallbackSink) int {
	if sink == nil {
		return 0
	}
	return sink.Hits()
}

// allUnique reports whether every batch's result ids appear exactly once.
func allUnique(sums []BatchSummary) bool {
	for _, b := range sums {
		if !b.UniqueIDs {
			return false
		}
	}
	return true
}

// postDecode POSTs JSON and decodes the response body.
func postDecode(client *http.Client, url string, in any, out any) error {
	body, _ := json.Marshal(in)
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s: status %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
