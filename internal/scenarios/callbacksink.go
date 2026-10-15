package scenarios

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// CallbackSink is the scenario-3 delivery sink: it rejects the first
// failFirst deliveries (HTTP 500) to prove the dispatcher's retry/backoff and
// resumption behavior, then accepts. It records the delivered payload for
// the callback-summary-vs-API-status comparison.
type CallbackSink struct {
	failFirst int

	mu          sync.Mutex
	hits        int
	payload     callbackPayload
	payloads    map[string]callbackPayload // per batch id (multi-batch scenarios)
	deliveredCh chan struct{}
	srv         *http.Server
	url         string
}

// callbackPayload mirrors the system's callback body.
type callbackPayload struct {
	BatchID    string      `json:"batch_id"`
	Status     string      `json:"status"`
	Successful int64       `json:"successful"`
	Failed     int64       `json:"failed"`
	Expired    int64       `json:"expired"`
	Rejected   int64       `json:"rejected"`
	Results    []ResultRow `json:"results,omitempty"`
}

// NewCallbackSink starts the sink on 127.0.0.1:0.
func NewCallbackSink(failFirst int) *CallbackSink {
	sink := &CallbackSink{failFirst: failFirst, deliveredCh: make(chan struct{}, 1), payloads: make(map[string]callbackPayload)}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /callback", sink.handle)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return sink // URL empty; runner reports the failure
	}
	sink.url = "http://" + ln.Addr().String() + "/callback"
	sink.srv = &http.Server{Handler: mux}
	go func() { _ = sink.srv.Serve(ln) }()
	return sink
}

func (sink *CallbackSink) handle(w http.ResponseWriter, r *http.Request) {
	var p callbackPayload
	_ = json.NewDecoder(r.Body).Decode(&p)
	sink.mu.Lock()
	sink.hits++
	hit := sink.hits
	sink.payload = p
	if sink.payloads == nil {
		sink.payloads = make(map[string]callbackPayload)
	}
	if p.BatchID != "" {
		sink.payloads[p.BatchID] = p
	}
	sink.mu.Unlock()
	accepted := hit > sink.failFirst
	fmt.Printf("[s3 sink] delivery attempt %d for batch %s -> %s (HTTP %d)\n",
		hit, p.BatchID, map[bool]string{true: "accepted", false: "rejected"}[accepted], statusFor(hit, sink.failFirst))
	if accepted {
		w.WriteHeader(http.StatusOK)
		select {
		case sink.deliveredCh <- struct{}{}:
		default:
		}
		return
	}
	w.WriteHeader(http.StatusInternalServerError)
}

// statusFor mirrors the sink's response code for logging.
func statusFor(hit, failFirst int) int {
	if hit <= failFirst {
		return 500
	}
	return 200
}

// URL returns the sink's callback URL (empty if the listener failed).
func (sink *CallbackSink) URL() string { return sink.url }

// AwaitDelivery waits for the first accepted (2xx) delivery.
func (sink *CallbackSink) AwaitDelivery(timeout time.Duration) bool {
	select {
	case <-sink.deliveredCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Hits returns the delivery attempts seen so far.
func (sink *CallbackSink) Hits() int {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.hits
}

// LastPayload returns the most recent callback payload (the summary the API
// must agree with).
func (sink *CallbackSink) LastPayload() callbackPayload {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.payload
}

// PayloadFor returns the most recent payload received for a batch id.
func (sink *CallbackSink) PayloadFor(batchID string) (callbackPayload, bool) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	p, ok := sink.payloads[batchID]
	return p, ok
}

// Close stops the sink.
func (sink *CallbackSink) Close() {
	if sink.srv != nil {
		_ = sink.srv.Close()
	}
}

// String renders the payload's summary (evidence output).
func (p callbackPayload) String() string {
	return fmt.Sprintf("batch %s status=%s successful=%d failed=%d expired=%d rejected=%d",
		p.BatchID, p.Status, p.Successful, p.Failed, p.Expired, p.Rejected)
}
