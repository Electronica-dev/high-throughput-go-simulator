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
	s := &CallbackSink{failFirst: failFirst, deliveredCh: make(chan struct{}, 1), payloads: make(map[string]callbackPayload)}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /callback", s.handle)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return s // URL empty; runner reports the failure
	}
	s.url = "http://" + ln.Addr().String() + "/callback"
	s.srv = &http.Server{Handler: mux}
	go func() { _ = s.srv.Serve(ln) }()
	return s
}

func (s *CallbackSink) handle(w http.ResponseWriter, r *http.Request) {
	var p callbackPayload
	_ = json.NewDecoder(r.Body).Decode(&p)
	s.mu.Lock()
	s.hits++
	hit := s.hits
	s.payload = p
	if s.payloads == nil {
		s.payloads = make(map[string]callbackPayload)
	}
	if p.BatchID != "" {
		s.payloads[p.BatchID] = p
	}
	s.mu.Unlock()
	accepted := hit > s.failFirst
	fmt.Printf("[s3 sink] delivery attempt %d for batch %s -> %s (HTTP %d)\n",
		hit, p.BatchID, map[bool]string{true: "accepted", false: "rejected"}[accepted], statusFor(hit, s.failFirst))
	if accepted {
		w.WriteHeader(http.StatusOK)
		select {
		case s.deliveredCh <- struct{}{}:
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
func (s *CallbackSink) URL() string { return s.url }

// AwaitDelivery waits for the first accepted (2xx) delivery.
func (s *CallbackSink) AwaitDelivery(timeout time.Duration) bool {
	select {
	case <-s.deliveredCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Hits returns the delivery attempts seen so far.
func (s *CallbackSink) Hits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits
}

// LastPayload returns the most recent callback payload (the summary the API
// must agree with).
func (s *CallbackSink) LastPayload() callbackPayload {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.payload
}

// PayloadFor returns the most recent payload received for a batch id.
func (s *CallbackSink) PayloadFor(batchID string) (callbackPayload, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.payloads[batchID]
	return p, ok
}

// Close stops the sink.
func (s *CallbackSink) Close() {
	if s.srv != nil {
		_ = s.srv.Close()
	}
}

// String renders the payload's summary (evidence output).
func (p callbackPayload) String() string {
	return fmt.Sprintf("batch %s status=%s successful=%d failed=%d expired=%d rejected=%d",
		p.BatchID, p.Status, p.Successful, p.Failed, p.Expired, p.Rejected)
}

