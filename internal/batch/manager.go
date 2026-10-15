// Package batch owns async batches: registration (one bulk SQLite
// transaction so a 10K batch acks in well under a second), live counting via
// the engine's OnTerminal hook, batch completion, and callback delivery with
// a persisted, resumable attempt schedule.
package batch

import (
	"fmt"
	"os"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"consuma/internal/config"
	"consuma/internal/engine"
	"consuma/internal/ledger"
	"consuma/internal/store"
)

// batchState tracks one in-flight batch's live counters.
type batchState struct {
	id          string
	total       int64
	callbackURL string
	terminal    atomic.Int64
	successful  atomic.Int64
	failed      atomic.Int64
	expired     atomic.Int64
	rejected    atomic.Int64
}

// Manager coordinates batches between the API, the engine and the store.
type Manager struct {
	st  *store.Store
	eng *engine.Engine
	cfg config.CallbackConfig

	mu      sync.Mutex
	batches map[string]*batchState

	client *http.Client
}

// New builds the manager and installs itself as the engine's terminal hook.
func New(st *store.Store, eng *engine.Engine, cfg config.CallbackConfig) *Manager {
	if cfg.PeriodicInterval <= 0 {
		cfg.PeriodicInterval = 5 * time.Second
	}
	if cfg.DeliveryTTL <= 0 {
		cfg.DeliveryTTL = 5 * time.Minute
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = 5 * time.Second
	}
	if len(cfg.InitialBackoffs) == 0 {
		cfg.InitialBackoffs = []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}
	}
	m := &Manager{
		st:      st,
		eng:     eng,
		cfg:     cfg,
		batches: make(map[string]*batchState),
		client:  &http.Client{Timeout: cfg.HTTPTimeout},
	}
	eng.OnTerminal = m.onTerminal
	return m
}

// Submit registers a batch, persists every request row in one bulk
// transaction, and hands the requests to the engine. Returns as soon as
// everything is accepted (well under 1s even for 10K requests).
func (m *Manager) Submit(model string, tokens, count int64, callbackURL string) (string, error) {
	id := uuid.NewString()
	if err := m.st.CreateBatch(store.Batch{
		ID:          id,
		Status:      "processing",
		CallbackURL: callbackURL,
		Counts:      store.Counts{Total: count},
		CreatedAt:   time.Now(),
	}); err != nil {
		return "", fmt.Errorf("batch: create %s: %w", id, err)
	}

	rows := make([]store.RequestRow, count)
	reqs := make([]*ledger.Request, count)
	now := time.Now()
	for i := int64(0); i < count; i++ {
		ext := uuid.NewString()
		rows[i] = store.RequestRow{ID: ext, BatchID: id, Model: model, State: "submitted", Tokens: tokens}
		reqs[i] = &ledger.Request{
			ExternalID:     ext,
			BatchExtID:     id,
			Model:          model,
			EstimateTokens: tokens,
			BornNS:         now.UnixNano(),
		}
	}
	if err := m.st.BulkInsertRequests(rows); err != nil {
		_ = m.st.SetBatchStatus(id, "failed")
		return "", fmt.Errorf("batch: persist %s: %w", id, err)
	}

	m.mu.Lock()
	m.batches[id] = &batchState{id: id, total: count, callbackURL: callbackURL}
	m.mu.Unlock()

	for _, r := range reqs {
		m.eng.SubmitUntracked(r)
	}
	return id, nil
}

// Restore re-registers batches that were mid-flight (status=processing)
// when the previous process died: without their in-memory batchState, the
// OnTerminal hook finds no batch, counts freeze at the crash-time values,
// and the batch never completes. Carries the persisted counts forward so
// the remaining terminal events finish the batch exactly.
func (m *Manager) Restore() error {
	rows, err := m.st.ProcessingBatches()
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range rows {
		if _, ok := m.batches[b.ID]; ok {
			continue
		}
		terminal := b.Counts.Successful + b.Counts.Failed + b.Counts.Expired + b.Counts.Rejected
		bs := &batchState{
			id:          b.ID,
			total:       b.Counts.Total,
			callbackURL: b.CallbackURL,
		}
		bs.terminal.Store(terminal)
		bs.successful.Store(b.Counts.Successful)
		bs.failed.Store(b.Counts.Failed)
		bs.expired.Store(b.Counts.Expired)
		bs.rejected.Store(b.Counts.Rejected)
		m.batches[b.ID] = bs
	}
	return nil
}

// onTerminal is the engine's terminal hook: counts the outcome, persists the
// running batch breakdown, and completes + dispatches the callback when the
// last request lands.
func (m *Manager) onTerminal(r *ledger.Request, outcome string, tokens int64) {
	m.mu.Lock()
	bs := m.batches[r.BatchExtID]
	m.mu.Unlock()
	if bs == nil {
		if os.Getenv("CONSUMA_DEBUG_BATCH") != "" {
			fmt.Println("DBG onTerminal: no batchState for", r.BatchExtID, "outcome", outcome)
		}
		return // not a batch request (sync path has no BatchExtID)
	}
	switch outcome {
	case "success":
		bs.successful.Add(1)
	case "failed":
		bs.failed.Add(1)
	case "expired":
		bs.expired.Add(1)
	default: // rejected
		bs.rejected.Add(1)
	}
	counts := store.Counts{
		Total:      bs.total,
		Successful: bs.successful.Load(),
		Failed:     bs.failed.Load(),
		Expired:    bs.expired.Load(),
		Rejected:   bs.rejected.Load(),
	}
	_ = m.st.UpdateBatchCounts(bs.id, counts)
	// >= not ==: a restored batch's carried-forward counts can drift by the
	// crash-window lag; the batch must complete when the LAST request lands,
	// never hang because an event was double- or under-counted
	if bs.terminal.Add(1) >= bs.total {
		_ = m.st.SetBatchStatus(bs.id, "completed")
		if bs.callbackURL != "" {
			go m.deliver(bs.id, bs.callbackURL)
		}
	}
}

// Status returns a batch's persisted record.
func (m *Manager) Status(id string) (store.Batch, bool) {
	b, ok, err := m.st.GetBatch(id)
	if err != nil || !ok {
		return store.Batch{}, false
	}
	return b, true
}

// ResultRow is one request's outcome inside a batch.
type ResultRow struct {
	ID     string
	State  string
	Outcome string
	Tokens int64
}

// Results lists a batch's per-request outcomes.
func (m *Manager) Results(id string) ([]ResultRow, bool) {
	if _, ok, _ := m.st.GetBatch(id); !ok {
		return nil, false
	}
	rows, err := m.st.RequestsByBatch(id)
	if err != nil {
		return nil, false
	}
	out := make([]ResultRow, len(rows))
	for i, r := range rows {
		out[i] = ResultRow{ID: r.ID, State: r.State, Outcome: r.Outcome, Tokens: r.Tokens}
	}
	return out, true
}

// Resume re-dispatches callbacks for every completed batch whose delivery
// never succeeded (crash between completion and delivery — the client only
// sees extra latency, nothing is lost).
func (m *Manager) Resume() error {
	ids, err := m.st.UndeliveredBatches()
	if err != nil {
		return err
	}
	for _, id := range ids {
		b, ok, _ := m.st.GetBatch(id)
		if !ok || b.CallbackURL == "" {
			continue
		}
		go m.deliver(id, b.CallbackURL)
	}
	return nil
}
