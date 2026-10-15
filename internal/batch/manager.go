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
// when the previous process died. Batch counters are RECOMPUTED from the
// request rows (row truth), not trusted from the persisted batch row — the
// persisted counters can lag (crash between a terminal write and the count
// write) and the status write itself can be lost (all rows terminal but
// the batch still reads processing). Three crash windows handled:
//
// (a) stale persisted counters → reconciled from rows before registering;
// (b) all rows terminal, status write lost → batch completed here (+ callback
//     dispatched — Resume() only scans completed batches, so this is where
//     the delivery promise is kept);
// (c) batch row persisted but request rows never bulk-inserted → marked
//     failed (it can never progress).
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
		scs, err := m.st.BatchRequestStates(b.ID)
		if err != nil {
			return err
		}
		var successful, failed, expired, rejected, terminal, rowCount int64
		for _, sc := range scs {
			rowCount += sc.N
			switch sc.State {
			case "completed":
				terminal += sc.N
				if sc.Outcome == "success" {
					successful += sc.N
				} else {
					failed += sc.N
				}
			case "expired":
				terminal += sc.N
				expired += sc.N
			case "rejected":
				terminal += sc.N
				rejected += sc.N
			}
		}
		counts := store.Counts{
			Total:      b.Counts.Total,
			Successful: successful,
			Failed:     failed,
			Expired:    expired,
			Rejected:   rejected,
		}

		switch {
		case rowCount == 0 && b.Counts.Total > 0:
			// window (c): rows never persisted; the batch can never progress
			_ = m.st.SetBatchStatus(b.ID, "failed")
			if b.CallbackURL != "" {
				go m.deliver(b.ID, b.CallbackURL)
			}
			continue
		case terminal >= b.Counts.Total:
			// window (b): every request terminal but the status write was lost
			_ = m.st.UpdateBatchCounts(b.ID, counts)
			_ = m.st.SetBatchStatus(b.ID, "completed")
			if b.CallbackURL != "" {
				go m.deliver(b.ID, b.CallbackURL)
			}
			continue
		}

		// mid-flight: register with row-truth counts; the recovered
		// non-terminal rows will fire the remaining terminals
		bs := &batchState{
			id:          b.ID,
			total:       b.Counts.Total,
			callbackURL: b.CallbackURL,
		}
		bs.terminal.Store(terminal)
		bs.successful.Store(successful)
		bs.failed.Store(failed)
		bs.expired.Store(expired)
		bs.rejected.Store(rejected)
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
	ID      string `json:"requestId"`
	State   string `json:"state"`
	Outcome string `json:"outcome"`
	Tokens  int64  `json:"tokens"`
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
