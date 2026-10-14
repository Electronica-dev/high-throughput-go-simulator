// Package store persists real-mode state in SQLite (WAL mode): batches,
// per-request lifecycle rows, callback delivery attempts, and the
// simulator's idempotency seen-keys table. Real-mode only — the benchmark
// hot path never touches it.
package store

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Counts is a batch's outcome breakdown.
type Counts struct {
	Total      int64
	Successful int64
	Failed     int64
	Expired    int64
	Rejected   int64
}

// Batch is one submitted batch.
type Batch struct {
	ID          string
	Status      string
	CallbackURL string
	Counts      Counts
	CreatedAt   time.Time
}

// RequestRow is one request's persisted lifecycle row.
type RequestRow struct {
	ID          string
	BatchID     string
	Model       string
	State       string // submitted | queued | in_flight | completed | expired | rejected
	Outcome     string
	Tokens      int64
	AdmittedSec int64
}

// Attempt is one callback delivery attempt.
type Attempt struct {
	At         time.Time
	StatusCode int
	Outcome    string
}

// Store wraps the SQLite handle.
type Store struct {
	db *sql.DB
}

// Open creates/opens the database file with WAL pragmas.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
		"PRAGMA synchronous=NORMAL",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("store: pragma %q: %w", pragma, err)
		}
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS batches (
	id TEXT PRIMARY KEY,
	status TEXT NOT NULL,
	callback_url TEXT NOT NULL,
	total INTEGER NOT NULL DEFAULT 0,
	successful INTEGER NOT NULL DEFAULT 0,
	failed INTEGER NOT NULL DEFAULT 0,
	expired INTEGER NOT NULL DEFAULT 0,
	rejected INTEGER NOT NULL DEFAULT 0,
	created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS requests (
	id TEXT PRIMARY KEY,
	batch_id TEXT NOT NULL,
	model TEXT NOT NULL,
	state TEXT NOT NULL,
	outcome TEXT NOT NULL DEFAULT '',
	tokens INTEGER NOT NULL DEFAULT 0,
	admitted_sec INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_requests_batch ON requests(batch_id);
CREATE INDEX IF NOT EXISTS idx_requests_state ON requests(state);
CREATE TABLE IF NOT EXISTS callback_attempts (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	batch_id TEXT NOT NULL,
	at INTEGER NOT NULL,
	status_code INTEGER NOT NULL,
	outcome TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempts_batch ON callback_attempts(batch_id);
CREATE TABLE IF NOT EXISTS seen_keys (
	key TEXT PRIMARY KEY,
	result TEXT NOT NULL
);
`)
	if err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	return nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// CreateBatch inserts a new batch row.
func (s *Store) CreateBatch(b Batch) error {
	_, err := s.db.Exec(`INSERT INTO batches (id, status, callback_url, total, successful, failed, expired, rejected, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		b.ID, b.Status, b.CallbackURL, b.Counts.Total, b.Counts.Successful, b.Counts.Failed, b.Counts.Expired, b.Counts.Rejected,
		b.CreatedAt.Unix())
	if err != nil {
		return fmt.Errorf("store: CreateBatch %s: %w", b.ID, err)
	}
	return nil
}

// GetBatch loads a batch by id.
func (s *Store) GetBatch(id string) (Batch, bool, error) {
	row := s.db.QueryRow(`SELECT status, callback_url, total, successful, failed, expired, rejected, created_at
		FROM batches WHERE id = ?`, id)
	var b Batch
	var created int64
	err := row.Scan(&b.Status, &b.CallbackURL, &b.Counts.Total, &b.Counts.Successful, &b.Counts.Failed, &b.Counts.Expired, &b.Counts.Rejected, &created)
	switch {
	case err == sql.ErrNoRows:
		return Batch{}, false, nil
	case err != nil:
		return Batch{}, false, fmt.Errorf("store: GetBatch %s: %w", id, err)
	}
	b.ID = id
	b.CreatedAt = time.Unix(created, 0)
	return b, true, nil
}

// UpdateBatchCounts rewrites the outcome breakdown.
func (s *Store) UpdateBatchCounts(id string, c Counts) error {
	_, err := s.db.Exec(`UPDATE batches SET total=?, successful=?, failed=?, expired=?, rejected=? WHERE id=?`,
		c.Total, c.Successful, c.Failed, c.Expired, c.Rejected, id)
	if err != nil {
		return fmt.Errorf("store: UpdateBatchCounts %s: %w", id, err)
	}
	return nil
}

// SetBatchStatus transitions a batch's status.
func (s *Store) SetBatchStatus(id, status string) error {
	_, err := s.db.Exec(`UPDATE batches SET status=? WHERE id=?`, status, id)
	if err != nil {
		return fmt.Errorf("store: SetBatchStatus %s: %w", id, err)
	}
	return nil
}

// InsertRequest persists a request row.
func (s *Store) InsertRequest(r RequestRow) error {
	_, err := s.db.Exec(`INSERT INTO requests (id, batch_id, model, state, outcome, tokens, admitted_sec)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		r.ID, r.BatchID, r.Model, r.State, r.Outcome, r.Tokens, r.AdmittedSec)
	if err != nil {
		return fmt.Errorf("store: InsertRequest %s: %w", r.ID, err)
	}
	return nil
}

// UpdateRequestTerminal records the final outcome and charged tokens.
func (s *Store) UpdateRequestTerminal(id, outcome string, tokens int64) error {
	_, err := s.db.Exec(`UPDATE requests SET state='completed', outcome=?, tokens=? WHERE id=?`,
		outcome, tokens, id)
	if err != nil {
		return fmt.Errorf("store: UpdateRequestTerminal %s: %w", id, err)
	}
	return nil
}

// RequestsNeedingRecovery returns rows in queued/in_flight state.
func (s *Store) RequestsNeedingRecovery() ([]RequestRow, error) {
	rows, err := s.db.Query(`SELECT id, batch_id, model, state, outcome, tokens, admitted_sec
		FROM requests WHERE state IN ('queued', 'in_flight')`)
	if err != nil {
		return nil, fmt.Errorf("store: RequestsNeedingRecovery: %w", err)
	}
	defer rows.Close()
	var out []RequestRow
	for rows.Next() {
		var r RequestRow
		if err := rows.Scan(&r.ID, &r.BatchID, &r.Model, &r.State, &r.Outcome, &r.Tokens, &r.AdmittedSec); err != nil {
			return nil, fmt.Errorf("store: RequestsNeedingRecovery scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkRequestQueued sets a request's state to queued (recovery re-enqueue).
func (s *Store) MarkRequestQueued(id string) error {
	_, err := s.db.Exec(`UPDATE requests SET state='queued' WHERE id=?`, id)
	if err != nil {
		return fmt.Errorf("store: MarkRequestQueued %s: %w", id, err)
	}
	return nil
}

// RecordCallbackAttempt appends one delivery attempt.
func (s *Store) RecordCallbackAttempt(batchID string, at time.Time, statusCode int, outcome string) error {
	_, err := s.db.Exec(`INSERT INTO callback_attempts (batch_id, at, status_code, outcome) VALUES (?, ?, ?, ?)`,
		batchID, at.Unix(), statusCode, outcome)
	if err != nil {
		return fmt.Errorf("store: RecordCallbackAttempt %s: %w", batchID, err)
	}
	return nil
}

// CallbackAttempts lists a batch's delivery attempts in order.
func (s *Store) CallbackAttempts(batchID string) ([]Attempt, error) {
	rows, err := s.db.Query(`SELECT at, status_code, outcome FROM callback_attempts WHERE batch_id=? ORDER BY id`, batchID)
	if err != nil {
		return nil, fmt.Errorf("store: CallbackAttempts %s: %w", batchID, err)
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		var a Attempt
		var at int64
		if err := rows.Scan(&at, &a.StatusCode, &a.Outcome); err != nil {
			return nil, fmt.Errorf("store: CallbackAttempts scan: %w", err)
		}
		a.At = time.Unix(at, 0)
		out = append(out, a)
	}
	return out, rows.Err()
}

// UndeliveredBatches lists completed batches with no successful delivery.
func (s *Store) UndeliveredBatches() ([]string, error) {
	rows, err := s.db.Query(`SELECT b.id FROM batches b
		WHERE b.status = 'completed'
		AND NOT EXISTS (SELECT 1 FROM callback_attempts a WHERE a.batch_id = b.id AND a.outcome = 'delivered')`)
	if err != nil {
		return nil, fmt.Errorf("store: UndeliveredBatches: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: UndeliveredBatches scan: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// SeenKey looks up an idempotency key.
func (s *Store) SeenKey(k string) (string, bool, error) {
	var res string
	err := s.db.QueryRow(`SELECT result FROM seen_keys WHERE key=?`, k).Scan(&res)
	switch {
	case err == sql.ErrNoRows:
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("store: SeenKey %s: %w", k, err)
	}
	return res, true, nil
}

// PutSeenKey records an executed idempotency key.
func (s *Store) PutSeenKey(k, result string) error {
	_, err := s.db.Exec(`INSERT INTO seen_keys (key, result) VALUES (?, ?)`, k, result)
	if err != nil {
		return fmt.Errorf("store: PutSeenKey %s: %w", k, err)
	}
	return nil
}
