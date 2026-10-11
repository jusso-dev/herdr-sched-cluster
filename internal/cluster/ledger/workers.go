package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

// WorkerRow is what the ledger remembers about a worker between runs.
type WorkerRow struct {
	ID           string          `json:"id"`
	Label        string          `json:"label"`
	Draining     bool            `json:"draining"`
	HeldFor      string          `json:"held_for,omitempty"`
	BackoffUntil time.Time       `json:"backoff_until,omitempty"`
	Failures     int             `json:"failures"`
	State        string          `json:"state"`
	LastError    string          `json:"last_error,omitempty"`
	LastSeen     time.Time       `json:"last_seen,omitempty"`
	LatencyMS    int64           `json:"latency_ms"`
	Snapshot     json.RawMessage `json:"snapshot,omitempty"`
}

// Workers returns every remembered worker.
func (l *Ledger) Workers(ctx context.Context) (map[string]WorkerRow, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT id,label,draining,held_for,backoff_until,failures,state,last_error,last_seen,latency_ms,snapshot FROM workers`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]WorkerRow{}
	for rows.Next() {
		var w WorkerRow
		var drain int
		var backoff, seen int64
		var snap string
		if err := rows.Scan(&w.ID, &w.Label, &drain, &w.HeldFor, &backoff, &w.Failures, &w.State, &w.LastError, &seen, &w.LatencyMS, &snap); err != nil {
			return nil, err
		}
		w.Draining = drain != 0
		if backoff > 0 {
			w.BackoffUntil = time.UnixMilli(backoff).UTC()
		}
		if seen > 0 {
			w.LastSeen = time.UnixMilli(seen).UTC()
		}
		if snap != "" {
			w.Snapshot = json.RawMessage(snap)
		}
		out[w.ID] = w
	}
	return out, rows.Err()
}

func (l *Ledger) ensureWorker(ctx context.Context, tx *sql.Tx, id, label string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO workers(id,label) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET label=CASE WHEN excluded.label<>'' THEN excluded.label ELSE label END`, id, label)
	return err
}

// RecordProbe stores a successful reading and clears any failure count.
func (l *Ledger) RecordProbe(ctx context.Context, id, label, state string, snapshot any, latencyMS int64) error {
	b, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	return l.tx(ctx, func(tx *sql.Tx) error {
		if err := l.ensureWorker(ctx, tx, id, label); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE workers SET state=?, snapshot=?, latency_ms=?, last_seen=?, last_error='', failures=0, backoff_until=0 WHERE id=?`,
			state, string(b), latencyMS, l.ms(), id)
		return err
	})
}

// RecordFailure stores a failed probe or dispatch and backs the worker off,
// doubling from base up to max with each consecutive failure.
func (l *Ledger) RecordFailure(ctx context.Context, id, label, state, reason string, base, max time.Duration) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		if err := l.ensureWorker(ctx, tx, id, label); err != nil {
			return err
		}
		var failures int
		if err := tx.QueryRowContext(ctx, `SELECT failures FROM workers WHERE id=?`, id).Scan(&failures); err != nil {
			return err
		}
		failures++
		wait := base
		for i := 1; i < failures && wait < max; i++ {
			wait *= 2
		}
		if wait > max {
			wait = max
		}
		_, err := tx.ExecContext(ctx, `UPDATE workers SET state=?, last_error=?, failures=?, backoff_until=? WHERE id=?`,
			state, truncate(reason, 500), failures, l.now().Add(wait).UnixMilli(), id)
		if err != nil {
			return err
		}
		return event(ctx, tx, l.ms(), "cluster.worker.failed", "", id, map[string]any{"state": state, "error": truncate(reason, 500), "backoff_ms": wait.Milliseconds()})
	})
}

// SetDraining starts or stops draining a worker.
func (l *Ledger) SetDraining(ctx context.Context, id string, draining bool, by string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		if err := l.ensureWorker(ctx, tx, id, ""); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE workers SET draining=? WHERE id=?`, boolInt(draining), id); err != nil {
			return err
		}
		kind := "cluster.worker.undrained"
		if draining {
			kind = "cluster.worker.draining"
		}
		return event(ctx, tx, l.ms(), kind, "", id, map[string]any{"by": by})
	})
}

// SetHolds replaces the starvation-guard holds.
func (l *Ledger) SetHolds(ctx context.Context, holds map[string]string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE workers SET held_for='' WHERE held_for<>''`); err != nil {
			return err
		}
		for w, j := range holds {
			if err := l.ensureWorker(ctx, tx, w, ""); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE workers SET held_for=? WHERE id=?`, j, w); err != nil {
				return err
			}
		}
		return nil
	})
}

// Event is one row of the cluster trail.
type Event struct {
	ID     int64           `json:"id"`
	At     time.Time       `json:"at"`
	Kind   string          `json:"kind"`
	JobID  string          `json:"job,omitempty"`
	Worker string          `json:"worker,omitempty"`
	Detail json.RawMessage `json:"detail"`
}

// Events returns events after the given id, oldest first, at most limit.
func (l *Ledger) Events(ctx context.Context, after int64, limit int) ([]Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := l.db.QueryContext(ctx, `SELECT id,at,kind,job_id,worker,detail FROM events WHERE id>? ORDER BY id LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		var at int64
		var d string
		if err := rows.Scan(&e.ID, &at, &e.Kind, &e.JobID, &e.Worker, &d); err != nil {
			return nil, err
		}
		e.At, e.Detail = time.UnixMilli(at).UTC(), json.RawMessage(d)
		out = append(out, e)
	}
	return out, rows.Err()
}

// AddEvent appends one event outside any other change.
func (l *Ledger) AddEvent(ctx context.Context, kind, job, worker string, detail any) error {
	return l.tx(ctx, func(tx *sql.Tx) error { return event(ctx, tx, l.ms(), kind, job, worker, detail) })
}

// Counts returns how many jobs are in each state.
func (l *Ledger) Counts(ctx context.Context) (map[string]int, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT state, COUNT(*) FROM jobs GROUP BY state`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		out[s] = n
	}
	return out, rows.Err()
}
