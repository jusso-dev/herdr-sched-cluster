// Package ledger is the cluster scheduler's durable state: jobs, the
// capacity they hold, the decisions made about them, what each worker last
// reported, and what agents of each kind have used before.
//
// It is one SQLite file. Every state change is one transaction, and every
// transaction takes the write lock up front (BEGIN IMMEDIATE), so two callers
// can never both read "there is room" and both take it.
package ledger

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver, registered as "sqlite"
)

// Job states.
const (
	Queued      = "queued"
	Reserved    = "reserved"
	Dispatching = "dispatching"
	Running     = "running"
	Finished    = "finished"
	Failed      = "failed"
	Cancelled   = "cancelled"
	Lost        = "lost"
	Detached    = "detached"
)

// Active states hold a reservation.
var Active = []string{Reserved, Dispatching, Running}

// ErrConflict means the row was not in the state the caller expected:
// someone else moved it first. The caller does nothing further.
var ErrConflict = errors.New("ledger: state changed underneath this call")

// ErrNotFound is an unknown job id.
var ErrNotFound = errors.New("ledger: no such job")

// Ledger is an open cluster database.
type Ledger struct {
	db   *sql.DB
	Path string
	now  func() time.Time
}

// Open opens or creates the database at path. The directory is created 0700
// and the file 0600; an existing file that other users can read is refused
// rather than silently fixed, because something already widened it.
func Open(path string) (*Ledger, error) {
	return OpenWithClock(path, time.Now)
}

// OpenWithClock is Open with a clock a test can control.
func OpenWithClock(path string, now func() time.Time) (*Ledger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	fi, err := os.Stat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		if f != nil {
			f.Close()
		}
	case err != nil:
		return nil, err
	case fi.Mode().Perm()&0o077 != 0:
		return nil, fmt.Errorf("ledger %s has mode %v; it must be private to you (chmod 600 %s)", path, fi.Mode().Perm(), path)
	}
	// SQLite gives the -wal and -shm files the main file's permissions.
	dsn := "file:" + path + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(1)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection per process: every call is serialised here, and the
	// file lock serialises processes.
	db.SetMaxOpenConns(1)
	l := &Ledger{db: db, Path: path, now: now}
	if err := l.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return l, nil
}

// Close closes the database.
func (l *Ledger) Close() error { return l.db.Close() }

func (l *Ledger) ms() int64 { return l.now().UnixMilli() }

const schema = `
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS jobs (
	id TEXT PRIMARY KEY,
	seq INTEGER NOT NULL UNIQUE,
	state TEXT NOT NULL,
	kind TEXT NOT NULL,
	repo TEXT NOT NULL,
	prompt TEXT NOT NULL DEFAULT '',
	prompt_sha TEXT NOT NULL DEFAULT '',
	agent_args TEXT NOT NULL DEFAULT '[]',
	memory_mb INTEGER NOT NULL,
	cpu REAL NOT NULL,
	estimate_source TEXT NOT NULL DEFAULT 'request',
	priority INTEGER NOT NULL DEFAULT 0,
	policy TEXT NOT NULL DEFAULT '',
	machine TEXT NOT NULL DEFAULT '',
	labels TEXT NOT NULL DEFAULT '[]',
	os TEXT NOT NULL DEFAULT '',
	arch TEXT NOT NULL DEFAULT '',
	isolation TEXT NOT NULL DEFAULT 'auto',
	submitted_by TEXT NOT NULL DEFAULT '',
	submitted_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL,
	attempts INTEGER NOT NULL DEFAULT 0,
	max_attempts INTEGER NOT NULL DEFAULT 3,
	worker TEXT NOT NULL DEFAULT '',
	agent_name TEXT NOT NULL DEFAULT '',
	workspace_id TEXT NOT NULL DEFAULT '',
	pane_id TEXT NOT NULL DEFAULT '',
	worktree INTEGER NOT NULL DEFAULT 0,
	token TEXT NOT NULL DEFAULT '',
	started_at INTEGER NOT NULL DEFAULT 0,
	finished_at INTEGER NOT NULL DEFAULT 0,
	disconnected INTEGER NOT NULL DEFAULT 0,
	observed_rss_mb INTEGER NOT NULL DEFAULT 0,
	observed_cpu REAL NOT NULL DEFAULT 0,
	peak_rss_mb INTEGER NOT NULL DEFAULT 0,
	last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS jobs_state ON jobs(state);
CREATE TABLE IF NOT EXISTS reservations (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	job_id TEXT NOT NULL REFERENCES jobs(id),
	worker TEXT NOT NULL,
	memory_mb INTEGER NOT NULL,
	cpu REAL NOT NULL,
	created_at INTEGER NOT NULL,
	released_at INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS one_live_reservation_per_job ON reservations(job_id) WHERE released_at IS NULL;
CREATE INDEX IF NOT EXISTS reservations_live ON reservations(worker) WHERE released_at IS NULL;
CREATE TABLE IF NOT EXISTS decisions (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	job_id TEXT NOT NULL,
	at INTEGER NOT NULL,
	chosen TEXT NOT NULL,
	policy TEXT NOT NULL,
	summary TEXT NOT NULL,
	detail TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS decisions_job ON decisions(job_id, id);
CREATE TABLE IF NOT EXISTS workers (
	id TEXT PRIMARY KEY,
	label TEXT NOT NULL DEFAULT '',
	draining INTEGER NOT NULL DEFAULT 0,
	held_for TEXT NOT NULL DEFAULT '',
	backoff_until INTEGER NOT NULL DEFAULT 0,
	failures INTEGER NOT NULL DEFAULT 0,
	state TEXT NOT NULL DEFAULT '',
	last_error TEXT NOT NULL DEFAULT '',
	last_seen INTEGER NOT NULL DEFAULT 0,
	latency_ms INTEGER NOT NULL DEFAULT 0,
	snapshot TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS history (
	kind TEXT NOT NULL,
	repo TEXT NOT NULL,
	peak_rss_mb REAL NOT NULL,
	samples INTEGER NOT NULL,
	PRIMARY KEY (kind, repo)
);
CREATE TABLE IF NOT EXISTS events (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	at INTEGER NOT NULL,
	kind TEXT NOT NULL,
	job_id TEXT NOT NULL DEFAULT '',
	worker TEXT NOT NULL DEFAULT '',
	detail TEXT NOT NULL DEFAULT '{}'
);
`

// SchemaVersion is the database layout this binary writes. Version 2 adds
// the columns and tables for lost sessions, dispatch intents, workspace
// cleanup, dependencies, CPU learning and run history.
const SchemaVersion = 2

// v2 is applied on top of the version 1 tables, both to a fresh file and to a
// version 1 file being upgraded, so there is exactly one path to the layout.
var v2 = []string{
	`ALTER TABLE jobs ADD COLUMN cpu_source TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE jobs ADD COLUMN restartable INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE jobs ADD COLUMN disposable INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE jobs ADD COLUMN depends_on TEXT NOT NULL DEFAULT '[]'`,
	`ALTER TABLE jobs ADD COLUMN start_intent INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE jobs ADD COLUMN prompt_intent INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE jobs ADD COLUMN shell_pid INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE jobs ADD COLUMN peak_cpu REAL NOT NULL DEFAULT 0`,
	`ALTER TABLE jobs ADD COLUMN end_reason TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE jobs ADD COLUMN cleanup TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE jobs ADD COLUMN cleanup_note TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE jobs ADD COLUMN branch TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE jobs ADD COLUMN no_worktree INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE jobs ADD COLUMN clone_url TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE jobs ADD COLUMN disconnected_at INTEGER NOT NULL DEFAULT 0`,
	`ALTER TABLE jobs ADD COLUMN worker_path TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE workers ADD COLUMN identity TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE workers ADD COLUMN herdr_status TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE workers ADD COLUMN herdr_error TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE workers ADD COLUMN pressure TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE workers ADD COLUMN pressure_since INTEGER NOT NULL DEFAULT 0`,
	`CREATE TABLE IF NOT EXISTS job_samples (
		job_id TEXT NOT NULL,
		at INTEGER NOT NULL,
		rss_mb INTEGER NOT NULL,
		cpu_pct REAL NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS job_samples_job ON job_samples(job_id, at)`,
	`CREATE TABLE IF NOT EXISTS runs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		kind TEXT NOT NULL,
		repo TEXT NOT NULL,
		at INTEGER NOT NULL,
		peak_rss_mb INTEGER NOT NULL,
		typical_rss_mb INTEGER NOT NULL,
		cpu_cores REAL NOT NULL,
		outcome TEXT NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS runs_kind_repo ON runs(kind, repo, id)`,
	// A version 1 file learned an exponentially weighted peak per kind and
	// repository. Each becomes one run, so its estimate carries over until
	// real runs replace it. CPU was never learned, so it is left unknown.
	`INSERT INTO runs(kind,repo,at,peak_rss_mb,typical_rss_mb,cpu_cores,outcome)
		SELECT kind, repo, 0, CAST(peak_rss_mb AS INTEGER), CAST(peak_rss_mb AS INTEGER), 0, 'migrated' FROM history WHERE repo<>''`,
	`INSERT INTO runs(kind,repo,at,peak_rss_mb,typical_rss_mb,cpu_cores,outcome)
		SELECT h.kind, '', 0, CAST(h.peak_rss_mb AS INTEGER), CAST(h.peak_rss_mb AS INTEGER), 0, 'migrated' FROM history h
		WHERE h.repo='' AND NOT EXISTS (SELECT 1 FROM history r WHERE r.kind=h.kind AND r.repo<>'')`,
}

func (l *Ledger) migrate(ctx context.Context) error {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("ledger schema: %w", err)
	}
	var v string
	err = tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='schema'`).Scan(&v)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		v = "1" // the tables just created are the version 1 layout
		if _, err := tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES('schema','1')`); err != nil {
			return err
		}
	case err != nil:
		return err
	}
	if v == "1" {
		for _, stmt := range v2 {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("ledger upgrade to schema 2: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE meta SET value='2' WHERE key='schema'`); err != nil {
			return err
		}
		v = "2"
	}
	if v != fmt.Sprint(SchemaVersion) {
		return fmt.Errorf("ledger %s is schema %s; this binary writes schema %d and will not touch it", l.Path, v, SchemaVersion)
	}
	return tx.Commit()
}

// Token is a random dispatch token. A dispatch step only moves a job whose
// row still carries the token it was given, so a stale caller cannot move a
// job someone else has since taken over.
func Token() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// tx runs fn in one IMMEDIATE transaction.
func (l *Ledger) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func event(ctx context.Context, tx *sql.Tx, at int64, kind, job, worker string, detail any) error {
	b, _ := json.Marshal(detail)
	if detail == nil {
		b = []byte("{}")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO events(at,kind,job_id,worker,detail) VALUES(?,?,?,?,?)`, at, kind, job, worker, string(b))
	return err
}

// Prune keeps the event and decision tables bounded.
func (l *Ledger) Prune(ctx context.Context, keepEvents, keepDecisions int) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM events WHERE id <= (SELECT COALESCE(MAX(id),0) FROM events) - ?`, keepEvents); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM decisions WHERE id <= (SELECT COALESCE(MAX(id),0) FROM decisions) - ?`, keepDecisions); err != nil {
			return err
		}
		// Run history keeps the newest RunsKept per kind and repository,
		// which is more than any estimate window reads.
		_, err := tx.ExecContext(ctx, `DELETE FROM runs WHERE id IN (SELECT id FROM (SELECT id, ROW_NUMBER() OVER
			(PARTITION BY kind, repo ORDER BY id DESC) AS n FROM runs) WHERE n > ?)`, RunsKept)
		return err
	})
}
