package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Worktree cleanup states on a job.
const (
	CleanupPending = "pending" // the job ended with a worktree the policy has not decided on yet
	CleanupRemoved = "removed"
	CleanupKept    = "kept"
)

// End reasons. A lost job is either confirmed (the worker answered and the
// session is gone) or unconfirmed (the worker stopped answering).
const (
	EndAgentExited       = "agent_exited"
	EndHerdrRestarted    = "herdr_restarted"
	EndSessionVanished   = "session_vanished"
	EndWorkerUnreachable = "worker_unreachable"
	EndDependencyFailed  = "dependency_failed"
	EndTerminated        = "terminated_under_pressure"
	EndEmptyRepository   = "empty_repository"
	EndInterrupted       = "dispatch_interrupted"
)

// ConfirmLost turns an unconfirmed loss (the worker was unreachable) into a
// confirmed one once the worker answers again and the agent is not there, so
// the retry policy can consider it.
func (l *Ledger) ConfirmLost(ctx context.Context, jobID, endReason, reason string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET end_reason=?, last_error=? WHERE id=? AND state=? AND end_reason=?`,
			endReason, truncate(reason, 500), jobID, Lost, EndWorkerUnreachable)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrConflict
		}
		return event(ctx, tx, l.ms(), "cluster.job.loss_confirmed", jobID, "", map[string]any{"end_reason": endReason, "reason": truncate(reason, 500)})
	})
}

// RecordStartIntent notes, before `agent start` is called, that it is about
// to be. A coordinator that dies after this and before MarkRunning knows the
// start may have happened.
func (l *Ledger) RecordStartIntent(ctx context.Context, jobID, token string) error {
	return l.intent(ctx, jobID, token, "start_intent")
}

// RecordPromptIntent notes, before the first prompt is sent, that it is about
// to be. Once this is set the agent may have done work, so an interrupted
// dispatch is never retried.
func (l *Ledger) RecordPromptIntent(ctx context.Context, jobID, token string) error {
	return l.intent(ctx, jobID, token, "prompt_intent")
}

func (l *Ledger) intent(ctx context.Context, jobID, token, col string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET `+col+`=?, updated_at=? WHERE id=? AND token=? AND state=?`,
			l.ms(), l.ms(), jobID, token, Dispatching)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrConflict
		}
		return nil
	})
}

// SetWorkerPath records where the repository is on the worker, and the
// worktree branch when one is made.
func (l *Ledger) SetWorkerPath(ctx context.Context, jobID, token, path, branch string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET worker_path=?, branch=? WHERE id=? AND token=? AND state=?`,
			path, branch, jobID, token, Dispatching)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrConflict
		}
		return nil
	})
}

// SetShellPID remembers the pane shell of a running job.
func (l *Ledger) SetShellPID(ctx context.Context, jobID string, pid int) error {
	_, err := l.db.ExecContext(ctx, `UPDATE jobs SET shell_pid=? WHERE id=? AND state=? AND shell_pid<>?`, pid, jobID, Running, pid)
	return err
}

// MarkNoWorktree records that a worktree cannot be made for this job (the
// repository has no commits), so later attempts use the checkout itself and
// wait until no other job is using it.
func (l *Ledger) MarkNoWorktree(ctx context.Context, jobID, reason string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET no_worktree=1 WHERE id=?`, jobID); err != nil {
			return err
		}
		return event(ctx, tx, l.ms(), "cluster.job.no_worktree", jobID, "", map[string]any{"reason": truncate(reason, 300)})
	})
}

// Readopt brings a lost job back to running when its agent turns up again
// on reconnect. The reservation is taken again without a capacity check: the
// agent is already running there, so the memory is in use whether or not the
// scheduler counts it, and counting it is what stops new work landing on it.
func (l *Ledger) Readopt(ctx context.Context, jobID, pane, workspace string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		var state, worker string
		var mem int64
		var cpu float64
		if err := tx.QueryRowContext(ctx, `SELECT state, worker, memory_mb, cpu FROM jobs WHERE id=?`, jobID).Scan(&state, &worker, &mem, &cpu); err != nil {
			return err
		}
		if state != Lost || worker == "" {
			return ErrConflict
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO reservations(job_id,worker,memory_mb,cpu,created_at) VALUES(?,?,?,?,?)`,
			jobID, worker, mem, cpu, l.ms()); err != nil {
			return err
		}
		if err := l.move(ctx, tx, jobID, "", Lost, Running, `, finished_at=0, end_reason='', disconnected=0, disconnected_at=0,
			cleanup='', cleanup_note='', last_error='', pane_id=CASE WHEN ?<>'' THEN ? ELSE pane_id END,
			workspace_id=CASE WHEN ?<>'' THEN ? ELSE workspace_id END`, pane, pane, workspace, workspace); err != nil {
			return err
		}
		return event(ctx, tx, l.ms(), "cluster.job.readopted", jobID, worker, map[string]any{"pane": pane})
	})
}

// RetryLost puts a lost job back in the queue. Only a job the submitter
// declared restartable, whose loss was confirmed (the worker answered and the
// session is gone, not merely unreachable), and with attempts left is
// retried; anything else stays lost with the reason.
func (l *Ledger) RetryLost(ctx context.Context, jobID string) (bool, string, error) {
	retried, why := false, ""
	err := l.tx(ctx, func(tx *sql.Tx) error {
		var state, end, ws, worker string
		var restartable, attempts, max int
		if err := tx.QueryRowContext(ctx, `SELECT state, end_reason, restartable, attempts, max_attempts, workspace_id, worker FROM jobs WHERE id=?`,
			jobID).Scan(&state, &end, &restartable, &attempts, &max, &ws, &worker); err != nil {
			return err
		}
		switch {
		case state != Lost:
			return ErrConflict
		case restartable == 0:
			why = "not retried: the job was not submitted as restartable"
		case end == EndWorkerUnreachable:
			why = "not retried: the worker is unreachable, so the agent may still be running there"
		case attempts >= max:
			why = fmt.Sprintf("not retried: all %d attempts used", max)
		}
		if why != "" {
			return event(ctx, tx, l.ms(), "cluster.job.retry_refused", jobID, worker, map[string]any{"reason": why})
		}
		if err := l.move(ctx, tx, jobID, "", Lost, Queued, `, worker='', workspace_id='', pane_id='', worktree=0, token='',
			finished_at=0, started_at=0, disconnected=0, disconnected_at=0, observed_rss_mb=0, observed_cpu=0, cleanup='', cleanup_note=''`); err != nil {
			return err
		}
		retried = true
		why = fmt.Sprintf("requeued for attempt %d of %d", attempts+1, max)
		return event(ctx, tx, l.ms(), "cluster.job.retried", jobID, worker, map[string]any{"after": end, "old_workspace": ws, "attempt": attempts + 1})
	})
	return retried, why, err
}

// Terminate ends a running disposable job that the pressure policy closed.
// A restartable one goes back in the queue; any other is failed.
func (l *Ledger) Terminate(ctx context.Context, jobID, reason string) (string, error) {
	to := Failed
	err := l.tx(ctx, func(tx *sql.Tx) error {
		var disposable, restartable, attempts, max int
		if err := tx.QueryRowContext(ctx, `SELECT disposable, restartable, attempts, max_attempts FROM jobs WHERE id=?`, jobID).
			Scan(&disposable, &restartable, &attempts, &max); err != nil {
			return err
		}
		if disposable == 0 {
			return fmt.Errorf("ledger: %s is not disposable and is never terminated", jobID)
		}
		set := `, finished_at=?, last_error=?, end_reason=?, cleanup=CASE WHEN worktree<>0 THEN 'pending' ELSE '' END`
		args := []any{l.ms(), truncate(reason, 500), EndTerminated}
		if restartable != 0 && attempts < max {
			to = Queued
			set = `, last_error=?, end_reason=?, worker='', workspace_id='', pane_id='', worktree=0, token='', started_at=0`
			args = []any{truncate(reason, 500), EndTerminated}
		}
		if err := l.move(ctx, tx, jobID, "", Running, to, set, args...); err != nil {
			return err
		}
		if err := l.release(ctx, tx, jobID); err != nil {
			return err
		}
		return event(ctx, tx, l.ms(), "cluster.job.terminated", jobID, "", map[string]any{"reason": truncate(reason, 500), "now": to})
	})
	return to, err
}

// FailQueued fails a job that is still waiting, such as one whose dependency
// ended without finishing.
func (l *Ledger) FailQueued(ctx context.Context, jobID, endReason, reason string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		if err := l.move(ctx, tx, jobID, "", Queued, Failed, `, finished_at=?, last_error=?, end_reason=?`, l.ms(), truncate(reason, 500), endReason); err != nil {
			return err
		}
		return event(ctx, tx, l.ms(), "cluster.job.failed", jobID, "", map[string]any{"end_reason": endReason, "reason": truncate(reason, 500)})
	})
}

// SetCleanup records what happened to a job's worktree.
func (l *Ledger) SetCleanup(ctx context.Context, jobID, state, note string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		var worker string
		if err := tx.QueryRowContext(ctx, `SELECT worker FROM jobs WHERE id=?`, jobID).Scan(&worker); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET cleanup=?, cleanup_note=? WHERE id=?`, state, truncate(note, 500), jobID); err != nil {
			return err
		}
		return event(ctx, tx, l.ms(), "cluster.workspace."+state, jobID, worker, map[string]any{"reason": truncate(note, 500)})
	})
}

// PendingCleanup lists ended jobs whose worktree is awaiting a decision.
func (l *Ledger) PendingCleanup(ctx context.Context) ([]Job, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT `+jobCols+` FROM jobs j WHERE j.cleanup=? ORDER BY j.seq`, CleanupPending)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// States returns the state of each named job; an unknown id is absent.
func (l *Ledger) States(ctx context.Context, ids []string) (map[string]string, error) {
	out := map[string]string{}
	for _, id := range ids {
		var st string
		err := l.db.QueryRowContext(ctx, `SELECT state FROM jobs WHERE id=?`, id).Scan(&st)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[id] = st
	}
	return out, nil
}

// CountQueued is the queue depth.
func (l *Ledger) CountQueued(ctx context.Context) (int, error) {
	var n int
	err := l.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE state=?`, Queued).Scan(&n)
	return n, err
}

// EndReasons counts ended jobs by state and end reason, for metrics.
func (l *Ledger) EndReasons(ctx context.Context) (map[string]map[string]int, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT state, end_reason, COUNT(*) FROM jobs WHERE end_reason<>'' GROUP BY state, end_reason`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]map[string]int{}
	for rows.Next() {
		var st, r string
		var n int
		if err := rows.Scan(&st, &r, &n); err != nil {
			return nil, err
		}
		if out[st] == nil {
			out[st] = map[string]int{}
		}
		out[st][r] = n
	}
	return out, rows.Err()
}

// CheckIdentity compares what a worker says it is with what it said the
// first time it answered. The first answer is remembered (trust on first
// use); a different one later is refused until the operator clears it with
// ForgetIdentity.
func (l *Ledger) CheckIdentity(ctx context.Context, id, label, identity string) (string, error) {
	prev := ""
	err := l.tx(ctx, func(tx *sql.Tx) error {
		if err := l.ensureWorker(ctx, tx, id, label); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT identity FROM workers WHERE id=?`, id).Scan(&prev); err != nil {
			return err
		}
		if prev == "" && identity != "" {
			if _, err := tx.ExecContext(ctx, `UPDATE workers SET identity=? WHERE id=?`, identity, id); err != nil {
				return err
			}
			return event(ctx, tx, l.ms(), "cluster.worker.registered", "", id, map[string]any{"identity": identity})
		}
		return nil
	})
	return prev, err
}

// ForgetIdentity clears a remembered worker identity, so the next probe
// registers it afresh (after the machine was rebuilt, for example).
func (l *Ledger) ForgetIdentity(ctx context.Context, id, by string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		if err := l.ensureWorker(ctx, tx, id, ""); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE workers SET identity='' WHERE id=?`, id); err != nil {
			return err
		}
		return event(ctx, tx, l.ms(), "cluster.worker.identity_cleared", "", id, map[string]any{"by": by})
	})
}

// SetHerdrStatus stores what `herdr machine status` last said about a worker.
func (l *Ledger) SetHerdrStatus(ctx context.Context, id, label, status, errText string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		if err := l.ensureWorker(ctx, tx, id, label); err != nil {
			return err
		}
		var prev string
		_ = tx.QueryRowContext(ctx, `SELECT herdr_status FROM workers WHERE id=?`, id).Scan(&prev)
		if _, err := tx.ExecContext(ctx, `UPDATE workers SET herdr_status=?, herdr_error=? WHERE id=?`, status, truncate(errText, 300), id); err != nil {
			return err
		}
		if prev != status {
			return event(ctx, tx, l.ms(), "cluster.worker.herdr_status", "", id, map[string]any{"from": prev, "to": status, "error": truncate(errText, 300)})
		}
		return nil
	})
}

// SetPressure records a worker's memory pressure level and returns how long
// it has been at that level. A change of level is an event, with the
// detail the caller gives (the largest jobs there and a recommendation).
func (l *Ledger) SetPressure(ctx context.Context, id, level string, detail map[string]any) (time.Duration, bool, error) {
	var since int64
	changed := false
	err := l.tx(ctx, func(tx *sql.Tx) error {
		if err := l.ensureWorker(ctx, tx, id, ""); err != nil {
			return err
		}
		var prev string
		if err := tx.QueryRowContext(ctx, `SELECT pressure, pressure_since FROM workers WHERE id=?`, id).Scan(&prev, &since); err != nil {
			return err
		}
		if prev == level {
			return nil
		}
		changed = true
		since = l.ms()
		if _, err := tx.ExecContext(ctx, `UPDATE workers SET pressure=?, pressure_since=? WHERE id=?`, level, since, id); err != nil {
			return err
		}
		d := map[string]any{"from": prev, "to": level}
		for k, v := range detail {
			d[k] = v
		}
		return event(ctx, tx, l.ms(), "cluster.worker.pressure", "", id, d)
	})
	if err != nil {
		return 0, false, err
	}
	return l.now().Sub(time.UnixMilli(since)), changed, nil
}

// WorkerExtra is the version 2 worker columns.
type WorkerExtra struct {
	Identity    string `json:"identity,omitempty"`
	HerdrStatus string `json:"herdr_status,omitempty"`
	HerdrError  string `json:"herdr_error,omitempty"`
	Pressure    string `json:"pressure_level,omitempty"`
}

// WorkerExtras returns the version 2 worker columns by worker id.
func (l *Ledger) WorkerExtras(ctx context.Context) (map[string]WorkerExtra, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT id, identity, herdr_status, herdr_error, pressure FROM workers`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]WorkerExtra{}
	for rows.Next() {
		var id string
		var w WorkerExtra
		if err := rows.Scan(&id, &w.Identity, &w.HerdrStatus, &w.HerdrError, &w.Pressure); err != nil {
			return nil, err
		}
		out[id] = w
	}
	return out, rows.Err()
}
