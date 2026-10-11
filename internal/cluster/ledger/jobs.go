package ledger

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// Job is one row.
type Job struct {
	ID             string    `json:"id"`
	Seq            int64     `json:"seq"`
	State          string    `json:"state"`
	Kind           string    `json:"kind"`
	Repo           string    `json:"repo"`
	Prompt         string    `json:"-"`
	PromptSHA      string    `json:"prompt_sha256,omitempty"`
	PromptLen      int       `json:"prompt_chars"`
	AgentArgs      []string  `json:"agent_args,omitempty"`
	MemoryMB       int64     `json:"memory_mb"`
	CPU            float64   `json:"cpu"`
	EstimateSource string    `json:"estimate_source"`
	Priority       int       `json:"priority"`
	Policy         string    `json:"policy,omitempty"`
	Machine        string    `json:"machine,omitempty"`
	Labels         []string  `json:"labels,omitempty"`
	OS             string    `json:"os,omitempty"`
	Arch           string    `json:"arch,omitempty"`
	Isolation      string    `json:"isolation"`
	SubmittedBy    string    `json:"submitted_by,omitempty"`
	SubmittedAt    time.Time `json:"submitted_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	Attempts       int       `json:"attempts"`
	MaxAttempts    int       `json:"max_attempts"`
	Worker         string    `json:"worker,omitempty"`
	AgentName      string    `json:"agent_name,omitempty"`
	WorkspaceID    string    `json:"workspace_id,omitempty"`
	PaneID         string    `json:"pane_id,omitempty"`
	Worktree       bool      `json:"worktree,omitempty"`
	Token          string    `json:"-"`
	StartedAt      time.Time `json:"started_at,omitempty"`
	FinishedAt     time.Time `json:"finished_at,omitempty"`
	Disconnected   bool      `json:"disconnected,omitempty"`
	ObservedRSSMB  int64     `json:"observed_rss_mb,omitempty"`
	ObservedCPU    float64   `json:"observed_cpu,omitempty"`
	PeakRSSMB      int64     `json:"peak_rss_mb,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
	// ReservedMB is the live reservation, which can grow past MemoryMB when
	// the agent overruns its estimate.
	ReservedMB int64 `json:"reserved_mb,omitempty"`
}

// NewJob is what Submit stores.
type NewJob struct {
	Kind, Repo, Prompt  string
	AgentArgs           []string
	MemoryMB            int64
	CPU                 float64
	EstimateSource      string
	Priority            int
	Policy, Machine     string
	Labels              []string
	OS, Arch, Isolation string
	SubmittedBy         string
	MaxAttempts         int
}

const jobCols = `j.id,j.seq,j.state,j.kind,j.repo,j.prompt,j.prompt_sha,j.agent_args,j.memory_mb,j.cpu,j.estimate_source,
j.priority,j.policy,j.machine,j.labels,j.os,j.arch,j.isolation,j.submitted_by,j.submitted_at,j.updated_at,j.attempts,
j.max_attempts,j.worker,j.agent_name,j.workspace_id,j.pane_id,j.worktree,j.token,j.started_at,j.finished_at,
j.disconnected,j.observed_rss_mb,j.observed_cpu,j.peak_rss_mb,j.last_error,
COALESCE((SELECT memory_mb FROM reservations r WHERE r.job_id=j.id AND r.released_at IS NULL),0)`

type scanner interface{ Scan(...any) error }

func scanJob(s scanner) (Job, error) {
	var j Job
	var args, labels string
	var sub, upd, started, finished int64
	var wt, disc int
	err := s.Scan(&j.ID, &j.Seq, &j.State, &j.Kind, &j.Repo, &j.Prompt, &j.PromptSHA, &args, &j.MemoryMB, &j.CPU,
		&j.EstimateSource, &j.Priority, &j.Policy, &j.Machine, &labels, &j.OS, &j.Arch, &j.Isolation, &j.SubmittedBy,
		&sub, &upd, &j.Attempts, &j.MaxAttempts, &j.Worker, &j.AgentName, &j.WorkspaceID, &j.PaneID, &wt, &j.Token,
		&started, &finished, &disc, &j.ObservedRSSMB, &j.ObservedCPU, &j.PeakRSSMB, &j.LastError, &j.ReservedMB)
	if err != nil {
		return j, err
	}
	_ = json.Unmarshal([]byte(args), &j.AgentArgs)
	_ = json.Unmarshal([]byte(labels), &j.Labels)
	j.SubmittedAt, j.UpdatedAt = time.UnixMilli(sub).UTC(), time.UnixMilli(upd).UTC()
	if started > 0 {
		j.StartedAt = time.UnixMilli(started).UTC()
	}
	if finished > 0 {
		j.FinishedAt = time.UnixMilli(finished).UTC()
	}
	j.Worktree, j.Disconnected = wt != 0, disc != 0
	j.PromptLen = len([]rune(j.Prompt))
	return j, nil
}

// AgentName is the Herdr agent name for a job. Herdr requires
// [a-z][a-z0-9_-]{0,31} and refuses a second live agent with the same name,
// which makes it a second guard against double dispatch.
func AgentName(id string) string { return "hsc-" + strings.ToLower(id) }

// Submit stores a new queued job and returns it.
func (l *Ledger) Submit(ctx context.Context, n NewJob) (Job, error) {
	if n.MaxAttempts <= 0 {
		n.MaxAttempts = 3
	}
	if n.Isolation == "" {
		n.Isolation = "auto"
	}
	if n.EstimateSource == "" {
		n.EstimateSource = "request"
	}
	args, _ := json.Marshal(nonNil(n.AgentArgs))
	labels, _ := json.Marshal(nonNil(n.Labels))
	sum := sha256.Sum256([]byte(n.Prompt))
	sha := ""
	if n.Prompt != "" {
		sha = hex.EncodeToString(sum[:])
	}
	var id string
	err := l.tx(ctx, func(tx *sql.Tx) error {
		var seq int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0)+1 FROM jobs`).Scan(&seq); err != nil {
			return err
		}
		id = fmt.Sprintf("j%d", seq)
		now := l.ms()
		_, err := tx.ExecContext(ctx, `INSERT INTO jobs(id,seq,state,kind,repo,prompt,prompt_sha,agent_args,memory_mb,cpu,
			estimate_source,priority,policy,machine,labels,os,arch,isolation,submitted_by,submitted_at,updated_at,max_attempts,agent_name)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			id, seq, Queued, n.Kind, n.Repo, n.Prompt, sha, string(args), n.MemoryMB, n.CPU, n.EstimateSource, n.Priority,
			n.Policy, n.Machine, string(labels), n.OS, n.Arch, n.Isolation, n.SubmittedBy, now, now, n.MaxAttempts, AgentName(id))
		if err != nil {
			return err
		}
		return event(ctx, tx, now, "cluster.job.submitted", id, "", map[string]any{
			"kind": n.Kind, "repo": n.Repo, "memory_mb": n.MemoryMB, "cpu": n.CPU, "estimate": n.EstimateSource,
			"priority": n.Priority, "prompt_chars": len([]rune(n.Prompt)), "prompt_sha256": sha,
		})
	})
	if err != nil {
		return Job{}, err
	}
	return l.Get(ctx, id)
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// Get reads one job.
func (l *Ledger) Get(ctx context.Context, id string) (Job, error) {
	j, err := scanJob(l.db.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs j WHERE j.id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return j, err
}

// List returns jobs in the given states (all when none), oldest first.
func (l *Ledger) List(ctx context.Context, states ...string) ([]Job, error) {
	q := `SELECT ` + jobCols + ` FROM jobs j`
	var args []any
	if len(states) > 0 {
		q += ` WHERE j.state IN (` + strings.TrimSuffix(strings.Repeat("?,", len(states)), ",") + `)`
		for _, s := range states {
			args = append(args, s)
		}
	}
	q += ` ORDER BY j.seq`
	rows, err := l.db.QueryContext(ctx, q, args...)
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

// Load is what one worker already holds, read inside the transaction that
// decides whether it can hold more.
type Load struct {
	PendingMemoryMB int64
	ReservedMB      int64
	CommittedCPU    float64
	ActiveAgents    int
	RunningRepos    map[string]int
}

// loadTx sums a worker's live reservations. For a measured worker, memory an
// agent is already seen using is in the operating system's own numbers, so
// only the unused part of its reservation is pending. For a static worker
// nothing is measured, so all of it is.
func loadTx(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, worker string, static bool) (Load, error) {
	rows, err := q.QueryContext(ctx, `SELECT r.memory_mb, r.cpu, j.observed_rss_mb, j.repo FROM reservations r
		JOIN jobs j ON j.id=r.job_id WHERE r.worker=? AND r.released_at IS NULL`, worker)
	if err != nil {
		return Load{}, err
	}
	defer rows.Close()
	ld := Load{RunningRepos: map[string]int{}}
	for rows.Next() {
		var mem, observed int64
		var cpu float64
		var repo string
		if err := rows.Scan(&mem, &cpu, &observed, &repo); err != nil {
			return Load{}, err
		}
		ld.ReservedMB += mem
		pending := mem
		if !static {
			pending = mem - min64(observed, mem)
		}
		ld.PendingMemoryMB += pending
		ld.CommittedCPU += cpu
		ld.ActiveAgents++
		if repo != "" {
			ld.RunningRepos[repo]++
		}
	}
	return ld, rows.Err()
}

// LoadOf reads a worker's load outside any decision, for display.
func (l *Ledger) LoadOf(ctx context.Context, worker string, static bool) (Load, error) {
	return loadTx(ctx, l.db, worker, static)
}

// Reserve takes capacity for a queued job on a worker, if check agrees given
// what the worker already holds. check runs inside the transaction, so no
// other reservation can land between its answer and the insert. It returns
// the dispatch token the later steps must present.
func (l *Ledger) Reserve(ctx context.Context, jobID, worker string, static bool, check func(Load) error) (string, error) {
	token := Token()
	err := l.tx(ctx, func(tx *sql.Tx) error {
		var state string
		var mem int64
		var cpu float64
		err := tx.QueryRowContext(ctx, `SELECT state, memory_mb, cpu FROM jobs WHERE id=?`, jobID).Scan(&state, &mem, &cpu)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		if state != Queued {
			return ErrConflict
		}
		ld, err := loadTx(ctx, tx, worker, static)
		if err != nil {
			return err
		}
		if err := check(ld); err != nil {
			return err
		}
		now := l.ms()
		if _, err := tx.ExecContext(ctx, `INSERT INTO reservations(job_id,worker,memory_mb,cpu,created_at) VALUES(?,?,?,?,?)`,
			jobID, worker, mem, cpu, now); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, worker=?, token=?, updated_at=?, attempts=attempts+1,
			disconnected=0, observed_rss_mb=0, observed_cpu=0 WHERE id=? AND state=?`,
			Reserved, worker, token, now, jobID, Queued)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrConflict
		}
		return event(ctx, tx, now, "cluster.job.reserved", jobID, worker, map[string]any{"memory_mb": mem, "cpu": cpu})
	})
	if err != nil {
		return "", err
	}
	return token, nil
}

// move changes a job from one state to another if, and only if, it is still
// in the expected state with the expected token.
func (l *Ledger) move(ctx context.Context, tx *sql.Tx, jobID, token, from, to string, set string, args ...any) error {
	q := `UPDATE jobs SET state=?, updated_at=?` + set + ` WHERE id=? AND state=?`
	all := append([]any{to, l.ms()}, args...)
	all = append(all, jobID, from)
	if token != "" {
		q += ` AND token=?`
		all = append(all, token)
	}
	res, err := tx.ExecContext(ctx, q, all...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrConflict
	}
	return nil
}

func (l *Ledger) release(ctx context.Context, tx *sql.Tx, jobID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE reservations SET released_at=? WHERE job_id=? AND released_at IS NULL`, l.ms(), jobID)
	return err
}

// MarkDispatching records that dispatch has begun.
func (l *Ledger) MarkDispatching(ctx context.Context, jobID, token string, worktree bool) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		if err := l.move(ctx, tx, jobID, token, Reserved, Dispatching, `, worktree=?`, boolInt(worktree)); err != nil {
			return err
		}
		return event(ctx, tx, l.ms(), "cluster.job.dispatching", jobID, "", nil)
	})
}

// SetWorkspace records the workspace and pane a dispatch created, before the
// agent is started, so a crash between the two can still be cleaned up.
func (l *Ledger) SetWorkspace(ctx context.Context, jobID, token, workspace, pane string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET workspace_id=?, pane_id=?, updated_at=? WHERE id=? AND token=? AND state=?`,
			workspace, pane, l.ms(), jobID, token, Dispatching)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrConflict
		}
		return nil
	})
}

// MarkRunning records a started agent.
func (l *Ledger) MarkRunning(ctx context.Context, jobID, token, pane string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		set := `, started_at=?`
		args := []any{l.ms()}
		if pane != "" {
			set += `, pane_id=?`
			args = append(args, pane)
		}
		if err := l.move(ctx, tx, jobID, token, Dispatching, Running, set, args...); err != nil {
			return err
		}
		return event(ctx, tx, l.ms(), "cluster.job.running", jobID, "", nil)
	})
}

// AdoptRunning moves a reserved or dispatching job to running when
// reconciliation finds its agent alive (after a coordinator restart).
func (l *Ledger) AdoptRunning(ctx context.Context, jobID, pane, workspace string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, started_at=CASE WHEN started_at=0 THEN ? ELSE started_at END,
			pane_id=CASE WHEN ?<>'' THEN ? ELSE pane_id END, workspace_id=CASE WHEN ?<>'' THEN ? ELSE workspace_id END,
			updated_at=?, disconnected=0 WHERE id=? AND state IN (?,?)`,
			Running, l.ms(), pane, pane, workspace, workspace, l.ms(), jobID, Reserved, Dispatching)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrConflict
		}
		return event(ctx, tx, l.ms(), "cluster.job.adopted", jobID, "", map[string]any{"pane": pane})
	})
}

// DispatchFailed releases the job's capacity and puts it back in the queue,
// or fails it when its attempts are used up. It returns the new state.
func (l *Ledger) DispatchFailed(ctx context.Context, jobID, token, reason string) (string, error) {
	var to string
	err := l.tx(ctx, func(tx *sql.Tx) error {
		var state string
		var attempts, max int
		if err := tx.QueryRowContext(ctx, `SELECT state, attempts, max_attempts FROM jobs WHERE id=?`, jobID).Scan(&state, &attempts, &max); err != nil {
			return err
		}
		if state != Reserved && state != Dispatching {
			return ErrConflict
		}
		to = Queued
		if attempts >= max {
			to = Failed
		}
		set := `, last_error=?, worker='', workspace_id='', pane_id='', worktree=0, token=''`
		args := []any{truncate(reason, 500)}
		if to == Failed {
			set += `, finished_at=?`
			args = append(args, l.ms())
		}
		if err := l.move(ctx, tx, jobID, token, state, to, set, args...); err != nil {
			return err
		}
		if err := l.release(ctx, tx, jobID); err != nil {
			return err
		}
		return event(ctx, tx, l.ms(), "cluster.job.dispatch_failed", jobID, "", map[string]any{"error": truncate(reason, 500), "now": to, "attempt": attempts})
	})
	return to, err
}

// Abandon fails a reserved or dispatching job without retrying it. It is
// used when an agent start may or may not have happened and the agent can
// no longer be found: running the job a second time is worse than asking
// the operator to resubmit it.
func (l *Ledger) Abandon(ctx context.Context, jobID, token, reason string) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		var state string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM jobs WHERE id=?`, jobID).Scan(&state); err != nil {
			return err
		}
		if state != Reserved && state != Dispatching {
			return ErrConflict
		}
		if err := l.move(ctx, tx, jobID, token, state, Failed, `, last_error=?, finished_at=?`, truncate(reason, 500), l.ms()); err != nil {
			return err
		}
		if err := l.release(ctx, tx, jobID); err != nil {
			return err
		}
		return event(ctx, tx, l.ms(), "cluster.job.abandoned", jobID, "", map[string]any{"reason": truncate(reason, 500)})
	})
}

// Finish ends a running job: its agent is gone (finished), its worker's
// Herdr restarted and took it (lost), or the operator stopped tracking it
// (detached). The reservation is released and what the agent used is
// remembered for the next estimate.
func (l *Ledger) Finish(ctx context.Context, jobID, to, reason string) error {
	if to != Finished && to != Lost && to != Detached {
		return fmt.Errorf("ledger: %s is not an end state for a running job", to)
	}
	return l.tx(ctx, func(tx *sql.Tx) error {
		var kind, repo string
		var peak int64
		if err := tx.QueryRowContext(ctx, `SELECT kind, repo, peak_rss_mb FROM jobs WHERE id=?`, jobID).Scan(&kind, &repo, &peak); err != nil {
			return err
		}
		if err := l.move(ctx, tx, jobID, "", Running, to, `, finished_at=?, last_error=?`, l.ms(), truncate(reason, 500)); err != nil {
			return err
		}
		if err := l.release(ctx, tx, jobID); err != nil {
			return err
		}
		if peak > 0 {
			if err := learn(ctx, tx, kind, repo, peak); err != nil {
				return err
			}
		}
		return event(ctx, tx, l.ms(), "cluster.job."+to, jobID, "", map[string]any{"reason": truncate(reason, 500), "peak_rss_mb": peak})
	})
}

// Cancel stops a job that has not started. A running job is refused: ending
// an agent is the operator's call in Herdr, and Detach is the way to stop
// tracking one.
func (l *Ledger) Cancel(ctx context.Context, jobID, by string) (Job, error) {
	err := l.tx(ctx, func(tx *sql.Tx) error {
		var state string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM jobs WHERE id=?`, jobID).Scan(&state); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		switch state {
		case Queued, Reserved:
		case Dispatching:
			return fmt.Errorf("job %s is being dispatched right now; try again in a moment", jobID)
		case Running:
			return fmt.Errorf("job %s is running; this scheduler never stops an agent. Close it in Herdr, or detach it to stop tracking", jobID)
		default:
			return fmt.Errorf("job %s is already %s", jobID, state)
		}
		if err := l.move(ctx, tx, jobID, "", state, Cancelled, `, finished_at=?, token=''`, l.ms()); err != nil {
			return err
		}
		if err := l.release(ctx, tx, jobID); err != nil {
			return err
		}
		return event(ctx, tx, l.ms(), "cluster.job.cancelled", jobID, "", map[string]any{"by": by, "was": state})
	})
	if err != nil {
		return Job{}, err
	}
	return l.Get(ctx, jobID)
}

// SetDisconnected flags a running job whose worker cannot be reached. The
// reservation stays: the agent is very likely still working.
func (l *Ledger) SetDisconnected(ctx context.Context, jobID string, disconnected bool) error {
	return l.tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET disconnected=?, updated_at=? WHERE id=? AND state=? AND disconnected<>?`,
			boolInt(disconnected), l.ms(), jobID, Running, boolInt(disconnected))
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 1 {
			kind := "cluster.job.reconnected"
			if disconnected {
				kind = "cluster.job.disconnected"
			}
			return event(ctx, tx, l.ms(), kind, jobID, "", nil)
		}
		return nil
	})
}

// Observe records what a running agent is using. When it uses more memory
// than its reservation, the reservation is raised to the observed figure
// plus marginPct, so the scheduler stops promising that memory to others.
// Nothing is killed. It reports whether the reservation grew.
func (l *Ledger) Observe(ctx context.Context, jobID string, rssMB int64, cpu float64, marginPct float64) (bool, error) {
	grew := false
	err := l.tx(ctx, func(tx *sql.Tx) error {
		var reserved int64
		err := tx.QueryRowContext(ctx, `SELECT memory_mb FROM reservations WHERE job_id=? AND released_at IS NULL`, jobID).Scan(&reserved)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET observed_rss_mb=?, observed_cpu=?, peak_rss_mb=MAX(peak_rss_mb,?) WHERE id=? AND state=?`,
			rssMB, cpu, rssMB, jobID, Running); err != nil {
			return err
		}
		if rssMB > reserved {
			raised := int64(math.Ceil(float64(rssMB) * (1 + marginPct/100)))
			if _, err := tx.ExecContext(ctx, `UPDATE reservations SET memory_mb=? WHERE job_id=? AND released_at IS NULL`, raised, jobID); err != nil {
				return err
			}
			grew = true
			return event(ctx, tx, l.ms(), "cluster.job.overrun", jobID, "", map[string]any{"reserved_mb": reserved, "observed_mb": rssMB, "raised_to_mb": raised})
		}
		return nil
	})
	return grew, err
}

// learn folds a finished job's peak into the history for its kind and
// repository, as an exponentially weighted average that leans on recent runs.
func learn(ctx context.Context, tx *sql.Tx, kind, repo string, peak int64) error {
	for _, r := range []string{repo, ""} {
		if _, err := tx.ExecContext(ctx, `INSERT INTO history(kind,repo,peak_rss_mb,samples) VALUES(?,?,?,1)
			ON CONFLICT(kind,repo) DO UPDATE SET peak_rss_mb = peak_rss_mb*0.7 + excluded.peak_rss_mb*0.3, samples = samples+1`,
			kind, r, float64(peak)); err != nil {
			return err
		}
		if repo == "" {
			break
		}
	}
	return nil
}

// Estimate is the learned memory estimate for a kind and repository: the
// repository's own history when there is some, otherwise the kind's. ok is
// false when nothing has been learned yet.
func (l *Ledger) Estimate(ctx context.Context, kind, repo string, headroomPct float64) (mb int64, source string, ok bool, err error) {
	for _, r := range []string{repo, ""} {
		var peak float64
		var n int
		e := l.db.QueryRowContext(ctx, `SELECT peak_rss_mb, samples FROM history WHERE kind=? AND repo=?`, kind, r).Scan(&peak, &n)
		if errors.Is(e, sql.ErrNoRows) {
			continue
		}
		if e != nil {
			return 0, "", false, e
		}
		src := fmt.Sprintf("history:%s (%d runs)", kind, n)
		if r != "" {
			src = fmt.Sprintf("history:%s@%s (%d runs)", kind, r, n)
		}
		return int64(math.Ceil(peak * (1 + headroomPct/100))), src, true, nil
	}
	return 0, "", false, nil
}

// Decision is a stored placement decision.
type Decision struct {
	ID      int64           `json:"id"`
	JobID   string          `json:"job"`
	At      time.Time       `json:"at"`
	Chosen  string          `json:"chosen,omitempty"`
	Policy  string          `json:"policy"`
	Summary string          `json:"summary"`
	Detail  json.RawMessage `json:"detail"`
}

// RecordDecision stores why a job went where it went, or why it waits. A
// waiting job's decision is only stored when it changed, so a job that sits
// in the queue for an hour does not write one row per tick.
func (l *Ledger) RecordDecision(ctx context.Context, jobID, chosen, policy, summary string, detail any) error {
	b, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	return l.tx(ctx, func(tx *sql.Tx) error {
		var last string
		_ = tx.QueryRowContext(ctx, `SELECT summary FROM decisions WHERE job_id=? ORDER BY id DESC LIMIT 1`, jobID).Scan(&last)
		if chosen == "" && last == summary {
			return nil
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO decisions(job_id,at,chosen,policy,summary,detail) VALUES(?,?,?,?,?,?)`,
			jobID, l.ms(), chosen, policy, summary, string(b))
		return err
	})
}

// Decisions returns a job's decisions, newest last.
func (l *Ledger) Decisions(ctx context.Context, jobID string) ([]Decision, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT id,job_id,at,chosen,policy,summary,detail FROM decisions WHERE job_id=? ORDER BY id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Decision
	for rows.Next() {
		var d Decision
		var at int64
		var detail string
		if err := rows.Scan(&d.ID, &d.JobID, &at, &d.Chosen, &d.Policy, &d.Summary, &detail); err != nil {
			return nil, err
		}
		d.At, d.Detail = time.UnixMilli(at).UTC(), json.RawMessage(detail)
		out = append(out, d)
	}
	return out, rows.Err()
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
