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
	// CPUSource says where the CPU figure came from: requested, default or
	// history.
	CPUSource string `json:"cpu_estimate_source,omitempty"`
	// Restartable says the submitter declared the job safe to run again
	// from the start, which is what lets a lost job be retried.
	Restartable bool `json:"restartable,omitempty"`
	// Disposable says the job may be terminated under memory pressure when
	// cluster_pressure_action allows it. Nothing else is ever terminated.
	Disposable bool     `json:"disposable,omitempty"`
	DependsOn  []string `json:"depends_on,omitempty"`
	// StartAttemptedAt and PromptSentAt are written just before the agent
	// start and the first prompt, so a coordinator that dies mid-dispatch
	// knows afterwards how far it got.
	StartAttemptedAt time.Time `json:"start_attempted_at,omitempty"`
	PromptSentAt     time.Time `json:"prompt_sent_at,omitempty"`
	// ShellPID is the pane's shell as last seen. A different shell in the
	// same pane means Herdr on the worker restarted underneath the agent.
	ShellPID int     `json:"shell_pid,omitempty"`
	PeakCPU  float64 `json:"peak_cpu_pct,omitempty"`
	// EndReason is a short machine-readable reason for an end state, such as
	// agent_exited, herdr_restarted, session_vanished or worker_unreachable.
	EndReason string `json:"end_reason,omitempty"`
	// Cleanup is what happened to the job's worktree: pending, removed or
	// kept, with CleanupNote saying why.
	Cleanup        string    `json:"workspace_cleanup,omitempty"`
	CleanupNote    string    `json:"workspace_cleanup_note,omitempty"`
	Branch         string    `json:"branch,omitempty"`
	NoWorktree     bool      `json:"no_worktree,omitempty"`
	CloneURL       string    `json:"clone_url,omitempty"`
	DisconnectedAt time.Time `json:"disconnected_at,omitempty"`
	// WorkerPath is the repository path on the worker, after per-worker
	// path mapping and ~ expansion.
	WorkerPath string `json:"worker_path,omitempty"`
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
	CPUSource           string
	Restartable         bool
	Disposable          bool
	DependsOn           []string
	CloneURL            string
}

const jobCols = `j.id,j.seq,j.state,j.kind,j.repo,j.prompt,j.prompt_sha,j.agent_args,j.memory_mb,j.cpu,j.estimate_source,
j.priority,j.policy,j.machine,j.labels,j.os,j.arch,j.isolation,j.submitted_by,j.submitted_at,j.updated_at,j.attempts,
j.max_attempts,j.worker,j.agent_name,j.workspace_id,j.pane_id,j.worktree,j.token,j.started_at,j.finished_at,
j.disconnected,j.observed_rss_mb,j.observed_cpu,j.peak_rss_mb,j.last_error,
COALESCE((SELECT memory_mb FROM reservations r WHERE r.job_id=j.id AND r.released_at IS NULL),0),
j.cpu_source,j.restartable,j.disposable,j.depends_on,j.start_intent,j.prompt_intent,j.shell_pid,j.peak_cpu,j.end_reason,
j.cleanup,j.cleanup_note,j.branch,j.no_worktree,j.clone_url,j.disconnected_at,j.worker_path`

type scanner interface{ Scan(...any) error }

func scanJob(s scanner) (Job, error) {
	var j Job
	var args, labels, deps string
	var sub, upd, started, finished, startIntent, promptIntent, discAt int64
	var wt, disc, restartable, disposable, noWT int
	err := s.Scan(&j.ID, &j.Seq, &j.State, &j.Kind, &j.Repo, &j.Prompt, &j.PromptSHA, &args, &j.MemoryMB, &j.CPU,
		&j.EstimateSource, &j.Priority, &j.Policy, &j.Machine, &labels, &j.OS, &j.Arch, &j.Isolation, &j.SubmittedBy,
		&sub, &upd, &j.Attempts, &j.MaxAttempts, &j.Worker, &j.AgentName, &j.WorkspaceID, &j.PaneID, &wt, &j.Token,
		&started, &finished, &disc, &j.ObservedRSSMB, &j.ObservedCPU, &j.PeakRSSMB, &j.LastError, &j.ReservedMB,
		&j.CPUSource, &restartable, &disposable, &deps, &startIntent, &promptIntent, &j.ShellPID, &j.PeakCPU, &j.EndReason,
		&j.Cleanup, &j.CleanupNote, &j.Branch, &noWT, &j.CloneURL, &discAt, &j.WorkerPath)
	if err != nil {
		return j, err
	}
	_ = json.Unmarshal([]byte(deps), &j.DependsOn)
	j.Restartable, j.Disposable, j.NoWorktree = restartable != 0, disposable != 0, noWT != 0
	if startIntent > 0 {
		j.StartAttemptedAt = time.UnixMilli(startIntent).UTC()
	}
	if promptIntent > 0 {
		j.PromptSentAt = time.UnixMilli(promptIntent).UTC()
	}
	if discAt > 0 {
		j.DisconnectedAt = time.UnixMilli(discAt).UTC()
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
	deps, _ := json.Marshal(nonNil(n.DependsOn))
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
		for _, d := range n.DependsOn {
			var st string
			if err := tx.QueryRowContext(ctx, `SELECT state FROM jobs WHERE id=?`, d).Scan(&st); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return fmt.Errorf("depends on %s, which is not a cluster job", d)
				}
				return err
			}
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO jobs(id,seq,state,kind,repo,prompt,prompt_sha,agent_args,memory_mb,cpu,
			estimate_source,priority,policy,machine,labels,os,arch,isolation,submitted_by,submitted_at,updated_at,max_attempts,agent_name,
			cpu_source,restartable,disposable,depends_on,clone_url)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			id, seq, Queued, n.Kind, n.Repo, n.Prompt, sha, string(args), n.MemoryMB, n.CPU, n.EstimateSource, n.Priority,
			n.Policy, n.Machine, string(labels), n.OS, n.Arch, n.Isolation, n.SubmittedBy, now, now, n.MaxAttempts, AgentName(id),
			n.CPUSource, boolInt(n.Restartable), boolInt(n.Disposable), string(deps), n.CloneURL)
		if err != nil {
			return err
		}
		return event(ctx, tx, now, "cluster.job.submitted", id, "", map[string]any{
			"kind": n.Kind, "repo": n.Repo, "memory_mb": n.MemoryMB, "cpu": n.CPU, "estimate": n.EstimateSource,
			"cpu_estimate": n.CPUSource, "priority": n.Priority, "prompt_chars": len([]rune(n.Prompt)), "prompt_sha256": sha,
			"depends_on": nonNil(n.DependsOn), "restartable": n.Restartable, "disposable": n.Disposable,
			"submitted_by": n.SubmittedBy,
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
	// PendingCPU is committed CPU the worker's utilisation reading does
	// not yet show: reservations not yet running, and the part of each
	// estimate the agent is not observed using. For a static worker it is
	// all of CommittedCPU.
	PendingCPU   float64
	ActiveAgents int
	RunningRepos map[string]int
	// ClusterRepos counts active jobs per repository across every worker,
	// for the per-repository concurrency limit.
	ClusterRepos map[string]int
}

// loadTx sums a worker's live reservations. For a measured worker, memory an
// agent is already seen using is in the operating system's own numbers, so
// only the unused part of its reservation is pending. For a static worker
// nothing is measured, so all of it is.
func loadTx(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, worker string, static bool) (Load, error) {
	rows, err := q.QueryContext(ctx, `SELECT r.memory_mb, r.cpu, j.observed_rss_mb, j.observed_cpu, j.repo, r.worker FROM reservations r
		JOIN jobs j ON j.id=r.job_id WHERE r.released_at IS NULL`)
	if err != nil {
		return Load{}, err
	}
	defer rows.Close()
	ld := Load{RunningRepos: map[string]int{}, ClusterRepos: map[string]int{}}
	for rows.Next() {
		var mem, observed int64
		var cpu, observedCPU float64
		var repo, w string
		if err := rows.Scan(&mem, &cpu, &observed, &observedCPU, &repo, &w); err != nil {
			return Load{}, err
		}
		if repo != "" {
			ld.ClusterRepos[repo]++
		}
		if w != worker {
			continue
		}
		ld.ReservedMB += mem
		pending := mem
		pendingCPU := cpu
		if !static {
			pending = mem - min64(observed, mem)
			pendingCPU = cpu - math.Min(observedCPU/100, cpu)
		}
		ld.PendingMemoryMB += pending
		ld.PendingCPU += pendingCPU
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
			disconnected=0, disconnected_at=0, observed_rss_mb=0, observed_cpu=0, start_intent=0, prompt_intent=0,
			shell_pid=0, end_reason='', worker_path='', cleanup='', cleanup_note='', branch='' WHERE id=? AND state=?`,
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
		// A failed earlier attempt stays on the event trail; on the job row
		// it would read as if the running agent were in trouble.
		set := `, started_at=?, last_error=''`
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
		if err := l.move(ctx, tx, jobID, token, state, Failed, `, last_error=?, finished_at=?, end_reason='dispatch_interrupted',
			cleanup=CASE WHEN worktree<>0 AND workspace_id<>'' THEN 'pending' ELSE '' END`, truncate(reason, 500), l.ms()); err != nil {
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
	return l.End(ctx, jobID, to, "", reason)
}

// End is Finish with a machine-readable end reason (agent_exited,
// herdr_restarted, session_vanished, worker_unreachable, detached, ...).
func (l *Ledger) End(ctx context.Context, jobID, to, endReason, reason string) error {
	if to != Finished && to != Lost && to != Detached {
		return fmt.Errorf("ledger: %s is not an end state for a running job", to)
	}
	if endReason == "" {
		endReason = map[string]string{Finished: "agent_exited", Lost: "session_vanished", Detached: "detached"}[to]
	}
	return l.tx(ctx, func(tx *sql.Tx) error {
		var kind, repo string
		var peak int64
		var worktree int
		if err := tx.QueryRowContext(ctx, `SELECT kind, repo, peak_rss_mb, worktree FROM jobs WHERE id=?`, jobID).Scan(&kind, &repo, &peak, &worktree); err != nil {
			return err
		}
		cleanup := ""
		if worktree != 0 && to != Detached {
			cleanup = CleanupPending
		}
		if err := l.move(ctx, tx, jobID, "", Running, to, `, finished_at=?, last_error=?, end_reason=?, cleanup=?`,
			l.ms(), truncate(reason, 500), endReason, cleanup); err != nil {
			return err
		}
		if err := l.release(ctx, tx, jobID); err != nil {
			return err
		}
		run, err := l.recordRun(ctx, tx, jobID, kind, repo, peak, to)
		if err != nil {
			return err
		}
		return event(ctx, tx, l.ms(), "cluster.job."+to, jobID, "", map[string]any{"reason": truncate(reason, 500),
			"end_reason": endReason, "peak_rss_mb": peak, "typical_rss_mb": run.TypicalMB, "cpu_cores_p90": run.CPU})
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
		at := int64(0)
		if disconnected {
			at = l.ms()
		}
		res, err := tx.ExecContext(ctx, `UPDATE jobs SET disconnected=?, disconnected_at=?, updated_at=? WHERE id=? AND state=? AND disconnected<>?`,
			boolInt(disconnected), at, l.ms(), jobID, Running, boolInt(disconnected))
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
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET observed_rss_mb=?, observed_cpu=?, peak_rss_mb=MAX(peak_rss_mb,?),
			peak_cpu=MAX(peak_cpu,?) WHERE id=? AND state=?`, rssMB, cpu, rssMB, cpu, jobID, Running); err != nil {
			return err
		}
		if err := addSample(ctx, tx, jobID, l.ms(), rssMB, cpu); err != nil {
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

// Estimate is the learned memory estimate for a kind and repository: the
// configured percentile of recent runs in this repository when there are
// some, otherwise of the kind's runs anywhere, plus headroom. ok is false
// when nothing has been learned yet. It uses the default window and
// percentile; Profile takes them as arguments.
func (l *Ledger) Estimate(ctx context.Context, kind, repo string, headroomPct float64) (mb int64, source string, ok bool, err error) {
	p, err := l.Profile(ctx, kind, repo, DefaultWindow, DefaultPercentile)
	if err != nil || p.MemoryRuns == 0 {
		return 0, "", false, err
	}
	return int64(math.Ceil(float64(p.MemoryMB) * (1 + headroomPct/100))), p.MemorySource, true, nil
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
