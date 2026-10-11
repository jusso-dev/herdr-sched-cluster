package coord

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/gitop"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/herdrx"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/placement"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

// Observe probes the workers that have running jobs, measuring each job's
// process tree, and records the usage. A job over its reservation has the
// reservation raised; nothing is ever killed for that.
func (c *Coordinator) Observe(ctx context.Context, pids map[string]map[int]string) error {
	if len(pids) == 0 {
		return nil
	}
	req := map[string][]int{}
	for w, m := range pids {
		for pid := range m {
			req[w] = append(req[w], pid)
		}
		sort.Ints(req[w])
	}
	workers, err := c.Workers(ctx, false, req)
	if err != nil {
		return err
	}
	for _, w := range workers {
		for _, p := range w.Snapshot.Processes {
			job, ok := pids[w.ID][p.PID]
			if !ok || !p.Found {
				continue
			}
			if _, err := c.L.Observe(ctx, job, p.RSSMB, p.CPUPct, c.Cfg.OverrunMarginPct); err != nil {
				return err
			}
		}
	}
	return nil
}

// Tick is one full round: reconcile, measure, react to memory pressure,
// clean up worktrees, then plan and dispatch. Every event it writes carries
// the tick number, so one round can be followed through the trail.
func (c *Coordinator) Tick(ctx context.Context) (Report, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cmu.Lock()
	c.tick++
	c.cmu.Unlock()
	pids, err := c.Reconcile(ctx)
	if err != nil {
		return Report{}, err
	}
	if err := c.Observe(ctx, pids); err != nil {
		return Report{}, err
	}
	if err := c.Pressure(ctx); err != nil {
		return Report{}, err
	}
	if err := c.Cleanup(ctx); err != nil {
		return Report{}, err
	}
	return c.pump(ctx)
}

// Drain stops new work going to a worker; running agents carry on.
func (c *Coordinator) Drain(ctx context.Context, worker string, draining bool, by string) (string, error) {
	t, err := c.find(ctx, worker)
	if err != nil {
		return "", err
	}
	return t.ID, c.L.SetDraining(ctx, t.ID, draining, by)
}

// Reregister forgets a worker's remembered identity, so the next probe
// accepts whatever answers there now.
func (c *Coordinator) Reregister(ctx context.Context, worker, by string) (string, error) {
	t, err := c.find(ctx, worker)
	if err != nil {
		return "", err
	}
	c.cmu.Lock()
	delete(c.cache, t.ID)
	c.cmu.Unlock()
	return t.ID, c.L.ForgetIdentity(ctx, t.ID, by)
}

func (c *Coordinator) find(ctx context.Context, worker string) (Target, error) {
	targets, _ := c.Targets(ctx)
	for _, t := range targets {
		if t.ID == worker || t.Label == worker {
			return t, nil
		}
	}
	return Target{}, fmt.Errorf("no worker called %s; see hsched cluster workers", worker)
}

// keep says whether the retention policy keeps a job's worktree, and why.
// ready is false under the age policy while the worktree is still young.
func (c *Coordinator) keep(j ledger.Job) (keep bool, ready bool, why string) {
	switch c.Cfg.WorktreeRetention {
	case AlwaysRetain:
		return true, true, "kept: cluster_worktree_retention is always-retain"
	case AlwaysRemove:
		return false, true, "removed: cluster_worktree_retention is always-remove"
	case RetainForAge:
		age := c.now().Sub(j.FinishedAt)
		if age < c.Cfg.WorktreeMaxAge {
			return true, false, ""
		}
		return false, true, fmt.Sprintf("removed: older than %s under the age policy", c.Cfg.WorktreeMaxAge)
	default:
		if j.State == ledger.Finished {
			return false, true, "removed: the job finished normally (retain-on-failure)"
		}
		reason := j.EndReason
		if reason == "" {
			reason = j.State
		}
		return true, true, fmt.Sprintf("kept for inspection: the job ended %s (%s) under retain-on-failure", j.State, reason)
	}
}

// Cleanup applies the worktree retention policy to jobs that ended with a
// scheduler-made worktree. A worktree is never removed while any agent is
// working in it, never while its worker cannot be reached (or a lost job's
// agent may still be running), and never with uncommitted or untracked
// changes unless cluster_worktree_remove_dirty says so. Every decision is
// recorded on the job with the reason.
func (c *Coordinator) Cleanup(ctx context.Context) error {
	jobs, err := c.L.PendingCleanup(ctx)
	if err != nil || len(jobs) == 0 {
		return err
	}
	agentsOn := map[string][]herdrx.Agent{}
	reachable := map[string]bool{}
	for _, j := range jobs {
		if j.State == ledger.Lost && j.EndReason == ledger.EndWorkerUnreachable {
			continue // the agent may still be running there
		}
		keep, ready, why := c.keep(j)
		if !ready {
			continue
		}
		if keep || j.WorkspaceID == "" {
			if j.WorkspaceID == "" && !keep {
				why = "nothing to remove: no workspace was recorded"
			}
			_ = c.L.SetCleanup(ctx, j.ID, ledger.CleanupKept, why)
			continue
		}
		m := machineArg(j.Worker)
		if _, seen := reachable[j.Worker]; !seen {
			as, err := c.H.Agents(ctx, m)
			reachable[j.Worker] = err == nil
			agentsOn[j.Worker] = as
		}
		if !reachable[j.Worker] {
			continue // try again when the worker answers
		}
		busy := false
		for _, a := range agentsOn[j.Worker] {
			busy = busy || a.WorkspaceID == j.WorkspaceID
		}
		if busy {
			continue // someone is working in it; look again next tick
		}
		err := c.H.WorktreeRemove(ctx, m, j.WorkspaceID, c.Cfg.WorktreeRemoveDirty)
		switch {
		case herdrx.Code(err) == "dirty_worktree_requires_force":
			_ = c.L.SetCleanup(ctx, j.ID, ledger.CleanupKept,
				"kept: the worktree has uncommitted or untracked changes (set cluster_worktree_remove_dirty to remove such worktrees)")
			continue
		case err != nil && (herdrx.Code(err) == "workspace_not_found" || strings.Contains(err.Error(), "not found")):
			_ = c.L.SetCleanup(ctx, j.ID, ledger.CleanupRemoved, "the workspace was already gone")
			continue
		case err != nil:
			_ = c.L.SetCleanup(ctx, j.ID, ledger.CleanupKept, "kept: removing it failed: "+err.Error())
			continue
		}
		_ = c.H.WorkspaceClose(ctx, m, j.WorkspaceID) // in case removing the checkout left it open
		note := why
		if c.Cfg.DeleteBranches && j.Branch != "" && j.WorkerPath != "" {
			res, gerr := c.G.Run(ctx, c.target(j.Worker), gitop.Request{Op: gitop.OpBranchDelete, Repo: j.WorkerPath, Branch: j.Branch})
			switch {
			case gerr != nil:
				note += "; branch " + j.Branch + " kept: " + gerr.Error()
			case !res.OK:
				note += "; branch " + j.Branch + " kept: " + res.Message
			default:
				note += "; branch " + j.Branch + " deleted"
			}
		}
		_ = c.L.SetCleanup(ctx, j.ID, ledger.CleanupRemoved, note)
	}
	return nil
}

// Pressure records each worker's memory pressure with the largest jobs there
// and a recommendation, and with cluster_pressure_action terminate-disposable
// closes the largest job submitted as disposable once critical pressure has
// lasted cluster_pressure_grace_seconds. Nothing else is ever stopped.
func (c *Coordinator) Pressure(ctx context.Context) error {
	ws, err := c.Workers(ctx, false, nil)
	if err != nil {
		return err
	}
	running, err := c.L.List(ctx, ledger.Running)
	if err != nil {
		return err
	}
	byWorker := map[string][]ledger.Job{}
	for _, j := range running {
		byWorker[j.Worker] = append(byWorker[j.Worker], j)
	}
	for _, w := range ws {
		level := w.Snapshot.Memory.Pressure
		if w.State != placement.StateReady && w.State != placement.StateDraining || level == "" || w.Snapshot.Backend == resource.BackendStatic {
			continue
		}
		jobs := byWorker[w.ID]
		sort.SliceStable(jobs, func(a, b int) bool { return jobs[a].ObservedRSSMB > jobs[b].ObservedRSSMB })
		top := []map[string]any{}
		for i, j := range jobs {
			if i == 3 {
				break
			}
			top = append(top, map[string]any{"job": j.ID, "kind": j.Kind, "rss_mb": j.ObservedRSSMB, "disposable": j.Disposable})
		}
		detail := map[string]any{"largest_jobs": top, "recommendation": recommend(level, w, jobs)}
		lasted, _, err := c.L.SetPressure(ctx, w.ID, level, detail)
		if err != nil {
			return err
		}
		if level != resource.PressureCritical || c.Cfg.PressureAction != PressureTerminateDisposable || lasted < c.Cfg.PressureGrace {
			continue
		}
		c.cmu.Lock()
		last := c.lastCut[w.ID]
		c.cmu.Unlock()
		if !last.IsZero() && c.now().Sub(last) < c.Cfg.PressureGrace {
			continue
		}
		for _, j := range jobs {
			if !j.Disposable {
				continue
			}
			reason := fmt.Sprintf("memory pressure on %s has been critical for %s; closed as disposable (%d MB)", w.ID, lasted.Round(1e9), j.ObservedRSSMB)
			if j.WorkspaceID != "" {
				if err := c.H.WorkspaceClose(ctx, machineArg(w.ID), j.WorkspaceID); err != nil {
					c.event(ctx, "cluster.pressure.terminate_failed", j.ID, w.ID, map[string]any{"error": err.Error()})
					break
				}
			}
			now, err := c.L.Terminate(ctx, j.ID, reason)
			if err == nil {
				c.event(ctx, "cluster.pressure.terminated", j.ID, w.ID, map[string]any{"now": now})
			}
			c.cmu.Lock()
			c.lastCut[w.ID] = c.now()
			c.cmu.Unlock()
			break
		}
	}
	return nil
}

func recommend(level string, w placement.Worker, jobs []ledger.Job) string {
	switch level {
	case resource.PressureCritical:
		if len(jobs) == 0 {
			return "memory is critical with no scheduled jobs here; something else is using it"
		}
		return fmt.Sprintf("memory is critical: no new work goes to %s; consider draining it or stopping %s (largest, %d MB)",
			w.ID, jobs[0].ID, jobs[0].ObservedRSSMB)
	case resource.PressureWarn:
		return fmt.Sprintf("memory is tight on %s; new work only goes there if it still fits after the safety margin", w.ID)
	}
	return "memory pressure is normal"
}
