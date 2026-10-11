package coord

import (
	"context"
	"fmt"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/herdrx"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
)

// Reconcile compares the ledger with what each worker's Herdr reports. It
// returns, per worker, the shell PIDs of running jobs, keyed so a probe can
// measure them.
//
// A running job whose agent is no longer listed either finished or was lost.
// The pane tells them apart: the agent's own shell still there means the
// agent exited (finished); the pane gone means the session vanished (lost);
// a different shell in the pane means Herdr restarted and the agent did not
// come back (lost). A worker that stops answering leaves its jobs running
// but disconnected, and after LostAfter they are lost with the reason
// worker_unreachable, which is never retried because the agent may still be
// working. If the agent turns up again on reconnect the job is readopted.
func (c *Coordinator) Reconcile(ctx context.Context) (map[string]map[int]string, error) {
	active, err := c.L.List(ctx, ledger.Reserved, ledger.Dispatching, ledger.Running)
	if err != nil {
		return nil, err
	}
	lost, err := c.L.List(ctx, ledger.Lost)
	if err != nil {
		return nil, err
	}
	for _, j := range lost {
		if j.EndReason == ledger.EndWorkerUnreachable && j.Worker != "" {
			active = append(active, j)
		}
	}
	byWorker := map[string][]ledger.Job{}
	for _, j := range active {
		byWorker[j.Worker] = append(byWorker[j.Worker], j)
	}
	pids := map[string]map[int]string{}
	now := c.now()
	for worker, jobs := range byWorker {
		m := machineArg(worker)
		agents, err := c.H.Agents(ctx, m)
		if err != nil {
			for _, j := range jobs {
				if j.State != ledger.Running {
					continue
				}
				_ = c.L.SetDisconnected(ctx, j.ID, true)
				if !j.DisconnectedAt.IsZero() && now.Sub(j.DisconnectedAt) >= c.Cfg.LostAfter {
					_ = c.L.End(ctx, j.ID, ledger.Lost, ledger.EndWorkerUnreachable, fmt.Sprintf(
						"%s has not answered for %s; the agent may still be running there and will be readopted if it is seen again",
						worker, now.Sub(j.DisconnectedAt).Round(1e9)))
				}
			}
			continue
		}
		live := map[string]herdrx.Agent{}
		for _, a := range agents {
			live[a.Name] = a
		}
		for _, j := range jobs {
			if c.isInflight(j.ID) {
				continue
			}
			a, ok := live[ledger.AgentName(j.ID)]
			switch j.State {
			case ledger.Lost:
				if ok {
					if err := c.L.Readopt(ctx, j.ID, a.PaneID, a.WorkspaceID); err == nil {
						c.event(ctx, "cluster.reconcile.readopted", j.ID, worker, nil)
					}
					continue
				}
				if err := c.L.ConfirmLost(ctx, j.ID, ledger.EndSessionVanished,
					worker+" answers again and the agent is not there"); err == nil {
					c.retryLost(ctx, j, worker, nil)
				}
			case ledger.Running:
				if !ok {
					c.ended(ctx, j, worker, m)
					continue
				}
				_ = c.L.SetDisconnected(ctx, j.ID, false)
				pane := a.PaneID
				if pane == "" {
					pane = j.PaneID
				}
				if pane == "" {
					continue
				}
				pi, err := c.H.ProcessInfo(ctx, m, pane)
				if err != nil {
					continue
				}
				if pi.ShellPID > 0 {
					_ = c.L.SetShellPID(ctx, j.ID, pi.ShellPID)
				}
				pid := pi.ShellPID
				if pid == 0 && len(pi.Foreground) > 0 {
					pid = pi.Foreground[0].PID
				}
				if pid > 0 {
					if pids[worker] == nil {
						pids[worker] = map[int]string{}
					}
					pids[worker][pid] = j.ID
				}
			case ledger.Reserved, ledger.Dispatching:
				if ok {
					_ = c.L.AdoptRunning(ctx, j.ID, a.PaneID, a.WorkspaceID)
					continue
				}
				if now.Sub(j.UpdatedAt) < c.Cfg.DispatchGrace {
					continue
				}
				c.interrupted(ctx, j, worker, m)
			}
		}
	}
	return pids, nil
}

// ended decides whether a running job whose agent is gone finished or was
// lost, from what is left in its pane.
func (c *Coordinator) ended(ctx context.Context, j ledger.Job, worker, m string) {
	if j.PaneID == "" {
		_ = c.L.End(ctx, j.ID, ledger.Finished, ledger.EndAgentExited, "agent is no longer listed by Herdr on "+worker)
		return
	}
	pi, err := c.H.ProcessInfo(ctx, m, j.PaneID)
	switch {
	case err != nil && herdrx.Code(err) == "pane_not_found":
		_ = c.L.End(ctx, j.ID, ledger.Lost, ledger.EndSessionVanished,
			"the agent and its pane are gone from "+worker+"; the session was closed or lost")
		j.EndReason = ledger.EndSessionVanished
		c.retryLost(ctx, j, worker, nil)
	case err != nil:
		// Not sure what happened; ask again next tick rather than guess.
		return
	case j.ShellPID > 0 && pi.ShellPID > 0 && pi.ShellPID != j.ShellPID:
		_ = c.L.End(ctx, j.ID, ledger.Lost, ledger.EndHerdrRestarted, fmt.Sprintf(
			"Herdr on %s restarted: the pane is back with a new shell (%d, was %d) and the agent did not resume", worker, pi.ShellPID, j.ShellPID))
		j.EndReason = ledger.EndHerdrRestarted
		c.retryLost(ctx, j, worker, &pi)
	default:
		_ = c.L.End(ctx, j.ID, ledger.Finished, ledger.EndAgentExited, "agent exited on "+worker+"; its pane is still there")
	}
}

// retryLost requeues a lost job when the policy allows it. A restored pane
// with something running in the foreground could be the agent resuming under
// another name, so that is never retried.
func (c *Coordinator) retryLost(ctx context.Context, j ledger.Job, worker string, pi *herdrx.ProcessInfo) {
	if !c.Cfg.RetryLost {
		c.event(ctx, "cluster.job.retry_refused", j.ID, worker, map[string]any{"reason": "cluster_retry_lost is off"})
		return
	}
	if pi != nil && busy(*pi) {
		c.event(ctx, "cluster.job.retry_refused", j.ID, worker, map[string]any{
			"reason": "the restored pane has a foreground process, which may be the agent resuming; not retried"})
		return
	}
	_, _, _ = c.L.RetryLost(ctx, j.ID)
}

// interrupted settles a dispatch that did not finish (the coordinator
// stopped part way, or the start answer was lost) and whose agent is not
// running. If the agent start was attempted the agent may have run, so the
// job is failed for a person to look at rather than launched a second time.
// Otherwise no agent can have started: whatever workspace was opened is
// closed (found by its label if the crash came before its id was saved) and
// the job is requeued.
func (c *Coordinator) interrupted(ctx context.Context, j ledger.Job, worker, m string) {
	if j.State == ledger.Dispatching && !j.StartAttemptedAt.IsZero() {
		why := "the agent start was attempted"
		if !j.PromptSentAt.IsZero() {
			why = "the prompt may have been delivered"
		}
		_ = c.L.Abandon(ctx, j.ID, j.Token, "dispatch was interrupted on "+worker+" after "+why+
			"; no agent is running now, so it may have started and finished. Not retried, so it cannot run twice; resubmit if needed")
		return
	}
	ws := j.WorkspaceID
	if j.State == ledger.Dispatching && ws == "" {
		list, err := c.H.WorkspaceList(ctx, m)
		if err != nil {
			return // cannot confirm there is no orphan; try next tick
		}
		for _, w := range list {
			if w.Label == ledger.AgentName(j.ID) {
				ws = w.ID
				break
			}
		}
	}
	if ws != "" {
		c.discard(ctx, m, ws, j.Worktree)
	}
	if _, err := c.L.DispatchFailed(ctx, j.ID, j.Token, "dispatch did not finish and no agent was found on "+worker+
		"; nothing was delivered, so it is safe to retry"); err == nil {
		c.event(ctx, "cluster.reconcile.dispatch_retried", j.ID, worker, map[string]any{"closed_workspace": ws})
	}
}

// busy says whether a pane runs something other than its own shell in the
// foreground. Herdr lists the shell itself as the foreground process of an
// idle pane.
func busy(pi herdrx.ProcessInfo) bool {
	for _, p := range pi.Foreground {
		if p.PID != pi.ShellPID {
			return true
		}
	}
	return false
}
