package coord

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/gitop"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/herdrx"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/placement"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

// decide records a placement decision. With explanations off only the
// summary is kept.
func (c *Coordinator) decide(ctx context.Context, jobID, chosen, policy, summary string, detail any) {
	if !c.Cfg.Explanations {
		detail = nil
	}
	_ = c.L.RecordDecision(ctx, jobID, chosen, policy, summary, detail)
}

// dependenciesReady says whether every job j depends on has finished. A
// dependency that ended any other way fails j; one still going leaves j
// waiting, with that recorded as its decision.
func (c *Coordinator) dependenciesReady(ctx context.Context, j ledger.Job) (bool, error) {
	if len(j.DependsOn) == 0 {
		return true, nil
	}
	states, err := c.L.States(ctx, j.DependsOn)
	if err != nil {
		return false, err
	}
	var waiting, broken []string
	for _, d := range j.DependsOn {
		st, ok := states[d]
		switch {
		case !ok:
			broken = append(broken, d+" (unknown)")
		case st == ledger.Finished:
		case st == ledger.Failed || st == ledger.Cancelled || st == ledger.Lost || st == ledger.Detached:
			broken = append(broken, d+" ("+st+")")
		default:
			waiting = append(waiting, d+" ("+st+")")
		}
	}
	if len(broken) > 0 {
		reason := "a dependency did not finish: " + strings.Join(broken, ", ")
		if err := c.L.FailQueued(ctx, j.ID, ledger.EndDependencyFailed, reason); err != nil && !errors.Is(err, ledger.ErrConflict) {
			return false, err
		}
		return false, nil
	}
	if len(waiting) > 0 {
		c.decide(ctx, j.ID, "", "", "waiting for dependencies: "+strings.Join(waiting, ", "), nil)
		return false, nil
	}
	return true, nil
}

// branchFor names the worktree branch for an attempt. A retry gets its own
// branch so it never collides with what an earlier attempt left behind.
func branchFor(job ledger.Job) string {
	attempt := job.Attempts + 1 // job is the queued row read before Reserve counted this attempt
	if attempt <= 1 {
		return "hsc/" + job.ID
	}
	return fmt.Sprintf("hsc/%s-%d", job.ID, attempt)
}

// emptyRepo says whether a worktree failure is Git refusing a repository
// with no commits, which has no HEAD to branch from.
func emptyRepo(err error) bool {
	m := err.Error()
	return strings.Contains(m, "invalid reference: HEAD") || strings.Contains(m, "Needed a single revision") ||
		strings.Contains(m, "unknown revision") || strings.Contains(m, "does not have any commits")
}

// dispatch opens a workspace (or worktree), starts the agent and gives it
// the prompt. It never requeues a job unless it has confirmed no agent of
// that name exists on the worker, which is what prevents a double dispatch.
// Before the agent start and before the prompt it writes an intent to the
// ledger, so a coordinator that dies part way knows on restart whether a
// retry could repeat work.
func (c *Coordinator) dispatch(ctx context.Context, job ledger.Job, w placement.Worker, ld ledger.Load) error {
	c.setInflight(job.ID, true)
	defer c.setInflight(job.ID, false)
	m := machineArg(w.ID)
	t := c.target(w.ID)
	name := ledger.AgentName(job.ID)
	worktree := false
	fail := func(stage string, err error, workspace string) error {
		if workspace != "" {
			c.discard(ctx, m, workspace, worktree)
		}
		state, ferr := c.L.DispatchFailed(ctx, job.ID, job.Token, stage+": "+err.Error())
		if ferr != nil {
			return fmt.Errorf("%s: %v (and recording it failed: %v)", stage, err, ferr)
		}
		c.event(ctx, "cluster.dispatch.failed", job.ID, w.ID, map[string]any{"stage": stage, "now": state})
		return fmt.Errorf("%s on %s: %w", stage, w.ID, err)
	}
	isolation := job.Isolation
	if isolation == "" {
		isolation = c.Cfg.Isolation
	}
	switch {
	case job.NoWorktree:
		// The repository had no commits on an earlier attempt; placement
		// already made sure nothing else is using it.
	case isolation == IsolationWorktree:
		worktree = true
	case isolation == IsolationAuto:
		// ld is the load just before this job's reservation, so any
		// count means another job already holds this repository here.
		worktree = ld.RunningRepos[job.Repo] > 0
	}
	if err := c.L.MarkDispatching(ctx, job.ID, job.Token, worktree); err != nil {
		return err
	}
	repo, err := resolveRepo(MapPath(t.PathMap, job.Repo), w.Snapshot)
	if err != nil {
		return fail("resolve repository", err, "")
	}
	if job.CloneURL != "" && w.Snapshot.Backend != resource.BackendStatic && !w.Snapshot.Paths[job.Repo] {
		res, err := c.G.Run(ctx, t, gitop.Request{Op: gitop.OpClone, URL: job.CloneURL, Path: repo})
		if err == nil && !res.OK {
			err = fmt.Errorf("%s", res.Message)
		}
		if err != nil {
			return fail("clone repository", err, "")
		}
		c.event(ctx, "cluster.dispatch.cloned", job.ID, w.ID, map[string]any{"path": repo})
	}
	branch := ""
	if worktree {
		branch = branchFor(job)
	}
	if err := c.L.SetWorkerPath(ctx, job.ID, job.Token, repo, branch); err != nil {
		return err
	}
	var pl herdrx.Placement
	if worktree {
		pl, err = c.H.WorktreeCreate(ctx, m, repo, branch, name)
	} else {
		pl, err = c.H.WorkspaceCreate(ctx, m, repo, name)
	}
	if err != nil {
		if worktree && emptyRepo(err) {
			return c.emptyRepository(ctx, job, w, isolation, err)
		}
		return fail("open workspace", err, "")
	}
	if pl.Branch != "" && pl.Branch != branch {
		_ = c.L.SetWorkerPath(ctx, job.ID, job.Token, repo, pl.Branch)
	}
	if err := c.L.SetWorkspace(ctx, job.ID, job.Token, pl.WorkspaceID, pl.PaneID); err != nil {
		return err
	}
	if err := c.L.RecordStartIntent(ctx, job.ID, job.Token); err != nil {
		return err
	}
	err = c.H.AgentStart(ctx, m, herdrx.StartRequest{Name: name, Kind: job.Kind, Pane: pl.PaneID, Timeout: c.Cfg.DispatchTimeout, Args: job.AgentArgs})
	if err != nil && herdrx.Code(err) != "timeout" {
		// The call failed, but the agent may still have started (a dropped
		// connection after Herdr acted). Look before deciding.
		agents, lerr := c.H.Agents(ctx, m)
		if lerr != nil {
			c.event(ctx, "cluster.dispatch.uncertain", job.ID, w.ID, map[string]any{"error": err.Error()})
			return fmt.Errorf("agent start on %s is uncertain; reconciliation will decide: %w", w.ID, err)
		}
		if !hasAgent(agents, name) {
			return fail("start agent", err, pl.WorkspaceID)
		}
	}
	if job.Prompt != "" {
		if err := c.L.RecordPromptIntent(ctx, job.ID, job.Token); err != nil {
			return err
		}
		if perr := c.H.AgentPrompt(ctx, m, pl.PaneID, job.Prompt); perr != nil {
			// The agent is running; it must not be started again elsewhere.
			c.event(ctx, "cluster.dispatch.prompt_failed", job.ID, w.ID, map[string]any{"error": perr.Error()})
		}
	}
	return c.L.MarkRunning(ctx, job.ID, job.Token, pl.PaneID)
}

// emptyRepository handles a worktree refused because the repository has no
// commits. With worktree isolation asked for explicitly the job cannot run
// as asked, so it fails with the reason. Under auto isolation it is
// requeued to run in the checkout itself once nothing else is using it.
func (c *Coordinator) emptyRepository(ctx context.Context, job ledger.Job, w placement.Worker, isolation string, cause error) error {
	msg := fmt.Sprintf("repository %s on %s has no commits, so Git cannot make a worktree from it", job.Repo, w.ID)
	_ = c.L.MarkNoWorktree(ctx, job.ID, msg)
	state, err := c.L.DispatchFailed(ctx, job.ID, job.Token, msg+": "+cause.Error())
	if err != nil {
		return err
	}
	if isolation == IsolationWorktree {
		if state == ledger.Queued {
			_ = c.L.FailQueued(ctx, job.ID, ledger.EndEmptyRepository,
				msg+". Make a first commit, or submit with isolation auto or none to run in the checkout itself")
		}
		return fmt.Errorf("%s; failed because worktree isolation was asked for", msg)
	}
	c.event(ctx, "cluster.dispatch.empty_repository", job.ID, w.ID, map[string]any{"now": state,
		"detail": "will run in the checkout itself once no other job is using it"})
	return fmt.Errorf("%s; requeued to run in the checkout itself", msg)
}

// discard closes a workspace the scheduler opened and never got an agent
// running in. A worktree's checkout is removed too, without force: an
// unexpected change in it is kept.
func (c *Coordinator) discard(ctx context.Context, m, workspace string, worktree bool) {
	if worktree {
		_ = c.H.WorktreeRemove(ctx, m, workspace, false)
	}
	// Removing a checkout may or may not close its workspace, depending on
	// the Herdr version; closing an already closed one is harmless.
	_ = c.H.WorkspaceClose(ctx, m, workspace)
}

func hasAgent(agents []herdrx.Agent, name string) bool {
	for _, a := range agents {
		if a.Name == name {
			return true
		}
	}
	return false
}
