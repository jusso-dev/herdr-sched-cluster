package sim

import (
	"strings"
	"testing"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/coord"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/placement"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

func (r *rig) events(kind string) []ledger.Event {
	r.t.Helper()
	evs, err := r.l.Events(ctx, 0, 0)
	if err != nil {
		r.t.Fatal(err)
	}
	var out []ledger.Event
	for _, e := range evs {
		if e.Kind == kind {
			out = append(out, e)
		}
	}
	return out
}

func pin(m string) func(*coord.SubmitRequest) {
	return func(s *coord.SubmitRequest) { s.Machine = m }
}

// Lost, not finished: Herdr restarts on the worker and the agent does not
// come back. The pane is there with a new shell, so the job is lost with the
// reason herdr_restarted, and it is not retried because it was not
// submitted as restartable.
func TestV05LostWhenHerdrRestarts(t *testing.T) {
	r := newRig(t, nil)
	a := r.submit(2048, pin("mini"))
	r.tick()
	r.tick() // records the pane's shell
	if j := r.job(a.ID); j.State != ledger.Running || j.ShellPID == 0 {
		t.Fatalf("%+v", j)
	}
	r.cl.RestartHerdr("mini")
	r.tick()
	j := r.job(a.ID)
	if j.State != ledger.Lost || j.EndReason != ledger.EndHerdrRestarted {
		t.Fatalf("want lost/herdr_restarted, got %s/%s", j.State, j.EndReason)
	}
	if len(r.events("cluster.job.retry_refused")) != 1 {
		t.Fatal("a job not submitted as restartable must not be retried, with the reason recorded")
	}
	if r.cl.Starts[ledger.AgentName(a.ID)] != 1 {
		t.Fatal("started again")
	}
	ex, err := r.c.Explain(ctx, a.ID)
	if err != nil || ex.Job.EndReason != ledger.EndHerdrRestarted {
		t.Fatal(ex, err)
	}
	st, err := r.c.Status(ctx)
	if err != nil || st.Ended[ledger.Lost][ledger.EndHerdrRestarted] != 1 || len(st.Recommendations) == 0 {
		t.Fatalf("%+v %v", st, err)
	}
}

// A finished agent leaves its pane and shell behind: that is finished, with
// the reason agent_exited, never lost.
func TestV05FinishedIsNotLost(t *testing.T) {
	r := newRig(t, nil)
	a := r.submit(2048, pin("mini"))
	r.tick()
	r.tick()
	r.cl.Exit("mini", ledger.AgentName(a.ID))
	r.tick()
	if j := r.job(a.ID); j.State != ledger.Finished || j.EndReason != ledger.EndAgentExited {
		t.Fatalf("%s/%s", j.State, j.EndReason)
	}
}

// A restartable job whose session vanished is retried, once, on a fresh
// worktree-safe attempt; the invariant that no agent name is started twice
// would catch a retry under the same name, so the retry goes through the
// queue as a new attempt of the same job.
func TestV05RestartableLostJobIsRetried(t *testing.T) {
	r := newRig(t, nil)
	a := r.submit(2048, pin("mini"), func(s *coord.SubmitRequest) { s.Restartable = true })
	r.tick()
	r.tick()
	r.cl.CloseSession("mini", ledger.AgentName(a.ID))
	// The sim counts starts per agent name; a retry reuses the name, so
	// forget the first start, which is gone for certain.
	r.cl.With(func() { r.cl.Starts[ledger.AgentName(a.ID)] = 0 })
	r.tick()
	j := r.job(a.ID)
	if j.State != ledger.Running || j.Attempts != 2 {
		t.Fatalf("want a second attempt running, got %s attempt %d", j.State, j.Attempts)
	}
	if len(r.events("cluster.job.retried")) != 1 {
		t.Fatal("no retried event")
	}
}

// A worker that stops answering: the running job is disconnected, then lost
// with worker_unreachable after cluster_lost_after_seconds, and never
// retried. When the worker comes back with the agent still running, the job
// is readopted and its reservation counted again.
func TestV05UnreachableThenLostThenReadopted(t *testing.T) {
	r := newRig(t, func(c *coord.Config) { c.LostAfter = 10 * time.Minute })
	a := r.submit(2048, pin("mini"), func(s *coord.SubmitRequest) { s.Restartable = true })
	r.tick()
	r.cl.With(func() { r.cl.Machines["mini"].Unreachable = true })
	r.tick()
	if j := r.job(a.ID); j.State != ledger.Running || !j.Disconnected {
		t.Fatalf("%+v", j)
	}
	r.cl.Clock.Advance(11 * time.Minute)
	r.tick()
	j := r.job(a.ID)
	if j.State != ledger.Lost || j.EndReason != ledger.EndWorkerUnreachable {
		t.Fatalf("%s/%s", j.State, j.EndReason)
	}
	if retried, _, _ := r.l.RetryLost(ctx, a.ID); retried {
		t.Fatal("a job whose worker is unreachable must never be retried")
	}
	r.cl.With(func() { r.cl.Machines["mini"].Unreachable = false })
	r.cl.Clock.Advance(time.Hour) // past any backoff
	r.tick()
	j = r.job(a.ID)
	if j.State != ledger.Running || j.Disconnected {
		t.Fatalf("not readopted: %s", j.State)
	}
	if ld, _ := r.l.LoadOf(ctx, "mini", false); ld.ActiveAgents != 1 {
		t.Fatalf("readopted job holds no reservation: %+v", ld)
	}
	if r.cl.Starts[ledger.AgentName(a.ID)] != 1 {
		t.Fatal("started twice")
	}
}

// The worker comes back and the agent is gone: the loss is confirmed and a
// restartable job is retried.
func TestV05UnreachableThenConfirmedLost(t *testing.T) {
	r := newRig(t, func(c *coord.Config) { c.LostAfter = time.Minute })
	a := r.submit(2048, pin("mini"), func(s *coord.SubmitRequest) { s.Restartable = true })
	r.tick()
	r.cl.With(func() { r.cl.Machines["mini"].Unreachable = true })
	r.tick()
	r.cl.Clock.Advance(2 * time.Minute)
	r.tick()
	r.cl.With(func() {
		r.cl.Machines["mini"].Unreachable = false
		delete(r.cl.Machines["mini"].Agents, ledger.AgentName(a.ID))
		r.cl.Starts[ledger.AgentName(a.ID)] = 0
	})
	r.cl.Clock.Advance(time.Hour)
	r.tick()
	if len(r.events("cluster.job.loss_confirmed")) != 1 {
		t.Fatal("loss not confirmed")
	}
	if j := r.job(a.ID); j.State != ledger.Running || j.Attempts != 2 {
		t.Fatalf("%s attempt %d", j.State, j.Attempts)
	}
}

// worktreeJob runs one job in a worktree on mini and ends it.
func worktreeJob(t *testing.T, r *rig, dirty bool, end func(name string)) ledger.Job {
	t.Helper()
	a := r.submit(2048, pin("mini"), func(s *coord.SubmitRequest) { s.Isolation = coord.IsolationWorktree })
	r.tick()
	j := r.job(a.ID)
	if !j.Worktree || j.Branch != "hsc/"+a.ID {
		t.Fatalf("%+v", j)
	}
	if dirty {
		r.cl.Dirty("mini", j.WorkspaceID)
	}
	end(ledger.AgentName(a.ID))
	r.tick()
	return r.job(a.ID)
}

func TestV05WorktreeRetention(t *testing.T) {
	exit := func(r *rig) func(string) { return func(n string) { r.cl.Exit("mini", n) } }
	restart := func(r *rig) func(string) { return func(string) { r.cl.RestartHerdr("mini") } }
	t.Run("retain-on-failure removes after a normal finish", func(t *testing.T) {
		r := newRig(t, nil)
		r.tick()
		j := worktreeJob(t, r, false, nil2(r, exit))
		if j.Cleanup != ledger.CleanupRemoved || r.cl.Machines["mini"].Worktrees != 0 {
			t.Fatalf("%s %q worktrees %d", j.Cleanup, j.CleanupNote, r.cl.Machines["mini"].Worktrees)
		}
	})
	t.Run("retain-on-failure keeps a lost job's worktree", func(t *testing.T) {
		r := newRig(t, nil)
		a := r.submit(2048, pin("mini"), func(s *coord.SubmitRequest) { s.Isolation = coord.IsolationWorktree })
		r.tick()
		r.tick()
		restart(r)(ledger.AgentName(a.ID))
		r.tick()
		j := r.job(a.ID)
		if j.State != ledger.Lost || j.Cleanup != ledger.CleanupKept || !strings.Contains(j.CleanupNote, "inspection") {
			t.Fatalf("%s %s %q", j.State, j.Cleanup, j.CleanupNote)
		}
		if r.cl.Machines["mini"].Worktrees != 1 {
			t.Fatal("removed")
		}
	})
	t.Run("always-retain", func(t *testing.T) {
		r := newRig(t, func(c *coord.Config) { c.WorktreeRetention = coord.AlwaysRetain })
		j := worktreeJob(t, r, false, nil2(r, exit))
		if j.Cleanup != ledger.CleanupKept || r.cl.Machines["mini"].Worktrees != 1 {
			t.Fatal(j.Cleanup)
		}
	})
	t.Run("always-remove never removes a dirty worktree without the flag", func(t *testing.T) {
		r := newRig(t, func(c *coord.Config) { c.WorktreeRetention = coord.AlwaysRemove })
		j := worktreeJob(t, r, true, nil2(r, exit))
		if j.Cleanup != ledger.CleanupKept || !strings.Contains(j.CleanupNote, "uncommitted") || r.cl.Machines["mini"].Worktrees != 1 {
			t.Fatalf("%s %q", j.Cleanup, j.CleanupNote)
		}
	})
	t.Run("always-remove with remove_dirty and branch deletion", func(t *testing.T) {
		r := newRig(t, func(c *coord.Config) {
			c.WorktreeRetention, c.WorktreeRemoveDirty, c.DeleteBranches = coord.AlwaysRemove, true, true
		})
		j := worktreeJob(t, r, true, nil2(r, exit))
		if j.Cleanup != ledger.CleanupRemoved || r.cl.Machines["mini"].Worktrees != 0 {
			t.Fatalf("%s %q", j.Cleanup, j.CleanupNote)
		}
		if len(r.cl.GitLog) != 1 || r.cl.GitLog[0] != "mini branch-delete hsc/"+j.ID || !strings.Contains(j.CleanupNote, "deleted") {
			t.Fatalf("%v %q", r.cl.GitLog, j.CleanupNote)
		}
	})
	t.Run("age removes only after the age", func(t *testing.T) {
		r := newRig(t, func(c *coord.Config) { c.WorktreeRetention, c.WorktreeMaxAge = coord.RetainForAge, 2*time.Hour })
		j := worktreeJob(t, r, false, nil2(r, exit))
		if j.Cleanup != ledger.CleanupPending {
			t.Fatal(j.Cleanup)
		}
		r.cl.Clock.Advance(3 * time.Hour)
		r.tick()
		if j = r.job(j.ID); j.Cleanup != ledger.CleanupRemoved {
			t.Fatal(j.Cleanup)
		}
	})
	t.Run("never while the worker is away", func(t *testing.T) {
		r := newRig(t, func(c *coord.Config) { c.WorktreeRetention = coord.AlwaysRemove })
		j := worktreeJob(t, r, false, func(name string) { r.cl.Exit("mini", name) })
		_ = j
		// Put a second ended job's cleanup behind an unreachable worker.
		k := worktreeJob(t, r, false, func(name string) {
			r.cl.Exit("mini", name)
		})
		_ = k
		b := r.submit(2048, pin("mini"), func(s *coord.SubmitRequest) { s.Isolation = coord.IsolationWorktree })
		r.tick()
		r.cl.Exit("mini", ledger.AgentName(b.ID))
		r.cl.With(func() { r.cl.Machines["mini"].Unreachable = true })
		r.tick()
		if got := r.job(b.ID); got.Cleanup == ledger.CleanupRemoved {
			t.Fatal("removed while the worker was unreachable")
		}
	})
}

// nil2 adapts the helper closures above.
func nil2(r *rig, f func(*rig) func(string)) func(string) { return f(r) }

// A coordinator that dies after opening the workspace but before recording
// the agent start: nothing can have run, so the orphan workspace is closed
// and the job requeued, then dispatched once.
func TestV05CrashMidDispatchIsRetriedSafely(t *testing.T) {
	r := newRig(t, func(c *coord.Config) { c.DispatchGrace = time.Minute })
	a := r.submit(2048, pin("mini"))
	// Simulate the crash: reserve and open the workspace by hand, the way
	// dispatch does, then stop.
	tok, err := r.l.Reserve(ctx, a.ID, "mini", false, func(ledger.Load) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := r.l.MarkDispatching(ctx, a.ID, tok, false); err != nil {
		t.Fatal(err)
	}
	if _, err := r.cl.Herdr().WorkspaceCreate(ctx, "mini", repo, ledger.AgentName(a.ID)); err != nil {
		t.Fatal(err)
	}
	// The workspace id was never saved.
	r.open()
	r.cl.Clock.Advance(2 * time.Minute)
	r.tick()
	j := r.job(a.ID)
	if j.State != ledger.Running || j.Attempts != 2 {
		t.Fatalf("%s attempt %d", j.State, j.Attempts)
	}
	if len(r.cl.Machines["mini"].Workspaces) != 1 {
		t.Fatalf("orphan workspace left: %v", r.cl.Machines["mini"].Workspaces)
	}
	if len(r.events("cluster.reconcile.dispatch_retried")) != 1 {
		t.Fatal("no retry event")
	}
}

// After the agent start was attempted, a crash is never retried: the agent
// may have run.
func TestV05CrashAfterStartIntentIsNotRetried(t *testing.T) {
	r := newRig(t, func(c *coord.Config) { c.DispatchGrace = time.Minute })
	a := r.submit(2048, pin("mini"))
	tok, _ := r.l.Reserve(ctx, a.ID, "mini", false, func(ledger.Load) error { return nil })
	_ = r.l.MarkDispatching(ctx, a.ID, tok, false)
	pl, _ := r.cl.Herdr().WorkspaceCreate(ctx, "mini", repo, ledger.AgentName(a.ID))
	_ = r.l.SetWorkspace(ctx, a.ID, tok, pl.WorkspaceID, pl.PaneID)
	if err := r.l.RecordStartIntent(ctx, a.ID, tok); err != nil {
		t.Fatal(err)
	}
	r.open()
	r.cl.Clock.Advance(2 * time.Minute)
	r.tick()
	if j := r.job(a.ID); j.State != ledger.Failed || j.EndReason != ledger.EndInterrupted {
		t.Fatalf("%s/%s", j.State, j.EndReason)
	}
}

// An empty repository cannot be branched. Under auto isolation the job runs
// in the checkout itself once nothing else uses it; with worktree isolation
// asked for, it fails with a clear reason.
func TestV05EmptyRepository(t *testing.T) {
	r := newRig(t, nil)
	r.cl.With(func() { r.cl.Machines["mini"].EmptyRepos[repo] = true })
	first := r.submit(2048, pin("mini"))
	r.tick()
	second := r.submit(2048, pin("mini")) // auto: the repo is busy, so a worktree is tried
	r.tick()
	j := r.job(second.ID)
	if j.State != ledger.Queued || !j.NoWorktree {
		t.Fatalf("%s no_worktree=%v", j.State, j.NoWorktree)
	}
	r.tick()
	if r.job(second.ID).State != ledger.Queued {
		t.Fatal("ran beside another job in an unbranchable checkout")
	}
	r.cl.Exit("mini", ledger.AgentName(first.ID))
	r.tick()
	if j := r.job(second.ID); j.State != ledger.Running || j.Worktree {
		t.Fatalf("%s worktree=%v", j.State, j.Worktree)
	}
	third := r.submit(2048, pin("mini"), func(s *coord.SubmitRequest) { s.Isolation = coord.IsolationWorktree })
	r.tick()
	if j := r.job(third.ID); j.State != ledger.Failed || j.EndReason != ledger.EndEmptyRepository || !strings.Contains(j.LastError, "no commits") {
		t.Fatalf("%s/%s %q", j.State, j.EndReason, j.LastError)
	}
}

// A worker that keeps checkouts somewhere else: the repository is looked
// for and opened at the mapped path.
func TestV05PathMapping(t *testing.T) {
	r := newRig(t, func(c *coord.Config) {
		c.Workers = map[string]coord.WorkerConfig{"mini": {PathMap: map[string]string{"/Users/sim/src": "/Volumes/work/src"}}}
	})
	r.cl.With(func() {
		r.cl.Machines["mini"].MissingPaths[repo] = true // not at the logical path
	})
	a := r.submit(2048, pin("mini"))
	r.tick()
	j := r.job(a.ID)
	if j.State != ledger.Running || j.WorkerPath != "/Volumes/work/src/app" {
		t.Fatalf("%s %q %s", j.State, j.WorkerPath, j.LastError)
	}
	if got := coord.MapPath(map[string]string{"/a": "/x", "/a/b": "/y"}, "/a/b/c"); got != "/y/c" {
		t.Fatal(got)
	}
	if got := coord.MapPath(map[string]string{"/a": "/x"}, "/ab"); got != "/ab" {
		t.Fatal("matched part of a path segment:", got)
	}
}

// Herdr's own `machine status` takes part in placement.
func TestV05MachineStatusHealth(t *testing.T) {
	r := newRig(t, nil)
	r.cl.With(func() { r.cl.Machines["mini"].HerdrStatus = "error" })
	ws, err := r.c.Workers(ctx, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range ws {
		if w.ID == "mini" && w.State != placement.StateHerdrDown {
			t.Fatal(w.State)
		}
	}
	a := r.submit(2048)
	r.tick()
	if j := r.job(a.ID); j.Worker != "mbp" {
		t.Fatal(j.Worker)
	}
}

// Dependencies: a job waits for the jobs it depends on and fails if one of
// them does not finish.
func TestV05Dependencies(t *testing.T) {
	r := newRig(t, nil)
	a := r.submit(2048, pin("mini"))
	b := r.submit(2048, func(s *coord.SubmitRequest) { s.DependsOn = []string{a.ID} })
	r.tick()
	if r.job(b.ID).State != ledger.Queued {
		t.Fatal("ran before its dependency finished")
	}
	r.cl.Exit("mini", ledger.AgentName(a.ID))
	r.tick()
	if r.job(b.ID).State != ledger.Running {
		t.Fatal("did not run after its dependency finished")
	}
	c := r.submit(2048, pin("mini"))
	d := r.submit(2048, func(s *coord.SubmitRequest) { s.DependsOn = []string{c.ID} })
	if _, err := r.l.Cancel(ctx, c.ID, "test"); err != nil {
		t.Fatal(err)
	}
	r.tick()
	if j := r.job(d.ID); j.State != ledger.Failed || j.EndReason != ledger.EndDependencyFailed {
		t.Fatalf("%s/%s", j.State, j.EndReason)
	}
	if _, _, err := r.c.Submit(ctx, coord.SubmitRequest{Kind: "claude", Repo: repo, DependsOn: []string{"j999"}}); err == nil {
		t.Fatal("accepted an unknown dependency")
	}
}

// Critical pressure for longer than the grace closes the largest disposable
// job, and only a disposable one.
func TestV05PressureTerminatesDisposable(t *testing.T) {
	r := newRig(t, func(c *coord.Config) {
		c.PressureAction, c.PressureGrace = coord.PressureTerminateDisposable, time.Minute
	})
	keep := r.submit(2048, pin("mini"))
	drop := r.submit(2048, pin("mini"), func(s *coord.SubmitRequest) { s.Disposable = true })
	r.tick()
	r.cl.With(func() { r.cl.Machines["mini"].Pressure = resource.PressureCritical })
	r.tick()
	if r.job(drop.ID).State != ledger.Running {
		t.Fatal("terminated before the grace")
	}
	if len(r.events("cluster.worker.pressure")) == 0 {
		t.Fatal("pressure change not recorded")
	}
	r.cl.Clock.Advance(2 * time.Minute)
	r.cl.With(func() { r.cl.AllowKill = true })
	r.tick()
	if j := r.job(drop.ID); j.State != ledger.Failed || j.EndReason != ledger.EndTerminated {
		t.Fatalf("%s/%s", j.State, j.EndReason)
	}
	if len(r.cl.Killed) != 1 || r.cl.Killed[0] != ledger.AgentName(drop.ID) {
		t.Fatal(r.cl.Killed)
	}
	if j := r.job(keep.ID); j.State != ledger.Running {
		t.Fatal("a job not marked disposable was touched")
	}
	r.cl.Clock.Advance(2 * time.Minute)
	r.tick()
	if j := r.job(keep.ID); j.State != ledger.Running {
		t.Fatal("a job not marked disposable was touched")
	}
}

// CPU is learned per kind and repository from process-tree CPU, then used
// for the estimate, and placement keeps projected CPU under the threshold.
func TestV05CPULearnedAndEnforced(t *testing.T) {
	r := newRig(t, nil)
	for i := 0; i < 3; i++ {
		a := r.submit(2048, pin("mini"))
		r.tick()
		r.tick()
		r.cl.Exit("mini", ledger.AgentName(a.ID))
		r.tick()
	}
	p, err := r.l.Profile(ctx, "claude", repo, 20, 90)
	if err != nil || p.CPURuns != 3 || p.CPU != 0.5 {
		t.Fatalf("%+v %v", p, err)
	}
	j, notes, err := r.c.Submit(ctx, coord.SubmitRequest{Kind: "claude", Repo: repo, MemoryMB: 1024})
	if err != nil || j.CPUSource == "requested" || j.CPU <= 0 || len(notes) == 0 {
		t.Fatalf("%+v %v %v", j, notes, err)
	}
	// mini has 10 cores; something else is using 80% of it, so a job
	// asking for 2 cores would take it past 85%.
	r.cl.With(func() { r.cl.Machines["mini"].UtilPct = 80 })
	b := r.submit(1024, pin("mini"), func(s *coord.SubmitRequest) { s.CPU = 2 })
	r.tick()
	ex, err := r.c.Explain(ctx, b.ID)
	if err != nil || ex.Job.State != ledger.Queued {
		t.Fatalf("%s %v", ex.Job.State, err)
	}
	found := false
	for _, c := range ex.Now.Candidates {
		for _, why := range c.Reasons {
			found = found || strings.Contains(why, "cpu: utilisation exceeds policy threshold")
		}
	}
	if !found {
		t.Fatalf("%+v", ex.Now)
	}
	plan, err := r.c.Plan(ctx, []coord.SubmitRequest{{Kind: "claude", Repo: repo, MemoryMB: 1024, CPU: 1, Machine: "mbp"}})
	if err != nil || plan.Jobs[0].CPUAfterPct <= 0 || plan.Jobs[0].MaxCPUPct != 85 {
		t.Fatalf("%+v %v", plan.Jobs, err)
	}
}

// A worker whose identity changes is refused until re-registered.
func TestV05IdentityChange(t *testing.T) {
	r := newRig(t, nil)
	r.tick()
	r.cl.With(func() { r.cl.Machines["mini"].Arch = "amd64" })
	ws, _ := r.c.Workers(ctx, true, nil)
	for _, w := range ws {
		if w.ID == "mini" && w.State != placement.StateIncompatible {
			t.Fatal(w.State)
		}
	}
	if _, err := r.c.Reregister(ctx, "mini", "test"); err != nil {
		t.Fatal(err)
	}
	ws, _ = r.c.Workers(ctx, true, nil)
	for _, w := range ws {
		if w.ID == "mini" && w.State != placement.StateReady {
			t.Fatal(w.State)
		}
	}
}

// A clone URL fetches a repository a worker does not have.
func TestV05CloneWhenMissing(t *testing.T) {
	r := newRig(t, nil)
	r.cl.With(func() { r.cl.Machines["mini"].MissingPaths[repo] = true })
	a := r.submit(2048, pin("mini"), func(s *coord.SubmitRequest) { s.CloneURL = "git@github.com:jusso-dev/app.git" })
	r.tick()
	if j := r.job(a.ID); j.State != ledger.Running {
		t.Fatalf("%s %s", j.State, j.LastError)
	}
	if len(r.cl.GitLog) != 1 || !strings.HasPrefix(r.cl.GitLog[0], "mini clone ") {
		t.Fatal(r.cl.GitLog)
	}
}

// The queue size cap refuses a submit beyond it.
func TestV05QueueCap(t *testing.T) {
	r := newRig(t, func(c *coord.Config) { c.MaxQueueSize = 2 })
	r.submit(2048)
	r.submit(2048)
	if _, _, err := r.c.Submit(ctx, coord.SubmitRequest{Kind: "claude", Repo: repo, MemoryMB: 2048, CPU: 1}); err == nil ||
		!strings.Contains(err.Error(), "queue is full") {
		t.Fatal(err)
	}
}

// A restored pane with something running in it may be the agent resuming
// its own session, so even a restartable job is not retried.
func TestV05RestoredPaneWithForegroundIsNotRetried(t *testing.T) {
	r := newRig(t, nil)
	a := r.submit(2048, pin("mini"), func(s *coord.SubmitRequest) { s.Restartable = true })
	r.tick()
	r.tick()
	r.cl.With(func() { r.cl.Machines["mini"].ResumeOnRestart = true })
	r.cl.RestartHerdr("mini")
	r.tick()
	if j := r.job(a.ID); j.State != ledger.Lost || j.EndReason != ledger.EndHerdrRestarted {
		t.Fatalf("%s/%s", j.State, j.EndReason)
	}
	ev := r.events("cluster.job.retry_refused")
	if len(ev) != 1 || !strings.Contains(string(ev[0].Detail), "foreground") {
		t.Fatal(ev)
	}
}

// A restartable job lost to a Herdr restart with an idle pane is retried.
func TestV05RestartableHerdrRestartIsRetried(t *testing.T) {
	r := newRig(t, nil)
	a := r.submit(2048, pin("mini"), func(s *coord.SubmitRequest) { s.Restartable = true })
	r.tick()
	r.tick()
	r.cl.RestartHerdr("mini")
	r.cl.With(func() { r.cl.Starts[ledger.AgentName(a.ID)] = 0 })
	r.tick()
	if j := r.job(a.ID); j.State != ledger.Running || j.Attempts != 2 {
		t.Fatalf("%s attempt %d", j.State, j.Attempts)
	}
}
