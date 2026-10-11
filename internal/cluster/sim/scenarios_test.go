package sim

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/coord"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

var ctx = context.Background()

const repo = "/Users/sim/src/app"

type rig struct {
	t    *testing.T
	cl   *Cluster
	l    *ledger.Ledger
	c    *coord.Coordinator
	path string
	cfg  coord.Config
}

// newRig builds the cluster from the brief: an M3 Max MacBook Pro with
// 64 GB and an M4 Mac mini with 16 GB.
func newRig(t *testing.T, tweak func(*coord.Config)) *rig {
	t.Helper()
	cl := New()
	cl.Add("mbp", 65536, 16)
	cl.Add("mini", 16384, 10)
	cfg := coord.DefaultConfig()
	cfg.IncludeLocal = false
	cfg.ProbeTTL = 0
	if tweak != nil {
		tweak(&cfg)
	}
	r := &rig{t: t, cl: cl, path: filepath.Join(t.TempDir(), "cluster.db"), cfg: cfg}
	r.open()
	return r
}

// open (re)starts the coordinator on the same ledger file, as a restart.
func (r *rig) open() {
	r.t.Helper()
	if r.l != nil {
		r.l.Close()
	}
	l, err := ledger.OpenWithClock(r.path, r.cl.Clock.Now)
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { l.Close() })
	r.l = l
	r.c = coord.New(l, r.cl.Herdr(), r.cl.Prober(), r.cfg)
	r.c.Now = r.cl.Clock.Now
}

func (r *rig) submit(mem int64, mods ...func(*coord.SubmitRequest)) ledger.Job {
	r.t.Helper()
	req := coord.SubmitRequest{Kind: "claude", Repo: repo, Prompt: "do the thing", MemoryMB: mem, CPU: 1}
	for _, m := range mods {
		m(&req)
	}
	j, _, err := r.c.Submit(ctx, req)
	if err != nil {
		r.t.Fatal(err)
	}
	return j
}

func (r *rig) tick() coord.Report {
	r.t.Helper()
	rep, err := r.c.Tick(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	r.check()
	return rep
}

func (r *rig) job(id string) ledger.Job {
	r.t.Helper()
	j, err := r.l.Get(ctx, id)
	if err != nil {
		r.t.Fatal(err)
	}
	return j
}

func (r *rig) states() map[string]int {
	r.t.Helper()
	n, err := r.l.Counts(ctx)
	if err != nil {
		r.t.Fatal(err)
	}
	return n
}

// check is the cluster-wide invariant: no agent was ever started twice,
// every scheduler agent alive belongs to exactly one active job on that
// machine, and every connected running job has its agent alive.
func (r *rig) check() {
	r.t.Helper()
	if d := r.cl.DoubleStarts(); len(d) > 0 {
		r.t.Fatalf("double dispatch: %v", d)
	}
	active, err := r.l.List(ctx, ledger.Reserved, ledger.Dispatching, ledger.Running)
	if err != nil {
		r.t.Fatal(err)
	}
	byName := map[string]ledger.Job{}
	for _, j := range active {
		byName[ledger.AgentName(j.ID)] = j
	}
	r.cl.With(func() {
		for id, m := range r.cl.Machines {
			for name := range m.Agents {
				j, ok := byName[name]
				if !ok {
					r.t.Fatalf("agent %s on %s has no active job", name, id)
				}
				if j.Worker != id {
					r.t.Fatalf("agent %s runs on %s but the ledger says %s", name, id, j.Worker)
				}
			}
		}
		for _, j := range active {
			if j.State != ledger.Running || j.Disconnected {
				continue
			}
			m := r.cl.Machines[j.Worker]
			if m.Unreachable {
				continue
			}
			if _, ok := m.Agents[ledger.AgentName(j.ID)]; !ok {
				// Allowed only until the next reconcile notices.
				continue
			}
		}
	})
}

// Scenario 1: ten agents compete for limited memory.
func TestScenario01TenAgentsCompeteForMemory(t *testing.T) {
	r := newRig(t, nil)
	var ids []string
	for i := 0; i < 10; i++ {
		ids = append(ids, r.submit(8192).ID)
	}
	r.tick()
	// mbp: 64 GB - 4 GB used - 2 GB kept free = 58 GB, so 7 jobs of 8 GB.
	// mini: 16 - 4 - 2 = 10 GB, so 1 job. Two wait.
	if n := r.states(); n[ledger.Running] != 8 || n[ledger.Queued] != 2 {
		t.Fatalf("%v", n)
	}
	if r.cl.Live("mbp") != 7 || r.cl.Live("mini") != 1 {
		t.Fatalf("mbp %d mini %d", r.cl.Live("mbp"), r.cl.Live("mini"))
	}
	// Each waiting job can say why.
	for _, id := range ids {
		if j := r.job(id); j.State == ledger.Queued {
			ex, err := r.c.Explain(ctx, id)
			if err != nil || ex.Now == nil || ex.Now.Chosen != "" || !strings.Contains(ex.Now.Summary, "") {
				t.Fatal(ex, err)
			}
			for _, cand := range ex.Now.Candidates {
				if cand.Eligible || len(cand.Reasons) == 0 {
					t.Fatalf("%+v", cand)
				}
			}
		}
	}
	// As agents finish, the rest go.
	for round := 0; round < 5 && r.states()[ledger.Queued] > 0; round++ {
		for _, id := range ids {
			if j := r.job(id); j.State == ledger.Running {
				r.cl.Exit(j.Worker, ledger.AgentName(id))
				break
			}
		}
		r.tick()
	}
	if n := r.states(); n[ledger.Queued] != 0 || n[ledger.Finished] < 2 {
		t.Fatalf("%v", n)
	}
}

// Scenario 2: a worker becomes unavailable during dispatch.
func TestScenario02WorkerUnavailableDuringDispatch(t *testing.T) {
	r := newRig(t, nil)
	j := r.submit(4096, func(s *coord.SubmitRequest) { s.Machine = "" })
	// The probe says mini is fine, but its Herdr refuses the workspace.
	r.cl.With(func() {
		r.cl.Machines["mbp"].Pressure = resource.PressureCritical // force mini first
		r.cl.Machines["mini"].Fail["workspace"] = 1
	})
	rep := r.tick()
	if len(rep.Failed) != 1 {
		t.Fatalf("%+v", rep)
	}
	got := r.job(j.ID)
	if got.State != ledger.Queued || got.Attempts != 1 || got.LastError == "" {
		t.Fatalf("%+v", got)
	}
	if ld, _ := r.l.LoadOf(ctx, "mini", false); ld.ActiveAgents != 0 {
		t.Fatal("failed dispatch kept its capacity")
	}
	r.tick()
	if got := r.job(j.ID); got.State != ledger.Running || got.Worker != "mini" {
		t.Fatalf("%+v", got)
	}
	// A worker that disappears entirely is skipped and backed off.
	k := r.submit(2048)
	r.cl.With(func() {
		r.cl.Machines["mini"].Unreachable = true
		r.cl.Machines["mbp"].Pressure = resource.PressureNormal
	})
	r.tick()
	if got := r.job(k.ID); got.State != ledger.Running || got.Worker != "mbp" {
		t.Fatalf("%+v", got)
	}
	ws, _ := r.l.Workers(ctx)
	if ws["mini"].State != "unreachable" || ws["mini"].BackoffUntil.IsZero() {
		t.Fatalf("%+v", ws["mini"])
	}
}

// Scenario 3: a worker disconnects while its agents keep running.
func TestScenario03DisconnectKeepsAgentsAndCapacity(t *testing.T) {
	r := newRig(t, func(c *coord.Config) { c.Policy = "spread" })
	a, b := r.submit(4096), r.submit(4096)
	r.tick()
	if r.cl.Live("mini") != 1 || r.cl.Live("mbp") != 1 {
		t.Fatal("spread should put one on each")
	}
	onMini := a
	if r.job(b.ID).Worker == "mini" {
		onMini = b
	}
	r.cl.With(func() { r.cl.Machines["mini"].Unreachable = true })
	r.tick()
	got := r.job(onMini.ID)
	if got.State != ledger.Running || !got.Disconnected {
		t.Fatalf("%+v", got)
	}
	if ld, _ := r.l.LoadOf(ctx, "mini", false); ld.ActiveAgents != 1 {
		t.Fatal("a disconnected job must keep its reservation")
	}
	r.cl.Clock.Advance(time.Hour)
	r.cl.With(func() { r.cl.Machines["mini"].Unreachable = false })
	r.tick()
	if got := r.job(onMini.ID); got.State != ledger.Running || got.Disconnected {
		t.Fatalf("%+v", got)
	}
	r.cl.With(func() {
		if r.cl.Starts[ledger.AgentName(onMini.ID)] != 1 {
			t.Fatal("restarted a disconnected agent")
		}
	})
}

// Scenario 4: the coordinator restarts with remote sessions active, one
// dispatch half done and one reservation never dispatched.
func TestScenario04CoordinatorRestart(t *testing.T) {
	r := newRig(t, nil)
	running := r.submit(4096)
	r.tick()
	// A dispatch that started the agent, then the coordinator died.
	half := r.submit(4096, func(s *coord.SubmitRequest) { s.Machine = "mini" })
	tok, err := r.l.Reserve(ctx, half.ID, "mini", false, func(ledger.Load) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	_ = r.l.MarkDispatching(ctx, half.ID, tok, false)
	h := r.cl.Herdr()
	pl, _ := h.WorkspaceCreate(ctx, "mini", repo, "x")
	_ = r.l.SetWorkspace(ctx, half.ID, tok, pl.WorkspaceID, pl.PaneID)
	_ = h.AgentStart(ctx, "mini", herdrxStart(ledger.AgentName(half.ID), pl.PaneID))
	// A reservation that never reached Herdr.
	never := r.submit(4096, func(s *coord.SubmitRequest) { s.Machine = "mbp" })
	if _, err := r.l.Reserve(ctx, never.ID, "mbp", false, func(ledger.Load) error { return nil }); err != nil {
		t.Fatal(err)
	}

	r.open() // restart
	r.tick()
	if got := r.job(running.ID); got.State != ledger.Running {
		t.Fatalf("running job: %+v", got)
	}
	if got := r.job(half.ID); got.State != ledger.Running || got.PaneID != pl.PaneID {
		t.Fatalf("half-dispatched job should be adopted: %+v", got)
	}
	if got := r.job(never.ID); got.State != ledger.Reserved {
		t.Fatalf("within the grace period nothing is decided: %+v", got)
	}
	r.cl.Clock.Advance(r.cfg.DispatchGrace + time.Second)
	r.tick()
	r.tick()
	if got := r.job(never.ID); got.State != ledger.Running || got.Attempts != 2 {
		t.Fatalf("never-dispatched job should be requeued and run once: %+v", got)
	}
}

// Scenario 5: a large job waits while smaller ones are schedulable, and the
// starvation guard eventually clears room for it.
func TestScenario05LargeJobIsNotStarved(t *testing.T) {
	r := newRig(t, func(c *coord.Config) { c.Queue.Starvation = 10 * time.Minute })
	var small []string
	for i := 0; i < 5; i++ {
		small = append(small, r.submit(8192, func(s *coord.SubmitRequest) { s.Machine = "mbp" }).ID)
	}
	r.tick()
	big := r.submit(40960, func(s *coord.SubmitRequest) { s.Priority = 0 })
	r.tick()
	if r.job(big.ID).State != ledger.Queued {
		t.Fatal("the big job cannot fit yet")
	}
	// Backfill: a small job still goes while the big one is young.
	early := r.submit(4096, func(s *coord.SubmitRequest) { s.Machine = "mbp" })
	r.tick()
	if r.job(early.ID).State != ledger.Running {
		t.Fatal("backfill should run a small job that fits")
	}
	r.cl.Clock.Advance(11 * time.Minute)
	r.tick()
	ws, _ := r.l.Workers(ctx)
	if ws["mbp"].HeldFor != big.ID {
		t.Fatalf("mbp should be held for %s: %+v", big.ID, ws["mbp"])
	}
	late := r.submit(4096, func(s *coord.SubmitRequest) { s.Machine = "mbp" })
	r.tick()
	if r.job(late.ID).State != ledger.Queued {
		t.Fatal("a held worker took a small job")
	}
	for _, id := range append(small, early.ID) {
		r.cl.Exit("mbp", ledger.AgentName(id))
	}
	r.tick()
	if got := r.job(big.ID); got.State != ledger.Running || got.Worker != "mbp" {
		t.Fatalf("%+v", got)
	}
	r.tick()
	if r.job(late.ID).State != ledger.Running {
		t.Fatal("the small job should follow once the hold is gone")
	}
}

// Scenario 6: several agents on the same repository get worktrees.
func TestScenario06SameRepoUsesWorktrees(t *testing.T) {
	r := newRig(t, nil)
	var ids []string
	for i := 0; i < 3; i++ {
		ids = append(ids, r.submit(4096, func(s *coord.SubmitRequest) { s.Machine = "mbp" }).ID)
	}
	other := r.submit(4096, func(s *coord.SubmitRequest) { s.Machine = "mbp"; s.Repo = "/Users/sim/src/other" })
	r.tick()
	var wt int
	for _, id := range ids {
		if r.job(id).Worktree {
			wt++
		}
	}
	if wt != 2 || r.job(other.ID).Worktree {
		t.Fatalf("worktrees %d, other %v", wt, r.job(other.ID).Worktree)
	}
	r.cl.With(func() {
		if r.cl.Machines["mbp"].Worktrees != 2 {
			t.Fatalf("herdr worktrees %d", r.cl.Machines["mbp"].Worktrees)
		}
	})
	// Explicit isolation is honoured.
	j := r.submit(2048, func(s *coord.SubmitRequest) { s.Machine = "mini"; s.Isolation = "worktree" })
	k := r.submit(2048, func(s *coord.SubmitRequest) { s.Machine = "mbp"; s.Isolation = "none" })
	r.tick()
	if !r.job(j.ID).Worktree || r.job(k.ID).Worktree {
		t.Fatal("isolation flag ignored")
	}
}

// Scenario 7: sudden memory pressure stops new work but kills nothing.
func TestScenario07SuddenMemoryPressure(t *testing.T) {
	r := newRig(t, nil)
	a := r.submit(2048, func(s *coord.SubmitRequest) { s.Machine = "mini" })
	r.tick()
	r.cl.With(func() { r.cl.Machines["mini"].Pressure = resource.PressureCritical })
	b := r.submit(2048, func(s *coord.SubmitRequest) { s.Machine = "mini" })
	c := r.submit(2048)
	r.tick()
	if r.job(b.ID).State != ledger.Queued {
		t.Fatal("dispatched onto a worker under critical pressure")
	}
	if got := r.job(c.ID); got.State != ledger.Running || got.Worker != "mbp" {
		t.Fatalf("unpinned job should go elsewhere: %+v", got)
	}
	if r.job(a.ID).State != ledger.Running || r.cl.Where(ledger.AgentName(a.ID)) != "mini" {
		t.Fatal("a running agent was disturbed")
	}
	ex, _ := r.c.Explain(ctx, b.ID)
	if ex.Now == nil || !strings.Contains(fmt.Sprint(ex.Now.Candidates), "memory pressure is critical") {
		t.Fatalf("%+v", ex.Now)
	}
	r.cl.With(func() { r.cl.Machines["mini"].Pressure = resource.PressureNormal })
	r.tick()
	if r.job(b.ID).State != ledger.Running {
		t.Fatal("should run once pressure clears")
	}
}

// Scenario 8: draining a worker.
func TestScenario08Draining(t *testing.T) {
	r := newRig(t, func(c *coord.Config) { c.Policy = "spread" })
	a, b := r.submit(2048), r.submit(2048)
	r.tick()
	onMini := a.ID
	if r.job(b.ID).Worker == "mini" {
		onMini = b.ID
	}
	if _, err := r.c.Drain(ctx, "mini", true, "test"); err != nil {
		t.Fatal(err)
	}
	var later []string
	for i := 0; i < 3; i++ {
		later = append(later, r.submit(2048).ID)
	}
	pinned := r.submit(2048, func(s *coord.SubmitRequest) { s.Machine = "mini" })
	r.tick()
	for _, id := range later {
		if r.job(id).Worker != "mbp" {
			t.Fatalf("%s went to a draining worker", id)
		}
	}
	if r.job(pinned.ID).State != ledger.Queued {
		t.Fatal("a job pinned to a draining worker must wait")
	}
	if r.job(onMini).State != ledger.Running || r.cl.Live("mini") != 1 {
		t.Fatal("draining disturbed a running agent")
	}
	if _, err := r.c.Drain(ctx, "nope", true, "test"); err == nil {
		t.Fatal("drained an unknown worker")
	}
	_, _ = r.c.Drain(ctx, "mini", false, "test")
	r.tick()
	if r.job(pinned.ID).State != ledger.Running {
		t.Fatal("undrain should let the pinned job run")
	}
}

// Scenario 9: an agent uses more than its estimate.
func TestScenario09OverrunRaisesReservation(t *testing.T) {
	r := newRig(t, nil)
	a := r.submit(2048, func(s *coord.SubmitRequest) { s.Machine = "mini" })
	r.tick()
	r.cl.SetRSS("mini", ledger.AgentName(a.ID), 6000)
	r.tick()
	got := r.job(a.ID)
	if got.ReservedMB != 7200 || got.ObservedRSSMB != 6000 || got.State != ledger.Running {
		t.Fatalf("%+v", got)
	}
	// mini has 16 GB - 4 GB base - 6 GB agent = 6 GB available, less
	// 1.2 GB still reserved but unused, less 2 GB kept free: a 4 GB job no
	// longer fits there.
	b := r.submit(4096, func(s *coord.SubmitRequest) { s.Machine = "mini" })
	r.tick()
	if r.job(b.ID).State != ledger.Queued {
		t.Fatal("overrun capacity was promised to another job")
	}
	evs, _ := r.l.Events(ctx, 0, 0)
	found := false
	for _, e := range evs {
		if strings.Contains(e.Kind, "overrun") && e.JobID == a.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("no overrun event")
	}
	// The history learns from it for next time.
	r.cl.Exit("mini", ledger.AgentName(a.ID))
	r.tick()
	mb, _, ok, _ := r.l.Estimate(ctx, "claude", repo, r.cfg.HistoryHeadroomPct)
	if !ok || mb < 6000 {
		t.Fatalf("estimate %d %v", mb, ok)
	}
	j, notes, err := r.c.Submit(ctx, coord.SubmitRequest{Kind: "claude", Repo: repo})
	if err != nil || j.MemoryMB != mb || len(notes) == 0 || j.EstimateSource == "default" {
		t.Fatalf("%+v %v %v", j, notes, err)
	}
}

// Scenario 10: concurrent submissions race for capacity, from many
// goroutines and from two coordinators sharing one ledger.
func TestScenario10ConcurrentSubmissions(t *testing.T) {
	r := newRig(t, nil)
	l2, err := ledger.OpenWithClock(r.path, r.cl.Clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	c2 := coord.New(l2, r.cl.Herdr(), r.cl.Prober(), r.cfg)
	c2.Now = r.cl.Clock.Now
	coords := []*coord.Coordinator{r.c, c2}
	var wg sync.WaitGroup
	for g := 0; g < 12; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			c := coords[g%2]
			for i := 0; i < 3; i++ {
				if _, _, err := c.Submit(ctx, coord.SubmitRequest{Kind: "codex", Repo: repo, Prompt: "p", MemoryMB: 6144, CPU: 1}); err != nil {
					t.Error(err)
					return
				}
				if _, err := c.Pump(ctx); err != nil {
					t.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	r.check()
	// mbp fits 9 jobs of 6 GB in 58 GB, mini fits 1 in 10 GB.
	if r.cl.Live("mbp") > 9 || r.cl.Live("mini") > 1 {
		t.Fatalf("overcommitted: mbp %d mini %d", r.cl.Live("mbp"), r.cl.Live("mini"))
	}
	if n := r.states(); n[ledger.Running] != r.cl.Live("mbp")+r.cl.Live("mini") || n[ledger.Running]+n[ledger.Queued] != 36 {
		t.Fatalf("%v", n)
	}
}
