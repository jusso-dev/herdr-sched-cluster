package coord

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/herdrx"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/placement"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/queue"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/transport"
)

// Target is one worker the coordinator could schedule onto.
type Target struct {
	ID        string
	Label     string
	Local     bool
	SSHTarget string
	Program   string
	Probe     string
	Static    resource.StaticSpec
	Settings  placement.Settings
	Disabled  bool
}

// Prober reads a worker's resources.
type Prober interface {
	Probe(ctx context.Context, t Target, req resource.Request) (resource.Snapshot, int64, error)
}

// DefaultProber reads this computer directly, a remote over SSH, and a
// static worker from its declaration.
type DefaultProber struct {
	SSH transport.SSH
}

// Probe implements Prober.
func (p DefaultProber) Probe(ctx context.Context, t Target, req resource.Request) (resource.Snapshot, int64, error) {
	switch {
	case t.Probe == ProbeStatic:
		return resource.Static(t.Static, time.Now()), 0, nil
	case t.Local:
		s, err := resource.Collect(ctx, req)
		return s, 0, err
	default:
		r, err := p.SSH.Probe(ctx, t.SSHTarget, t.Program, req)
		return r.Snapshot, r.LatencyMS, err
	}
}

// Coordinator ties the pieces together. One coordinator runs per state
// directory (the daemon's lock guarantees it), but every step is also safe
// against a second one because capacity is only ever taken in the ledger.
type Coordinator struct {
	L   *ledger.Ledger
	H   herdrx.Herdr
	P   Prober
	Cfg Config
	Now func() time.Time

	mu sync.Mutex // one Tick or Pump at a time in this process

	imu      sync.Mutex
	inflight map[string]bool

	cmu   sync.Mutex
	cache map[string]cached
}

type cached struct {
	snap    resource.Snapshot
	latency int64
	at      time.Time
}

// New builds a coordinator.
func New(l *ledger.Ledger, h herdrx.Herdr, p Prober, cfg Config) *Coordinator {
	return &Coordinator{L: l, H: h, P: p, Cfg: cfg, Now: time.Now, inflight: map[string]bool{}, cache: map[string]cached{}}
}

func (c *Coordinator) now() time.Time { return c.Now() }

func (c *Coordinator) setInflight(id string, on bool) {
	c.imu.Lock()
	defer c.imu.Unlock()
	if on {
		c.inflight[id] = true
	} else {
		delete(c.inflight, id)
	}
}

func (c *Coordinator) isInflight(id string) bool {
	c.imu.Lock()
	defer c.imu.Unlock()
	return c.inflight[id]
}

func (c *Coordinator) workerConfig(id, label string) WorkerConfig {
	if w, ok := c.Cfg.Workers[id]; ok {
		return w
	}
	if w, ok := c.Cfg.Workers[label]; ok {
		return w
	}
	return WorkerConfig{}
}

// Targets lists the workers: this computer (when included) and every
// enabled machine saved in Herdr. A failure to read Herdr's machine list
// still returns this computer, with the error.
func (c *Coordinator) Targets(ctx context.Context) ([]Target, error) {
	var out []Target
	add := func(t Target, wc WorkerConfig) {
		t.Probe = wc.Probe
		if t.Probe == "" {
			t.Probe = ProbeAuto
		}
		t.Static = wc.Static
		t.Settings = merge(c.Cfg.Defaults, wc.Settings)
		if t.Probe == ProbeStatic && len(t.Settings.Agents) == 0 {
			// A static worker cannot be asked which agents it has, so
			// the declared list is authoritative.
			t.Settings.Agents = wc.Static.Agents
		}
		t.Program = wc.Program
		if t.Program == "" {
			t.Program = c.Cfg.ProbeProgram
		}
		if t.Probe == ProbeOff {
			t.Disabled = true
		}
		out = append(out, t)
	}
	if c.Cfg.IncludeLocal {
		add(Target{ID: herdrx.Local, Label: "local", Local: true}, c.workerConfig(herdrx.Local, "local"))
	}
	ms, err := c.H.Machines(ctx)
	for _, m := range ms {
		t := Target{ID: m.ID, Label: m.Label, SSHTarget: m.Target, Disabled: !m.Enabled}
		if t.Label == "" {
			t.Label = m.ID
		}
		add(t, c.workerConfig(m.ID, m.Label))
	}
	return out, err
}

// Workers probes (or reuses a fresh probe of) every target and returns
// them as placement sees them, load from the ledger included. pids asks a
// worker's probe to measure those process trees, which forces a fresh
// probe of that worker.
func (c *Coordinator) Workers(ctx context.Context, force bool, pids map[string][]int) ([]placement.Worker, error) {
	return c.workersWith(ctx, force, pids, nil)
}

// workersWith is Workers with extra repository paths every probe must also
// answer for, which is what a plan for repositories nothing has queued yet
// needs. A cached reading that never looked for one of them is not reused.
func (c *Coordinator) workersWith(ctx context.Context, force bool, pids map[string][]int, extra []string) ([]placement.Worker, error) {
	targets, terr := c.Targets(ctx)
	rows, err := c.L.Workers(ctx)
	if err != nil {
		return nil, err
	}
	paths, err := c.repoPaths(ctx)
	if err != nil {
		return nil, err
	}
	paths = mergePaths(paths, extra)
	out := make([]placement.Worker, len(targets))
	sem := make(chan struct{}, max(1, c.Cfg.ProbeConcurrency))
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		go func(i int, t Target) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			out[i] = c.worker(ctx, t, rows[t.ID], force, paths, pids[t.ID])
		}(i, t)
	}
	wg.Wait()
	for i := range out {
		static := out[i].Snapshot.Backend == resource.BackendStatic
		ld, err := c.L.LoadOf(ctx, out[i].ID, static)
		if err != nil {
			return nil, err
		}
		out[i].PendingMemoryMB = ld.PendingMemoryMB
		out[i].CommittedCPU = ld.CommittedCPU
		out[i].ActiveAgents = ld.ActiveAgents
		out[i].RunningRepos = ld.RunningRepos
	}
	sort.SliceStable(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	if terr != nil {
		_ = c.L.AddEvent(ctx, "cluster.herdr.machine_list_failed", "", "", map[string]any{"error": terr.Error()})
	}
	return out, nil
}

func (c *Coordinator) worker(ctx context.Context, t Target, row ledger.WorkerRow, force bool, paths []string, pids []int) placement.Worker {
	w := placement.Worker{ID: t.ID, Label: t.Label, Local: t.Local, Settings: t.Settings, HeldFor: row.HeldFor, State: placement.StateReady}
	now := c.now()
	c.cmu.Lock()
	prev, have := c.cache[t.ID]
	c.cmu.Unlock()
	switch {
	case t.Disabled:
		w.State = placement.StateDisabled
		w.Snapshot = prev.snap
		return w
	case !force && len(pids) == 0 && have && now.Sub(prev.at) < c.Cfg.ProbeTTL && covers(prev.snap, paths):
		w.Snapshot, w.LatencyMS = prev.snap, prev.latency
	case !force && row.BackoffUntil.After(now):
		w.State = placement.StateBackoff
		w.Snapshot = prev.snap
		return w
	default:
		req := resource.Request{Paths: paths, PIDs: pids}
		snap, lat, err := c.P.Probe(ctx, t, req)
		if err != nil {
			state := placement.StateUnreachable
			var te *transport.Error
			switch {
			case errors.As(err, &te) && te.Auth:
				state = placement.StateAuth
			case strings.Contains(err.Error(), "schema"):
				state = placement.StateIncompatible
			}
			_ = c.L.RecordFailure(ctx, t.ID, t.Label, state, err.Error(), c.Cfg.BackoffBase, c.Cfg.BackoffMax)
			c.cmu.Lock()
			delete(c.cache, t.ID)
			c.cmu.Unlock()
			w.State = state
			return w
		}
		_ = c.L.RecordProbe(ctx, t.ID, t.Label, placement.StateReady, snap, lat)
		c.cmu.Lock()
		c.cache[t.ID] = cached{snap: snap, latency: lat, at: now}
		c.cmu.Unlock()
		w.Snapshot, w.LatencyMS = snap, lat
	}
	if row.Draining {
		w.State = placement.StateDraining
	}
	return w
}

// covers says whether a reading already answered for every path, so a job
// naming a repository the last probe never looked for is checked rather than
// assumed present.
func covers(s resource.Snapshot, paths []string) bool {
	if s.Backend == resource.BackendStatic {
		return true // a declaration has no paths to ask about
	}
	for _, p := range paths {
		if _, ok := s.Paths[p]; !ok {
			return false
		}
	}
	return true
}

func mergePaths(a, b []string) []string {
	if len(b) == 0 {
		return a
	}
	seen := map[string]bool{}
	var out []string
	for _, p := range append(append([]string{}, a...), b...) {
		if p != "" && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// repoPaths is every repository a queued or active job names, so a probe
// can say which of them exist on each worker.
func (c *Coordinator) repoPaths(ctx context.Context) ([]string, error) {
	jobs, err := c.L.List(ctx, ledger.Queued, ledger.Reserved, ledger.Dispatching, ledger.Running)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	for _, j := range jobs {
		if j.Repo != "" && !seen[j.Repo] {
			seen[j.Repo] = true
			out = append(out, j.Repo)
		}
	}
	sort.Strings(out)
	return out, nil
}

// PlacementJob is how placement sees a ledger job.
func (c *Coordinator) PlacementJob(j ledger.Job) placement.Job {
	p := j.Policy
	if p == "" {
		p = c.Cfg.Policy
	}
	return placement.Job{ID: j.ID, Kind: j.Kind, Repo: j.Repo, MemoryMB: j.MemoryMB, CPU: j.CPU, Policy: p,
		Machine: j.Machine, Labels: j.Labels, OS: j.OS, Arch: j.Arch}
}

// Report is what one Pump did.
type Report struct {
	Dispatched []string          `json:"dispatched,omitempty"`
	Waiting    []string          `json:"waiting,omitempty"`
	Failed     map[string]string `json:"failed,omitempty"`
	Holds      map[string]string `json:"holds,omitempty"`
}

// Pump plans one pass over the queue and dispatches what fits.
func (c *Coordinator) Pump(ctx context.Context) (Report, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pump(ctx)
}

func (c *Coordinator) pump(ctx context.Context) (Report, error) {
	rep := Report{Failed: map[string]string{}}
	queued, err := c.L.List(ctx, ledger.Queued)
	if err != nil || len(queued) == 0 {
		if err == nil {
			err = c.L.SetHolds(ctx, map[string]string{})
		}
		return rep, err
	}
	workers, err := c.Workers(ctx, false, nil)
	if err != nil {
		return rep, err
	}
	rows, err := c.L.Workers(ctx)
	if err != nil {
		return rep, err
	}
	held := map[string]string{}
	for id, r := range rows {
		if r.HeldFor != "" {
			held[id] = r.HeldFor
		}
	}
	byID := map[string]ledger.Job{}
	var entries []queue.Entry
	for _, j := range queued {
		byID[j.ID] = j
		entries = append(entries, queue.Entry{Job: c.PlacementJob(j), Priority: j.Priority, SubmittedAt: j.SubmittedAt, Seq: j.Seq})
	}
	now := c.now()
	plan := queue.Pass(entries, workers, now, c.Cfg.Queue, held)
	if err := c.L.SetHolds(ctx, plan.Holds); err != nil {
		return rep, err
	}
	rep.Holds = plan.Holds
	for _, d := range plan.Waiting {
		rep.Waiting = append(rep.Waiting, d.JobID)
		_ = c.L.RecordDecision(ctx, d.JobID, "", d.Policy, d.Summary, d)
	}
	wByID := map[string]placement.Worker{}
	for _, w := range workers {
		w.HeldFor = plan.Holds[w.ID]
		wByID[w.ID] = w
	}
	for _, a := range plan.Assignments {
		job := byID[a.JobID]
		w := wByID[a.WorkerID]
		pj := c.PlacementJob(job)
		static := w.Snapshot.Backend == resource.BackendStatic
		var lastLoad ledger.Load
		token, err := c.L.Reserve(ctx, job.ID, w.ID, static, func(ld ledger.Load) error {
			fresh := w
			fresh.PendingMemoryMB, fresh.CommittedCPU, fresh.ActiveAgents, fresh.RunningRepos = ld.PendingMemoryMB, ld.CommittedCPU, ld.ActiveAgents, ld.RunningRepos
			if r := placement.Eligible(pj, fresh); len(r) > 0 {
				return fmt.Errorf("%s no longer fits: %s", w.ID, strings.Join(r, "; "))
			}
			lastLoad = ld
			return nil
		})
		if err != nil {
			rep.Waiting = append(rep.Waiting, job.ID)
			_ = c.L.RecordDecision(ctx, job.ID, "", a.Decision.Policy, "capacity changed before the reservation: "+err.Error(), a.Decision)
			continue
		}
		_ = c.L.RecordDecision(ctx, job.ID, w.ID, a.Decision.Policy, a.Decision.Summary, a.Decision)
		job.Token = token
		if err := c.dispatch(ctx, job, w, lastLoad); err != nil {
			rep.Failed[job.ID] = err.Error()
			continue
		}
		rep.Dispatched = append(rep.Dispatched, job.ID)
	}
	return rep, nil
}

func machineArg(worker string) string {
	if worker == herdrx.Local {
		return ""
	}
	return worker
}

// resolveRepo turns ~/x into the worker's own home directory.
func resolveRepo(repo string, snap resource.Snapshot) (string, error) {
	if repo == "~" || strings.HasPrefix(repo, "~/") {
		if snap.Home == "" {
			return "", fmt.Errorf("repository %s starts with ~ but the worker did not report a home directory; use an absolute path", repo)
		}
		if snap.OS == "windows" {
			return strings.TrimRight(snap.Home, `\/`) + `\` + strings.ReplaceAll(strings.TrimPrefix(repo, "~/"), "/", `\`), nil
		}
		return path.Join(snap.Home, strings.TrimPrefix(repo, "~")), nil
	}
	return repo, nil
}

// dispatch opens a workspace (or worktree), starts the agent and gives it
// the prompt. It never requeues a job unless it has confirmed no agent of
// that name exists on the worker, which is what prevents a double dispatch.
func (c *Coordinator) dispatch(ctx context.Context, job ledger.Job, w placement.Worker, ld ledger.Load) error {
	c.setInflight(job.ID, true)
	defer c.setInflight(job.ID, false)
	m := machineArg(w.ID)
	name := ledger.AgentName(job.ID)
	fail := func(stage string, err error, workspace string) error {
		if workspace != "" {
			_ = c.H.WorkspaceClose(ctx, m, workspace)
		}
		state, ferr := c.L.DispatchFailed(ctx, job.ID, job.Token, stage+": "+err.Error())
		if ferr != nil {
			return fmt.Errorf("%s: %v (and recording it failed: %v)", stage, err, ferr)
		}
		_ = c.L.AddEvent(ctx, "cluster.dispatch.failed", job.ID, w.ID, map[string]any{"stage": stage, "now": state})
		return fmt.Errorf("%s on %s: %w", stage, w.ID, err)
	}
	worktree := false
	isolation := job.Isolation
	if isolation == "" {
		isolation = c.Cfg.Isolation
	}
	switch isolation {
	case IsolationWorktree:
		worktree = true
	case IsolationAuto:
		// ld is the load just before this job's reservation, so any
		// count means another job already holds this repository here.
		worktree = ld.RunningRepos[job.Repo] > 0
	}
	if err := c.L.MarkDispatching(ctx, job.ID, job.Token, worktree); err != nil {
		return err
	}
	repo, err := resolveRepo(job.Repo, w.Snapshot)
	if err != nil {
		return fail("resolve repository", err, "")
	}
	var pl herdrx.Placement
	if worktree {
		pl, err = c.H.WorktreeCreate(ctx, m, repo, "hsc/"+job.ID, name)
	} else {
		pl, err = c.H.WorkspaceCreate(ctx, m, repo, name)
	}
	if err != nil {
		return fail("open workspace", err, "")
	}
	if err := c.L.SetWorkspace(ctx, job.ID, job.Token, pl.WorkspaceID, pl.PaneID); err != nil {
		return err
	}
	err = c.H.AgentStart(ctx, m, herdrx.StartRequest{Name: name, Kind: job.Kind, Pane: pl.PaneID, Timeout: c.Cfg.DispatchTimeout, Args: job.AgentArgs})
	if err != nil && herdrx.Code(err) != "timeout" {
		// The call failed, but the agent may still have started (a dropped
		// connection after Herdr acted). Look before deciding.
		agents, lerr := c.H.Agents(ctx, m)
		if lerr != nil {
			_ = c.L.AddEvent(ctx, "cluster.dispatch.uncertain", job.ID, w.ID, map[string]any{"error": err.Error()})
			return fmt.Errorf("agent start on %s is uncertain; reconciliation will decide: %w", w.ID, err)
		}
		if !hasAgent(agents, name) {
			return fail("start agent", err, pl.WorkspaceID)
		}
	}
	if job.Prompt != "" {
		if perr := c.H.AgentPrompt(ctx, m, pl.PaneID, job.Prompt); perr != nil {
			// The agent is running; it must not be started again elsewhere.
			_ = c.L.AddEvent(ctx, "cluster.dispatch.prompt_failed", job.ID, w.ID, map[string]any{"error": perr.Error()})
		}
	}
	return c.L.MarkRunning(ctx, job.ID, job.Token, pl.PaneID)
}

func hasAgent(agents []herdrx.Agent, name string) bool {
	for _, a := range agents {
		if a.Name == name {
			return true
		}
	}
	return false
}

// Reconcile compares the ledger with what each worker's Herdr reports. It
// returns, per worker, the shell PIDs of running jobs, keyed so a probe can
// measure them.
func (c *Coordinator) Reconcile(ctx context.Context) (map[string]map[int]string, error) {
	active, err := c.L.List(ctx, ledger.Reserved, ledger.Dispatching, ledger.Running)
	if err != nil {
		return nil, err
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
				if j.State == ledger.Running {
					_ = c.L.SetDisconnected(ctx, j.ID, true)
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
			case ledger.Running:
				if !ok {
					_ = c.L.Finish(ctx, j.ID, ledger.Finished, "agent is no longer listed by Herdr on "+worker)
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
				if j.State == ledger.Dispatching && j.WorkspaceID != "" {
					// The agent start may have run, and the agent may
					// already have finished. Starting it again could do
					// the work twice, so the job is failed for a person
					// to look at instead.
					_ = c.L.Abandon(ctx, j.ID, j.Token, "dispatch was interrupted after the agent start on "+worker+
						"; no agent is running now, so it may have started and finished. Not retried; resubmit if needed")
					continue
				}
				if j.WorkspaceID != "" {
					_ = c.H.WorkspaceClose(ctx, m, j.WorkspaceID)
				}
				_, _ = c.L.DispatchFailed(ctx, j.ID, j.Token, "dispatch did not finish and no agent was found on "+worker)
			}
		}
	}
	return pids, nil
}

// Observe probes the workers that have running jobs, measuring each job's
// process tree, and records the usage. A job over its reservation has the
// reservation raised; nothing is ever killed.
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

// Tick is one full round: reconcile, measure, then plan and dispatch.
func (c *Coordinator) Tick(ctx context.Context) (Report, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pids, err := c.Reconcile(ctx)
	if err != nil {
		return Report{}, err
	}
	if err := c.Observe(ctx, pids); err != nil {
		return Report{}, err
	}
	return c.pump(ctx)
}

// Drain stops new work going to a worker; running agents carry on.
func (c *Coordinator) Drain(ctx context.Context, worker string, draining bool, by string) (string, error) {
	targets, _ := c.Targets(ctx)
	for _, t := range targets {
		if t.ID == worker || t.Label == worker {
			return t.ID, c.L.SetDraining(ctx, t.ID, draining, by)
		}
	}
	return "", fmt.Errorf("no worker called %s; see hsched cluster workers", worker)
}

// SubmitRequest is a request to run an agent somewhere in the cluster.
type SubmitRequest struct {
	Kind        string
	Repo        string
	Prompt      string
	AgentArgs   []string
	MemoryMB    int64
	CPU         float64
	Priority    int
	Policy      string
	Machine     string
	Labels      []string
	OS, Arch    string
	Isolation   string
	SubmittedBy string
}

var (
	argRe   = regexp.MustCompile(`^[A-Za-z0-9._/:=,@+%-]{1,256}$`)
	labelRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
)

// MaxPromptChars bounds a prompt.
const MaxPromptChars = 100_000

// Submit validates a request, estimates what it needs when the caller did
// not say, checks that some worker could ever run it, and queues it.
func (c *Coordinator) Submit(ctx context.Context, r SubmitRequest) (ledger.Job, []string, error) {
	r, source, notes, err := c.prepare(ctx, r)
	if err != nil {
		return ledger.Job{}, nil, err
	}
	pj := placement.Job{Kind: r.Kind, Repo: r.Repo, MemoryMB: r.MemoryMB, CPU: r.CPU, Policy: r.Policy, Machine: r.Machine, Labels: r.Labels, OS: r.OS, Arch: r.Arch}
	workers, err := c.Workers(ctx, false, nil)
	if err != nil {
		return ledger.Job{}, nil, err
	}
	if ok, why := placement.FitsAnywhereEver(pj, workers); !ok {
		return ledger.Job{}, nil, fmt.Errorf("no worker could ever run this job:\n  %s", strings.Join(why, "\n  "))
	}
	job, err := c.L.Submit(ctx, ledger.NewJob{Kind: r.Kind, Repo: r.Repo, Prompt: r.Prompt, AgentArgs: r.AgentArgs,
		MemoryMB: r.MemoryMB, CPU: r.CPU, EstimateSource: source, Priority: r.Priority, Policy: r.Policy,
		Machine: r.Machine, Labels: r.Labels, OS: r.OS, Arch: r.Arch, Isolation: r.Isolation,
		SubmittedBy: r.SubmittedBy, MaxAttempts: c.Cfg.MaxAttempts})
	return job, notes, err
}

// prepare validates a request and fills in the memory and CPU estimates the
// caller left out. Submit and Plan share it, so a plan refuses exactly what a
// submit would.
func (c *Coordinator) prepare(ctx context.Context, r SubmitRequest) (SubmitRequest, string, []string, error) {
	var notes []string
	if _, ok := resource.AgentExecutables[r.Kind]; !ok {
		return r, "", nil, fmt.Errorf("unknown agent kind %q; one of %v", r.Kind, resource.AgentKinds())
	}
	if err := transport.ValidatePath(r.Repo); err != nil {
		return r, "", nil, fmt.Errorf("repository: %w", err)
	}
	if len(r.Prompt) > MaxPromptChars {
		return r, "", nil, fmt.Errorf("prompt is %d characters; the limit is %d", len(r.Prompt), MaxPromptChars)
	}
	for _, a := range r.AgentArgs {
		if !argRe.MatchString(a) {
			return r, "", nil, fmt.Errorf("agent argument %q has characters outside [A-Za-z0-9._/:=,@+%%-]", a)
		}
	}
	for _, l := range r.Labels {
		if !labelRe.MatchString(l) {
			return r, "", nil, fmt.Errorf("label %q must be lower case letters, digits, dot, dash or underscore", l)
		}
	}
	if r.Policy != "" {
		if _, ok := placement.Policies[r.Policy]; !ok {
			return r, "", nil, fmt.Errorf("policy %q is not one of %v", r.Policy, placement.PolicyNames)
		}
	}
	switch r.Isolation {
	case "", IsolationAuto, IsolationWorktree, IsolationNone:
	default:
		return r, "", nil, fmt.Errorf("isolation %q is not auto, worktree or none", r.Isolation)
	}
	if r.Priority < -100 || r.Priority > 100 {
		return r, "", nil, fmt.Errorf("priority must be between -100 and 100")
	}
	if r.MemoryMB < 0 || r.CPU < 0 {
		return r, "", nil, fmt.Errorf("memory and cpu cannot be negative")
	}
	source := "requested"
	if r.MemoryMB == 0 {
		mb, src, ok, err := c.L.Estimate(ctx, r.Kind, r.Repo, c.Cfg.HistoryHeadroomPct)
		if err != nil {
			return r, "", nil, err
		}
		if ok {
			r.MemoryMB, source = mb, src
		} else {
			r.MemoryMB, source = c.Cfg.DefaultMemoryMB, "default"
		}
		notes = append(notes, fmt.Sprintf("memory estimate %d MB (%s)", r.MemoryMB, source))
	}
	if r.CPU == 0 {
		r.CPU = c.Cfg.DefaultCPU
	}
	return r, source, notes, nil
}

// Explanation is why a job is where it is.
type Explanation struct {
	Job       ledger.Job          `json:"job"`
	Decisions []ledger.Decision   `json:"decisions"`
	Now       *placement.Decision `json:"now,omitempty"`
}

// Explain returns a job's recorded decisions and, for a queued job, how
// placement sees it right now.
func (c *Coordinator) Explain(ctx context.Context, id string) (Explanation, error) {
	j, err := c.L.Get(ctx, id)
	if err != nil {
		return Explanation{}, err
	}
	ds, err := c.L.Decisions(ctx, id)
	if err != nil {
		return Explanation{}, err
	}
	ex := Explanation{Job: j, Decisions: ds}
	if j.State == ledger.Queued {
		ws, err := c.Workers(ctx, false, nil)
		if err != nil {
			return ex, err
		}
		d := placement.Place(c.PlacementJob(j), ws)
		ex.Now = &d
	}
	return ex, nil
}
