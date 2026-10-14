package coord

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/gitop"
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
	// Session is the Herdr session the machine is saved with, for attach
	// hints.
	Session string
	// PathMap rewrites repository paths for this worker.
	PathMap map[string]string
}

// GitOps runs the scheduler's two Git operations on a worker: deleting a
// branch it made, and cloning a repository a job needs.
type GitOps interface {
	Run(ctx context.Context, t Target, r gitop.Request) (gitop.Result, error)
}

// DefaultGitOps runs on this computer directly and on a remote worker over
// SSH, through `hsched cluster gitop`.
type DefaultGitOps struct {
	SSH transport.SSH
}

// Run implements GitOps.
func (g DefaultGitOps) Run(ctx context.Context, t Target, r gitop.Request) (gitop.Result, error) {
	if t.Local {
		return gitop.Run(ctx, r), nil
	}
	if t.SSHTarget == "" {
		return gitop.Result{}, fmt.Errorf("worker %s has no SSH target to run git on", t.ID)
	}
	return g.SSH.GitOp(ctx, t.SSHTarget, t.Program, r)
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

// Nighthawk reads `nighthawk report` on the worker. A failure is down.
func (p DefaultProber) Nighthawk(ctx context.Context, t Target) (string, string) {
	var (
		b   []byte
		err error
	)
	switch {
	case t.Local:
		b, err = exec.CommandContext(ctx, "nighthawk", "report").Output()
	case t.SSHTarget != "":
		b, err = p.SSH.Report(ctx, t.SSHTarget)
	default:
		return "down", ""
	}
	return placement.ReadReport(b, err)
}

// Coordinator ties the pieces together. One coordinator runs per state
// directory (the daemon's lock guarantees it), but every step is also safe
// against a second one because capacity is only ever taken in the ledger.
type Coordinator struct {
	L   *ledger.Ledger
	H   herdrx.Herdr
	P   Prober
	G   GitOps
	Cfg Config
	Now func() time.Time

	mu sync.Mutex // one Tick or Pump at a time in this process

	imu      sync.Mutex
	inflight map[string]bool

	cmu     sync.Mutex
	cache   map[string]cached
	cpu     map[string]smoothed // per worker, smoothed CPU utilisation
	targets map[string]Target   // the last Targets answer, by worker id
	mstatus map[string]herdrx.MachineStatus
	msAt    time.Time
	msErr   bool
	lastCut map[string]time.Time // last pressure termination per worker

	tick int64 // correlation id for one Tick's events
}

type cached struct {
	snap     resource.Snapshot
	latency  int64
	at       time.Time
	mismatch string // a changed identity, which makes the worker incompatible
}

type smoothed struct {
	pct float64
	at  time.Time // CollectedAt of the last reading folded in
}

// cpuAlpha is the weight of the newest CPU reading in the moving average.
const cpuAlpha = 0.5

// New builds a coordinator.
func New(l *ledger.Ledger, h herdrx.Herdr, p Prober, cfg Config) *Coordinator {
	return &Coordinator{L: l, H: h, P: p, G: DefaultGitOps{}, Cfg: cfg, Now: time.Now, inflight: map[string]bool{},
		cache: map[string]cached{}, cpu: map[string]smoothed{}, targets: map[string]Target{}, lastCut: map[string]time.Time{}}
}

// event adds an event carrying the current tick as its correlation id.
func (c *Coordinator) event(ctx context.Context, kind, job, worker string, detail map[string]any) {
	if detail == nil {
		detail = map[string]any{}
	}
	c.cmu.Lock()
	if c.tick > 0 {
		detail["tick"] = c.tick
	}
	c.cmu.Unlock()
	_ = c.L.AddEvent(ctx, kind, job, worker, detail)
}

// target is the last known Target for a worker id.
func (c *Coordinator) target(id string) Target {
	c.cmu.Lock()
	defer c.cmu.Unlock()
	if t, ok := c.targets[id]; ok {
		return t
	}
	return Target{ID: id, Local: id == herdrx.Local}
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
		t.PathMap = wc.PathMap
		out = append(out, t)
	}
	if c.Cfg.IncludeLocal {
		add(Target{ID: herdrx.Local, Label: "local", Local: true}, c.workerConfig(herdrx.Local, "local"))
	}
	ms, err := c.H.Machines(ctx)
	for _, m := range ms {
		t := Target{ID: m.ID, Label: m.Label, SSHTarget: m.Target, Disabled: !m.Enabled, Session: m.Session}
		if t.Label == "" {
			t.Label = m.ID
		}
		add(t, c.workerConfig(m.ID, m.Label))
	}
	c.cmu.Lock()
	for _, t := range out {
		c.targets[t.ID] = t
	}
	c.cmu.Unlock()
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
			if out[i].State == placement.StateReady && t.Settings.RequireNighthawk {
				out[i].Nighthawk, out[i].NighthawkMode = c.nighthawk(ctx, t)
			}
		}(i, t)
	}
	wg.Wait()
	status := c.machineStatus(ctx)
	for i := range out {
		static := out[i].Snapshot.Backend == resource.BackendStatic
		ld, err := c.L.LoadOf(ctx, out[i].ID, static)
		if err != nil {
			return nil, err
		}
		out[i].PendingMemoryMB = ld.PendingMemoryMB
		out[i].CommittedCPU = ld.CommittedCPU
		out[i].PendingCPU = ld.PendingCPU
		out[i].ActiveAgents = ld.ActiveAgents
		out[i].RunningRepos = ld.RunningRepos
		out[i].ClusterRepos = ld.ClusterRepos
		out[i].CPUUtilPct = c.smoothCPU(out[i])
		if ms, ok := status[out[i].ID]; ok && out[i].State == placement.StateReady {
			switch ms.Status {
			case "error":
				out[i].State = placement.StateHerdrDown
			case "auth required":
				out[i].State = placement.StateAuth
			}
		}
	}
	sort.SliceStable(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	if terr != nil {
		_ = c.L.AddEvent(ctx, "cluster.herdr.machine_list_failed", "", "", map[string]any{"error": terr.Error()})
	}
	return out, nil
}

type nighthawkProber interface {
	Nighthawk(ctx context.Context, t Target) (health, mode string)
}

func (c *Coordinator) nighthawk(ctx context.Context, t Target) (string, string) {
	np, ok := c.P.(nighthawkProber)
	if !ok {
		return "down", ""
	}
	return np.Nighthawk(ctx, t)
}

// smoothCPU folds a worker's newest CPU reading into its moving average, so
// one busy sample does not swing admission. A reading already folded in is
// not counted twice.
func (c *Coordinator) smoothCPU(w placement.Worker) float64 {
	if w.Snapshot.CollectedAt.IsZero() || w.Snapshot.CPU.UtilSource == "declared" || w.Snapshot.CPU.UtilSource == "" {
		return 0
	}
	c.cmu.Lock()
	defer c.cmu.Unlock()
	prev, ok := c.cpu[w.ID]
	switch {
	case !ok:
		prev = smoothed{pct: w.Snapshot.CPU.UtilPct, at: w.Snapshot.CollectedAt}
	case w.Snapshot.CollectedAt.After(prev.at):
		prev = smoothed{pct: cpuAlpha*w.Snapshot.CPU.UtilPct + (1-cpuAlpha)*prev.pct, at: w.Snapshot.CollectedAt}
	}
	c.cpu[w.ID] = prev
	return prev.pct
}

// machineStatus is what `herdr machine status` last said, refreshed at most
// every MachineStatusEvery. A Herdr too old to have the verb is remembered and
// not asked again.
func (c *Coordinator) machineStatus(ctx context.Context) map[string]herdrx.MachineStatus {
	if c.Cfg.MachineStatusEvery <= 0 {
		return nil
	}
	now := c.now()
	c.cmu.Lock()
	fresh := c.mstatus != nil && now.Sub(c.msAt) < c.Cfg.MachineStatusEvery
	failed := c.msErr && now.Sub(c.msAt) < c.Cfg.MachineStatusEvery
	cur := c.mstatus
	c.cmu.Unlock()
	if fresh || failed {
		return cur
	}
	rows, err := c.H.MachineStatus(ctx)
	c.cmu.Lock()
	c.msAt = now
	if err != nil {
		if !c.msErr {
			c.cmu.Unlock()
			_ = c.L.AddEvent(ctx, "cluster.herdr.machine_status_failed", "", "", map[string]any{"error": err.Error()})
			c.cmu.Lock()
		}
		c.msErr = true
		cur = c.mstatus
		c.cmu.Unlock()
		return cur
	}
	c.msErr = false
	m := map[string]herdrx.MachineStatus{}
	for _, r := range rows {
		m[r.ID] = r
	}
	c.mstatus = m
	c.cmu.Unlock()
	for _, r := range rows {
		_ = c.L.SetHerdrStatus(ctx, r.ID, r.Label, r.Status, r.Error)
	}
	return m
}

// identity is what a worker says it is: host, operating system and
// architecture. A change means a different machine answers at that address.
func identity(s resource.Snapshot) string {
	if s.Host == "" {
		return ""
	}
	return s.Host + "/" + s.OS + "/" + s.Arch
}

// mapPaths asks a worker about each repository at its mapped location and
// returns the request plus the way back to the logical path.
func mapPaths(m map[string]string, paths []string) ([]string, map[string]string) {
	if len(m) == 0 {
		return paths, nil
	}
	back := map[string]string{}
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		q := MapPath(m, p)
		back[q] = p
		out = append(out, q)
	}
	return out, back
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
		if prev.mismatch != "" {
			w.State = placement.StateIncompatible
			return w
		}
	case !force && row.BackoffUntil.After(now):
		w.State = placement.StateBackoff
		w.Snapshot = prev.snap
		return w
	default:
		asked, back := mapPaths(t.PathMap, paths)
		req := resource.Request{Paths: asked, PIDs: pids}
		snap, lat, err := c.P.Probe(ctx, t, req)
		if err == nil && back != nil {
			logical := map[string]bool{}
			for k, v := range snap.Paths {
				if l, ok := back[k]; ok {
					k = l
				}
				logical[k] = v
			}
			snap.Paths = logical
		}
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
		mismatch := ""
		if c.Cfg.VerifyIdentity && snap.Backend != resource.BackendStatic {
			id := identity(snap)
			if prevID, err := c.L.CheckIdentity(ctx, t.ID, t.Label, id); err == nil && prevID != "" && id != "" && prevID != id {
				mismatch = fmt.Sprintf("identity changed: was %s, now %s; if the machine was rebuilt on purpose, run hsched cluster reregister %s", prevID, id, t.ID)
			}
		}
		state := placement.StateReady
		if mismatch != "" {
			state = placement.StateIncompatible
			if row.LastError != mismatch {
				_ = c.L.AddEvent(ctx, "cluster.worker.identity_mismatch", "", t.ID, map[string]any{"detail": mismatch})
			}
		}
		_ = c.L.RecordProbe(ctx, t.ID, t.Label, state, snap, lat)
		c.cmu.Lock()
		c.cache[t.ID] = cached{snap: snap, latency: lat, at: now, mismatch: mismatch}
		c.cmu.Unlock()
		w.Snapshot, w.LatencyMS = snap, lat
		if mismatch != "" {
			w.State = state
			return w
		}
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
		Machine: j.Machine, Labels: j.Labels, OS: j.OS, Arch: j.Arch,
		ExclusiveRepo: j.NoWorktree, MaxPerRepo: c.Cfg.MaxPerRepo, Cloneable: j.CloneURL != ""}
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
		if ready, err := c.dependenciesReady(ctx, j); err != nil {
			return rep, err
		} else if !ready {
			rep.Waiting = append(rep.Waiting, j.ID)
			continue
		}
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
		c.decide(ctx, d.JobID, "", d.Policy, d.Summary, d)
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
			fresh.PendingCPU, fresh.ClusterRepos = ld.PendingCPU, ld.ClusterRepos
			if r := placement.Eligible(pj, fresh); len(r) > 0 {
				return fmt.Errorf("%s no longer fits: %s", w.ID, strings.Join(r, "; "))
			}
			lastLoad = ld
			return nil
		})
		if err != nil {
			rep.Waiting = append(rep.Waiting, job.ID)
			c.decide(ctx, job.ID, "", a.Decision.Policy, "capacity changed before the reservation: "+err.Error(), a.Decision)
			continue
		}
		c.decide(ctx, job.ID, w.ID, a.Decision.Policy, a.Decision.Summary, a.Decision)
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
