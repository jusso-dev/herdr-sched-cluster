// Package sim is a simulated cluster for testing the coordinator: an
// in-memory Herdr per machine, a prober that reports what the simulated
// agents use, failure injection, and a clock the test moves. Nothing here
// touches a real machine.
package sim

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/coord"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/gitop"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/herdrx"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

// Clock is a test clock.
type Clock struct {
	mu sync.Mutex
	t  time.Time
}

// NewClock starts a clock at a fixed instant.
func NewClock() *Clock { return &Clock{t: time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)} }

// Now reads the clock.
func (c *Clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

// Advance moves the clock on.
func (c *Clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// Agent is a simulated running agent.
type Agent struct {
	Name, Kind, Pane, Workspace, Cwd string
	PID                              int
	RSSMB                            int64
}

// Machine is one simulated computer with its own Herdr server.
type Machine struct {
	ID, Label  string
	Enabled    bool
	TotalMB    int64
	BaseUsedMB int64
	Cores      int
	Pressure   string
	Load5      float64
	OS, Arch   string

	// Failure injection.
	Unreachable     bool           // probe and every Herdr call fail
	ProbeFails      bool           // only the probe fails
	Fail            map[string]int // op name -> how many next calls fail
	AmbiguousStarts int            // starts that succeed but report an error, and the next agent list fails too
	ListFails       int            // next agent list calls fail
	LostReplies     int            // starts that succeed but report an error; the agent list still works
	MissingPaths    map[string]bool
	// EmptyRepos are repositories with no commits: a worktree of one fails
	// the way Git does.
	EmptyRepos map[string]bool
	// HerdrStatus overrides what `herdr machine status` says ("" is
	// reachable, or error when Unreachable).
	HerdrStatus string
	// UtilPct is CPU use from things other than scheduled agents.
	UtilPct float64
	// ResumeOnRestart makes every pane show a foreground process after a
	// Herdr restart, as an agent resuming its own session would.
	ResumeOnRestart bool

	Agents     map[string]*Agent
	Workspaces map[string]string // id -> cwd
	Worktrees  int
	nextWS     int
	// Meta is what else Herdr knows about each workspace.
	Meta map[string]*WS
}

// WS is a simulated workspace: its label, whether it is a worktree, whether
// that worktree has uncommitted changes, and its pane's shell process.
type WS struct {
	Label    string
	Branch   string
	Worktree bool
	Dirty    bool
	Shell    int
}

// Cluster is the whole simulated cluster.
type Cluster struct {
	mu       sync.Mutex
	Machines map[string]*Machine
	Clock    *Clock
	nextPID  int
	// Starts counts successful agent starts per agent name, across every
	// machine and every coordinator. More than one is a double dispatch.
	Starts map[string]int
	// Prompts counts prompts per agent name.
	Prompts map[string]int
	Calls   map[string]int
	// GitOps records each Git operation the coordinator ran, as
	// "machine op branch-or-url".
	GitLog []string
	// AllowKill lets closing a workspace end the agent in it, as Herdr
	// does. Only the pressure policy may do that; every other test keeps it
	// off so an accidental close is caught.
	AllowKill bool
	Killed    []string
}

// New makes an empty cluster.
func New() *Cluster {
	return &Cluster{Machines: map[string]*Machine{}, Clock: NewClock(), nextPID: 1000,
		Starts: map[string]int{}, Prompts: map[string]int{}, Calls: map[string]int{}}
}

// Add adds a machine. The id "local" is the coordinator's own computer.
func (c *Cluster) Add(id string, totalMB int64, cores int) *Machine {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := &Machine{ID: id, Label: id, Enabled: true, TotalMB: totalMB, BaseUsedMB: 4096, Cores: cores,
		Pressure: resource.PressureNormal, OS: "darwin", Arch: "arm64", Fail: map[string]int{},
		MissingPaths: map[string]bool{}, EmptyRepos: map[string]bool{}, Agents: map[string]*Agent{}, Workspaces: map[string]string{},
		Meta: map[string]*WS{}}
	c.Machines[id] = m
	return m
}

// With runs f with the cluster locked, for changing a machine mid-test.
func (c *Cluster) With(f func()) { c.mu.Lock(); defer c.mu.Unlock(); f() }

// ErrUnreachable is what an unreachable machine answers.
var ErrUnreachable = errors.New("ssh: connect to host: Connection timed out")

func (c *Cluster) machine(id string) (*Machine, error) {
	if id == "" {
		id = herdrx.Local
	}
	m, ok := c.Machines[id]
	if !ok {
		return nil, fmt.Errorf("no machine %s", id)
	}
	if m.Unreachable {
		return nil, ErrUnreachable
	}
	return m, nil
}

func (c *Cluster) call(id, op string) (*Machine, error) {
	c.Calls[op]++
	m, err := c.machine(id)
	if err != nil {
		return nil, err
	}
	if m.Fail[op] > 0 {
		m.Fail[op]--
		return nil, fmt.Errorf("herdr %s: injected failure", op)
	}
	return m, nil
}

// Herdr returns the cluster's Herdr, as the coordinator sees it.
func (c *Cluster) Herdr() herdrx.Herdr { return fakeHerdr{c} }

type fakeHerdr struct{ c *Cluster }

func (f fakeHerdr) Machines(ctx context.Context) ([]herdrx.Machine, error) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	var out []herdrx.Machine
	for id, m := range f.c.Machines {
		if id == herdrx.Local {
			continue
		}
		out = append(out, herdrx.Machine{ID: id, Label: m.Label, Target: "sim@" + id, Session: "default", Enabled: m.Enabled})
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out, nil
}

func (f fakeHerdr) MachineStatus(ctx context.Context) ([]herdrx.MachineStatus, error) {
	ms, _ := f.Machines(ctx)
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	var out []herdrx.MachineStatus
	for _, m := range ms {
		st := "reachable"
		if f.c.Machines[m.ID].Unreachable {
			st = "error"
		}
		if o := f.c.Machines[m.ID].HerdrStatus; o != "" {
			st = o
		}
		out = append(out, herdrx.MachineStatus{ID: m.ID, Label: m.Label, Status: st})
	}
	return out, nil
}

func (f fakeHerdr) open(machine, op, cwd, branch, label string) (herdrx.Placement, error) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	m, err := f.c.call(machine, op)
	if err != nil {
		return herdrx.Placement{}, err
	}
	if m.MissingPaths[cwd] {
		return herdrx.Placement{}, fmt.Errorf("herdr %s: %s does not exist", op, cwd)
	}
	if op == "worktree" && m.EmptyRepos[cwd] {
		return herdrx.Placement{}, &herdrx.Error{Code: "worktree_create_failed", Message: "git worktree add failed: fatal: invalid reference: HEAD"}
	}
	m.nextWS++
	ws := fmt.Sprintf("w%d", m.nextWS)
	m.Workspaces[ws] = cwd
	f.c.nextPID++
	m.Meta[ws] = &WS{Label: label, Shell: f.c.nextPID}
	if op == "worktree" {
		m.Worktrees++
		m.Meta[ws].Worktree, m.Meta[ws].Branch = true, branch
	}
	return herdrx.Placement{WorkspaceID: ws, PaneID: ws + ":p1", Branch: branch}, nil
}

func (f fakeHerdr) WorkspaceCreate(ctx context.Context, machine, cwd, label string) (herdrx.Placement, error) {
	return f.open(machine, "workspace", cwd, "", label)
}

func (f fakeHerdr) WorktreeCreate(ctx context.Context, machine, cwd, branch, label string) (herdrx.Placement, error) {
	return f.open(machine, "worktree", cwd, branch, label)
}

func (f fakeHerdr) WorkspaceList(ctx context.Context, machine string) ([]herdrx.Workspace, error) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	m, err := f.c.call(machine, "workspace-list")
	if err != nil {
		return nil, err
	}
	var out []herdrx.Workspace
	for id := range m.Workspaces {
		w := herdrx.Workspace{ID: id}
		if meta := m.Meta[id]; meta != nil {
			w.Label = meta.Label
		}
		out = append(out, w)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out, nil
}

func (f fakeHerdr) WorktreeRemove(ctx context.Context, machine, ws string, force bool) error {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	m, err := f.c.call(machine, "worktree-remove")
	if err != nil {
		return err
	}
	meta, ok := m.Meta[ws]
	if _, open := m.Workspaces[ws]; !open || !ok {
		return &herdrx.Error{Code: "workspace_not_found", Message: ws}
	}
	if !meta.Worktree {
		return &herdrx.Error{Code: "not_a_worktree", Message: ws}
	}
	for _, a := range m.Agents {
		if a.Workspace == ws {
			return fmt.Errorf("refusing to remove a worktree with a live agent (test invariant)")
		}
	}
	if meta.Dirty && !force {
		return &herdrx.Error{Code: "dirty_worktree_requires_force", Message: "worktree has modified or untracked files"}
	}
	delete(m.Workspaces, ws)
	delete(m.Meta, ws)
	m.Worktrees--
	return nil
}

func (f fakeHerdr) WorkspaceClose(ctx context.Context, machine, ws string) error {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	m, err := f.c.call(machine, "close")
	if err != nil {
		return err
	}
	for name, a := range m.Agents {
		if a.Workspace == ws {
			if !f.c.AllowKill {
				return fmt.Errorf("refusing to close a workspace with a live agent (test invariant)")
			}
			delete(m.Agents, name)
			f.c.Killed = append(f.c.Killed, name)
		}
	}
	delete(m.Workspaces, ws)
	delete(m.Meta, ws)
	return nil
}

func (f fakeHerdr) AgentStart(ctx context.Context, machine string, req herdrx.StartRequest) error {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	m, err := f.c.call(machine, "start")
	if err != nil {
		return err
	}
	if !herdrx.ValidAgentName(req.Name) {
		return &herdrx.Error{Code: "invalid_name", Message: req.Name}
	}
	if _, ok := m.Agents[req.Name]; ok {
		return &herdrx.Error{Code: "agent_name_taken", Message: req.Name}
	}
	ws := ""
	for id := range m.Workspaces {
		if id+":p1" == req.Pane {
			ws = id
		}
	}
	if ws == "" {
		return &herdrx.Error{Code: "pane_not_found", Message: req.Pane}
	}
	f.c.nextPID++
	m.Agents[req.Name] = &Agent{Name: req.Name, Kind: req.Kind, Pane: req.Pane, Workspace: ws, Cwd: m.Workspaces[ws], PID: f.c.nextPID, RSSMB: 512}
	f.c.Starts[req.Name]++
	if m.LostReplies > 0 {
		m.LostReplies--
		return errors.New("herdr agent start: reply lost")
	}
	if m.AmbiguousStarts > 0 {
		m.AmbiguousStarts--
		m.ListFails++
		return errors.New("herdr agent start: ssh connection reset")
	}
	return nil
}

func (f fakeHerdr) AgentPrompt(ctx context.Context, machine, target, text string) error {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	m, err := f.c.call(machine, "prompt")
	if err != nil {
		return err
	}
	for _, a := range m.Agents {
		if a.Pane == target || a.Name == target {
			f.c.Prompts[a.Name]++
			return nil
		}
	}
	return &herdrx.Error{Code: "agent_not_found", Message: target}
}

func (f fakeHerdr) Agents(ctx context.Context, machine string) ([]herdrx.Agent, error) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	m, err := f.c.call(machine, "list")
	if err != nil {
		return nil, err
	}
	if m.ListFails > 0 {
		m.ListFails--
		return nil, errors.New("herdr agent list: ssh connection reset")
	}
	var out []herdrx.Agent
	for _, a := range m.Agents {
		out = append(out, herdrx.Agent{Name: a.Name, Agent: a.Kind, Status: "working", PaneID: a.Pane, WorkspaceID: a.Workspace, Cwd: a.Cwd})
	}
	return out, nil
}

func (f fakeHerdr) ProcessInfo(ctx context.Context, machine, pane string) (herdrx.ProcessInfo, error) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	m, err := f.c.call(machine, "process-info")
	if err != nil {
		return herdrx.ProcessInfo{}, err
	}
	ws := strings.TrimSuffix(pane, ":p1")
	meta, ok := m.Meta[ws]
	if _, open := m.Workspaces[ws]; !open || !ok {
		return herdrx.ProcessInfo{}, &herdrx.Error{Code: "pane_not_found", Message: pane}
	}
	pi := herdrx.ProcessInfo{PaneID: pane, ShellPID: meta.Shell}
	busy := false
	for _, a := range m.Agents {
		busy = busy || a.Pane == pane
	}
	if !busy || m.ResumeOnRestart {
		// Herdr lists an idle pane's shell as its foreground process.
		pi.Foreground = append(pi.Foreground, struct {
			PID int `json:"pid"`
		}{meta.Shell})
	}
	if m.ResumeOnRestart {
		pi.Foreground = append(pi.Foreground, struct {
			PID int `json:"pid"`
		}{meta.Shell + 100000})
	}
	for _, a := range m.Agents {
		if a.Pane == pane {
			pi.Foreground = append(pi.Foreground, struct {
				PID int `json:"pid"`
			}{a.PID})
		}
	}
	return pi, nil
}

// Prober returns the cluster's prober.
func (c *Cluster) Prober() coord.Prober { return fakeProber{c} }

type fakeProber struct{ c *Cluster }

func (p fakeProber) Probe(ctx context.Context, t coord.Target, req resource.Request) (resource.Snapshot, int64, error) {
	p.c.mu.Lock()
	defer p.c.mu.Unlock()
	p.c.Calls["probe"]++
	if t.Probe == coord.ProbeStatic {
		return resource.Static(t.Static, p.c.Clock.Now()), 0, nil
	}
	m, err := p.c.machine(t.ID)
	if err != nil {
		return resource.Snapshot{}, 0, err
	}
	if m.ProbeFails {
		return resource.Snapshot{}, 0, errors.New("probe failed")
	}
	used := m.BaseUsedMB
	util := m.UtilPct
	for _, a := range m.Agents {
		used += a.RSSMB
		util += 50 / float64(m.Cores) // each agent uses half a core
	}
	if util > 100 {
		util = 100
	}
	avail := m.TotalMB - used
	if avail < 0 {
		avail = 0
	}
	s := resource.Snapshot{Schema: resource.SchemaVersion, Backend: resource.BackendDarwin, Host: m.ID, OS: m.OS, Arch: m.Arch,
		CollectedAt: p.c.Clock.Now(), Home: "/Users/sim", Thermal: resource.ThermalNominal,
		Memory: resource.Memory{TotalMB: m.TotalMB, AvailableMB: avail, Pressure: m.Pressure, PressureSource: "sim"},
		CPU:    resource.CPU{LogicalCores: m.Cores, PhysicalCores: m.Cores, Load5: m.Load5, UtilPct: util, UtilSource: "sim"},
		Disk:   resource.Disk{Path: "/Users/sim", TotalGB: 1000, FreeGB: 500},
		Power:  resource.Power{Source: resource.PowerAC, BatteryPct: -1},
		Agents: resource.AgentKinds()}
	if len(req.Paths) > 0 {
		s.Paths = map[string]bool{}
		for _, path := range req.Paths {
			s.Paths[path] = !m.MissingPaths[path]
		}
	}
	for _, pid := range req.PIDs {
		// A pane's shell is measured with its descendants, so the agent
		// running in it is folded in.
		ps := resource.ProcSample{PID: pid}
		for id, meta := range m.Meta {
			if meta.Shell != pid {
				continue
			}
			ps.Found, ps.RSSMB, ps.Procs = true, 5, 1
			for _, a := range m.Agents {
				if a.Workspace == id {
					ps.RSSMB, ps.CPUPct, ps.Procs = a.RSSMB, 50, 3
				}
			}
		}
		for _, a := range m.Agents {
			if a.PID == pid {
				ps.Found, ps.RSSMB, ps.CPUPct, ps.Procs = true, a.RSSMB, 50, 3
			}
		}
		s.Processes = append(s.Processes, ps)
	}
	lat := int64(0)
	if t.ID != herdrx.Local {
		lat = 8
	}
	return s, lat, nil
}

// RestartHerdr restarts a machine's Herdr server: every agent ends, and each
// workspace comes back with a new shell in its pane.
func (c *Cluster) RestartHerdr(machine string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.Machines[machine]
	m.Agents = map[string]*Agent{}
	for _, meta := range m.Meta {
		c.nextPID++
		meta.Shell = c.nextPID
	}
}

// CloseSession closes the workspace an agent runs in, from outside the
// scheduler, as a person closing it in Herdr would.
func (c *Cluster) CloseSession(machine, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.Machines[machine]
	a, ok := m.Agents[name]
	if !ok {
		return
	}
	delete(m.Agents, name)
	delete(m.Workspaces, a.Workspace)
	delete(m.Meta, a.Workspace)
}

// Dirty marks the worktree an agent ran in as having uncommitted changes.
func (c *Cluster) Dirty(machine, workspace string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if meta := c.Machines[machine].Meta[workspace]; meta != nil {
		meta.Dirty = true
	}
}

// GitOps returns the cluster's Git operations runner.
func (c *Cluster) GitOps() coord.GitOps { return fakeGit{c} }

type fakeGit struct{ c *Cluster }

func (g fakeGit) Run(ctx context.Context, t coord.Target, r gitop.Request) (gitop.Result, error) {
	g.c.mu.Lock()
	defer g.c.mu.Unlock()
	m, err := g.c.machine(t.ID)
	if err != nil {
		return gitop.Result{}, err
	}
	if err := r.Validate(); err != nil {
		return gitop.Result{OK: false, Refused: true, Message: err.Error()}, nil
	}
	switch r.Op {
	case gitop.OpClone:
		g.c.GitLog = append(g.c.GitLog, t.ID+" clone "+r.URL)
		delete(m.MissingPaths, r.Path)
	case gitop.OpBranchDelete:
		g.c.GitLog = append(g.c.GitLog, t.ID+" branch-delete "+r.Branch)
	}
	return gitop.Result{OK: true}, nil
}

// Exit ends an agent, as if it finished its work.
func (c *Cluster) Exit(machine, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.Machines[machine].Agents, name)
}

// SetRSS sets what an agent is using.
func (c *Cluster) SetRSS(machine, name string, mb int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Machines[machine].Agents[name].RSSMB = mb
}

// Where reports which machine runs an agent, or "".
func (c *Cluster) Where(name string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, m := range c.Machines {
		if _, ok := m.Agents[name]; ok {
			return id
		}
	}
	return ""
}

// Live counts live agents on a machine.
func (c *Cluster) Live(machine string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.Machines[machine].Agents)
}

// DoubleStarts lists agent names started more than once.
func (c *Cluster) DoubleStarts() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for n, k := range c.Starts {
		if k > 1 {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}
