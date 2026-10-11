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
	"sync"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/coord"
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

	Agents     map[string]*Agent
	Workspaces map[string]string // id -> cwd
	Worktrees  int
	nextWS     int
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
		MissingPaths: map[string]bool{}, Agents: map[string]*Agent{}, Workspaces: map[string]string{}}
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
		out = append(out, herdrx.MachineStatus{ID: m.ID, Label: m.Label, Status: st})
	}
	return out, nil
}

func (f fakeHerdr) open(machine, op, cwd string) (herdrx.Placement, error) {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	m, err := f.c.call(machine, op)
	if err != nil {
		return herdrx.Placement{}, err
	}
	if m.MissingPaths[cwd] {
		return herdrx.Placement{}, fmt.Errorf("herdr %s: %s does not exist", op, cwd)
	}
	m.nextWS++
	ws := fmt.Sprintf("w%d", m.nextWS)
	m.Workspaces[ws] = cwd
	if op == "worktree" {
		m.Worktrees++
	}
	return herdrx.Placement{WorkspaceID: ws, PaneID: ws + ":p1"}, nil
}

func (f fakeHerdr) WorkspaceCreate(ctx context.Context, machine, cwd, label string) (herdrx.Placement, error) {
	return f.open(machine, "workspace", cwd)
}

func (f fakeHerdr) WorktreeCreate(ctx context.Context, machine, cwd, branch, label string) (herdrx.Placement, error) {
	return f.open(machine, "worktree", cwd)
}

func (f fakeHerdr) WorkspaceClose(ctx context.Context, machine, ws string) error {
	f.c.mu.Lock()
	defer f.c.mu.Unlock()
	m, err := f.c.call(machine, "close")
	if err != nil {
		return err
	}
	for _, a := range m.Agents {
		if a.Workspace == ws {
			return fmt.Errorf("refusing to close a workspace with a live agent (test invariant)")
		}
	}
	delete(m.Workspaces, ws)
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
	for _, a := range m.Agents {
		if a.Pane == pane {
			return herdrx.ProcessInfo{PaneID: pane, ShellPID: a.PID}, nil
		}
	}
	return herdrx.ProcessInfo{}, &herdrx.Error{Code: "pane_not_found", Message: pane}
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
	for _, a := range m.Agents {
		used += a.RSSMB
	}
	avail := m.TotalMB - used
	if avail < 0 {
		avail = 0
	}
	s := resource.Snapshot{Schema: resource.SchemaVersion, Backend: resource.BackendDarwin, Host: m.ID, OS: m.OS, Arch: m.Arch,
		CollectedAt: p.c.Clock.Now(), Home: "/Users/sim", Thermal: resource.ThermalNominal,
		Memory: resource.Memory{TotalMB: m.TotalMB, AvailableMB: avail, Pressure: m.Pressure, PressureSource: "sim"},
		CPU:    resource.CPU{LogicalCores: m.Cores, PhysicalCores: m.Cores, Load5: m.Load5},
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
		ps := resource.ProcSample{PID: pid}
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
