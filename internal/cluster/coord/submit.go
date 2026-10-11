package coord

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/gitop"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/herdrx"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/placement"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/transport"
)

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
	// Restartable says the job is safe to start again from scratch if it
	// is lost. Disposable says it may be closed under critical memory
	// pressure when cluster_pressure_action allows it.
	Restartable bool
	Disposable  bool
	// DependsOn names jobs that must finish first.
	DependsOn []string
	// CloneURL is cloned to Repo on a worker that does not have it.
	CloneURL string
}

var (
	argRe   = regexp.MustCompile(`^[A-Za-z0-9._/:=,@+%-]{1,256}$`)
	labelRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	jobIDRe = regexp.MustCompile(`^j[0-9]{1,9}$`)
)

// MaxPromptChars bounds a prompt.
const MaxPromptChars = 100_000

// MaxDependencies bounds depends_on.
const MaxDependencies = 20

// Submit validates a request, estimates what it needs when the caller did
// not say, checks that some worker could ever run it, and queues it.
func (c *Coordinator) Submit(ctx context.Context, r SubmitRequest) (ledger.Job, []string, error) {
	r, est, notes, err := c.prepare(ctx, r)
	if err != nil {
		return ledger.Job{}, nil, err
	}
	if c.Cfg.MaxQueueSize > 0 {
		n, err := c.L.CountQueued(ctx)
		if err != nil {
			return ledger.Job{}, nil, err
		}
		if n >= c.Cfg.MaxQueueSize {
			return ledger.Job{}, nil, fmt.Errorf("the queue is full (%d jobs waiting, cluster_max_queue_size is %d); try again later", n, c.Cfg.MaxQueueSize)
		}
	}
	pj := placement.Job{Kind: r.Kind, Repo: r.Repo, MemoryMB: r.MemoryMB, CPU: r.CPU, Policy: r.Policy, Machine: r.Machine,
		Labels: r.Labels, OS: r.OS, Arch: r.Arch, MaxPerRepo: c.Cfg.MaxPerRepo, Cloneable: r.CloneURL != ""}
	workers, err := c.Workers(ctx, false, nil)
	if err != nil {
		return ledger.Job{}, nil, err
	}
	if ok, why := placement.FitsAnywhereEver(pj, workers); !ok {
		return ledger.Job{}, nil, fmt.Errorf("no worker could ever run this job:\n  %s", strings.Join(why, "\n  "))
	}
	if !c.Cfg.QueueEnabled {
		if pj.Policy == "" {
			pj.Policy = c.Cfg.Policy
		}
		if d := placement.Place(pj, workers); d.Chosen == "" {
			return ledger.Job{}, nil, fmt.Errorf("no worker can take this job now and queueing is off (cluster_queue_enabled): %s", d.Summary)
		}
	}
	job, err := c.L.Submit(ctx, ledger.NewJob{Kind: r.Kind, Repo: r.Repo, Prompt: r.Prompt, AgentArgs: r.AgentArgs,
		MemoryMB: r.MemoryMB, CPU: r.CPU, EstimateSource: est.memory, CPUSource: est.cpu, Priority: r.Priority, Policy: r.Policy,
		Machine: r.Machine, Labels: r.Labels, OS: r.OS, Arch: r.Arch, Isolation: r.Isolation,
		SubmittedBy: r.SubmittedBy, MaxAttempts: c.Cfg.MaxAttempts, Restartable: r.Restartable, Disposable: r.Disposable,
		DependsOn: r.DependsOn, CloneURL: r.CloneURL})
	return job, notes, err
}

type sources struct{ memory, cpu string }

// prepare validates a request and fills in the memory and CPU estimates the
// caller left out, learned from the process trees of earlier runs. Submit and
// Plan share it, so a plan refuses exactly what a submit would.
func (c *Coordinator) prepare(ctx context.Context, r SubmitRequest) (SubmitRequest, sources, []string, error) {
	var notes []string
	src := sources{memory: "requested", cpu: "requested"}
	if _, ok := resource.AgentExecutables[r.Kind]; !ok {
		return r, src, nil, fmt.Errorf("unknown agent kind %q; one of %v", r.Kind, resource.AgentKinds())
	}
	if err := transport.ValidatePath(r.Repo); err != nil {
		return r, src, nil, fmt.Errorf("repository: %w", err)
	}
	if len(r.Prompt) > MaxPromptChars {
		return r, src, nil, fmt.Errorf("prompt is %d characters; the limit is %d", len(r.Prompt), MaxPromptChars)
	}
	for _, a := range r.AgentArgs {
		if !argRe.MatchString(a) {
			return r, src, nil, fmt.Errorf("agent argument %q has characters outside [A-Za-z0-9._/:=,@+%%-]", a)
		}
	}
	for _, l := range r.Labels {
		if !labelRe.MatchString(l) {
			return r, src, nil, fmt.Errorf("label %q must be lower case letters, digits, dot, dash or underscore", l)
		}
	}
	if r.Policy != "" && !placement.ValidPolicy(r.Policy) {
		return r, src, nil, fmt.Errorf("policy %q is not one of %v", r.Policy, placement.PolicyNames)
	}
	switch r.Isolation {
	case "", IsolationAuto, IsolationWorktree, IsolationNone:
	default:
		return r, src, nil, fmt.Errorf("isolation %q is not auto, worktree or none", r.Isolation)
	}
	if r.Priority < -100 || r.Priority > 100 {
		return r, src, nil, fmt.Errorf("priority must be between -100 and 100")
	}
	if r.MemoryMB < 0 || r.CPU < 0 {
		return r, src, nil, fmt.Errorf("memory and cpu cannot be negative")
	}
	if len(r.DependsOn) > MaxDependencies {
		return r, src, nil, fmt.Errorf("at most %d dependencies", MaxDependencies)
	}
	for _, d := range r.DependsOn {
		if !jobIDRe.MatchString(d) {
			return r, src, nil, fmt.Errorf("dependency %q is not a job id like j12", d)
		}
	}
	if r.CloneURL != "" {
		if err := gitop.ValidURL(r.CloneURL); err != nil {
			return r, src, nil, err
		}
	}
	if r.MemoryMB == 0 || r.CPU == 0 {
		p, err := c.L.Profile(ctx, r.Kind, r.Repo, c.Cfg.HistoryWindow, c.Cfg.HistoryPercentile)
		if err != nil {
			return r, src, nil, err
		}
		head := 1 + c.Cfg.HistoryHeadroomPct/100
		if r.MemoryMB == 0 {
			if p.MemoryRuns > 0 {
				r.MemoryMB, src.memory = int64(math.Ceil(float64(p.MemoryMB)*head)), p.MemorySource
			} else {
				r.MemoryMB, src.memory = c.Cfg.DefaultMemoryMB, "default"
			}
			notes = append(notes, fmt.Sprintf("memory estimate %d MB (%s)", r.MemoryMB, src.memory))
		}
		if r.CPU == 0 {
			if p.CPURuns > 0 {
				// A tenth of a core is the finest step worth reserving.
				r.CPU, src.cpu = math.Max(0.1, math.Ceil(p.CPU*head*10)/10), p.CPUSource
			} else {
				r.CPU, src.cpu = c.Cfg.DefaultCPU, "default"
			}
			notes = append(notes, fmt.Sprintf("cpu estimate %.1f cores (%s)", r.CPU, src.cpu))
		}
	}
	return r, src, notes, nil
}

// AttachHint says how a person can look at a running job's agent.
type AttachHint struct {
	Worker   string   `json:"worker"`
	Agent    string   `json:"agent"`
	Commands []string `json:"commands"`
	Note     string   `json:"note,omitempty"`
}

// Attach returns how to open a job's agent in Herdr. It changes nothing.
func (c *Coordinator) Attach(ctx context.Context, id string) (AttachHint, error) {
	j, err := c.L.Get(ctx, id)
	if err != nil {
		return AttachHint{}, err
	}
	if j.Worker == "" || (j.State != ledger.Running && j.State != ledger.Dispatching && j.State != ledger.Lost) {
		return AttachHint{}, fmt.Errorf("%s is %s, with no agent to attach to", id, j.State)
	}
	return c.attachHint(ctx, j), nil
}

func (c *Coordinator) attachHint(ctx context.Context, j ledger.Job) AttachHint {
	name := ledger.AgentName(j.ID)
	h := AttachHint{Worker: j.Worker, Agent: name}
	if j.Worker == herdrx.Local {
		h.Commands = []string{"herdr agent focus " + name}
		return h
	}
	t := c.target(j.Worker)
	if t.SSHTarget == "" {
		if ts, err := c.find(ctx, j.Worker); err == nil {
			t = ts
		}
	}
	remote := "herdr --remote " + t.SSHTarget
	if t.Session != "" {
		remote += " --session " + t.Session
	}
	h.Commands = []string{remote, "herdr agent focus " + name}
	h.Note = "the first command opens the worker's Herdr in this terminal; run the second inside it. " +
		"From this machine, herdr --machine " + j.Worker + " agent list shows it without attaching"
	if t.SSHTarget == "" {
		h.Commands = []string{"herdr --machine " + j.Worker + " agent list"}
		h.Note = "the worker's SSH target is not known here; see herdr machine list"
	}
	return h
}

// Explanation is why a job is where it is.
type Explanation struct {
	Job          ledger.Job          `json:"job"`
	Decisions    []ledger.Decision   `json:"decisions"`
	Now          *placement.Decision `json:"now,omitempty"`
	Dependencies map[string]string   `json:"dependencies,omitempty"`
	Attach       *AttachHint         `json:"attach,omitempty"`
	// Learned is what history says about this kind of job, for comparison
	// with what it asked for.
	Learned *ledger.Profile `json:"learned,omitempty"`
	// NextAction says what happens next, for a job still waiting.
	NextAction string `json:"next_action,omitempty"`
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
	if len(j.DependsOn) > 0 {
		if ex.Dependencies, err = c.L.States(ctx, j.DependsOn); err != nil {
			return ex, err
		}
	}
	if p, err := c.L.Profile(ctx, j.Kind, j.Repo, c.Cfg.HistoryWindow, c.Cfg.HistoryPercentile); err == nil && (p.MemoryRuns > 0 || p.CPURuns > 0) {
		ex.Learned = &p
	}
	switch j.State {
	case ledger.Queued:
		ws, err := c.Workers(ctx, false, nil)
		if err != nil {
			return ex, err
		}
		d := placement.Place(c.PlacementJob(j), ws)
		ex.Now = &d
		ex.NextAction = nextAction(d.Chosen != "", false, j)
	case ledger.Running, ledger.Dispatching:
		h := c.attachHint(ctx, j)
		ex.Attach = &h
	}
	return ex, nil
}

// Estimates lists what has been learned per agent kind and repository.
func (c *Coordinator) Estimates(ctx context.Context) ([]ledger.Profile, error) {
	return c.L.Profiles(ctx, c.Cfg.HistoryWindow, c.Cfg.HistoryPercentile)
}
