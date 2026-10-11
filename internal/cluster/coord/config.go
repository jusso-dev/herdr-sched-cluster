// Package coord is the cluster coordinator. It discovers workers from
// Herdr's saved machines, probes them, plans with the queue and placement
// packages, takes capacity in the ledger, and dispatches through Herdr. It
// also reconciles what the ledger believes against what Herdr reports, so a
// restart, a dropped connection or a failed dispatch never leaves a job
// counted twice or started twice.
package coord

import (
	"fmt"
	"strings"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/placement"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/queue"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/transport"
)

const ledgerRunsKept = ledger.RunsKept

// Probe modes for a worker.
const (
	ProbeAuto   = "auto"   // local collector here, SSH probe on a remote
	ProbeStatic = "static" // declared capacity only (Windows, or no hsched)
	ProbeOff    = "off"    // never schedule here
)

// Isolation modes for a job.
const (
	IsolationAuto     = "auto"     // a worktree when the repo is already busy on that worker
	IsolationWorktree = "worktree" // always a new worktree
	IsolationNone     = "none"     // always the checkout itself
)

// WorkerConfig is what the operator declares about one worker. Workers
// themselves come from Herdr; this only adjusts them.
type WorkerConfig struct {
	// Probe is auto, static or off.
	Probe string
	// Static is the declared capacity when Probe is static.
	Static resource.StaticSpec
	// Settings are this worker's limits and preferences.
	Settings placement.Settings
	// Program is how the remote hsched is invoked over SSH, overriding
	// Config.ProbeProgram for this worker.
	Program string
	// PathMap rewrites repository paths for this worker, by prefix: the
	// key is the path as jobs name it, the value is where that tree lives
	// on this worker. The longest matching prefix wins, matched on whole
	// path segments.
	PathMap map[string]string
}

// Worktree retention policies.
const (
	RetainOnFailure = "retain-on-failure" // remove after a normal finish, keep after failed or lost
	AlwaysRetain    = "always-retain"
	AlwaysRemove    = "always-remove"
	RetainForAge    = "age" // keep for cluster_worktree_max_age_hours after the job ends, then remove
)

// Pressure actions.
const (
	PressureRecommend           = "recommend"
	PressureTerminateDisposable = "terminate-disposable"
)

// Config is the coordinator's configuration.
type Config struct {
	Policy             string
	Queue              queue.Config
	ProbeTTL           time.Duration
	BackoffBase        time.Duration
	BackoffMax         time.Duration
	OverrunMarginPct   float64
	HistoryHeadroomPct float64
	DefaultMemoryMB    int64
	DefaultCPU         float64
	MaxAttempts        int
	DispatchTimeout    time.Duration
	// DispatchGrace is how long a reserved or dispatching job may sit
	// without this process working on it before reconciliation decides
	// what happened to it.
	DispatchGrace time.Duration
	Isolation     string
	ProbeProgram  string
	// IncludeLocal schedules onto the computer the coordinator runs on.
	IncludeLocal bool
	// Defaults apply to every worker; Workers override per worker id or
	// label (the key "local" is this computer).
	Defaults placement.Settings
	Workers  map[string]WorkerConfig
	// ProbeConcurrency bounds parallel probes.
	ProbeConcurrency int

	// HistoryWindow and HistoryPercentile are how estimates are learned:
	// this percentile of the newest HistoryWindow runs.
	HistoryWindow     int
	HistoryPercentile float64

	// WorktreeRetention is the cleanup policy for scheduler-made worktrees.
	WorktreeRetention string
	// WorktreeMaxAge is how long the age policy keeps a worktree.
	WorktreeMaxAge time.Duration
	// WorktreeRemoveDirty lets cleanup remove a worktree with uncommitted
	// or untracked changes. Off by default: such a worktree is kept.
	WorktreeRemoveDirty bool
	// DeleteBranches also deletes the scheduler branch after its worktree
	// is removed, with git branch -d, which keeps unmerged work.
	DeleteBranches bool

	// LostAfter is how long a running job's worker may stay unreachable
	// before the job is recorded as lost. Its agent may still be running;
	// it is readopted if it turns up again.
	LostAfter time.Duration
	// RetryLost retries a lost job whose loss is confirmed, when the job was
	// submitted as restartable and has attempts left.
	RetryLost bool

	// MachineStatusEvery is how often `herdr machine status` is read and
	// folded into placement. Zero turns it off.
	MachineStatusEvery time.Duration

	// MaxQueueSize refuses a submit when this many jobs already wait.
	// Zero means no limit.
	MaxQueueSize int
	// QueueEnabled false refuses a submit that cannot be placed at once.
	QueueEnabled bool
	// MaxPerRepo caps active jobs per repository across the cluster.
	MaxPerRepo int

	// PressureAction is what happens when a worker's memory pressure is
	// critical: recommend only, or also terminate jobs submitted as
	// disposable once it has lasted PressureGrace.
	PressureAction string
	PressureGrace  time.Duration

	// VerifyIdentity remembers each worker's identity (host, OS,
	// architecture) on first contact and refuses work on it if that changes.
	VerifyIdentity bool
	// OwnerChecks limits cancel and detach to the job's submitter or the
	// operator.
	OwnerChecks bool
	// Explanations records a placement decision per job. Off keeps only
	// the summary on the event trail.
	Explanations bool
}

// DefaultConfig is the configuration with every default filled in.
func DefaultConfig() Config {
	return Config{
		Policy:             placement.PolicyBalanced,
		Queue:              queue.Config{Ageing: 10 * time.Minute, Starvation: 30 * time.Minute},
		ProbeTTL:           20 * time.Second,
		BackoffBase:        15 * time.Second,
		BackoffMax:         10 * time.Minute,
		OverrunMarginPct:   20,
		HistoryHeadroomPct: 25,
		DefaultMemoryMB:    3072,
		DefaultCPU:         1,
		MaxAttempts:        3,
		DispatchTimeout:    60 * time.Second,
		DispatchGrace:      5 * time.Minute,
		Isolation:          IsolationAuto,
		ProbeProgram:       "~/.local/bin/hsched",
		IncludeLocal:       true,
		Defaults:           placement.DefaultSettings(),
		Workers:            map[string]WorkerConfig{},
		ProbeConcurrency:   4,
		HistoryWindow:      20,
		HistoryPercentile:  90,
		WorktreeRetention:  RetainOnFailure,
		WorktreeMaxAge:     72 * time.Hour,
		LostAfter:          30 * time.Minute,
		RetryLost:          true,
		MachineStatusEvery: time.Minute,
		MaxQueueSize:       100,
		QueueEnabled:       true,
		PressureAction:     PressureRecommend,
		PressureGrace:      2 * time.Minute,
		VerifyIdentity:     true,
		OwnerChecks:        true,
		Explanations:       true,
	}
}

// Validate checks the values a person could get wrong.
func (c Config) Validate() error {
	if !placement.ValidPolicy(c.Policy) {
		return fmt.Errorf("cluster_policy %q is not one of %v", c.Policy, placement.PolicyNames)
	}
	switch c.Isolation {
	case IsolationAuto, IsolationWorktree, IsolationNone:
	default:
		return fmt.Errorf("cluster_isolation %q is not auto, worktree or none", c.Isolation)
	}
	if c.DefaultMemoryMB <= 0 || c.DefaultCPU <= 0 {
		return fmt.Errorf("cluster default memory and cpu must be positive")
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("cluster_max_attempts must be at least 1")
	}
	if c.HistoryWindow < 1 || c.HistoryWindow > ledgerRunsKept {
		return fmt.Errorf("cluster_history_window must be between 1 and %d", ledgerRunsKept)
	}
	if c.HistoryPercentile <= 0 || c.HistoryPercentile > 100 {
		return fmt.Errorf("cluster_history_percentile must be above 0 and at most 100")
	}
	switch c.WorktreeRetention {
	case RetainOnFailure, AlwaysRetain, AlwaysRemove, RetainForAge:
	default:
		return fmt.Errorf("cluster_worktree_retention %q is not retain-on-failure, always-retain, always-remove or age", c.WorktreeRetention)
	}
	if c.WorktreeRetention == RetainForAge && c.WorktreeMaxAge <= 0 {
		return fmt.Errorf("cluster_worktree_max_age_hours must be positive with the age policy")
	}
	switch c.PressureAction {
	case PressureRecommend, PressureTerminateDisposable:
	default:
		return fmt.Errorf("cluster_pressure_action %q is not recommend or terminate-disposable", c.PressureAction)
	}
	if c.MaxQueueSize < 0 || c.MaxPerRepo < 0 {
		return fmt.Errorf("cluster_max_queue_size and cluster_max_agents_per_repo cannot be negative")
	}
	if c.LostAfter <= 0 {
		return fmt.Errorf("cluster_lost_after_seconds must be positive")
	}
	if err := validSettings("defaults", c.Defaults); err != nil {
		return err
	}
	for k, w := range c.Workers {
		switch w.Probe {
		case "", ProbeAuto, ProbeOff:
		case ProbeStatic:
			if w.Static.MemoryMB <= 0 || w.Static.Cores <= 0 {
				return fmt.Errorf("worker %s: a static worker needs memory_mb and cores", k)
			}
		default:
			return fmt.Errorf("worker %s: probe %q is not auto, static or off", k, w.Probe)
		}
		if err := validSettings("worker "+k, w.Settings); err != nil {
			return err
		}
		for from, to := range w.PathMap {
			if err := transport.ValidatePath(from); err != nil {
				return fmt.Errorf("worker %s: path_map key: %v", k, err)
			}
			if err := transport.ValidatePath(to); err != nil {
				return fmt.Errorf("worker %s: path_map value: %v", k, err)
			}
		}
	}
	return nil
}

func validSettings(where string, s placement.Settings) error {
	if s.MaxCPUUtilPct > 100 {
		return fmt.Errorf("%s: max_cpu_utilisation_percent %.0f is above 100", where, s.MaxCPUUtilPct)
	}
	if s.MemoryReservePct < 0 || s.MemoryReservePct >= 100 {
		return fmt.Errorf("%s: memory_reserve_percent must be from 0 to below 100", where)
	}
	if s.MaxSwapPressure != "" {
		ok := false
		for _, l := range placement.SwapLevels {
			ok = ok || l == s.MaxSwapPressure
		}
		if !ok {
			return fmt.Errorf("%s: max_swap_pressure %q is not one of %v", where, s.MaxSwapPressure, placement.SwapLevels)
		}
	}
	if s.MaxAgents < 0 {
		return fmt.Errorf("%s: max_agents cannot be negative", where)
	}
	return nil
}

// merge lays the worker's own settings over the defaults: a zero value in
// the worker's settings means "use the default".
func merge(def, w placement.Settings) placement.Settings {
	out := def
	if w.MaxAgents != 0 {
		out.MaxAgents = w.MaxAgents
	}
	if w.MinFreeMemoryMB != 0 {
		out.MinFreeMemoryMB = w.MinFreeMemoryMB
	}
	if w.MaxCPUCommit != 0 {
		out.MaxCPUCommit = w.MaxCPUCommit
	}
	if w.MaxLoadPerCore != 0 {
		out.MaxLoadPerCore = w.MaxLoadPerCore
	}
	if w.MinFreeDiskGB != 0 {
		out.MinFreeDiskGB = w.MinFreeDiskGB
	}
	if w.MinBatteryPct != 0 {
		out.MinBatteryPct = w.MinBatteryPct
	}
	if w.Weight != 0 {
		out.Weight = w.Weight
	}
	if len(w.Labels) > 0 {
		out.Labels = w.Labels
	}
	if len(w.Agents) > 0 {
		out.Agents = w.Agents
	}
	if w.MaxCPUUtilPct != 0 {
		out.MaxCPUUtilPct = w.MaxCPUUtilPct
	}
	if w.MemoryReservePct != 0 {
		out.MemoryReservePct = w.MemoryReservePct
	}
	if w.MaxSwapPressure != "" {
		out.MaxSwapPressure = w.MaxSwapPressure
	}
	return out
}

// MapPath rewrites a repository path for one worker by the longest matching
// prefix in m, on whole path segments. A path with no match is unchanged.
func MapPath(m map[string]string, p string) string {
	best, to := "", ""
	for from, dst := range m {
		f := strings.TrimRight(from, "/")
		if f == "" {
			f = "/"
		}
		if p == f || strings.HasPrefix(p, f+"/") || (f == "/" && strings.HasPrefix(p, "/")) {
			if len(f) > len(best) {
				best, to = f, strings.TrimRight(dst, "/")
			}
		}
	}
	if best == "" {
		return p
	}
	if best == "/" {
		return to + p
	}
	return to + strings.TrimPrefix(p, best)
}
