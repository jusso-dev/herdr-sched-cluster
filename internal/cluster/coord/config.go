// Package coord is the cluster coordinator. It discovers workers from
// Herdr's saved machines, probes them, plans with the queue and placement
// packages, takes capacity in the ledger, and dispatches through Herdr. It
// also reconciles what the ledger believes against what Herdr reports, so a
// restart, a dropped connection or a failed dispatch never leaves a job
// counted twice or started twice.
package coord

import (
	"fmt"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/placement"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/queue"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

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
}

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
	}
}

// Validate checks the values a person could get wrong.
func (c Config) Validate() error {
	if _, ok := placement.Policies[c.Policy]; !ok {
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
	return out
}
