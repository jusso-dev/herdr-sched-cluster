// Package placement decides which worker a job should run on.
//
// It is pure: it is handed workers and a job and returns a decision with its
// reasons. Hard constraints are applied first and a worker that breaks one is
// never chosen, whatever its score. Only eligible workers are scored, and the
// highest score wins, ties broken by worker id so the same inputs always give
// the same answer.
package placement

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

// Worker states. Only StateReady takes new work.
const (
	StateReady        = "ready"
	StateDraining     = "draining"
	StateDisabled     = "disabled"
	StateUnreachable  = "unreachable"
	StateAuth         = "auth_required"
	StateIncompatible = "incompatible"
	StateBackoff      = "backoff"
)

// Settings are the operator's limits for one worker.
type Settings struct {
	// MaxAgents caps how many scheduled agents run here. Zero means no cap.
	MaxAgents int `json:"max_agents"`
	// MinFreeMemoryMB is what must stay free after a job is placed.
	MinFreeMemoryMB int64 `json:"min_free_memory_mb"`
	// MaxCPUCommit is how many cores' worth of job CPU estimates may be
	// committed per core. 1.0 means one estimated core per real core.
	MaxCPUCommit float64 `json:"max_cpu_commit"`
	// MaxLoadPerCore refuses new work while the five-minute load average per
	// core is above it. Zero means the default; a negative value turns the
	// check off.
	MaxLoadPerCore float64 `json:"max_load_per_core"`
	// MinFreeDiskGB must stay free on the home filesystem. Zero means the
	// default; a negative value turns the check off.
	MinFreeDiskGB float64 `json:"min_free_disk_gb"`
	// MinBatteryPct refuses new work on battery below this level. Zero turns
	// the check off.
	MinBatteryPct int `json:"min_battery_pct"`
	// Weight is the operator's preference, 0 to 2, default 1.
	Weight float64 `json:"weight"`
	// Labels are free-form tags a job can require, such as "gpu" or "big".
	Labels []string `json:"labels,omitempty"`
	// Agents, when set, is the authoritative list of agent kinds here and
	// overrides what the probe found on PATH.
	Agents []string `json:"agents,omitempty"`
}

// DefaultSettings are used for anything the operator did not set.
func DefaultSettings() Settings {
	return Settings{MinFreeMemoryMB: 2048, MaxCPUCommit: 1.0, MaxLoadPerCore: 2.0, MinFreeDiskGB: 5, Weight: 1}
}

// Worker is one machine as placement sees it.
type Worker struct {
	ID       string            `json:"id"`
	Label    string            `json:"label"`
	Local    bool              `json:"local"`
	State    string            `json:"state"`
	Snapshot resource.Snapshot `json:"snapshot"`
	// LatencyMS is the probe round trip, zero for Local.
	LatencyMS int64    `json:"latency_ms"`
	Settings  Settings `json:"settings"`
	// ActiveAgents is how many scheduled jobs hold a reservation here.
	ActiveAgents int `json:"active_agents"`
	// PendingMemoryMB is reserved memory the snapshot does not yet reflect:
	// jobs reserved, dispatching or still settling, plus overruns. For a
	// static worker it is every reservation.
	PendingMemoryMB int64 `json:"pending_memory_mb"`
	// CommittedCPU is the sum of CPU estimates of every job here.
	CommittedCPU float64 `json:"committed_cpu"`
	// RunningRepos counts active jobs per repository here.
	RunningRepos map[string]int `json:"running_repos,omitempty"`
	// HeldFor names a queued job this worker is being kept clear for by the
	// starvation guard. Only that job may be placed here.
	HeldFor string `json:"held_for,omitempty"`
}

// Job is what placement needs to know about a request.
type Job struct {
	ID       string   `json:"id"`
	Kind     string   `json:"kind"`
	Repo     string   `json:"repo"`
	MemoryMB int64    `json:"memory_mb"`
	CPU      float64  `json:"cpu"`
	Policy   string   `json:"policy"`
	Machine  string   `json:"machine,omitempty"`
	Labels   []string `json:"labels,omitempty"`
	OS       string   `json:"os,omitempty"`
	Arch     string   `json:"arch,omitempty"`
}

// Candidate is one worker's part in a decision.
type Candidate struct {
	WorkerID string             `json:"worker"`
	Label    string             `json:"label"`
	Eligible bool               `json:"eligible"`
	Reasons  []string           `json:"reasons,omitempty"`
	Warnings []string           `json:"warnings,omitempty"`
	Score    float64            `json:"score"`
	Factors  map[string]float64 `json:"factors,omitempty"`
	// FreeAfterMB is memory left after this job, before the safety margin.
	FreeAfterMB int64 `json:"free_after_mb"`
}

// Decision is the answer for one job.
type Decision struct {
	JobID      string      `json:"job"`
	Policy     string      `json:"policy"`
	Chosen     string      `json:"chosen,omitempty"`
	Candidates []Candidate `json:"candidates"`
	Summary    string      `json:"summary"`
}

// Effective returns the worker's settings with defaults filled in.
func (w Worker) Effective() Settings {
	s, d := w.Settings, DefaultSettings()
	if s.MinFreeMemoryMB <= 0 {
		s.MinFreeMemoryMB = d.MinFreeMemoryMB
	}
	if s.MaxCPUCommit <= 0 {
		s.MaxCPUCommit = d.MaxCPUCommit
	}
	switch {
	case s.MaxLoadPerCore == 0:
		s.MaxLoadPerCore = d.MaxLoadPerCore
	case s.MaxLoadPerCore < 0:
		s.MaxLoadPerCore = 0 // off
	}
	switch {
	case s.MinFreeDiskGB == 0:
		s.MinFreeDiskGB = d.MinFreeDiskGB
	case s.MinFreeDiskGB < 0:
		s.MinFreeDiskGB = 0 // off
	}
	if s.Weight <= 0 {
		s.Weight = d.Weight
	}
	if s.Weight > 2 {
		s.Weight = 2
	}
	return s
}

// FreeMemoryMB is what this worker can still hand out, before the margin.
func (w Worker) FreeMemoryMB() int64 {
	return w.Snapshot.Memory.AvailableMB - w.PendingMemoryMB
}

// Cores is the core count capacity is measured against.
func (w Worker) Cores() int {
	if w.Snapshot.CPU.LogicalCores > 0 {
		return w.Snapshot.CPU.LogicalCores
	}
	return w.Snapshot.CPU.PhysicalCores
}

// Static checks every constraint that does not depend on how busy the
// worker is: state, pinning, platform, labels and agent kind. A job that
// fails one of these will not fit here however long it waits.
func Static(job Job, w Worker) []string {
	var r []string
	if job.Machine != "" && job.Machine != w.ID && job.Machine != w.Label {
		r = append(r, fmt.Sprintf("job is pinned to %s", job.Machine))
	}
	switch w.State {
	case StateReady:
	case StateDraining:
		r = append(r, "worker is draining")
	case "":
		r = append(r, "worker state unknown")
	default:
		r = append(r, "worker is "+strings.ReplaceAll(w.State, "_", " "))
	}
	if job.OS != "" && job.OS != w.Snapshot.OS {
		r = append(r, fmt.Sprintf("job needs %s, worker runs %s", job.OS, w.Snapshot.OS))
	}
	if job.Arch != "" && job.Arch != w.Snapshot.Arch {
		r = append(r, fmt.Sprintf("job needs %s, worker is %s", job.Arch, w.Snapshot.Arch))
	}
	s := w.Effective()
	for _, l := range job.Labels {
		if !contains(s.Labels, l) {
			r = append(r, fmt.Sprintf("worker lacks label %q", l))
		}
	}
	if len(s.Agents) > 0 && job.Kind != "" && !contains(s.Agents, job.Kind) {
		r = append(r, fmt.Sprintf("agent %s is not offered on this worker", job.Kind))
	}
	if job.Repo != "" && w.Snapshot.Paths != nil {
		if present, asked := w.Snapshot.Paths[job.Repo]; asked && !present {
			r = append(r, fmt.Sprintf("repository %s does not exist on this worker", job.Repo))
		}
	}
	return r
}

// Eligible returns every hard constraint the worker breaks for this job. An
// empty answer means the job may be placed there.
func Eligible(job Job, w Worker) []string {
	r := Static(job, w)
	s := w.Effective()
	snap := w.Snapshot
	if w.HeldFor != "" && w.HeldFor != job.ID {
		r = append(r, fmt.Sprintf("held clear for waiting job %s", w.HeldFor))
	}
	if snap.Memory.TotalMB <= 0 {
		r = append(r, "no memory reading")
	} else {
		free := w.FreeMemoryMB() - job.MemoryMB
		if free < s.MinFreeMemoryMB {
			r = append(r, fmt.Sprintf("memory: %d MB free after this job, needs %d MB kept free", free, s.MinFreeMemoryMB))
		}
	}
	if snap.Memory.Pressure == resource.PressureCritical {
		r = append(r, "memory pressure is critical")
	}
	if cores := w.Cores(); cores > 0 {
		if w.CommittedCPU+job.CPU > float64(cores)*s.MaxCPUCommit+1e-9 {
			r = append(r, fmt.Sprintf("cpu: %.1f of %.1f committable cores would be committed", w.CommittedCPU+job.CPU, float64(cores)*s.MaxCPUCommit))
		}
		if s.MaxLoadPerCore > 0 && snap.CPU.Load5/float64(cores) > s.MaxLoadPerCore {
			r = append(r, fmt.Sprintf("cpu: 5-minute load %.1f per core is above %.1f", snap.CPU.Load5/float64(cores), s.MaxLoadPerCore))
		}
	} else if job.CPU > 0 {
		r = append(r, "no cpu reading")
	}
	if s.MaxAgents > 0 && w.ActiveAgents+1 > s.MaxAgents {
		r = append(r, fmt.Sprintf("already running %d of at most %d agents", w.ActiveAgents, s.MaxAgents))
	}
	if snap.Backend != resource.BackendStatic && snap.Disk.TotalGB > 0 && snap.Disk.FreeGB < s.MinFreeDiskGB {
		r = append(r, fmt.Sprintf("disk: %.1f GB free, needs %.1f GB", snap.Disk.FreeGB, s.MinFreeDiskGB))
	}
	if s.MinBatteryPct > 0 && snap.Power.Source == resource.PowerBattery && snap.Power.BatteryPct >= 0 && snap.Power.BatteryPct < s.MinBatteryPct {
		r = append(r, fmt.Sprintf("on battery at %d%%, below %d%%", snap.Power.BatteryPct, s.MinBatteryPct))
	}
	if snap.Thermal == resource.ThermalCritical {
		r = append(r, "thermal state is critical")
	}
	return r
}

// Place decides where a job goes.
func Place(job Job, workers []Worker) Decision {
	policy := job.Policy
	if _, ok := Policies[policy]; !ok {
		policy = PolicyBalanced
	}
	d := Decision{JobID: job.ID, Policy: policy}
	for _, w := range workers {
		c := Candidate{WorkerID: w.ID, Label: w.Label, FreeAfterMB: w.FreeMemoryMB() - job.MemoryMB}
		c.Reasons = Eligible(job, w)
		c.Eligible = len(c.Reasons) == 0
		c.Warnings = warnings(job, w)
		if c.Eligible {
			c.Score, c.Factors = Score(job, w, policy)
		}
		d.Candidates = append(d.Candidates, c)
	}
	sort.SliceStable(d.Candidates, func(i, j int) bool {
		a, b := d.Candidates[i], d.Candidates[j]
		if a.Eligible != b.Eligible {
			return a.Eligible
		}
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		return a.WorkerID < b.WorkerID
	})
	if len(d.Candidates) > 0 && d.Candidates[0].Eligible {
		best := d.Candidates[0]
		d.Chosen = best.WorkerID
		d.Summary = fmt.Sprintf("placed on %s (score %.1f, %s policy, %d MB free after)", nameOf(best), best.Score, policy, best.FreeAfterMB)
		if len(d.Candidates) > 1 && d.Candidates[1].Eligible {
			d.Summary += fmt.Sprintf("; next best %s at %.1f", nameOf(d.Candidates[1]), d.Candidates[1].Score)
		}
	} else {
		d.Summary = "no worker can take this job now"
		if len(d.Candidates) == 0 {
			d.Summary = "no workers are registered"
		} else {
			parts := []string{}
			for _, c := range d.Candidates {
				parts = append(parts, nameOf(c)+": "+strings.Join(c.Reasons, "; "))
			}
			d.Summary += " (" + strings.Join(parts, " | ") + ")"
		}
	}
	return d
}

// Nearest is the worker the starvation guard holds clear for a job that
// fits nowhere: among workers it could ever run on, the one with the most
// memory left after it. Empty when no worker could ever take it.
func Nearest(job Job, workers []Worker) string {
	best, bestFree := "", int64(math.MinInt64)
	for _, w := range workers {
		if len(Static(job, w)) > 0 {
			continue
		}
		if w.Snapshot.Memory.TotalMB-w.Effective().MinFreeMemoryMB < job.MemoryMB {
			// Bigger than the machine: waiting will never help.
			continue
		}
		free := w.FreeMemoryMB() - job.MemoryMB
		if free > bestFree || (free == bestFree && w.ID < best) {
			best, bestFree = w.ID, free
		}
	}
	return best
}

// FitsAnywhereEver says whether some worker could take the job once it was
// empty. A job that cannot is refused at submit rather than queued forever.
func FitsAnywhereEver(job Job, workers []Worker) (bool, []string) {
	var why []string
	for _, w := range workers {
		if r := Static(job, w); len(r) > 0 {
			if !(len(r) == 1 && (w.State == StateDraining || w.State == StateUnreachable || w.State == StateBackoff || w.State == StateAuth)) {
				why = append(why, nameOfW(w)+": "+strings.Join(r, "; "))
				continue
			}
		}
		s := w.Effective()
		if w.Snapshot.Memory.TotalMB > 0 && w.Snapshot.Memory.TotalMB-s.MinFreeMemoryMB < job.MemoryMB {
			why = append(why, fmt.Sprintf("%s: %d MB total, job needs %d MB plus %d MB kept free", nameOfW(w), w.Snapshot.Memory.TotalMB, job.MemoryMB, s.MinFreeMemoryMB))
			continue
		}
		if c := w.Cores(); c > 0 && job.CPU > float64(c)*s.MaxCPUCommit {
			why = append(why, fmt.Sprintf("%s: job asks for %.1f cores, at most %.1f can be committed", nameOfW(w), job.CPU, float64(c)*s.MaxCPUCommit))
			continue
		}
		return true, nil
	}
	return false, why
}

func warnings(job Job, w Worker) []string {
	var out []string
	s := w.Effective()
	if len(s.Agents) == 0 && job.Kind != "" && w.Snapshot.Backend != resource.BackendStatic && !contains(w.Snapshot.Agents, job.Kind) {
		out = append(out, fmt.Sprintf("%s was not found on this worker's PATH over SSH; Herdr's shell may still have it", job.Kind))
	}
	if w.Snapshot.Memory.Pressure == resource.PressureWarn {
		out = append(out, "memory pressure is elevated")
	}
	if w.Snapshot.Memory.SwapOutPerSec > 50 {
		out = append(out, fmt.Sprintf("swapping out %.0f pages/s", w.Snapshot.Memory.SwapOutPerSec))
	}
	if w.Snapshot.Thermal == resource.ThermalSerious {
		out = append(out, "thermal state is serious")
	}
	if w.Snapshot.Backend == resource.BackendStatic {
		out = append(out, "capacity is declared, not measured")
	}
	return out
}

func nameOf(c Candidate) string {
	if c.Label != "" && c.Label != c.WorkerID {
		return c.Label
	}
	return c.WorkerID
}

func nameOfW(w Worker) string { return nameOf(Candidate{WorkerID: w.ID, Label: w.Label}) }

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
