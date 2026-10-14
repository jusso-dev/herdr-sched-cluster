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
	// StateHerdrDown is a worker whose own probe answers but which Herdr's
	// `machine status` reports as not reachable, so a dispatch through
	// `herdr --machine` would fail.
	StateHerdrDown = "herdr_unreachable"
)

// Swap pressure levels for MaxSwapPressure, from calm to thrashing, judged
// on pages swapped out per second over the probe's sample.
const (
	SwapNone     = "none"
	SwapLow      = "low"
	SwapModerate = "moderate"
	SwapHigh     = "high"
)

// SwapLevels lists the levels in order.
var SwapLevels = []string{SwapNone, SwapLow, SwapModerate, SwapHigh}

// SwapLevel classifies a swap-out rate. A negative rate is unknown, "".
func SwapLevel(outPerSec float64) string {
	switch {
	case outPerSec < 0:
		return ""
	case outPerSec < 1:
		return SwapNone
	case outPerSec < 50:
		return SwapLow
	case outPerSec < 500:
		return SwapModerate
	default:
		return SwapHigh
	}
}

func swapRank(l string) int {
	for i, x := range SwapLevels {
		if x == l {
			return i
		}
	}
	return -1
}

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
	// MaxCPUUtilPct refuses a job when the worker's measured CPU use plus
	// what is committed and not yet visible plus this job would pass it.
	// Zero means the default; a negative value turns the check off.
	MaxCPUUtilPct float64 `json:"max_cpu_utilisation_percent"`
	// MemoryReservePct keeps this share of total memory free for the
	// operating system and other applications, on top of MinFreeMemoryMB
	// (the larger of the two applies). Zero turns it off.
	MemoryReservePct float64 `json:"memory_reserve_percent"`
	// MaxSwapPressure is the worst swap activity (none, low, moderate,
	// high) at which new work is still admitted. Empty means the default,
	// moderate, so only heavy swapping refuses work.
	MaxSwapPressure string `json:"max_swap_pressure,omitempty"`
	// RequireNighthawk refuses a worker unless `nighthawk report` says
	// health is ok and the mode is unattended. A down daemon or attended
	// mode cannot take the job.
	RequireNighthawk bool `json:"require_nighthawk,omitempty"`
}

// DefaultSettings are used for anything the operator did not set.
func DefaultSettings() Settings {
	return Settings{MinFreeMemoryMB: 2048, MaxCPUCommit: 1.0, MaxLoadPerCore: 2.0, MinFreeDiskGB: 5, Weight: 1,
		MaxCPUUtilPct: 85, MaxSwapPressure: SwapModerate}
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
	// PendingCPU is the part of CommittedCPU the utilisation reading does
	// not show yet (jobs still starting, and estimates above what agents
	// are observed using). For a static worker it is all of it.
	PendingCPU float64 `json:"pending_cpu"`
	// CPUUtilPct is the coordinator's smoothed CPU utilisation, so one busy
	// quarter-second sample does not swing admission. Zero means use the
	// snapshot's own reading.
	CPUUtilPct float64 `json:"cpu_util_smoothed_pct,omitempty"`
	// ClusterRepos counts active jobs per repository across all workers,
	// for the per-repository concurrency limit.
	ClusterRepos map[string]int `json:"-"`
	// RunningRepos counts active jobs per repository here.
	RunningRepos map[string]int `json:"running_repos,omitempty"`
	// HeldFor names a queued job this worker is being kept clear for by the
	// starvation guard. Only that job may be placed here.
	HeldFor string `json:"held_for,omitempty"`
	// Nighthawk is "ok" or "down" from `nighthawk report`. Empty means it
	// was not required and was not read.
	Nighthawk string `json:"nighthawk,omitempty"`
	// NighthawkMode is attended or unattended when the report was read.
	NighthawkMode string `json:"nighthawk_mode,omitempty"`
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
	// ExclusiveRepo is set when a worktree cannot be made for this job (the
	// repository has no commits), so it may only run where no other job is
	// using the repository.
	ExclusiveRepo bool `json:"exclusive_repo,omitempty"`
	// MaxPerRepo caps active jobs in this job's repository across the whole
	// cluster. Zero means no cap.
	MaxPerRepo int `json:"max_per_repo,omitempty"`
	// Cloneable means the repository will be cloned onto a worker that does
	// not have it, so a missing checkout is not a hard constraint.
	Cloneable bool `json:"cloneable,omitempty"`
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
	// CPUAfterPct is the projected CPU utilisation with this job, against
	// MaxCPUPct.
	CPUAfterPct float64 `json:"cpu_after_pct"`
	MaxCPUPct   float64 `json:"max_cpu_pct,omitempty"`
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
	switch {
	case s.MaxCPUUtilPct == 0:
		s.MaxCPUUtilPct = d.MaxCPUUtilPct
	case s.MaxCPUUtilPct < 0:
		s.MaxCPUUtilPct = 0 // off
	}
	if s.MaxSwapPressure == "" {
		s.MaxSwapPressure = d.MaxSwapPressure
	}
	return s
}

// KeepFreeMB is the memory that must stay free here: the larger of the fixed
// floor and the reserve share of total memory.
func (w Worker) KeepFreeMB() int64 {
	s := w.Effective()
	keep := s.MinFreeMemoryMB
	if s.MemoryReservePct > 0 && w.Snapshot.Memory.TotalMB > 0 {
		if pct := int64(math.Ceil(float64(w.Snapshot.Memory.TotalMB) * s.MemoryReservePct / 100)); pct > keep {
			keep = pct
		}
	}
	return keep
}

// UtilPct is the CPU utilisation placement works from: the smoothed figure
// when the coordinator has one, else the snapshot's. A declared (static)
// worker has no measurement, so it is zero and PendingCPU carries the load.
func (w Worker) UtilPct() float64 {
	if w.Snapshot.CPU.UtilSource == "declared" {
		return 0
	}
	if w.CPUUtilPct > 0 {
		return w.CPUUtilPct
	}
	return w.Snapshot.CPU.UtilPct
}

// CPUAfterPct is the projected utilisation once job runs here.
func (w Worker) CPUAfterPct(job Job) float64 {
	cores := w.Cores()
	if cores <= 0 {
		return 0
	}
	return w.UtilPct() + (w.PendingCPU+job.CPU)/float64(cores)*100
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
	if job.Repo != "" && w.Snapshot.Paths != nil && !job.Cloneable {
		if present, asked := w.Snapshot.Paths[job.Repo]; asked && !present {
			r = append(r, fmt.Sprintf("repository %s does not exist on this worker", job.Repo))
		}
	}
	return r
}

// transient says whether a worker state can clear by itself, so a job that
// only fails on it is still worth queueing.
func transient(state string) bool {
	switch state {
	case StateDraining, StateUnreachable, StateBackoff, StateAuth, StateHerdrDown:
		return true
	}
	return false
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
		if keep := w.KeepFreeMB(); free < keep {
			r = append(r, fmt.Sprintf("memory: insufficient unreserved memory (%d MB free after this job, needs %d MB kept free)", free, keep))
		}
	}
	if lvl := SwapLevel(snap.Memory.SwapOutPerSec); lvl != "" && swapRank(s.MaxSwapPressure) >= 0 && swapRank(lvl) > swapRank(s.MaxSwapPressure) {
		r = append(r, fmt.Sprintf("swap pressure is %s (%.0f pages/s out), above the %s limit", lvl, snap.Memory.SwapOutPerSec, s.MaxSwapPressure))
	}
	if job.ExclusiveRepo && job.Repo != "" && w.RunningRepos[job.Repo] > 0 {
		r = append(r, "repository has no commits, so no worktree can be made, and another job is using its checkout here")
	}
	if job.MaxPerRepo > 0 && job.Repo != "" && w.ClusterRepos[job.Repo] >= job.MaxPerRepo {
		r = append(r, fmt.Sprintf("repository already has %d of at most %d agents across the cluster", w.ClusterRepos[job.Repo], job.MaxPerRepo))
	}
	if snap.Memory.Pressure == resource.PressureCritical {
		r = append(r, "memory pressure is critical")
	}
	if cores := w.Cores(); cores > 0 {
		if w.CommittedCPU+job.CPU > float64(cores)*s.MaxCPUCommit+1e-9 {
			r = append(r, fmt.Sprintf("cpu: %.1f of %.1f committable cores would be committed", w.CommittedCPU+job.CPU, float64(cores)*s.MaxCPUCommit))
		}
		if s.MaxCPUUtilPct > 0 {
			if after := w.CPUAfterPct(job); after > s.MaxCPUUtilPct+1e-9 {
				r = append(r, fmt.Sprintf("cpu: utilisation exceeds policy threshold (%.0f%% with this job, limit %.0f%%)", after, s.MaxCPUUtilPct))
			}
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
	if s.RequireNighthawk {
		switch {
		case w.Nighthawk != "ok":
			r = append(r, "nighthawk is down")
		case w.NighthawkMode != "unattended":
			r = append(r, "nighthawk is attended-only")
		}
	}
	if snap.Thermal == resource.ThermalCritical {
		r = append(r, "thermal state is critical")
	}
	return r
}

// Place decides where a job goes.
func Place(job Job, workers []Worker) Decision {
	policy := job.Policy
	why := ""
	if policy == PolicyAdaptive {
		policy, why = ResolveAdaptive(workers)
	}
	if _, ok := Policies[policy]; !ok {
		policy = PolicyBalanced
	}
	d := Decision{JobID: job.ID, Policy: policy}
	if why != "" {
		d.Policy = PolicyAdaptive + ":" + policy
	}
	for _, w := range workers {
		c := Candidate{WorkerID: w.ID, Label: w.Label, FreeAfterMB: w.FreeMemoryMB() - job.MemoryMB,
			CPUAfterPct: math.Round(w.CPUAfterPct(job)*10) / 10, MaxCPUPct: w.Effective().MaxCPUUtilPct}
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
	if why != "" {
		d.Summary += "; adaptive policy chose " + policy + " because " + why
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
		if w.Snapshot.Memory.TotalMB-w.KeepFreeMB() < job.MemoryMB {
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
			if !(len(r) == 1 && transient(w.State)) {
				why = append(why, nameOfW(w)+": "+strings.Join(r, "; "))
				continue
			}
		}
		s := w.Effective()
		if w.Snapshot.Memory.TotalMB > 0 && w.Snapshot.Memory.TotalMB-w.KeepFreeMB() < job.MemoryMB {
			why = append(why, fmt.Sprintf("%s: %d MB total, job needs %d MB plus %d MB kept free", nameOfW(w), w.Snapshot.Memory.TotalMB, job.MemoryMB, w.KeepFreeMB()))
			continue
		}
		if c := w.Cores(); c > 0 && s.MaxCPUUtilPct > 0 && job.CPU/float64(c)*100 > s.MaxCPUUtilPct {
			why = append(why, fmt.Sprintf("%s: job asks for %.1f cores, more than %.0f%% of %d cores", nameOfW(w), job.CPU, s.MaxCPUUtilPct, c))
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
		out = append(out, fmt.Sprintf("%s was not found on the PATH the probe saw; Herdr's own shell may still have it", job.Kind))
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
	if job.Cloneable && job.Repo != "" && w.Snapshot.Paths != nil {
		if present, asked := w.Snapshot.Paths[job.Repo]; asked && !present {
			out = append(out, fmt.Sprintf("repository %s is not here and will be cloned first", job.Repo))
		}
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
