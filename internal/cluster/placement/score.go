package placement

import (
	"math"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

// Policy names.
const (
	PolicyBalanced    = "balanced"
	PolicyMemory      = "memory"
	PolicyPerformance = "performance"
	PolicyPack        = "pack"
	PolicySpread      = "spread"
	PolicyLocalFirst  = "local-first"
)

// Factor names, each normalised to 0..1 where 1 is better.
const (
	FMemory     = "memory_headroom"
	FPressure   = "memory_pressure"
	FCPU        = "cpu_headroom"
	FAgents     = "agent_count"
	FLocality   = "repo_locality"
	FAffinity   = "repo_affinity"
	FLatency    = "latency"
	FPreference = "preference"
	FPower      = "power_thermal"
	FCores      = "core_count"
	FPacking    = "packing"
	FLocal      = "local"
)

// Weights is one policy's weight per factor.
type Weights map[string]float64

// Policies are the weight presets. A factor missing from a preset counts for
// nothing in it.
var Policies = map[string]Weights{
	PolicyBalanced: {FMemory: 25, FPressure: 15, FCPU: 15, FAgents: 10, FLocality: 10, FAffinity: 5, FLatency: 5, FPreference: 10, FPower: 5},
	PolicyMemory:   {FMemory: 45, FPressure: 25, FCPU: 5, FAgents: 5, FLocality: 5, FPreference: 10, FPower: 5},
	PolicyPerformance: {FCores: 20, FCPU: 25, FMemory: 15, FPressure: 10, FLatency: 10,
		FLocality: 5, FPreference: 10, FPower: 5},
	PolicyPack:       {FPacking: 35, FAffinity: 20, FLocality: 15, FPressure: 10, FPreference: 10, FLatency: 10},
	PolicySpread:     {FAgents: 35, FMemory: 20, FCPU: 15, FPressure: 10, FPreference: 10, FLatency: 5, FPower: 5},
	PolicyLocalFirst: {FLocal: 50, FMemory: 13, FPressure: 8, FCPU: 8, FAgents: 5, FLocality: 5, FAffinity: 3, FPreference: 5, FPower: 3},
}

// PolicyNames lists the presets in documentation order.
var PolicyNames = []string{PolicyBalanced, PolicyMemory, PolicyPerformance, PolicyPack, PolicySpread, PolicyLocalFirst}

// Score rates an eligible worker for a job on 0..100. The factors map holds
// each factor's raw 0..1 value, so an explanation can show both.
func Score(job Job, w Worker, policy string) (float64, map[string]float64) {
	weights, ok := Policies[policy]
	if !ok {
		weights = Policies[PolicyBalanced]
	}
	f := Factors(job, w)
	var sum, total float64
	for name, wt := range weights {
		sum += wt * f[name]
		total += wt
	}
	if total == 0 {
		return 0, f
	}
	return math.Round(1000*sum/total) / 10, f
}

// Factors computes every factor for a worker, whatever the policy.
func Factors(job Job, w Worker) map[string]float64 {
	snap := w.Snapshot
	s := w.Effective()
	f := map[string]float64{}

	if snap.Memory.TotalMB > 0 {
		f[FMemory] = clamp01(float64(w.FreeMemoryMB()-job.MemoryMB) / float64(snap.Memory.TotalMB))
	}
	f[FPacking] = 1 - f[FMemory]
	switch snap.Memory.Pressure {
	case resource.PressureNormal:
		f[FPressure] = 1
	case resource.PressureWarn:
		f[FPressure] = 0.4
	case resource.PressureUnknown, "":
		f[FPressure] = 0.6
	}
	if cores := w.Cores(); cores > 0 {
		capacity := float64(cores) * s.MaxCPUCommit
		committed := clamp01((w.CommittedCPU + job.CPU) / capacity)
		busy := clamp01(snap.CPU.UtilPct / 100)
		if snap.CPU.UtilSource == "declared" {
			busy = committed
		}
		f[FCPU] = 1 - math.Max(committed, busy)
		perf := snap.CPU.PerfCores
		if perf == 0 {
			perf = snap.CPU.PhysicalCores
		}
		f[FCores] = clamp01(float64(perf) / 16)
	}
	if s.MaxAgents > 0 {
		f[FAgents] = clamp01(1 - float64(w.ActiveAgents)/float64(s.MaxAgents))
	} else {
		f[FAgents] = 1 / (1 + float64(w.ActiveAgents))
	}
	switch {
	case job.Repo == "":
		f[FLocality] = 0.5
	case snap.Paths != nil && snap.Paths[job.Repo]:
		f[FLocality] = 1
	default:
		f[FLocality] = 0.5
	}
	if job.Repo != "" && w.RunningRepos[job.Repo] > 0 {
		f[FAffinity] = 1
	}
	f[FLatency] = 1 / (1 + float64(w.LatencyMS)/100)
	f[FPreference] = s.Weight / 2
	power := 0.8
	switch snap.Power.Source {
	case resource.PowerAC:
		power = 1
	case resource.PowerBattery:
		power = 0.5
	}
	switch snap.Thermal {
	case resource.ThermalFair:
		power *= 0.8
	case resource.ThermalSerious:
		power *= 0.5
	}
	f[FPower] = power
	if w.Local {
		f[FLocal] = 1
	}
	for k, v := range f {
		f[k] = math.Round(v*1000) / 1000
	}
	return f
}

func clamp01(x float64) float64 {
	if math.IsNaN(x) || x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}
