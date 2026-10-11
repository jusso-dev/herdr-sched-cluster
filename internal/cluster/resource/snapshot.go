// Package resource measures a machine: memory, CPU, disk, power, thermal
// state, which agent CLIs it has, and what given processes are using.
//
// The collectors are per operating system and live behind build tags. The
// types and the pressure rules are shared and pure, so the scheduler can be
// tested with snapshots it builds by hand.
package resource

import (
	"sort"
	"time"
)

// SchemaVersion is bumped when a field changes meaning. A coordinator refuses a
// probe answer with a different schema rather than guessing.
const SchemaVersion = 1

// Pressure levels, from best to worst.
const (
	PressureNormal   = "normal"
	PressureWarn     = "warn"
	PressureCritical = "critical"
	PressureUnknown  = "unknown"
)

// Thermal states, using Apple's names because they read well everywhere.
const (
	ThermalNominal  = "nominal"
	ThermalFair     = "fair"
	ThermalSerious  = "serious"
	ThermalCritical = "critical"
	ThermalUnknown  = "unknown"
)

// Power sources.
const (
	PowerAC      = "ac"
	PowerBattery = "battery"
	PowerUnknown = "unknown"
)

// Backends say how a snapshot was produced.
const (
	BackendLinux  = "linux"
	BackendDarwin = "darwin"
	BackendStatic = "static"
)

// Snapshot is one reading of one machine.
type Snapshot struct {
	Schema      int       `json:"schema"`
	Backend     string    `json:"backend"`
	Host        string    `json:"host"`
	OS          string    `json:"os"`
	OSVersion   string    `json:"os_version,omitempty"`
	Arch        string    `json:"arch"`
	CollectedAt time.Time `json:"collected_at"`
	// SampleMS is how long the collector watched counters that need two
	// readings, such as CPU time and swap activity.
	SampleMS      int64   `json:"sample_ms"`
	UptimeSeconds int64   `json:"uptime_seconds"`
	Memory        Memory  `json:"memory"`
	CPU           CPU     `json:"cpu"`
	Disk          Disk    `json:"disk"`
	Power         Power   `json:"power"`
	Thermal       string  `json:"thermal"`
	GPUs          []GPU   `json:"gpus,omitempty"`
	IOPressure    float64 `json:"io_pressure_pct,omitempty"`
	// Agents are the agent kinds whose CLI was found on this machine.
	Agents []string `json:"agents,omitempty"`
	// Paths answers "does this path exist here" for paths the caller asked
	// about, keyed by the path exactly as asked.
	Paths map[string]bool `json:"paths,omitempty"`
	// Processes are samples for the PIDs the caller asked about, each with its
	// descendants folded in.
	Processes []ProcSample `json:"processes,omitempty"`
	// Warnings are things the collector could not read. A missing reading is
	// reported, never invented.
	Warnings []string `json:"warnings,omitempty"`
}

// Memory is in mebibytes.
type Memory struct {
	TotalMB     int64 `json:"total_mb"`
	AvailableMB int64 `json:"available_mb"`
	SwapTotalMB int64 `json:"swap_total_mb"`
	SwapUsedMB  int64 `json:"swap_used_mb"`
	// SwapInPerSec and SwapOutPerSec are pages per second over the sample,
	// or -1 when the platform does not expose them.
	SwapInPerSec  float64 `json:"swap_in_per_sec"`
	SwapOutPerSec float64 `json:"swap_out_per_sec"`
	Pressure      string  `json:"pressure"`
	// PressureSource says which signal decided Pressure: "psi", "kernel" or
	// "estimate".
	PressureSource string `json:"pressure_source"`
}

// CPU is the processor.
type CPU struct {
	Model         string  `json:"model,omitempty"`
	LogicalCores  int     `json:"logical_cores"`
	PhysicalCores int     `json:"physical_cores"`
	PerfCores     int     `json:"perf_cores,omitempty"`
	UtilPct       float64 `json:"util_pct"`
	UtilSource    string  `json:"util_source"`
	Load1         float64 `json:"load1"`
	Load5         float64 `json:"load5"`
	Load15        float64 `json:"load15"`
}

// Disk is the filesystem holding the home directory.
type Disk struct {
	Path    string  `json:"path"`
	TotalGB float64 `json:"total_gb"`
	FreeGB  float64 `json:"free_gb"`
}

// Power is the power source and battery.
type Power struct {
	Source string `json:"source"`
	// BatteryPct is -1 when there is no battery or it cannot be read.
	BatteryPct int `json:"battery_pct"`
}

// GPU is a graphics processor, when one can be seen cheaply.
type GPU struct {
	Name     string `json:"name"`
	MemoryMB int64  `json:"memory_mb,omitempty"`
	UsedMB   int64  `json:"used_mb,omitempty"`
	// Unified is true for Apple Silicon, where the GPU shares system memory.
	Unified bool `json:"unified,omitempty"`
}

// ProcSample is what one process tree is using.
type ProcSample struct {
	PID    int     `json:"pid"`
	Found  bool    `json:"found"`
	RSSMB  int64   `json:"rss_mb"`
	CPUPct float64 `json:"cpu_pct"`
	Procs  int     `json:"procs"`
}

// Request is what a caller wants measured besides the machine itself.
type Request struct {
	Paths []string
	PIDs  []int
	// Sample is how long to watch CPU and swap counters. Zero uses
	// DefaultSample.
	Sample time.Duration
}

// DefaultSample is short on purpose: the probe runs every few seconds per
// worker at most, and must not cost more than the agents it is measuring.
const DefaultSample = 250 * time.Millisecond

// AgentExecutables maps Herdr's agent kinds to the executable Herdr launches
// for each, from Herdr's src/detect/mod.rs interactive_agent_executable.
var AgentExecutables = map[string]string{
	"pi": "pi", "claude": "claude", "codex": "codex", "gemini": "gemini",
	"cursor": "cursor-agent", "devin": "devin", "agy": "agy", "cline": "cline",
	"omp": "omp", "mastracode": "mastracode", "opencode": "opencode",
	"copilot": "copilot", "kimi": "kimi", "kiro": "kiro-cli", "droid": "droid",
	"amp": "amp", "grok": "grok", "hermes": "hermes", "kilo": "kilo",
	"qodercli": "qodercli", "qwen": "qwen", "letta": "letta", "maki": "maki",
	"muse": "muse",
}

// AgentKinds is every kind, sorted.
func AgentKinds() []string {
	out := make([]string, 0, len(AgentExecutables))
	for k := range AgentExecutables {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ClassifyPSI turns Linux pressure-stall averages into a level. some10 is the
// share of the last ten seconds in which at least one task waited on memory;
// full10 is the share in which every task did.
func ClassifyPSI(some10, full10 float64) string {
	switch {
	case full10 >= 10 || some10 >= 40:
		return PressureCritical
	case full10 >= 2 || some10 >= 10:
		return PressureWarn
	default:
		return PressureNormal
	}
}

// ClassifyEstimate is the fallback when the kernel gives no pressure signal:
// a judgement from free memory and swap activity alone.
func ClassifyEstimate(totalMB, availableMB int64, swapOutPerSec float64) string {
	if totalMB <= 0 {
		return PressureUnknown
	}
	free := float64(availableMB) / float64(totalMB)
	switch {
	case free < 0.05 || swapOutPerSec > 500:
		return PressureCritical
	case free < 0.12 || swapOutPerSec > 50:
		return PressureWarn
	default:
		return PressureNormal
	}
}

// ClassifyDarwinLevel maps kern.memorystatus_vm_pressure_level.
func ClassifyDarwinLevel(level uint32) string {
	switch level {
	case 1:
		return PressureNormal
	case 2:
		return PressureWarn
	case 4:
		return PressureCritical
	default:
		return PressureUnknown
	}
}

// ClassifyTemperature maps the hottest sensor reading, in Celsius, to a
// thermal state. Zero or less means no reading.
func ClassifyTemperature(maxC float64) string {
	switch {
	case maxC <= 0:
		return ThermalUnknown
	case maxC >= 95:
		return ThermalCritical
	case maxC >= 85:
		return ThermalSerious
	case maxC >= 75:
		return ThermalFair
	default:
		return ThermalNominal
	}
}

// Rank orders pressure levels so callers can compare them. Unknown ranks
// with warn: a machine that cannot say how it is doing is not treated as
// fine, and not as on fire.
func Rank(pressure string) int {
	switch pressure {
	case PressureNormal:
		return 0
	case PressureWarn, PressureUnknown:
		return 1
	case PressureCritical:
		return 2
	default:
		return 1
	}
}
