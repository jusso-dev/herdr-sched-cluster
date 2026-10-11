package resource

import "time"

// StaticSpec is a worker's capacity written by hand. It is how a machine with
// no live collector (Windows today) takes part: placement uses the declared
// numbers minus what this scheduler has reserved there, because nothing on
// the machine can be asked.
type StaticSpec struct {
	OS       string
	Arch     string
	MemoryMB int64
	Cores    int
	Agents   []string
}

// Static builds the snapshot a static worker is placed against.
func Static(spec StaticSpec, now time.Time) Snapshot {
	return Snapshot{
		Schema:      SchemaVersion,
		Backend:     BackendStatic,
		OS:          spec.OS,
		Arch:        spec.Arch,
		CollectedAt: now.UTC(),
		Memory: Memory{
			TotalMB:        spec.MemoryMB,
			AvailableMB:    spec.MemoryMB,
			SwapInPerSec:   -1,
			SwapOutPerSec:  -1,
			Pressure:       PressureNormal,
			PressureSource: "declared",
		},
		CPU:     CPU{LogicalCores: spec.Cores, PhysicalCores: spec.Cores, UtilSource: "declared"},
		Power:   Power{Source: PowerAC, BatteryPct: -1},
		Thermal: ThermalUnknown,
		Agents:  append([]string(nil), spec.Agents...),
		Warnings: []string{"static worker: capacity is declared in config, not measured; " +
			"memory and CPU used by anything other than this scheduler's agents is invisible"},
	}
}
