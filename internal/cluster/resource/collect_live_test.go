//go:build linux || darwin

package resource

import (
	"context"
	"os"
	"testing"
	"time"
)

// This reads the machine the test runs on. On Linux CI and the audit box it
// is real Linux; on the macOS CI runner it is real macOS.
func TestCollectReadsThisMachine(t *testing.T) {
	dir := t.TempDir()
	s, err := Collect(context.Background(), Request{
		Paths:  []string{dir, dir + "/missing"},
		PIDs:   []int{os.Getpid(), 1 << 30},
		Sample: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.Schema != SchemaVersion || s.Memory.TotalMB <= 0 || s.Memory.AvailableMB <= 0 || s.Memory.AvailableMB > s.Memory.TotalMB {
		t.Fatalf("memory %+v", s.Memory)
	}
	if s.CPU.LogicalCores <= 0 || s.CPU.PhysicalCores <= 0 {
		t.Fatalf("cpu %+v", s.CPU)
	}
	if s.Memory.Pressure == "" || s.Thermal == "" || s.Power.Source == "" {
		t.Fatalf("levels missing: %+v", s)
	}
	if s.Disk.TotalGB <= 0 {
		t.Fatalf("disk %+v", s.Disk)
	}
	if !s.Paths[dir] || s.Paths[dir+"/missing"] {
		t.Fatalf("paths %v", s.Paths)
	}
	if len(s.Processes) != 2 || !s.Processes[0].Found || s.Processes[0].RSSMB <= 0 || s.Processes[1].Found {
		t.Fatalf("processes %+v", s.Processes)
	}
	t.Logf("backend=%s mem=%d/%dMB pressure=%s(%s) cpu=%.1f%%(%s) cores=%d/%d load=%.2f power=%s thermal=%s",
		s.Backend, s.Memory.AvailableMB, s.Memory.TotalMB, s.Memory.Pressure, s.Memory.PressureSource,
		s.CPU.UtilPct, s.CPU.UtilSource, s.CPU.PhysicalCores, s.CPU.LogicalCores, s.CPU.Load1, s.Power.Source, s.Thermal)
}
