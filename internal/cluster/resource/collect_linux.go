//go:build linux

package resource

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// procRoot and sysRoot are variables so tests can point them at fixtures.
var (
	procRoot = "/proc"
	sysRoot  = "/sys"
)

func readProc(name string) (string, error) {
	b, err := os.ReadFile(filepath.Join(procRoot, name))
	return string(b), err
}

func collectPlatform(ctx context.Context, s *Snapshot, req Request) error {
	s.Backend = BackendLinux
	if b, err := os.ReadFile("/etc/os-release"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if v, ok := strings.CutPrefix(line, "PRETTY_NAME="); ok {
				s.OSVersion = strings.Trim(v, `"`)
			}
		}
	}

	// First readings of the counters that need two.
	stat1, _ := readProc("stat")
	vm1, _ := readProc("vmstat")
	cpu1, cpuOK := parseProcStatCPU(stat1)
	in1, out1, swapOK := parseVmstatSwap(vm1)
	tree1 := sampleTrees(req.PIDs)
	start := time.Now()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(req.Sample):
	}
	elapsed := time.Since(start).Seconds()

	stat2, _ := readProc("stat")
	cpu2, cpuOK2 := parseProcStatCPU(stat2)
	s.CPU.LogicalCores = runtime.NumCPU()
	if cpuOK && cpuOK2 {
		s.CPU.UtilPct = round1(utilBetween(cpu1, cpu2))
		s.CPU.UtilSource = "proc_stat"
	} else {
		s.CPU.UtilSource = "unavailable"
		s.warn("cpu: /proc/stat unreadable")
	}
	if info, err := readProc("cpuinfo"); err == nil {
		s.CPU.Model, s.CPU.PhysicalCores = parseCPUInfo(info)
	}
	if s.CPU.PhysicalCores == 0 {
		s.CPU.PhysicalCores = s.CPU.LogicalCores
	}
	if la, err := readProc("loadavg"); err == nil {
		s.CPU.Load1, s.CPU.Load5, s.CPU.Load15, _ = parseLoadavg(la)
	}
	if up, err := readProc("uptime"); err == nil {
		s.UptimeSeconds = parseUptime(up)
	}

	// Memory.
	mi, err := readProc("meminfo")
	if err != nil {
		s.warn("memory: %v", err)
	}
	m := parseMeminfo(mi)
	s.Memory.TotalMB = m.TotalKB / 1024
	s.Memory.AvailableMB = m.AvailableKB / 1024
	s.Memory.SwapTotalMB = m.SwapTotalKB / 1024
	s.Memory.SwapUsedMB = (m.SwapTotalKB - m.SwapFreeKB) / 1024
	s.Memory.SwapInPerSec, s.Memory.SwapOutPerSec = -1, -1
	if vm2, err := readProc("vmstat"); err == nil && swapOK && elapsed > 0 {
		if in2, out2, ok := parseVmstatSwap(vm2); ok {
			s.Memory.SwapInPerSec = round1(float64(in2-in1) / elapsed)
			s.Memory.SwapOutPerSec = round1(float64(out2-out1) / elapsed)
		}
	}
	if psi, err := readProc("pressure/memory"); err == nil {
		if some, full, ok := parsePSI(psi); ok {
			s.Memory.Pressure, s.Memory.PressureSource = ClassifyPSI(some, full), "psi"
		}
	}
	if s.Memory.Pressure == "" {
		s.Memory.Pressure = ClassifyEstimate(s.Memory.TotalMB, s.Memory.AvailableMB, s.Memory.SwapOutPerSec)
		s.Memory.PressureSource = "estimate"
	}
	if psi, err := readProc("pressure/io"); err == nil {
		if some, _, ok := parsePSI(psi); ok {
			s.IOPressure = some
		}
	}

	s.Power = linuxPower()
	s.Thermal = linuxThermal()
	s.GPUs = nvidiaGPUs(ctx)

	if len(req.PIDs) > 0 {
		tree2 := sampleTrees(req.PIDs)
		ticks := float64(clockTicks())
		page := int64(os.Getpagesize())
		for _, pid := range req.PIDs {
			a, b := tree1[pid], tree2[pid]
			ps := ProcSample{PID: pid, Found: b.found, Procs: b.procs, RSSMB: b.rssPages * page / (1 << 20)}
			if a.found && b.found && elapsed > 0 && b.ticks >= a.ticks {
				ps.CPUPct = round1(100 * float64(b.ticks-a.ticks) / ticks / elapsed)
			}
			s.Processes = append(s.Processes, ps)
		}
	}
	return nil
}

type treeSample struct {
	found    bool
	procs    int
	ticks    uint64
	rssPages int64
}

// sampleTrees reads every process once and folds each requested PID's
// descendants into it.
func sampleTrees(pids []int) map[int]treeSample {
	out := map[int]treeSample{}
	if len(pids) == 0 {
		return out
	}
	entries, err := os.ReadDir(procRoot)
	if err != nil {
		return out
	}
	stats := map[int]procStat{}
	parent := map[int]int{}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join(procRoot, e.Name(), "stat"))
		if err != nil {
			continue
		}
		st, ok := parsePIDStat(string(b))
		if !ok {
			continue
		}
		stats[pid] = st
		parent[pid] = st.PPID
	}
	for _, root := range pids {
		if _, ok := stats[root]; !ok {
			out[root] = treeSample{}
			continue
		}
		var t treeSample
		t.found = true
		for _, p := range treeOf(root, parent) {
			st := stats[p]
			t.procs++
			t.ticks += st.CPUTicks
			t.rssPages += st.RSSPages
		}
		out[root] = t
	}
	return out
}

// clockTicks is USER_HZ. It is 100 on every mainstream Linux build, and
// reading it properly needs cgo.
func clockTicks() int { return 100 }

func linuxPower() Power {
	p := Power{Source: PowerUnknown, BatteryPct: -1}
	dir := filepath.Join(sysRoot, "class", "power_supply")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return p
	}
	sawAC := false
	for _, e := range entries {
		base := filepath.Join(dir, e.Name())
		typ := strings.TrimSpace(readSmall(filepath.Join(base, "type")))
		switch typ {
		case "Mains", "USB":
			sawAC = true
			if strings.TrimSpace(readSmall(filepath.Join(base, "online"))) == "1" {
				p.Source = PowerAC
			}
		case "Battery":
			if c := strings.TrimSpace(readSmall(filepath.Join(base, "capacity"))); c != "" {
				p.BatteryPct = int(atoi64(c))
			}
			if p.Source != PowerAC && strings.TrimSpace(readSmall(filepath.Join(base, "status"))) == "Discharging" {
				p.Source = PowerBattery
			}
		}
	}
	if p.Source == PowerUnknown && sawAC {
		p.Source = PowerBattery
	}
	if p.Source == PowerUnknown && p.BatteryPct < 0 {
		// A desktop or server with no battery is on mains.
		p.Source = PowerAC
	}
	return p
}

func linuxThermal() string {
	matches, _ := filepath.Glob(filepath.Join(sysRoot, "class", "thermal", "thermal_zone*", "temp"))
	maxC := 0.0
	for _, m := range matches {
		milli := atof(readSmall(m))
		if c := milli / 1000; c > maxC && c < 150 {
			maxC = c
		}
	}
	return ClassifyTemperature(maxC)
}

func readSmall(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	if len(b) > 4096 {
		b = b[:4096]
	}
	return string(b)
}
