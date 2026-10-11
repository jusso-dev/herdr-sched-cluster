//go:build darwin

package resource

import (
	"context"
	"os/exec"
	"runtime"
	"time"

	"golang.org/x/sys/unix"
)

// collectPlatform reads macOS through sysctl, which needs no cgo, and through
// three small commands (ps, pmset) run with fixed argv. Untested on Apple
// Silicon hardware at the time of writing; built and unit tested on GitHub's
// macOS runners.
func collectPlatform(ctx context.Context, s *Snapshot, req Request) error {
	s.Backend = BackendDarwin
	if v, err := unix.Sysctl("kern.osproductversion"); err == nil {
		s.OSVersion = "macOS " + v
	}
	if v, err := unix.Sysctl("machdep.cpu.brand_string"); err == nil {
		s.CPU.Model = v
	}
	s.CPU.LogicalCores = runtime.NumCPU()
	if v, err := unix.SysctlUint32("hw.physicalcpu"); err == nil {
		s.CPU.PhysicalCores = int(v)
	} else {
		s.CPU.PhysicalCores = s.CPU.LogicalCores
	}
	if v, err := unix.SysctlUint32("hw.perflevel0.physicalcpu"); err == nil {
		s.CPU.PerfCores = int(v)
	}
	if b, err := unix.SysctlRaw("vm.loadavg"); err == nil {
		s.CPU.Load1, s.CPU.Load5, s.CPU.Load15, _ = parseLoadavgStruct(b)
	}
	if tv, err := unix.SysctlTimeval("kern.boottime"); err == nil {
		s.UptimeSeconds = time.Now().Unix() - tv.Sec
	}

	// Memory.
	if v, err := unix.SysctlUint64("hw.memsize"); err == nil {
		s.Memory.TotalMB = int64(v >> 20)
	} else {
		s.warn("memory: hw.memsize: %v", err)
	}
	page := uint64(unix.Getpagesize())
	pages := func(name string) (uint64, bool) {
		v, err := unix.SysctlUint32(name)
		return uint64(v), err == nil
	}
	free, okFree := pages("vm.page_free_count")
	spec, _ := pages("vm.page_speculative_count")
	purge, _ := pages("vm.page_purgeable_count")
	ext, okExt := pages("vm.page_pageable_external_count")
	if okFree {
		avail := free + spec + purge
		if okExt {
			// File-backed pages the kernel can drop without writing.
			avail = free + purge + ext
		}
		s.Memory.AvailableMB = int64(avail * page >> 20)
	} else {
		s.warn("memory: vm.page_free_count unreadable")
	}
	if b, err := unix.SysctlRaw("vm.swapusage"); err == nil {
		if total, used, ok := parseSwapUsage(b); ok {
			s.Memory.SwapTotalMB, s.Memory.SwapUsedMB = int64(total>>20), int64(used>>20)
		}
	}
	s.Memory.SwapInPerSec, s.Memory.SwapOutPerSec = -1, -1
	if lvl, err := unix.SysctlUint32("kern.memorystatus_vm_pressure_level"); err == nil {
		s.Memory.Pressure, s.Memory.PressureSource = ClassifyDarwinLevel(lvl), "kernel"
	}
	if s.Memory.Pressure == "" || s.Memory.Pressure == PressureUnknown {
		s.Memory.Pressure = ClassifyEstimate(s.Memory.TotalMB, s.Memory.AvailableMB, 0)
		s.Memory.PressureSource = "estimate"
	}

	// CPU and processes: two ps listings across the sample window. %cpu
	// from ps is a decaying average, so the second listing alone is used;
	// the window just keeps the probe's own start-up out of it.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(req.Sample):
	}
	if out, err := run(ctx, "/bin/ps", "-A", "-o", "pid=,ppid=,rss=,%cpu="); err == nil {
		rows := parsePS(out)
		if s.CPU.LogicalCores > 0 {
			s.CPU.UtilPct = round1x(clampPct(sumCPU(rows) / float64(s.CPU.LogicalCores)))
			s.CPU.UtilSource = "ps_sum"
		}
		if len(req.PIDs) > 0 {
			s.Processes = foldPS(rows, req.PIDs)
		}
	} else {
		s.CPU.UtilSource = "unavailable"
		s.warn("cpu: ps: %v", err)
	}

	if out, err := run(ctx, "/usr/bin/pmset", "-g", "batt"); err == nil {
		s.Power = parsePmsetBatt(out)
	}
	if out, err := run(ctx, "/usr/bin/pmset", "-g", "therm"); err == nil {
		s.Thermal = parsePmsetTherm(out)
	}
	if runtime.GOARCH == "arm64" {
		s.GPUs = []GPU{{Name: "Apple Silicon GPU", MemoryMB: s.Memory.TotalMB, Unified: true}}
	}
	return nil
}

func run(ctx context.Context, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).Output()
	return string(out), err
}
