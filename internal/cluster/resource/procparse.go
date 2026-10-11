package resource

import (
	"bufio"
	"strconv"
	"strings"
)

// The parsers below read Linux /proc text. They are pure and build on every
// platform so they can be tested with fixtures anywhere.

// meminfo is the part of /proc/meminfo the scheduler uses, in kibibytes.
type meminfo struct {
	TotalKB, AvailableKB, FreeKB, BuffersKB, CachedKB, SwapTotalKB, SwapFreeKB int64
	HasAvailable                                                               bool
}

func parseMeminfo(text string) meminfo {
	var m meminfo
	sc := bufio.NewScanner(strings.NewReader(text))
	for sc.Scan() {
		key, rest, ok := strings.Cut(sc.Text(), ":")
		if !ok {
			continue
		}
		f := strings.Fields(rest)
		if len(f) == 0 {
			continue
		}
		v := atoi64(f[0])
		switch key {
		case "MemTotal":
			m.TotalKB = v
		case "MemAvailable":
			m.AvailableKB, m.HasAvailable = v, true
		case "MemFree":
			m.FreeKB = v
		case "Buffers":
			m.BuffersKB = v
		case "Cached":
			m.CachedKB = v
		case "SwapTotal":
			m.SwapTotalKB = v
		case "SwapFree":
			m.SwapFreeKB = v
		}
	}
	if !m.HasAvailable {
		// Kernels before 3.14 have no MemAvailable. Free plus page cache is
		// the usual stand-in.
		m.AvailableKB = m.FreeKB + m.BuffersKB + m.CachedKB
	}
	return m
}

// parsePSI reads a /proc/pressure/* file and returns the avg10 values of its
// "some" and "full" lines. ok is false when the text has no "some" line.
func parsePSI(text string) (some10, full10 float64, ok bool) {
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		for _, kv := range f[1:] {
			k, v, found := strings.Cut(kv, "=")
			if !found || k != "avg10" {
				continue
			}
			switch f[0] {
			case "some":
				some10, ok = atof(v), true
			case "full":
				full10 = atof(v)
			}
		}
	}
	return some10, full10, ok
}

// cpuTimes is the aggregate "cpu" line of /proc/stat, in clock ticks.
type cpuTimes struct{ Busy, Total uint64 }

func parseProcStatCPU(text string) (cpuTimes, bool) {
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(line)
		if len(f) < 5 || f[0] != "cpu" {
			continue
		}
		var vals []uint64
		for _, s := range f[1:] {
			n, err := strconv.ParseUint(s, 10, 64)
			if err != nil {
				return cpuTimes{}, false
			}
			vals = append(vals, n)
		}
		var total uint64
		for _, v := range vals {
			total += v
		}
		// user nice system idle iowait irq softirq steal guest guest_nice.
		// Guest time is already counted in user and nice, so it is taken
		// back out of the total.
		idle := vals[3]
		if len(vals) > 4 {
			idle += vals[4]
		}
		if len(vals) > 9 {
			total -= vals[8] + vals[9]
		} else if len(vals) > 8 {
			total -= vals[8]
		}
		return cpuTimes{Busy: total - idle, Total: total}, true
	}
	return cpuTimes{}, false
}

// utilBetween is the busy share between two readings, as a percentage.
func utilBetween(a, b cpuTimes) float64 {
	if b.Total <= a.Total {
		return 0
	}
	busy := float64(b.Busy - a.Busy)
	if b.Busy < a.Busy {
		busy = 0
	}
	return clampPct(100 * busy / float64(b.Total-a.Total))
}

func clampPct(f float64) float64 {
	if f < 0 {
		return 0
	}
	if f > 100 {
		return 100
	}
	return f
}

// parseVmstatSwap returns pswpin and pswpout from /proc/vmstat.
func parseVmstatSwap(text string) (in, out uint64, ok bool) {
	var gotIn, gotOut bool
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		switch f[0] {
		case "pswpin":
			in, _ = strconv.ParseUint(f[1], 10, 64)
			gotIn = true
		case "pswpout":
			out, _ = strconv.ParseUint(f[1], 10, 64)
			gotOut = true
		}
	}
	return in, out, gotIn && gotOut
}

func parseLoadavg(text string) (l1, l5, l15 float64, ok bool) {
	f := strings.Fields(text)
	if len(f) < 3 {
		return 0, 0, 0, false
	}
	return atof(f[0]), atof(f[1]), atof(f[2]), true
}

func parseUptime(text string) int64 {
	f := strings.Fields(text)
	if len(f) == 0 {
		return 0
	}
	return int64(atof(f[0]))
}

// parseCPUInfo returns the model name and the number of distinct physical
// cores. Cores is zero when the file does not say (common on ARM), in which
// case the caller uses the logical count.
func parseCPUInfo(text string) (model string, cores int) {
	seen := map[string]bool{}
	phys, core := "0", ""
	flush := func() {
		if core != "" {
			seen[phys+"/"+core] = true
		}
		phys, core = "0", ""
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		switch k {
		case "model name", "Model", "cpu model":
			if model == "" {
				model = v
			}
		case "physical id":
			phys = v
		case "core id":
			core = v
		}
	}
	flush()
	return model, len(seen)
}

// procStat is the part of /proc/<pid>/stat process sampling needs.
type procStat struct {
	PID, PPID   int
	CPUTicks    uint64
	RSSPages    int64
	Comm, State string
}

// parsePIDStat reads /proc/<pid>/stat. The command name is in parentheses
// and may itself contain spaces and parentheses, so the fields are counted
// from the LAST closing parenthesis.
func parsePIDStat(text string) (procStat, bool) {
	open := strings.IndexByte(text, '(')
	closeIdx := strings.LastIndexByte(text, ')')
	if open < 0 || closeIdx < open {
		return procStat{}, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(text[:open]))
	if err != nil {
		return procStat{}, false
	}
	f := strings.Fields(text[closeIdx+1:])
	// f[0] is state (field 3); utime is field 14, stime 15, rss 24.
	if len(f) < 22 {
		return procStat{}, false
	}
	ppid, _ := strconv.Atoi(f[1])
	ut, _ := strconv.ParseUint(f[11], 10, 64)
	st, _ := strconv.ParseUint(f[12], 10, 64)
	rss, _ := strconv.ParseInt(f[21], 10, 64)
	return procStat{PID: pid, PPID: ppid, CPUTicks: ut + st, RSSPages: rss, Comm: text[open+1 : closeIdx], State: f[0]}, true
}

// treeOf returns pid and every descendant of it, using a pid to parent map.
func treeOf(root int, parent map[int]int) []int {
	children := map[int][]int{}
	for c, p := range parent {
		children[p] = append(children[p], c)
	}
	out := []int{}
	seen := map[int]bool{}
	stack := []int{root}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[n] {
			continue
		}
		seen[n] = true
		if _, ok := parent[n]; ok || n == root {
			out = append(out, n)
		}
		stack = append(stack, children[n]...)
	}
	return out
}
