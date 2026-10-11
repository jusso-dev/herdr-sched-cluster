package resource

import (
	"encoding/binary"
	"strconv"
	"strings"
)

// These parsers read macOS command output and sysctl structs. They are pure
// and build everywhere so they are tested on Linux CI as well as macOS.

// parsePmsetBatt reads `pmset -g batt`.
func parsePmsetBatt(text string) Power {
	p := Power{Source: PowerUnknown, BatteryPct: -1}
	switch {
	case strings.Contains(text, "'AC Power'"):
		p.Source = PowerAC
	case strings.Contains(text, "'Battery Power'"):
		p.Source = PowerBattery
	}
	for _, line := range strings.Split(text, "\n") {
		if !strings.Contains(line, "InternalBattery") {
			continue
		}
		for _, f := range strings.FieldsFunc(line, func(r rune) bool { return r == '\t' || r == ';' || r == ' ' }) {
			if v, ok := strings.CutSuffix(f, "%"); ok {
				if n, err := strconv.Atoi(v); err == nil {
					p.BatteryPct = n
				}
			}
		}
	}
	return p
}

// parsePmsetTherm reads `pmset -g therm`. Apple Silicon prints that no
// warning has been recorded when all is well; Intel Macs print a CPU speed
// limit, where anything under 100 means the machine is being held back.
func parsePmsetTherm(text string) string {
	state := ThermalUnknown
	if strings.Contains(text, "No thermal warning level has been recorded") {
		state = ThermalNominal
	}
	if strings.Contains(text, "thermal warning level") && !strings.Contains(text, "No thermal warning level") {
		state = ThermalSerious
	}
	for _, line := range strings.Split(text, "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(k) != "CPU_Speed_Limit" {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			continue
		}
		switch {
		case n >= 100:
			if state == ThermalUnknown {
				state = ThermalNominal
			}
		case n >= 70:
			state = ThermalFair
		case n >= 40:
			state = ThermalSerious
		default:
			state = ThermalCritical
		}
	}
	return state
}

// psRow is one line of `ps -A -o pid=,ppid=,rss=,%cpu=`.
type psRow struct {
	PID, PPID int
	RSSKB     int64
	CPU       float64
}

func parsePS(text string) []psRow {
	var out []psRow
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(line)
		if len(f) != 4 {
			continue
		}
		pid, err1 := strconv.Atoi(f[0])
		ppid, err2 := strconv.Atoi(f[1])
		if err1 != nil || err2 != nil {
			continue
		}
		out = append(out, psRow{PID: pid, PPID: ppid, RSSKB: atoi64(f[2]), CPU: atof(strings.ReplaceAll(f[3], ",", "."))})
	}
	return out
}

// foldPS sums each requested PID's tree from one ps listing.
func foldPS(rows []psRow, pids []int) []ProcSample {
	byPID := map[int]psRow{}
	parent := map[int]int{}
	for _, r := range rows {
		byPID[r.PID] = r
		parent[r.PID] = r.PPID
	}
	var out []ProcSample
	for _, root := range pids {
		ps := ProcSample{PID: root}
		if _, ok := byPID[root]; ok {
			ps.Found = true
			var rss int64
			var cpu float64
			for _, p := range treeOf(root, parent) {
				rss += byPID[p].RSSKB
				cpu += byPID[p].CPU
				ps.Procs++
			}
			ps.RSSMB = rss / 1024
			ps.CPUPct = round1x(cpu)
		}
		out = append(out, ps)
	}
	return out
}

// sumCPU is every process's %CPU added up, which is how busy the machine is
// as a share of ONE core. Divided by the core count it is the utilisation.
func sumCPU(rows []psRow) float64 {
	var t float64
	for _, r := range rows {
		t += r.CPU
	}
	return t
}

// parseSwapUsage decodes struct xsw_usage from sysctl vm.swapusage:
// total, avail, used (uint64 each), then pagesize and an encrypted flag.
func parseSwapUsage(b []byte) (totalB, usedB uint64, ok bool) {
	if len(b) < 24 {
		return 0, 0, false
	}
	le := binary.LittleEndian
	return le.Uint64(b[0:8]), le.Uint64(b[16:24]), true
}

// parseLoadavgStruct decodes struct loadavg from sysctl vm.loadavg: three
// fixed-point uint32 values, padding, then the int64 scale.
func parseLoadavgStruct(b []byte) (l1, l5, l15 float64, ok bool) {
	if len(b) < 24 {
		return 0, 0, 0, false
	}
	le := binary.LittleEndian
	scale := float64(le.Uint64(b[16:24]))
	if scale == 0 {
		return 0, 0, 0, false
	}
	f := func(i int) float64 { return round2(float64(le.Uint32(b[i*4:i*4+4])) / scale) }
	return f(0), f(1), f(2), true
}

func round1x(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }
func round2(f float64) float64  { return float64(int64(f*100+0.5)) / 100 }
