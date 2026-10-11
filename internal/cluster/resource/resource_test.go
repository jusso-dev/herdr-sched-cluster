package resource

import (
	"encoding/binary"
	"reflect"
	"testing"
	"time"
)

func TestMeminfoUsesMemAvailable(t *testing.T) {
	m := parseMeminfo("MemTotal:       16384000 kB\nMemFree:  1000 kB\nMemAvailable:    8192000 kB\nSwapTotal: 2048000 kB\nSwapFree: 1024000 kB\n")
	if m.TotalKB != 16384000 || m.AvailableKB != 8192000 || m.SwapTotalKB-m.SwapFreeKB != 1024000 {
		t.Fatalf("%+v", m)
	}
}

func TestMeminfoFallsBackWithoutMemAvailable(t *testing.T) {
	m := parseMeminfo("MemTotal: 1000 kB\nMemFree: 100 kB\nBuffers: 50 kB\nCached: 200 kB\n")
	if m.AvailableKB != 350 {
		t.Fatalf("available %d, want 350", m.AvailableKB)
	}
}

func TestPSI(t *testing.T) {
	some, full, ok := parsePSI("some avg10=12.50 avg60=3.00 avg300=1.00 total=1\nfull avg10=2.10 avg60=0 avg300=0 total=1\n")
	if !ok || some != 12.5 || full != 2.1 {
		t.Fatalf("%v %v %v", some, full, ok)
	}
	if got := ClassifyPSI(some, full); got != PressureWarn {
		t.Fatalf("got %s", got)
	}
	if ClassifyPSI(0.1, 0) != PressureNormal || ClassifyPSI(50, 0) != PressureCritical || ClassifyPSI(0, 11) != PressureCritical {
		t.Fatal("psi thresholds")
	}
	if _, _, ok := parsePSI("garbage"); ok {
		t.Fatal("garbage parsed")
	}
}

func TestProcStatCPUExcludesGuestAndIdle(t *testing.T) {
	a, ok := parseProcStatCPU("cpu  100 0 100 800 0 0 0 0 0 0\ncpu0 1 1 1 1\n")
	b, ok2 := parseProcStatCPU("cpu  200 0 200 1400 0 0 0 0 0 0\n")
	if !ok || !ok2 {
		t.Fatal("parse")
	}
	if got := utilBetween(a, b); got != 25 {
		t.Fatalf("util %v, want 25", got)
	}
	if utilBetween(b, a) != 0 {
		t.Fatal("a counter going backwards must read as idle, not negative")
	}
}

func TestVmstatSwapAndLoadAndUptime(t *testing.T) {
	in, out, ok := parseVmstatSwap("nr_free_pages 1\npswpin 10\npswpout 20\n")
	if !ok || in != 10 || out != 20 {
		t.Fatal(in, out, ok)
	}
	l1, l5, l15, ok := parseLoadavg("1.50 0.75 0.25 2/300 1234\n")
	if !ok || l1 != 1.5 || l5 != 0.75 || l15 != 0.25 {
		t.Fatal(l1, l5, l15)
	}
	if parseUptime("12345.67 999.0\n") != 12345 {
		t.Fatal("uptime")
	}
}

func TestCPUInfoCountsDistinctPhysicalCores(t *testing.T) {
	text := "processor : 0\nmodel name : Test CPU\nphysical id : 0\ncore id : 0\n\n" +
		"processor : 1\nmodel name : Test CPU\nphysical id : 0\ncore id : 0\n\n" +
		"processor : 2\nphysical id : 0\ncore id : 1\n\n"
	model, cores := parseCPUInfo(text)
	if model != "Test CPU" || cores != 2 {
		t.Fatalf("%q %d", model, cores)
	}
	if _, cores := parseCPUInfo("processor : 0\nBogoMIPS : 50\n\n"); cores != 0 {
		t.Fatal("ARM cpuinfo without core ids must report zero, so the logical count is used")
	}
}

func TestPIDStatWithAwkwardCommandName(t *testing.T) {
	line := "4242 (node (claude) x) S 4000 4242 4242 0 -1 4194304 1 0 0 0 150 50 0 0 20 0 11 0 100 1000000 2560 18446744073709551615"
	st, ok := parsePIDStat(line)
	if !ok || st.PID != 4242 || st.PPID != 4000 || st.CPUTicks != 200 || st.RSSPages != 2560 || st.Comm != "node (claude) x" {
		t.Fatalf("%+v %v", st, ok)
	}
}

func TestTreeOfFoldsDescendantsOnly(t *testing.T) {
	parent := map[int]int{10: 1, 11: 10, 12: 11, 20: 1, 13: 10}
	got := treeOf(10, parent)
	want := map[int]bool{10: true, 11: true, 12: true, 13: true}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for _, p := range got {
		if !want[p] {
			t.Fatalf("unexpected %d in %v", p, got)
		}
	}
	parent[10] = 12 // a cycle must not loop forever
	_ = treeOf(10, parent)
}

func TestEstimatePressure(t *testing.T) {
	cases := []struct {
		total, avail int64
		swapOut      float64
		want         string
	}{
		{16000, 8000, 0, PressureNormal},
		{16000, 1500, 0, PressureWarn},
		{16000, 500, 0, PressureCritical},
		{16000, 8000, 1000, PressureCritical},
		{0, 0, 0, PressureUnknown},
	}
	for _, c := range cases {
		if got := ClassifyEstimate(c.total, c.avail, c.swapOut); got != c.want {
			t.Errorf("%+v: got %s", c, got)
		}
	}
}

func TestDarwinPressureAndThermal(t *testing.T) {
	if ClassifyDarwinLevel(1) != PressureNormal || ClassifyDarwinLevel(2) != PressureWarn || ClassifyDarwinLevel(4) != PressureCritical || ClassifyDarwinLevel(9) != PressureUnknown {
		t.Fatal("darwin levels")
	}
	if got := parsePmsetTherm("Note: No thermal warning level has been recorded\nNote: No performance warning level has been recorded\n"); got != ThermalNominal {
		t.Fatal(got)
	}
	if got := parsePmsetTherm("CPU_Scheduler_Limit \t= 100\nCPU_Available_CPUs \t= 8\nCPU_Speed_Limit \t= 45\n"); got != ThermalSerious {
		t.Fatal(got)
	}
	if got := parsePmsetTherm(""); got != ThermalUnknown {
		t.Fatal(got)
	}
}

func TestPmsetBatt(t *testing.T) {
	p := parsePmsetBatt("Now drawing from 'Battery Power'\n -InternalBattery-0 (id=1234)\t42%; discharging; 3:10 remaining present: true\n")
	if p.Source != PowerBattery || p.BatteryPct != 42 {
		t.Fatalf("%+v", p)
	}
	p = parsePmsetBatt("Now drawing from 'AC Power'\n")
	if p.Source != PowerAC || p.BatteryPct != -1 {
		t.Fatalf("%+v", p)
	}
}

func TestPSFold(t *testing.T) {
	rows := parsePS("  1     0  1000  0.0\n 100     1 204800 50.5\n 101   100 102400 20,0\n 200     1  5000  1.0\nbad line\n")
	if len(rows) != 4 {
		t.Fatalf("rows %d", len(rows))
	}
	got := foldPS(rows, []int{100, 999})
	if !got[0].Found || got[0].RSSMB != 300 || got[0].CPUPct != 70.5 || got[0].Procs != 2 {
		t.Fatalf("%+v", got[0])
	}
	if got[1].Found {
		t.Fatal("a missing pid must say so")
	}
	if sumCPU(rows) != 71.5 {
		t.Fatal(sumCPU(rows))
	}
}

func TestSysctlStructs(t *testing.T) {
	b := make([]byte, 32)
	binary.LittleEndian.PutUint64(b[0:], 4<<30)
	binary.LittleEndian.PutUint64(b[8:], 3<<30)
	binary.LittleEndian.PutUint64(b[16:], 1<<30)
	total, used, ok := parseSwapUsage(b)
	if !ok || total != 4<<30 || used != 1<<30 {
		t.Fatal(total, used)
	}
	l := make([]byte, 24)
	binary.LittleEndian.PutUint32(l[0:], 2048)
	binary.LittleEndian.PutUint32(l[4:], 1024)
	binary.LittleEndian.PutUint32(l[8:], 512)
	binary.LittleEndian.PutUint64(l[16:], 2048)
	l1, l5, l15, ok := parseLoadavgStruct(l)
	if !ok || l1 != 1 || l5 != 0.5 || l15 != 0.25 {
		t.Fatal(l1, l5, l15)
	}
	if _, _, ok := parseSwapUsage(b[:8]); ok {
		t.Fatal("short struct accepted")
	}
}

func TestNvidiaSMI(t *testing.T) {
	got := parseNvidiaSMI("NVIDIA RTX 4090, 24564, 1024\n")
	want := []GPU{{Name: "NVIDIA RTX 4090", MemoryMB: 24564, UsedMB: 1024}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%+v", got)
	}
}

func TestStaticWorkerSaysItIsDeclared(t *testing.T) {
	s := Static(StaticSpec{OS: "windows", Arch: "amd64", MemoryMB: 131072, Cores: 32, Agents: []string{"codex"}}, time.Unix(0, 0))
	if s.Backend != BackendStatic || s.Memory.AvailableMB != 131072 || s.CPU.LogicalCores != 32 || len(s.Warnings) == 0 {
		t.Fatalf("%+v", s)
	}
}

func TestFindAgentsUsesHerdrExecutableNames(t *testing.T) {
	got := findAgents(func(exe string) bool { return exe == "cursor-agent" || exe == "claude" })
	if !reflect.DeepEqual(got, []string{"claude", "cursor"}) {
		t.Fatalf("%v", got)
	}
}

func TestRankTreatsUnknownAsWarn(t *testing.T) {
	if Rank(PressureUnknown) != Rank(PressureWarn) || Rank(PressureNormal) >= Rank(PressureWarn) || Rank(PressureWarn) >= Rank(PressureCritical) {
		t.Fatal("rank order")
	}
}
