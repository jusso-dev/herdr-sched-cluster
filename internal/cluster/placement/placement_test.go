package placement

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

func mac(id string, totalMB, availMB int64, cores int) Worker {
	return Worker{
		ID: id, Label: id, State: StateReady,
		Snapshot: resource.Snapshot{
			Backend: resource.BackendDarwin, OS: "darwin", Arch: "arm64",
			Memory:  resource.Memory{TotalMB: totalMB, AvailableMB: availMB, Pressure: resource.PressureNormal},
			CPU:     resource.CPU{LogicalCores: cores, PhysicalCores: cores, UtilPct: 10},
			Disk:    resource.Disk{TotalGB: 1000, FreeGB: 500},
			Power:   resource.Power{Source: resource.PowerAC, BatteryPct: -1},
			Thermal: resource.ThermalNominal,
			Agents:  []string{"claude", "codex"},
		},
	}
}

func TestTheBigMachineWinsForABigJob(t *testing.T) {
	mbp := mac("mbp", 65536, 40000, 16)
	mini := mac("mini", 16384, 9000, 10)
	d := Place(Job{ID: "j1", Kind: "claude", MemoryMB: 6000, CPU: 1}, []Worker{mini, mbp})
	if d.Chosen != "mbp" {
		t.Fatalf("chose %s: %s", d.Chosen, d.Summary)
	}
	if !strings.Contains(d.Summary, "placed on mbp") {
		t.Fatal(d.Summary)
	}
}

func TestEveryHardConstraintExcludes(t *testing.T) {
	job := Job{ID: "j", Kind: "claude", Repo: "/src/app", MemoryMB: 4000, CPU: 1, Labels: []string{"big"}}
	base := mac("w", 32768, 20000, 8)
	base.Settings.Labels = []string{"big"}
	base.Snapshot.Paths = map[string]bool{"/src/app": true}
	if r := Eligible(job, base); len(r) != 0 {
		t.Fatalf("base should be eligible: %v", r)
	}
	cases := map[string]func(w *Worker){
		"draining":       func(w *Worker) { w.State = StateDraining },
		"unreachable":    func(w *Worker) { w.State = StateUnreachable },
		"memory":         func(w *Worker) { w.Snapshot.Memory.AvailableMB = 5000 },
		"pending memory": func(w *Worker) { w.PendingMemoryMB = 15000 },
		"pressure":       func(w *Worker) { w.Snapshot.Memory.Pressure = resource.PressureCritical },
		"cpu commit":     func(w *Worker) { w.CommittedCPU = 7.5 },
		"load":           func(w *Worker) { w.Snapshot.CPU.Load5 = 40 },
		"max agents":     func(w *Worker) { w.Settings.MaxAgents = 2; w.ActiveAgents = 2 },
		"disk":           func(w *Worker) { w.Snapshot.Disk.FreeGB = 1 },
		"battery": func(w *Worker) {
			w.Settings.MinBatteryPct = 30
			w.Snapshot.Power = resource.Power{Source: resource.PowerBattery, BatteryPct: 10}
		},
		"thermal":         func(w *Worker) { w.Snapshot.Thermal = resource.ThermalCritical },
		"label":           func(w *Worker) { w.Settings.Labels = nil },
		"declared agents": func(w *Worker) { w.Settings.Agents = []string{"codex"} },
		"repo missing":    func(w *Worker) { w.Snapshot.Paths = map[string]bool{"/src/app": false} },
		"held":            func(w *Worker) { w.HeldFor = "other" },
		"os":              func(w *Worker) { w.Snapshot.OS = "windows" },
	}
	for name, mutate := range cases {
		w := base
		w.Settings.Labels = append([]string(nil), base.Settings.Labels...)
		w.Snapshot.Paths = map[string]bool{"/src/app": true}
		mutate(&w)
		j := job
		if name == "os" {
			j.OS = "darwin"
		}
		if len(Eligible(j, w)) == 0 {
			t.Errorf("%s: still eligible", name)
		}
		if d := Place(j, []Worker{w}); d.Chosen != "" {
			t.Errorf("%s: placed anyway", name)
		}
	}
}

func TestPinningAndHeldForTheSameJob(t *testing.T) {
	a, b := mac("a", 32768, 30000, 8), mac("b", 32768, 30000, 8)
	b.HeldFor = "j"
	d := Place(Job{ID: "j", MemoryMB: 1000, Machine: "b"}, []Worker{a, b})
	if d.Chosen != "b" {
		t.Fatalf("pinned job went to %s", d.Chosen)
	}
}

func TestPoliciesDiffer(t *testing.T) {
	busy := mac("busy", 65536, 30000, 16)
	busy.ActiveAgents = 3
	busy.RunningRepos = map[string]int{"/src/app": 1}
	idle := mac("idle", 65536, 50000, 16)
	job := Job{ID: "j", Repo: "/src/app", MemoryMB: 4000, CPU: 0.5}
	job.Policy = PolicyPack
	if d := Place(job, []Worker{busy, idle}); d.Chosen != "busy" {
		t.Fatalf("pack chose %s", d.Chosen)
	}
	job.Policy = PolicySpread
	if d := Place(job, []Worker{busy, idle}); d.Chosen != "idle" {
		t.Fatalf("spread chose %s", d.Chosen)
	}
	local := mac("local", 16384, 8000, 8)
	local.Local = true
	job.Policy = PolicyLocalFirst
	if d := Place(job, []Worker{idle, local}); d.Chosen != "local" {
		t.Fatalf("local-first chose %s", d.Chosen)
	}
	job.Policy = "nonsense"
	if d := Place(job, []Worker{idle}); d.Policy != PolicyBalanced {
		t.Fatalf("unknown policy not mapped to balanced: %s", d.Policy)
	}
}

func TestPressureNotFreeRAMDecides(t *testing.T) {
	// Lots of "available" memory but the kernel says pressure is critical:
	// macOS compresses aggressively, so free RAM alone lies.
	w := mac("w", 65536, 40000, 16)
	w.Snapshot.Memory.Pressure = resource.PressureCritical
	if d := Place(Job{ID: "j", MemoryMB: 1000}, []Worker{w}); d.Chosen != "" {
		t.Fatal("placed onto a machine under critical memory pressure")
	}
}

func TestNearestAndFitsAnywhereEver(t *testing.T) {
	a, b := mac("a", 65536, 3000, 16), mac("b", 16384, 2500, 8)
	job := Job{ID: "big", MemoryMB: 20000}
	if got := Nearest(job, []Worker{a, b}); got != "a" {
		t.Fatalf("nearest %s", got)
	}
	if ok, _ := FitsAnywhereEver(job, []Worker{a, b}); !ok {
		t.Fatal("a 20 GB job fits a 64 GB machine once it empties")
	}
	if ok, why := FitsAnywhereEver(Job{ID: "huge", MemoryMB: 100000}, []Worker{a, b}); ok || len(why) != 2 {
		t.Fatalf("huge job: %v %v", ok, why)
	}
	a.State = StateDraining
	if ok, _ := FitsAnywhereEver(job, []Worker{a}); !ok {
		t.Fatal("a draining worker can still take the job later")
	}
}

func TestStaticWorkerUsesDeclaredNumbers(t *testing.T) {
	w := Worker{ID: "win", State: StateReady, Snapshot: resource.Static(resource.StaticSpec{OS: "windows", Arch: "amd64", MemoryMB: 131072, Cores: 32}, resourceEpoch)}
	w.PendingMemoryMB = 120000
	if d := Place(Job{ID: "j", MemoryMB: 10000}, []Worker{w}); d.Chosen != "" {
		t.Fatalf("static worker over-committed: %s", d.Summary)
	}
	w.PendingMemoryMB = 0
	d := Place(Job{ID: "j", MemoryMB: 10000}, []Worker{w})
	if d.Chosen != "win" || len(d.Candidates[0].Warnings) == 0 {
		t.Fatalf("%+v", d)
	}
}

// ---- property-based tests ----

func randomWorker(r *rand.Rand, id string) Worker {
	states := []string{StateReady, StateReady, StateReady, StateDraining, StateUnreachable, StateBackoff}
	pressures := []string{resource.PressureNormal, resource.PressureNormal, resource.PressureWarn, resource.PressureCritical, resource.PressureUnknown}
	total := int64(4096 * (1 + r.Intn(32)))
	w := Worker{
		ID: id, Label: id, Local: r.Intn(4) == 0,
		State:     states[r.Intn(len(states))],
		LatencyMS: int64(r.Intn(300)),
		Snapshot: resource.Snapshot{
			Backend: resource.BackendLinux, OS: []string{"darwin", "linux"}[r.Intn(2)], Arch: "arm64",
			Memory: resource.Memory{TotalMB: total, AvailableMB: r.Int63n(total + 1), Pressure: pressures[r.Intn(len(pressures))]},
			CPU:    resource.CPU{LogicalCores: 1 + r.Intn(32), UtilPct: float64(r.Intn(101)), Load5: r.Float64() * 64},
			Disk:   resource.Disk{TotalGB: 500, FreeGB: r.Float64() * 50},
			Power:  resource.Power{Source: []string{resource.PowerAC, resource.PowerBattery}[r.Intn(2)], BatteryPct: r.Intn(101)},
		},
		ActiveAgents:    r.Intn(8),
		PendingMemoryMB: r.Int63n(8192),
		CommittedCPU:    r.Float64() * 16,
		Settings: Settings{
			MaxAgents: r.Intn(6), MinFreeMemoryMB: int64(r.Intn(4096)), MaxCPUCommit: 0.5 + r.Float64()*2,
			MaxLoadPerCore: r.Float64() * 3, MinBatteryPct: r.Intn(50), Weight: r.Float64() * 2,
		},
	}
	if r.Intn(3) == 0 {
		w.Snapshot.Thermal = resource.ThermalCritical
	}
	if r.Intn(3) == 0 {
		w.Settings.Labels = []string{"gpu"}
	}
	return w
}

func randomJob(r *rand.Rand) Job {
	j := Job{ID: "job", Kind: "claude", MemoryMB: int64(256 * (1 + r.Intn(64))), CPU: r.Float64() * 4,
		Policy: PolicyNames[r.Intn(len(PolicyNames))]}
	if r.Intn(4) == 0 {
		j.Labels = []string{"gpu"}
	}
	if r.Intn(5) == 0 {
		j.Machine = "w2"
	}
	return j
}

// A hard constraint is never overridden by a score: whatever the inputs, the
// chosen worker breaks none, and if any worker is eligible one is chosen and
// it has the top score.
func TestPropertyHardConstraintsAreNeverOverriddenByScore(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		n := 1 + r.Intn(6)
		var ws []Worker
		for k := 0; k < n; k++ {
			ws = append(ws, randomWorker(r, "w"+string(rune('0'+k))))
		}
		job := randomJob(r)
		d := Place(job, ws)
		anyEligible := false
		for _, w := range ws {
			if len(Eligible(job, w)) == 0 {
				anyEligible = true
			}
		}
		if anyEligible != (d.Chosen != "") {
			t.Fatalf("case %d: eligible=%v chosen=%q", i, anyEligible, d.Chosen)
		}
		if d.Chosen == "" {
			continue
		}
		var chosen Worker
		for _, w := range ws {
			if w.ID == d.Chosen {
				chosen = w
			}
		}
		if r := Eligible(job, chosen); len(r) != 0 {
			t.Fatalf("case %d: chose %s which breaks %v", i, d.Chosen, r)
		}
		if chosen.FreeMemoryMB()-job.MemoryMB < chosen.Effective().MinFreeMemoryMB {
			t.Fatalf("case %d: memory margin broken", i)
		}
		for _, c := range d.Candidates {
			if c.Eligible && c.Score > d.Candidates[0].Score {
				t.Fatalf("case %d: a higher score lost", i)
			}
		}
	}
}

// The same inputs in any order give the same answer.
func TestPropertyPlacementIsDeterministicUnderReordering(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for i := 0; i < 2000; i++ {
		var ws []Worker
		for k := 0; k < 5; k++ {
			ws = append(ws, randomWorker(r, "w"+string(rune('0'+k))))
		}
		job := randomJob(r)
		first := Place(job, ws).Chosen
		r.Shuffle(len(ws), func(a, b int) { ws[a], ws[b] = ws[b], ws[a] })
		if again := Place(job, ws).Chosen; again != first {
			t.Fatalf("case %d: %q then %q", i, first, again)
		}
	}
}

// Every score is within 0..100 and every factor within 0..1.
func TestPropertyScoresAreBounded(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	for i := 0; i < 3000; i++ {
		w := randomWorker(r, "w")
		job := randomJob(r)
		for _, p := range PolicyNames {
			s, f := Score(job, w, p)
			if s < 0 || s > 100 {
				t.Fatalf("score %v", s)
			}
			for k, v := range f {
				if v < 0 || v > 1 {
					t.Fatalf("factor %s=%v", k, v)
				}
			}
		}
	}
}
