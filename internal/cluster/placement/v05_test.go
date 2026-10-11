package placement

import (
	"strings"
	"testing"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

func reasons(job Job, w Worker) string { return strings.Join(Eligible(job, w), "; ") }

func TestCPUUtilisationThreshold(t *testing.T) {
	w := mac("w", 32768, 20000, 10)
	w.Snapshot.CPU.UtilPct = 70
	job := Job{ID: "j", Kind: "claude", MemoryMB: 1000, CPU: 1}
	// 70% + 1 core of 10 = 80%: under the default 85%.
	if r := reasons(job, w); r != "" {
		t.Fatal(r)
	}
	// Pending CPU (jobs still starting) counts too: 70 + (1+1)/10 = 90%.
	w.PendingCPU = 1
	if r := reasons(job, w); !strings.Contains(r, "cpu: utilisation exceeds policy threshold (90% with this job, limit 85%)") {
		t.Fatal(r)
	}
	// The smoothed figure is used when the coordinator has one.
	w.PendingCPU, w.CPUUtilPct = 0, 50
	if r := reasons(job, w); r != "" {
		t.Fatal(r)
	}
	// A negative threshold turns the check off.
	w.CPUUtilPct = 99
	w.Settings.MaxCPUUtilPct = -1
	if r := reasons(job, w); strings.Contains(r, "cpu: utilisation") {
		t.Fatal(r)
	}
	// The candidate carries the projection for explain and plan.
	w.Settings.MaxCPUUtilPct = 0
	w.CPUUtilPct = 20
	d := Place(job, []Worker{w})
	if d.Candidates[0].CPUAfterPct != 30 || d.Candidates[0].MaxCPUPct != 85 {
		t.Fatalf("%+v", d.Candidates[0])
	}
}

func TestMemoryReservePercent(t *testing.T) {
	w := mac("w", 32768, 8000, 10)
	w.Settings.MemoryReservePct = 20 // 6554 MB kept free, more than the 2048 default
	if w.KeepFreeMB() != 6554 {
		t.Fatal(w.KeepFreeMB())
	}
	if r := reasons(Job{ID: "j", Kind: "claude", MemoryMB: 2000, CPU: 1}, w); !strings.Contains(r, "memory: insufficient unreserved memory") {
		t.Fatal(r)
	}
}

func TestSwapPressureLimit(t *testing.T) {
	w := mac("w", 32768, 20000, 10)
	w.Snapshot.Memory.SwapOutPerSec = 800 // high
	job := Job{ID: "j", Kind: "claude", MemoryMB: 1000, CPU: 1}
	if r := reasons(job, w); !strings.Contains(r, "swap pressure is high") {
		t.Fatal(r)
	}
	w.Settings.MaxSwapPressure = SwapHigh
	if r := reasons(job, w); r != "" {
		t.Fatal(r)
	}
	if SwapLevel(-1) != "" || SwapLevel(0) != SwapNone || SwapLevel(10) != SwapLow || SwapLevel(100) != SwapModerate {
		t.Fatal("levels")
	}
}

func TestRepoRules(t *testing.T) {
	w := mac("w", 32768, 20000, 10)
	w.RunningRepos = map[string]int{"/src/app": 1}
	w.ClusterRepos = map[string]int{"/src/app": 3}
	job := Job{ID: "j", Kind: "claude", Repo: "/src/app", MemoryMB: 1000, CPU: 1}
	if r := reasons(job, w); r != "" {
		t.Fatal(r)
	}
	job.ExclusiveRepo = true
	if r := reasons(job, w); r == "" {
		t.Fatal("an exclusive job ran beside another in the same checkout")
	}
	job.ExclusiveRepo, job.MaxPerRepo = false, 3
	if r := reasons(job, w); !strings.Contains(r, "at most 3 agents across the cluster") {
		t.Fatal(r)
	}
}

func TestAdaptivePolicy(t *testing.T) {
	a := mac("a", 32768, 30000, 10)
	b := mac("b", 32768, 30000, 10)
	if p, _ := ResolveAdaptive([]Worker{a, b}); p != PolicyPerformance {
		t.Fatal(p)
	}
	a.ActiveAgents = 2
	if p, _ := ResolveAdaptive([]Worker{a, b}); p != PolicyBalanced {
		t.Fatal(p)
	}
	b.Snapshot.Memory.Pressure = resource.PressureWarn
	if p, why := ResolveAdaptive([]Worker{a, b}); p != PolicyMemory || !strings.Contains(why, "warn") {
		t.Fatal(p, why)
	}
	d := Place(Job{ID: "j", Kind: "claude", MemoryMB: 1000, CPU: 1, Policy: PolicyAdaptive}, []Worker{a, b})
	if !strings.HasPrefix(d.Policy, "adaptive:") {
		t.Fatal(d.Policy)
	}
	if !ValidPolicy(PolicyAdaptive) || ValidPolicy("nope") {
		t.Fatal("ValidPolicy")
	}
}
