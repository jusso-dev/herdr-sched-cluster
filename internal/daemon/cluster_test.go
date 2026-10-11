package daemon

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/coord"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/sim"
	"github.com/jusso-dev/herdr-sched-cluster/internal/codes"
	"github.com/jusso-dev/herdr-sched-cluster/internal/verbs"
)

func clusterDaemon(t *testing.T) (*Daemon, *sim.Cluster) {
	t.Helper()
	d := newDaemon(t)
	cl := sim.New()
	cl.Add("mbp", 65536, 16)
	cl.Add("mini", 16384, 10)
	l, err := ledger.OpenWithClock(filepath.Join(t.TempDir(), "cluster.db"), cl.Clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	cfg := coord.DefaultConfig()
	cfg.IncludeLocal = false
	c := coord.New(l, cl.Herdr(), cl.Prober(), cfg)
	c.Now = cl.Clock.Now
	d.Cluster = &Cluster{Coord: c}
	return d, cl
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestClusterVerbsEndToEnd(t *testing.T) {
	d, cl := clusterDaemon(t)
	raw, err := call(t, d, "cluster.submit", map[string]any{
		"kind": "claude", "repo": "~/src/app", "prompt": "secret plan: fix the build",
		"memory_mb": float64(4096), "cpu": "1.5", "labels": "", "agent_args": "--model opus",
	})
	if err != nil {
		t.Fatal(err)
	}
	var sub ClusterSubmitted
	if err := json.Unmarshal(raw, &sub); err != nil {
		t.Fatal(err)
	}
	if sub.Job.ID != "j1" || sub.Job.CPU != 1.5 || sub.Job.SubmittedBy != "agent:wT:p1" {
		t.Fatalf("%+v", sub.Job)
	}
	if strings.Contains(string(raw), "secret plan") {
		t.Fatal("the prompt was echoed back")
	}
	// The submit pumps straight away.
	waitFor(t, "dispatch", func() bool { return cl.Where("hsc-j1") != "" })
	waitFor(t, "running", func() bool {
		raw, _ := call(t, d, "cluster.jobs", map[string]any{"state": "running"})
		return strings.Contains(string(raw), `"id":"j1"`)
	})

	raw, err = call(t, d, "cluster.workers", map[string]any{"refresh": true})
	if err != nil {
		t.Fatal(err)
	}
	var ws WorkersReport
	_ = json.Unmarshal(raw, &ws)
	if ws.Count != 2 {
		t.Fatalf("%s", raw)
	}

	raw, err = call(t, d, "cluster.explain", map[string]any{"id": "j1"})
	if err != nil || !strings.Contains(string(raw), "candidates") {
		t.Fatalf("%s %v", raw, err)
	}

	raw, err = call(t, d, "cluster.metrics", nil)
	if err != nil || !strings.Contains(string(raw), `hsched_cluster_jobs{state=\"running\"} 1`) {
		t.Fatalf("%s %v", raw, err)
	}

	raw, err = call(t, d, "cluster.events", map[string]any{"limit": float64(100)})
	if err != nil || strings.Contains(string(raw), "secret plan") {
		t.Fatalf("events leak the prompt or failed: %v", err)
	}

	if _, err := call(t, d, "cluster.cancel", map[string]any{"id": "j1"}); err == nil {
		t.Fatal("cancelled a running job")
	}
	if _, err := call(t, d, "cluster.detach", map[string]any{"id": "j1"}); err != nil {
		t.Fatal(err)
	}
	if cl.Where("hsc-j1") == "" {
		t.Fatal("detach stopped the agent")
	}
	if _, err := call(t, d, "cluster.detach", map[string]any{"id": "nope"}); codes.Of(err) != codes.NotFound {
		t.Fatalf("%v", err)
	}

	if _, err := call(t, d, "cluster.drain", map[string]any{"worker": "mini"}); err != nil {
		t.Fatal(err)
	}
	if _, err := call(t, d, "cluster.drain", map[string]any{"worker": "ghost"}); err == nil {
		t.Fatal("drained a ghost")
	}
	if _, err := call(t, d, "cluster.submit", map[string]any{"kind": "claude", "repo": "relative"}); err == nil {
		t.Fatal("accepted a relative repository")
	}
	_ = context.Background()
}

func TestClusterOffRefusesWithTheReason(t *testing.T) {
	d := newDaemon(t)
	d.Cluster = &Cluster{Disabled: "the cluster scheduler is off (cluster_enabled = false)"}
	_, err := call(t, d, "cluster.workers", nil)
	if err == nil || !strings.Contains(err.Error(), "cluster_enabled = false") {
		t.Fatalf("%v", err)
	}
	d.Cluster = nil
	if _, err := call(t, d, "cluster.jobs", nil); err == nil {
		t.Fatal("answered without a cluster")
	}
}

// Every cluster verb the registry declares is one the daemon serves. A verb
// that is declared and routed nowhere passes every unit test of its own logic
// and fails the first real call.
func TestEveryClusterVerbIsServed(t *testing.T) {
	d, _ := clusterDaemon(t)
	for _, v := range verbs.All {
		if !strings.HasPrefix(v.Name, "cluster.") {
			continue
		}
		_, err := call(t, d, v.Name, map[string]any{})
		if err != nil && strings.Contains(err.Error(), "declared and not served") {
			t.Errorf("%s: %v", v.Name, err)
		}
	}
}

func TestClusterPlanStatusAndQueueOverTheDaemon(t *testing.T) {
	d, cl := clusterDaemon(t)
	raw, err := call(t, d, "cluster.plan", map[string]any{"kind": "claude", "repo": "~/src/app", "count": float64(10), "memory_mb": float64(8192)})
	if err != nil {
		t.Fatal(err)
	}
	var p coord.Plan
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Jobs) != 10 || p.Placed == 0 || p.Queued == 0 {
		t.Fatalf("%s", raw)
	}
	if cl.Live("mbp")+cl.Live("mini") != 0 {
		t.Fatal("plan launched an agent")
	}
	raw, err = call(t, d, "cluster.plan", map[string]any{"jobs": []any{
		map[string]any{"kind": "codex", "repo": "~/src/a", "memory_mb": float64(4096), "priority": float64(10)},
		map[string]any{"kind": "claude", "repo": "~/src/b", "cpu": 2.0},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &p); err != nil || len(p.Jobs) != 2 || p.Jobs[1].CPU != 2 {
		t.Fatalf("%s %v", raw, err)
	}
	for name, args := range map[string]map[string]any{
		"neither":      {},
		"both":         {"kind": "claude", "repo": "~/x", "jobs": []any{map[string]any{"kind": "claude", "repo": "~/x"}}},
		"no repo":      {"kind": "claude"},
		"bad field":    {"jobs": []any{map[string]any{"kind": "claude", "repo": "~/x", "prompt": "hi"}}},
		"not a list":   {"jobs": "claude"},
		"count beside": {"jobs": []any{map[string]any{"kind": "claude", "repo": "~/x"}}, "count": float64(2)},
		"huge":         {"kind": "claude", "repo": "~/x", "count": float64(1000)},
	} {
		if _, err := call(t, d, "cluster.plan", args); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	raw, err = call(t, d, "cluster.status", nil)
	if err != nil || !strings.Contains(string(raw), `"room_for_default_agents"`) {
		t.Fatalf("%s %v", raw, err)
	}
	raw, err = call(t, d, "cluster.queue", nil)
	if err != nil || !strings.Contains(string(raw), `"count":0`) {
		t.Fatalf("%s %v", raw, err)
	}
}
