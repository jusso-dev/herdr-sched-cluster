package clusterconf

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/config"
)

func TestDefaultsAndOverrides(t *testing.T) {
	dir := t.TempDir()
	workers := filepath.Join(dir, "w.json")
	os.WriteFile(workers, []byte(`{"mini":{"max_agents":3,"weight":0.5,"labels":["arm"]},
"win":{"probe":"static","os":"windows","arch":"amd64","memory_mb":32768,"cores":8,"agents":["codex"]}}`), 0o600)
	s, err := Build(&config.Config{Cluster: map[string]string{
		"cluster_policy": "memory", "cluster_tick_seconds": "5", "cluster_max_agents": "6",
		"cluster_starvation_minutes": "45", "cluster_workers_file": workers, "cluster_metrics_addr": "127.0.0.1:9464",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !s.Enabled || s.Tick != 5*time.Second || s.Coord.Policy != "memory" || s.Coord.Defaults.MaxAgents != 6 ||
		s.Coord.Queue.Starvation != 45*time.Minute || s.MetricsAddr != "127.0.0.1:9464" {
		t.Fatalf("%+v", s)
	}
	if s.Coord.Workers["mini"].Settings.MaxAgents != 3 || s.Coord.Workers["win"].Static.MemoryMB != 32768 {
		t.Fatalf("%+v", s.Coord.Workers)
	}
}

func TestRefusals(t *testing.T) {
	for _, kv := range []map[string]string{
		{"cluster_policy": "fastest"},
		{"cluster_isolation": "docker"},
		{"cluster_metrics_addr": "0.0.0.0:9464"},
		{"cluster_tick_seconds": "0"},
	} {
		if _, err := Build(&config.Config{Cluster: kv}); err == nil {
			t.Errorf("accepted %v", kv)
		}
	}
	dir := t.TempDir()
	bad := filepath.Join(dir, "w.json")
	os.WriteFile(bad, []byte(`{"mini":{"max_agentz":3}}`), 0o600)
	if _, err := LoadWorkers(bad); err == nil {
		t.Error("accepted a misspelt key")
	}
}

func TestTOMLKeysParse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sched.toml")
	os.WriteFile(path, []byte("cluster_policy = \"pack\"\ncluster_max_cpu_commit = 0.8\ncluster_enabled = true\n"), 0o600)
	c, err := config.LoadFrom(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Cluster["cluster_policy"] != "pack" || c.Cluster["cluster_max_cpu_commit"] != "0.8" {
		t.Fatalf("%v", c.Cluster)
	}
	os.WriteFile(path, []byte("cluster_max_cpu_commit = \"lots\"\n"), 0o600)
	if _, err := config.LoadFrom(path); err == nil {
		t.Fatal("accepted a non-number")
	}
}
