package clusterconf

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/coord"
	"github.com/jusso-dev/herdr-sched-cluster/internal/config"
)

func TestV05Keys(t *testing.T) {
	dir := t.TempDir()
	workers := filepath.Join(dir, "w.json")
	os.WriteFile(workers, []byte(`{"mini":{"path_map":{"~/src":"/Volumes/work/src"},"max_cpu_utilisation_percent":70,"max_swap_pressure":"low"}}`), 0o600)
	s, err := Build(&config.Config{Cluster: map[string]string{
		"cluster_workers_file": workers, "cluster_worktree_retention": "age", "cluster_worktree_max_age_hours": "12",
		"cluster_history_window": "10", "cluster_history_percentile": "75", "cluster_lost_after_seconds": "600",
		"cluster_retry_lost": "false", "cluster_max_queue_size": "5", "cluster_pressure_action": "terminate-disposable",
		"cluster_machine_status_seconds": "0", "cluster_max_cpu_utilisation_percent": "90", "cluster_memory_reserve_percent": "10",
		"cluster_owner_checks": "false", "cluster_delete_branches": "true",
	}})
	if err != nil {
		t.Fatal(err)
	}
	c := s.Coord
	if c.WorktreeRetention != coord.RetainForAge || c.WorktreeMaxAge != 12*time.Hour || c.HistoryWindow != 10 ||
		c.HistoryPercentile != 75 || c.LostAfter != 10*time.Minute || c.RetryLost || c.MaxQueueSize != 5 ||
		c.PressureAction != coord.PressureTerminateDisposable || c.MachineStatusEvery != 0 || c.Defaults.MaxCPUUtilPct != 90 ||
		c.Defaults.MemoryReservePct != 10 || c.OwnerChecks || !c.DeleteBranches {
		t.Fatalf("%+v", c)
	}
	w := c.Workers["mini"]
	if w.PathMap["~/src"] != "/Volumes/work/src" || w.Settings.MaxCPUUtilPct != 70 || w.Settings.MaxSwapPressure != "low" {
		t.Fatalf("%+v", w)
	}
	for _, kv := range []map[string]string{
		{"cluster_worktree_retention": "sometimes"},
		{"cluster_pressure_action": "kill-everything"},
		{"cluster_history_percentile": "0"},
		{"cluster_max_cpu_utilisation_percent": "150"},
		{"cluster_max_swap_pressure": "extreme"},
		{"cluster_memory_reserve_percent": "100"},
	} {
		if _, err := Build(&config.Config{Cluster: kv}); err == nil {
			t.Errorf("accepted %v", kv)
		}
	}
}
