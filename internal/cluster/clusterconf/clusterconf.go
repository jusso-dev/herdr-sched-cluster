// Package clusterconf turns the cluster_* keys of sched.toml and the
// optional per-worker file into the coordinator's configuration.
package clusterconf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/coord"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/placement"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
	"github.com/jusso-dev/herdr-sched-cluster/internal/config"
)

// WorkersFileName is the per-worker file's default name in the config dir.
const WorkersFileName = "cluster-workers.json"

// Settings is everything the daemon needs to run the cluster.
type Settings struct {
	Enabled      bool
	Tick         time.Duration
	ProbeTimeout time.Duration
	MetricsAddr  string
	WorkersFile  string
	Coord        coord.Config
}

// WorkerEntry is one worker in the per-worker file. Keys are a Herdr
// machine id or label, or "local" for the coordinator's own computer.
type WorkerEntry struct {
	Probe           string   `json:"probe,omitempty"`
	Program         string   `json:"program,omitempty"`
	MaxAgents       int      `json:"max_agents,omitempty"`
	MinFreeMemoryMB int64    `json:"min_free_memory_mb,omitempty"`
	MaxCPUCommit    float64  `json:"max_cpu_commit,omitempty"`
	MaxLoadPerCore  float64  `json:"max_load_per_core,omitempty"`
	MinFreeDiskGB   float64  `json:"min_free_disk_gb,omitempty"`
	MinBatteryPct   int      `json:"min_battery_pct,omitempty"`
	Weight          float64  `json:"weight,omitempty"`
	Labels          []string `json:"labels,omitempty"`
	Agents          []string `json:"agents,omitempty"`
	// Static capacity, for probe = "static".
	OS       string `json:"os,omitempty"`
	Arch     string `json:"arch,omitempty"`
	MemoryMB int64  `json:"memory_mb,omitempty"`
	Cores    int    `json:"cores,omitempty"`
	// Policy thresholds for this worker.
	MaxCPUUtilPct    float64 `json:"max_cpu_utilisation_percent,omitempty"`
	MemoryReservePct float64 `json:"memory_reserve_percent,omitempty"`
	MaxSwapPressure  string  `json:"max_swap_pressure,omitempty"`
	// PathMap rewrites repository paths for this worker by prefix, for a
	// machine that keeps its checkouts somewhere else.
	PathMap map[string]string `json:"path_map,omitempty"`
}

// Build reads the cluster settings out of a loaded config.
func Build(c *config.Config) (Settings, error) {
	s := Settings{Enabled: true, Tick: 15 * time.Second, ProbeTimeout: 20 * time.Second, Coord: coord.DefaultConfig()}
	s.WorkersFile = filepath.Join(config.ConfigDir(), WorkersFileName)
	kv := map[string]string{}
	if c != nil && c.Cluster != nil {
		kv = c.Cluster
	}
	i := func(k string) (int64, bool) {
		v, ok := kv[k]
		if !ok {
			return 0, false
		}
		n, _ := strconv.ParseInt(v, 10, 64)
		return n, true
	}
	f := func(k string) (float64, bool) {
		v, ok := kv[k]
		if !ok {
			return 0, false
		}
		n, _ := strconv.ParseFloat(v, 64)
		return n, true
	}
	secs := func(k string, into *time.Duration, min int64) error {
		if n, ok := i(k); ok {
			if n < min {
				return fmt.Errorf("%s must be at least %d", k, min)
			}
			*into = time.Duration(n) * time.Second
		}
		return nil
	}
	cc := &s.Coord
	if v, ok := kv["cluster_enabled"]; ok {
		s.Enabled, _ = strconv.ParseBool(v)
	}
	if v, ok := kv["cluster_include_local"]; ok {
		cc.IncludeLocal, _ = strconv.ParseBool(v)
	}
	if v, ok := kv["cluster_policy"]; ok {
		cc.Policy = v
	}
	if v, ok := kv["cluster_isolation"]; ok {
		cc.Isolation = v
	}
	if v, ok := kv["cluster_probe_program"]; ok {
		cc.ProbeProgram = v
	}
	if v, ok := kv["cluster_workers_file"]; ok {
		s.WorkersFile = resource.ExpandHome(v)
	}
	if v, ok := kv["cluster_metrics_addr"]; ok && v != "off" && v != "" {
		if err := loopbackOnly(v); err != nil {
			return s, err
		}
		s.MetricsAddr = v
	}
	for k, into := range map[string]*time.Duration{
		"cluster_tick_seconds":             &s.Tick,
		"cluster_probe_ttl_seconds":        &cc.ProbeTTL,
		"cluster_probe_timeout_seconds":    &s.ProbeTimeout,
		"cluster_dispatch_timeout_seconds": &cc.DispatchTimeout,
		"cluster_dispatch_grace_seconds":   &cc.DispatchGrace,
	} {
		if err := secs(k, into, 1); err != nil {
			return s, err
		}
	}
	if n, ok := i("cluster_max_attempts"); ok {
		cc.MaxAttempts = int(n)
	}
	if n, ok := i("cluster_default_memory_mb"); ok {
		cc.DefaultMemoryMB = n
	}
	if n, ok := f("cluster_default_cpu"); ok {
		cc.DefaultCPU = n
	}
	if n, ok := f("cluster_overrun_margin_pct"); ok {
		cc.OverrunMarginPct = n
	}
	if n, ok := f("cluster_history_headroom_pct"); ok {
		cc.HistoryHeadroomPct = n
	}
	if n, ok := i("cluster_ageing_minutes"); ok {
		cc.Queue.Ageing = time.Duration(n) * time.Minute
	}
	if n, ok := i("cluster_starvation_minutes"); ok {
		cc.Queue.Starvation = time.Duration(n) * time.Minute
	}
	d := &cc.Defaults
	if n, ok := i("cluster_min_free_memory_mb"); ok {
		d.MinFreeMemoryMB = n
	}
	if n, ok := f("cluster_max_cpu_commit"); ok {
		d.MaxCPUCommit = n
	}
	if n, ok := f("cluster_max_load_per_core"); ok {
		d.MaxLoadPerCore = n
	}
	if n, ok := f("cluster_min_free_disk_gb"); ok {
		d.MinFreeDiskGB = n
	}
	if n, ok := i("cluster_min_battery_pct"); ok {
		d.MinBatteryPct = int(n)
	}
	if n, ok := i("cluster_max_agents"); ok {
		d.MaxAgents = int(n)
	}
	if n, ok := f("cluster_max_cpu_utilisation_percent"); ok {
		d.MaxCPUUtilPct = n
	}
	if n, ok := f("cluster_memory_reserve_percent"); ok {
		d.MemoryReservePct = n
	}
	if v, ok := kv["cluster_max_swap_pressure"]; ok {
		d.MaxSwapPressure = v
	}
	if n, ok := i("cluster_history_window"); ok {
		cc.HistoryWindow = int(n)
	}
	if n, ok := f("cluster_history_percentile"); ok {
		cc.HistoryPercentile = n
	}
	if n, ok := i("cluster_max_agents_per_repo"); ok {
		cc.MaxPerRepo = int(n)
	}
	if n, ok := i("cluster_max_queue_size"); ok {
		cc.MaxQueueSize = int(n)
	}
	if v, ok := kv["cluster_worktree_retention"]; ok {
		cc.WorktreeRetention = v
	}
	if n, ok := i("cluster_worktree_max_age_hours"); ok {
		if n < 1 {
			return s, fmt.Errorf("cluster_worktree_max_age_hours must be at least 1")
		}
		cc.WorktreeMaxAge = time.Duration(n) * time.Hour
	}
	if v, ok := kv["cluster_pressure_action"]; ok {
		cc.PressureAction = v
	}
	if n, ok := i("cluster_machine_status_seconds"); ok {
		if n < 0 {
			return s, fmt.Errorf("cluster_machine_status_seconds cannot be negative (0 turns it off)")
		}
		cc.MachineStatusEvery = time.Duration(n) * time.Second
	}
	for k, into := range map[string]*time.Duration{
		"cluster_lost_after_seconds":     &cc.LostAfter,
		"cluster_pressure_grace_seconds": &cc.PressureGrace,
	} {
		if err := secs(k, into, 1); err != nil {
			return s, err
		}
	}
	for k, into := range map[string]*bool{
		"cluster_queue_enabled":         &cc.QueueEnabled,
		"cluster_worktree_remove_dirty": &cc.WorktreeRemoveDirty,
		"cluster_delete_branches":       &cc.DeleteBranches,
		"cluster_retry_lost":            &cc.RetryLost,
		"cluster_verify_identity":       &cc.VerifyIdentity,
		"cluster_owner_checks":          &cc.OwnerChecks,
		"cluster_explanations":          &cc.Explanations,
	} {
		if v, ok := kv[k]; ok {
			*into, _ = strconv.ParseBool(v)
		}
	}
	workers, err := LoadWorkers(s.WorkersFile)
	if err != nil {
		return s, err
	}
	cc.Workers = workers
	return s, cc.Validate()
}

// LoadWorkers reads the per-worker file. A missing file means no
// per-worker settings.
func LoadWorkers(path string) (map[string]coord.WorkerConfig, error) {
	out := map[string]coord.WorkerConfig{}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	var doc map[string]WorkerEntry
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("%s: %v", path, err)
	}
	for id, e := range doc {
		out[id] = coord.WorkerConfig{
			Probe:   e.Probe,
			Program: e.Program,
			Static:  resource.StaticSpec{OS: e.OS, Arch: e.Arch, MemoryMB: e.MemoryMB, Cores: e.Cores, Agents: e.Agents},
			Settings: placement.Settings{MaxAgents: e.MaxAgents, MinFreeMemoryMB: e.MinFreeMemoryMB, MaxCPUCommit: e.MaxCPUCommit,
				MaxLoadPerCore: e.MaxLoadPerCore, MinFreeDiskGB: e.MinFreeDiskGB, MinBatteryPct: e.MinBatteryPct,
				Weight: e.Weight, Labels: e.Labels, Agents: e.Agents, MaxCPUUtilPct: e.MaxCPUUtilPct,
				MemoryReservePct: e.MemoryReservePct, MaxSwapPressure: e.MaxSwapPressure},
			PathMap: e.PathMap,
		}
	}
	return out, nil
}

func loopbackOnly(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("cluster_metrics_addr %q: %v", addr, err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("cluster_metrics_addr must be a loopback address such as 127.0.0.1:9464; got %q", addr)
	}
	return nil
}
