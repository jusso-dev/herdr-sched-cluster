package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/coord"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/placement"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
	"github.com/jusso-dev/herdr-sched-cluster/internal/codes"
	"github.com/jusso-dev/herdr-sched-cluster/internal/protocol"
)

// Cluster is the distributed scheduler as the daemon runs it. A daemon
// without one answers every cluster verb with a refusal saying why.
type Cluster struct {
	Coord *coord.Coordinator
	// Tick is how often reconcile, measure and dispatch run.
	Tick time.Duration
	// MetricsAddr is a loopback address to serve /metrics on, or empty.
	MetricsAddr string
	// Disabled says why there is no cluster, for the refusal.
	Disabled string
}

func (d *Daemon) clusterOn() (*coord.Coordinator, error) {
	if d.Cluster == nil || d.Cluster.Coord == nil {
		why := "this daemon was started without the cluster scheduler"
		if d.Cluster != nil && d.Cluster.Disabled != "" {
			why = d.Cluster.Disabled
		}
		return nil, codes.Refusef(codes.NotRunning, "%s", why)
	}
	return d.Cluster.Coord, nil
}

// clusterTick runs the coordinator on its own rhythm. A round that fails is
// logged and the next one tries again.
func (d *Daemon) clusterTick(ctx context.Context) {
	if d.Cluster == nil || d.Cluster.Coord == nil {
		return
	}
	every := d.Cluster.Tick
	if every <= 0 {
		every = 15 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	prune := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
			rep, err := d.Cluster.Coord.Tick(rctx)
			cancel()
			if err != nil {
				d.logf("cluster tick: %v", err)
			}
			for id, why := range rep.Failed {
				d.logf("cluster: dispatch of %s failed: %s", id, why)
			}
			if prune++; prune%240 == 0 {
				_ = d.Cluster.Coord.L.Prune(ctx, 20000, 20000)
			}
		}
	}
}

// pumpSoon runs one pass right after a submit, so a job does not wait for
// the next tick to be placed.
func (d *Daemon) pumpSoon(c *coord.Coordinator) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		if rep, err := c.Pump(ctx); err != nil {
			d.logf("cluster pump: %v", err)
		} else {
			for id, why := range rep.Failed {
				d.logf("cluster: dispatch of %s failed: %s", id, why)
			}
		}
	}()
}

// WorkerView is one worker as cluster.workers answers.
type WorkerView struct {
	placement.Worker
	FreeMemoryMB int64     `json:"free_memory_mb"`
	Draining     bool      `json:"draining"`
	Failures     int       `json:"failures"`
	LastError    string    `json:"last_error,omitempty"`
	BackoffUntil time.Time `json:"backoff_until,omitempty"`
	LastSeen     time.Time `json:"last_seen,omitempty"`
}

// WorkersReport is what cluster.workers answers.
type WorkersReport struct {
	Workers []WorkerView `json:"workers"`
	Count   int          `json:"count"`
}

// ClusterJobsReport is what cluster.jobs answers.
type ClusterJobsReport struct {
	Jobs  []ledger.Job `json:"jobs"`
	Count int          `json:"count"`
}

// ClusterSubmitted is what cluster.submit answers.
type ClusterSubmitted struct {
	Job   ledger.Job `json:"job"`
	Notes []string   `json:"notes,omitempty"`
}

// ClusterWorkerChange is what drain and undrain answer.
type ClusterWorkerChange struct {
	Worker   string `json:"worker"`
	Draining bool   `json:"draining"`
}

// ClusterEventsReport is what cluster.events answers.
type ClusterEventsReport struct {
	Events []ledger.Event `json:"events"`
	Count  int            `json:"count"`
}

// MetricsReport is what cluster.metrics answers.
type MetricsReport struct {
	Text string `json:"text"`
}

func (d *Daemon) serveCluster(ctx context.Context, verb string, req protocol.Request) (json.RawMessage, error) {
	c, err := d.clusterOn()
	if err != nil {
		return nil, err
	}
	str := func(k string) string { s, _ := req.Args[k].(string); return strings.TrimSpace(s) }
	num := func(k string) (int, bool, error) {
		raw, ok := req.Args[k]
		if !ok {
			return 0, false, nil
		}
		n, err := wholeNumber(raw)
		if err != nil {
			return 0, false, codes.Refusef(codes.Invalid, "%s must be a whole number", k)
		}
		return n, true, nil
	}
	switch verb {
	case "cluster.workers":
		refresh, _ := req.Args["refresh"].(bool)
		ws, err := c.Workers(ctx, refresh, nil)
		if err != nil {
			return nil, err
		}
		rows, err := c.L.Workers(ctx)
		if err != nil {
			return nil, err
		}
		rep := WorkersReport{}
		for _, w := range ws {
			r := rows[w.ID]
			rep.Workers = append(rep.Workers, WorkerView{Worker: w, FreeMemoryMB: w.FreeMemoryMB(), Draining: r.Draining,
				Failures: r.Failures, LastError: r.LastError, BackoffUntil: r.BackoffUntil, LastSeen: r.LastSeen})
		}
		rep.Count = len(rep.Workers)
		return encode(rep, nil)
	case "cluster.submit":
		r := coord.SubmitRequest{Kind: str("kind"), Repo: str("repo"), Prompt: stringArg(req, "prompt"),
			Policy: str("policy"), Machine: str("machine"), OS: str("os"), Arch: str("arch"), Isolation: str("isolation"),
			SubmittedBy: req.Caller()}
		if n, ok, err := num("memory_mb"); err != nil {
			return nil, err
		} else if ok {
			r.MemoryMB = int64(n)
		}
		if n, ok, err := num("priority"); err != nil {
			return nil, err
		} else if ok {
			r.Priority = n
		}
		if s := str("cpu"); s != "" {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil || f <= 0 {
				return nil, codes.Refusef(codes.Invalid, "cpu must be a positive number, got %q", s)
			}
			r.CPU = f
		}
		if s := str("labels"); s != "" {
			for _, l := range strings.Split(s, ",") {
				if l = strings.TrimSpace(l); l != "" {
					r.Labels = append(r.Labels, l)
				}
			}
		}
		r.AgentArgs = strings.Fields(str("agent_args"))
		job, notes, err := c.Submit(ctx, r)
		if err != nil {
			if errors.Is(err, ledger.ErrNotFound) {
				return nil, codes.Errorf(codes.NotFound, "%v", err)
			}
			return nil, codes.Refusef(codes.Invalid, "%v", err)
		}
		d.logf("%s queued cluster job %s (%s in %s)", req.Caller(), job.ID, job.Kind, job.Repo)
		d.pumpSoon(c)
		return encode(ClusterSubmitted{Job: job, Notes: notes}, nil)
	case "cluster.status":
		st, err := c.Status(ctx)
		return encode(st, err)
	case "cluster.queue":
		q, err := c.Queue(ctx)
		return encode(q, err)
	case "cluster.plan":
		reqs, err := planRequests(req)
		if err != nil {
			return nil, err
		}
		p, err := c.Plan(ctx, reqs)
		if err != nil {
			return nil, codes.Refusef(codes.Invalid, "%v", err)
		}
		return encode(p, nil)
	case "cluster.jobs":
		var states []string
		for _, s := range strings.Split(str("state"), ",") {
			if s = strings.TrimSpace(s); s != "" {
				states = append(states, s)
			}
		}
		jobs, err := c.L.List(ctx, states...)
		if err != nil {
			return nil, err
		}
		sort.SliceStable(jobs, func(a, b int) bool { return jobs[a].Seq > jobs[b].Seq })
		if n, ok, err := num("limit"); err != nil {
			return nil, err
		} else if ok && n >= 0 && n < len(jobs) {
			jobs = jobs[:n]
		}
		return encode(ClusterJobsReport{Jobs: jobs, Count: len(jobs)}, nil)
	case "cluster.explain":
		ex, err := c.Explain(ctx, str("id"))
		return encode(ex, notFound(err, str("id")))
	case "cluster.cancel":
		j, err := c.L.Cancel(ctx, str("id"), req.Caller())
		if err == nil {
			d.logf("%s cancelled cluster job %s", req.Caller(), j.ID)
		}
		return encode(j, notFound(err, str("id")))
	case "cluster.detach":
		id := str("id")
		if _, err := c.L.Get(ctx, id); err != nil {
			return nil, notFound(err, id)
		}
		if err := c.L.Finish(ctx, id, ledger.Detached, "detached by "+req.Caller()); err != nil {
			if errors.Is(err, ledger.ErrConflict) {
				return nil, codes.Refusef(codes.Invalid, "%s is not running; only a running job can be detached", id)
			}
			return nil, notFound(err, id)
		}
		j, err := c.L.Get(ctx, id)
		return encode(j, err)
	case "cluster.drain", "cluster.undrain":
		on := verb == "cluster.drain"
		id, err := c.Drain(ctx, str("worker"), on, req.Caller())
		if err != nil {
			return nil, codes.Errorf(codes.NotFound, "%v", err)
		}
		d.logf("%s set worker %s draining=%v", req.Caller(), id, on)
		return encode(ClusterWorkerChange{Worker: id, Draining: on}, nil)
	case "cluster.events":
		since, _, err := num("since")
		if err != nil {
			return nil, err
		}
		limit, _, err := num("limit")
		if err != nil {
			return nil, err
		}
		evs, err := c.L.Events(ctx, int64(since), limit)
		return encode(ClusterEventsReport{Events: evs, Count: len(evs)}, err)
	case "cluster.metrics":
		text, err := Metrics(ctx, c)
		return encode(MetricsReport{Text: text}, err)
	}
	return nil, codes.Refusef(codes.Invalid, "verb %q is declared and not served", verb)
}

// planRequests reads cluster.plan's arguments: a jobs list, or kind, repo and
// count for N identical jobs. Naming both is refused rather than merged, so
// the caller is never left guessing which one was planned.
func planRequests(req protocol.Request) ([]coord.SubmitRequest, error) {
	caller := req.Caller()
	list, hasList := req.Args["jobs"].([]any)
	kind, _ := req.Args["kind"].(string)
	repo, _ := req.Args["repo"].(string)
	short := kind != "" || repo != ""
	if hasList && short {
		return nil, codes.Refusef(codes.Invalid, "cluster.plan takes jobs, or kind and repo with count, not both")
	}
	if !hasList && !short {
		return nil, codes.Refusef(codes.Invalid, "cluster.plan needs jobs, or kind and repo (with an optional count)")
	}
	if short {
		if kind == "" || repo == "" {
			return nil, codes.Refusef(codes.Invalid, "cluster.plan needs both kind and repo")
		}
		count := 1
		if raw, ok := req.Args["count"]; ok {
			n, err := wholeNumber(raw)
			if err != nil || n < 1 {
				return nil, codes.Refusef(codes.Invalid, "count must be a whole number of at least 1")
			}
			count = n
		}
		if count > coord.MaxPlanJobs {
			return nil, codes.Refusef(codes.Invalid, "count is %d; a plan takes at most %d jobs", count, coord.MaxPlanJobs)
		}
		one := map[string]any{"kind": kind, "repo": repo}
		for _, k := range []string{"memory_mb", "priority", "policy"} {
			if v, ok := req.Args[k]; ok {
				one[k] = v
			}
		}
		for i := 0; i < count; i++ {
			list = append(list, one)
		}
	} else {
		for _, k := range []string{"count", "memory_mb", "priority", "policy"} {
			if _, ok := req.Args[k]; ok {
				return nil, codes.Refusef(codes.Invalid, "%s goes inside each entry of jobs, not beside it", k)
			}
		}
	}
	var out []coord.SubmitRequest
	for i, raw := range list {
		m, _ := raw.(map[string]any)
		s := func(k string) string { v, _ := m[k].(string); return strings.TrimSpace(v) }
		r := coord.SubmitRequest{Kind: s("kind"), Repo: s("repo"), Policy: s("policy"), Machine: s("machine"),
			OS: s("os"), Arch: s("arch"), SubmittedBy: caller}
		if v, ok := m["memory_mb"]; ok {
			n, err := wholeNumber(v)
			if err != nil {
				return nil, codes.Refusef(codes.Invalid, "entry %d: memory_mb must be a whole number", i+1)
			}
			r.MemoryMB = int64(n)
		}
		if v, ok := m["priority"]; ok {
			n, err := wholeNumber(v)
			if err != nil {
				return nil, codes.Refusef(codes.Invalid, "entry %d: priority must be a whole number", i+1)
			}
			r.Priority = n
		}
		switch v := m["cpu"].(type) {
		case float64:
			r.CPU = v
		case string:
			if strings.TrimSpace(v) != "" {
				f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
				if err != nil || f <= 0 {
					return nil, codes.Refusef(codes.Invalid, "entry %d: cpu must be a positive number", i+1)
				}
				r.CPU = f
			}
		}
		for _, l := range strings.Split(s("labels"), ",") {
			if l = strings.TrimSpace(l); l != "" {
				r.Labels = append(r.Labels, l)
			}
		}
		out = append(out, r)
	}
	return out, nil
}

func stringArg(req protocol.Request, k string) string { s, _ := req.Args[k].(string); return s }

func notFound(err error, id string) error {
	if errors.Is(err, ledger.ErrNotFound) {
		return codes.Errorf(codes.NotFound, "no cluster job %s", id)
	}
	if err != nil && !isCoded(err) {
		return codes.Refusef(codes.Invalid, "%v", err)
	}
	return err
}

func isCoded(err error) bool {
	var ce *codes.Error
	return errors.As(err, &ce)
}

// Metrics renders the Prometheus text format from the last recorded probe
// of each worker and the ledger. It never probes, so a scrape is cheap.
func Metrics(ctx context.Context, c *coord.Coordinator) (string, error) {
	rows, err := c.L.Workers(ctx)
	if err != nil {
		return "", err
	}
	counts, err := c.L.Counts(ctx)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	gauge := func(name, help string) { fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name) }
	ids := make([]string, 0, len(rows))
	for id := range rows {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	type series struct {
		name, help string
		val        func(id string, s resource.Snapshot, r ledger.WorkerRow, ld ledger.Load) float64
	}
	snaps := map[string]resource.Snapshot{}
	loads := map[string]ledger.Load{}
	for _, id := range ids {
		var s resource.Snapshot
		_ = json.Unmarshal(rows[id].Snapshot, &s)
		snaps[id] = s
		ld, err := c.L.LoadOf(ctx, id, s.Backend == resource.BackendStatic)
		if err != nil {
			return "", err
		}
		loads[id] = ld
	}
	all := []series{
		{"hsched_cluster_worker_up", "1 when the last probe succeeded", func(_ string, _ resource.Snapshot, r ledger.WorkerRow, _ ledger.Load) float64 {
			return b2f(r.State == placement.StateReady)
		}},
		{"hsched_cluster_worker_draining", "1 when the worker is draining", func(_ string, _ resource.Snapshot, r ledger.WorkerRow, _ ledger.Load) float64 { return b2f(r.Draining) }},
		{"hsched_cluster_memory_total_mb", "Physical memory", func(_ string, s resource.Snapshot, _ ledger.WorkerRow, _ ledger.Load) float64 {
			return float64(s.Memory.TotalMB)
		}},
		{"hsched_cluster_memory_available_mb", "Memory available at the last probe", func(_ string, s resource.Snapshot, _ ledger.WorkerRow, _ ledger.Load) float64 {
			return float64(s.Memory.AvailableMB)
		}},
		{"hsched_cluster_memory_pressure", "0 normal, 1 warn, 2 critical, -1 unknown", func(_ string, s resource.Snapshot, _ ledger.WorkerRow, _ ledger.Load) float64 {
			return float64(resource.Rank(s.Memory.Pressure))
		}},
		{"hsched_cluster_swap_used_mb", "Swap in use", func(_ string, s resource.Snapshot, _ ledger.WorkerRow, _ ledger.Load) float64 {
			return float64(s.Memory.SwapUsedMB)
		}},
		{"hsched_cluster_cpu_util_pct", "CPU use over the probe's sample", func(_ string, s resource.Snapshot, _ ledger.WorkerRow, _ ledger.Load) float64 { return s.CPU.UtilPct }},
		{"hsched_cluster_load5", "Five-minute load average", func(_ string, s resource.Snapshot, _ ledger.WorkerRow, _ ledger.Load) float64 { return s.CPU.Load5 }},
		{"hsched_cluster_disk_free_gb", "Free disk in the home directory's filesystem", func(_ string, s resource.Snapshot, _ ledger.WorkerRow, _ ledger.Load) float64 { return s.Disk.FreeGB }},
		{"hsched_cluster_reserved_memory_mb", "Memory reserved by active cluster jobs", func(_ string, _ resource.Snapshot, _ ledger.WorkerRow, ld ledger.Load) float64 {
			return float64(ld.ReservedMB)
		}},
		{"hsched_cluster_active_agents", "Active cluster jobs on the worker", func(_ string, _ resource.Snapshot, _ ledger.WorkerRow, ld ledger.Load) float64 {
			return float64(ld.ActiveAgents)
		}},
		{"hsched_cluster_probe_latency_ms", "Round trip of the last probe", func(_ string, _ resource.Snapshot, r ledger.WorkerRow, _ ledger.Load) float64 {
			return float64(r.LatencyMS)
		}},
	}
	for _, s := range all {
		gauge(s.name, s.help)
		for _, id := range ids {
			fmt.Fprintf(&b, "%s{worker=%q} %s\n", s.name, id, strconv.FormatFloat(s.val(id, snaps[id], rows[id], loads[id]), 'f', -1, 64))
		}
	}
	gauge("hsched_cluster_jobs", "Cluster jobs by state")
	states := []string{ledger.Queued, ledger.Reserved, ledger.Dispatching, ledger.Running, ledger.Finished, ledger.Failed, ledger.Cancelled, ledger.Lost, ledger.Detached}
	for _, st := range states {
		fmt.Fprintf(&b, "hsched_cluster_jobs{state=%q} %d\n", st, counts[st])
	}
	return b.String(), nil
}

func b2f(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// serveMetrics serves /metrics on a loopback address until ctx ends.
func (d *Daemon) serveMetrics(ctx context.Context) {
	if d.Cluster == nil || d.Cluster.Coord == nil || d.Cluster.MetricsAddr == "" {
		return
	}
	ln, err := net.Listen("tcp", d.Cluster.MetricsAddr)
	if err != nil {
		d.logf("cluster metrics: cannot listen on %s: %v", d.Cluster.MetricsAddr, err)
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		text, err := Metrics(r.Context(), d.Cluster.Coord)
		if err != nil {
			http.Error(w, "metrics unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(text))
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); _ = srv.Close() }()
	d.logf("cluster metrics on http://%s/metrics", d.Cluster.MetricsAddr)
	_ = srv.Serve(ln)
}
