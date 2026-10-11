package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
)

// Learning defaults. The coordinator passes its own window and percentile;
// these are what Estimate uses and what the config defaults to.
const (
	// DefaultWindow is how many recent runs an estimate reads.
	DefaultWindow = 20
	// DefaultPercentile is the percentile of those runs an estimate takes.
	DefaultPercentile = 90.0
	// RunsKept bounds the run history per kind and repository.
	RunsKept = 50
	// MaxSamplesPerJob bounds one job's usage samples. At the default tick
	// that is an hour of readings; older ones are dropped first.
	MaxSamplesPerJob = 240
)

// Percentile is the nearest-rank percentile of xs (0 for an empty slice).
// Nearest-rank always answers with a value that was actually observed, so an
// estimate never invents a figure between two runs.
func Percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	if p <= 0 {
		return s[0]
	}
	if p >= 100 {
		return s[len(s)-1]
	}
	rank := int(math.Ceil(p / 100 * float64(len(s))))
	if rank < 1 {
		rank = 1
	}
	return s[rank-1]
}

func addSample(ctx context.Context, tx *sql.Tx, jobID string, at, rssMB int64, cpu float64) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO job_samples(job_id,at,rss_mb,cpu_pct) VALUES(?,?,?,?)`, jobID, at, rssMB, cpu); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM job_samples WHERE job_id=? AND rowid NOT IN
		(SELECT rowid FROM job_samples WHERE job_id=? ORDER BY at DESC, rowid DESC LIMIT ?)`, jobID, jobID, MaxSamplesPerJob)
	return err
}

// Run is one finished job's usage, as the history keeps it.
type Run struct {
	PeakMB    int64   `json:"peak_rss_mb"`
	TypicalMB int64   `json:"typical_rss_mb"`
	CPU       float64 `json:"cpu_cores"`
}

// recordRun turns a job's samples into one history row: peak memory, the
// median memory (what the agent typically holds), and the 90th percentile of
// its process-tree CPU in cores. A job never measured adds nothing.
func (l *Ledger) recordRun(ctx context.Context, tx *sql.Tx, jobID, kind, repo string, peak int64, outcome string) (Run, error) {
	rows, err := tx.QueryContext(ctx, `SELECT rss_mb, cpu_pct FROM job_samples WHERE job_id=?`, jobID)
	if err != nil {
		return Run{}, err
	}
	var rss, cpu []float64
	for rows.Next() {
		var r int64
		var c float64
		if err := rows.Scan(&r, &c); err != nil {
			rows.Close()
			return Run{}, err
		}
		rss = append(rss, float64(r))
		cpu = append(cpu, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Run{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM job_samples WHERE job_id=?`, jobID); err != nil {
		return Run{}, err
	}
	if peak <= 0 && len(rss) == 0 {
		return Run{}, nil
	}
	run := Run{PeakMB: peak, TypicalMB: int64(Percentile(rss, 50))}
	if run.TypicalMB == 0 {
		run.TypicalMB = peak
	}
	if len(cpu) > 0 {
		run.CPU = math.Round(Percentile(cpu, 90)) / 100
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO runs(kind,repo,at,peak_rss_mb,typical_rss_mb,cpu_cores,outcome) VALUES(?,?,?,?,?,?,?)`,
		kind, repo, l.ms(), run.PeakMB, run.TypicalMB, run.CPU, outcome)
	return run, err
}

// Profile is what has been learned about a kind of agent in a repository.
type Profile struct {
	Kind string `json:"kind"`
	Repo string `json:"repo,omitempty"`
	// MemoryMB is the chosen percentile of peak memory, before headroom.
	MemoryMB     int64  `json:"memory_mb"`
	TypicalMB    int64  `json:"typical_rss_mb"`
	MemoryRuns   int    `json:"memory_runs"`
	MemorySource string `json:"memory_source,omitempty"`
	// CPU is the chosen percentile of each run's 90th percentile CPU, in
	// cores. Runs carried over from schema 1 have no CPU and are skipped.
	CPU       float64 `json:"cpu_cores"`
	CPURuns   int     `json:"cpu_runs"`
	CPUSource string  `json:"cpu_source,omitempty"`
}

// Profile learns from the newest window runs of kind in repo, falling back to
// the kind's runs in any repository when this repository has none. Memory and
// CPU fall back independently.
func (l *Ledger) Profile(ctx context.Context, kind, repo string, window int, pct float64) (Profile, error) {
	if window <= 0 {
		window = DefaultWindow
	}
	p := Profile{Kind: kind, Repo: repo}
	type scope struct {
		where string
		args  []any
		label string
	}
	scopes := []scope{}
	if repo != "" {
		scopes = append(scopes, scope{`kind=? AND repo=?`, []any{kind, repo}, fmt.Sprintf("history:%s@%s", kind, repo)})
	}
	scopes = append(scopes, scope{`kind=?`, []any{kind}, "history:" + kind})
	for _, sc := range scopes {
		rows, err := l.db.QueryContext(ctx, `SELECT peak_rss_mb, typical_rss_mb, cpu_cores FROM runs WHERE `+sc.where+
			` ORDER BY id DESC LIMIT ?`, append(sc.args, window)...)
		if err != nil {
			return p, err
		}
		var peaks, typical, cpus []float64
		for rows.Next() {
			var pk, ty int64
			var c float64
			if err := rows.Scan(&pk, &ty, &c); err != nil {
				rows.Close()
				return p, err
			}
			if pk > 0 {
				peaks = append(peaks, float64(pk))
				typical = append(typical, float64(ty))
			}
			if c > 0 {
				cpus = append(cpus, c)
			}
		}
		rows.Close()
		if p.MemoryRuns == 0 && len(peaks) > 0 {
			p.MemoryMB, p.TypicalMB, p.MemoryRuns = int64(Percentile(peaks, pct)), int64(Percentile(typical, 50)), len(peaks)
			p.MemorySource = fmt.Sprintf("%s (p%g of %d runs)", sc.label, pct, len(peaks))
		}
		if p.CPURuns == 0 && len(cpus) > 0 {
			p.CPU, p.CPURuns = Percentile(cpus, pct), len(cpus)
			p.CPUSource = fmt.Sprintf("%s (p%g of %d runs)", sc.label, pct, len(cpus))
		}
	}
	return p, nil
}

// Profiles lists what has been learned for every kind and repository with
// history, for the estimates view.
func (l *Ledger) Profiles(ctx context.Context, window int, pct float64) ([]Profile, error) {
	rows, err := l.db.QueryContext(ctx, `SELECT DISTINCT kind, repo FROM runs ORDER BY kind, repo`)
	if err != nil {
		return nil, err
	}
	type key struct{ kind, repo string }
	var keys []key
	kinds := map[string]bool{}
	for rows.Next() {
		var k key
		if err := rows.Scan(&k.kind, &k.repo); err != nil {
			rows.Close()
			return nil, err
		}
		keys = append(keys, k)
		kinds[k.kind] = true
	}
	rows.Close()
	var out []Profile
	names := make([]string, 0, len(kinds))
	for k := range kinds {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		p, err := l.Profile(ctx, k, "", window, pct)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	for _, k := range keys {
		if k.repo == "" {
			continue
		}
		p, err := l.Profile(ctx, k.kind, k.repo, window, pct)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}
