package ledger

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPercentileNearestRank(t *testing.T) {
	xs := []float64{5, 1, 4, 2, 3}
	for _, c := range []struct{ p, want float64 }{{50, 3}, {90, 5}, {100, 5}, {20, 1}, {0, 1}} {
		if got := Percentile(xs, c.p); got != c.want {
			t.Errorf("p%g = %g, want %g", c.p, got, c.want)
		}
	}
	if Percentile(nil, 90) != 0 {
		t.Fatal("empty")
	}
}

// A version 1 ledger is upgraded in place: jobs survive and the learned
// memory estimate carries over as one run.
func TestMigrateFromSchema1(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "cluster.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{schema, `INSERT INTO meta(key,value) VALUES('schema','1')`,
		`INSERT INTO history(kind,repo,peak_rss_mb,samples) VALUES('claude','/src/app',3000,4)`,
		`INSERT INTO jobs(id,seq,state,kind,repo,memory_mb,cpu,submitted_at,updated_at) VALUES('j1',1,'finished','claude','/src/app',2048,1,1,1)`} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	_ = os.Chmod(path, 0o600)
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	j, err := l.Get(ctx, "j1")
	if err != nil || j.State != Finished || j.EndReason != "" {
		t.Fatalf("%+v %v", j, err)
	}
	p, err := l.Profile(ctx, "claude", "/src/app", DefaultWindow, DefaultPercentile)
	if err != nil || p.MemoryMB != 3000 || p.MemoryRuns != 1 || p.CPURuns != 0 {
		t.Fatalf("%+v %v", p, err)
	}
	var v string
	_ = l.db.QueryRow(`SELECT value FROM meta WHERE key='schema'`).Scan(&v)
	if v != "2" {
		t.Fatal(v)
	}
	l.Close()
	// Opening again is a no-op.
	l2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	l2.Close()
}

func run(t *testing.T, l *Ledger, j Job, rss int64, cpus ...float64) {
	t.Helper()
	tok, err := l.Reserve(ctx, j.ID, "w", false, func(Load) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	_ = l.MarkDispatching(ctx, j.ID, tok, false)
	_ = l.MarkRunning(ctx, j.ID, tok, "p")
	for _, c := range cpus {
		if _, err := l.Observe(ctx, j.ID, rss, c, 20); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCPUIsLearnedPerKindAndRepo(t *testing.T) {
	l, _ := open(t)
	for i := 0; i < 5; i++ {
		j := submit(t, l, 1000)
		run(t, l, j, 1500, 100, 150, 200) // p90 of the samples is 200% = 2 cores
		if err := l.End(ctx, j.ID, Finished, EndAgentExited, "done"); err != nil {
			t.Fatal(err)
		}
	}
	p, err := l.Profile(ctx, "claude", "/src/app", 20, 90)
	if err != nil || p.CPU != 2 || p.CPURuns != 5 || p.MemoryMB != 1500 {
		t.Fatalf("%+v %v", p, err)
	}
	// Another repository falls back to the kind's runs.
	p, _ = l.Profile(ctx, "claude", "/src/other", 20, 90)
	if p.CPURuns != 5 || p.CPUSource != "history:claude (p90 of 5 runs)" {
		t.Fatalf("%+v", p)
	}
}

func TestLostReadoptAndRetry(t *testing.T) {
	l, _ := open(t)
	n, err := l.Submit(ctx, NewJob{Kind: "claude", Repo: "/src/app", MemoryMB: 1000, CPU: 1, Restartable: true, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	run(t, l, n, 500)
	if err := l.End(ctx, n.ID, Lost, EndWorkerUnreachable, "gone"); err != nil {
		t.Fatal(err)
	}
	if ok, why, _ := l.RetryLost(ctx, n.ID); ok || why == "" {
		t.Fatal("retried an unreachable loss")
	}
	if err := l.Readopt(ctx, n.ID, "p2", ""); err != nil {
		t.Fatal(err)
	}
	if j, _ := l.Get(ctx, n.ID); j.State != Running || j.PaneID != "p2" || j.ReservedMB == 0 {
		t.Fatalf("%+v", j)
	}
	if err := l.End(ctx, n.ID, Lost, EndSessionVanished, "gone"); err != nil {
		t.Fatal(err)
	}
	if ok, _, _ := l.RetryLost(ctx, n.ID); !ok {
		t.Fatal("a restartable confirmed loss with attempts left was not retried")
	}
	run(t, l, n, 500)
	_ = l.End(ctx, n.ID, Lost, EndSessionVanished, "again")
	if ok, why, _ := l.RetryLost(ctx, n.ID); ok || why == "" {
		t.Fatal("retried past max attempts")
	}
}

func TestIntentsNeedTheToken(t *testing.T) {
	l, _ := open(t)
	j := submit(t, l, 1000)
	tok, _ := l.Reserve(ctx, j.ID, "w", false, func(Load) error { return nil })
	if err := l.RecordStartIntent(ctx, j.ID, tok); err == nil {
		t.Fatal("intent before dispatching")
	}
	_ = l.MarkDispatching(ctx, j.ID, tok, false)
	if err := l.RecordStartIntent(ctx, j.ID, "wrong"); err == nil {
		t.Fatal("intent with a stale token")
	}
	if err := l.RecordStartIntent(ctx, j.ID, tok); err != nil {
		t.Fatal(err)
	}
	if got, _ := l.Get(ctx, j.ID); got.StartAttemptedAt.IsZero() {
		t.Fatal("not recorded")
	}
}

func TestIdentityTrustOnFirstUse(t *testing.T) {
	l, _ := open(t)
	if prev, _ := l.CheckIdentity(ctx, "mini", "mini", "mini/darwin/arm64"); prev != "" {
		t.Fatal(prev)
	}
	if prev, _ := l.CheckIdentity(ctx, "mini", "mini", "other/linux/amd64"); prev != "mini/darwin/arm64" {
		t.Fatal(prev)
	}
	_ = l.ForgetIdentity(ctx, "mini", "test")
	if prev, _ := l.CheckIdentity(ctx, "mini", "mini", "other/linux/amd64"); prev != "" {
		t.Fatal(prev)
	}
}

func TestPressureDuration(t *testing.T) {
	now := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)
	l, err := OpenWithClock(filepath.Join(t.TempDir(), "s", "c.db"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, changed, _ := l.SetPressure(ctx, "w", "critical", nil); !changed {
		t.Fatal("first level is a change")
	}
	now = now.Add(3 * time.Minute)
	d, changed, _ := l.SetPressure(ctx, "w", "critical", nil)
	if changed || d != 3*time.Minute {
		t.Fatal(d, changed)
	}
}
