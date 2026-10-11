package ledger

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var ctx = context.Background()

func open(t *testing.T) (*Ledger, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state", "cluster.db")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l, path
}

func submit(t *testing.T, l *Ledger, mem int64) Job {
	t.Helper()
	j, err := l.Submit(ctx, NewJob{Kind: "claude", Repo: "/src/app", Prompt: "fix the tests", MemoryMB: mem, CPU: 1})
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestFilesArePrivate(t *testing.T) {
	l, path := open(t)
	submit(t, l, 1000) // forces the WAL into existence
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		fi, err := os.Stat(p)
		if err != nil {
			continue
		}
		if fi.Mode().Perm()&0o077 != 0 {
			t.Errorf("%s is %v", filepath.Base(p), fi.Mode().Perm())
		}
	}
	dir, _ := os.Stat(filepath.Dir(path))
	if dir.Mode().Perm() != 0o700 {
		t.Errorf("state dir is %v", dir.Mode().Perm())
	}
}

func TestAWorldReadableLedgerIsRefused(t *testing.T) {
	_, path := open(t)
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("opened a ledger other users can read")
	}
}

func TestLifecycle(t *testing.T) {
	l, _ := open(t)
	j := submit(t, l, 4000)
	if j.State != Queued || j.ID != "j1" || j.AgentName != "hsc-j1" || j.PromptSHA == "" || j.PromptLen != 13 {
		t.Fatalf("%+v", j)
	}
	tok, err := l.Reserve(ctx, j.ID, "mbp", false, func(Load) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Reserve(ctx, j.ID, "mbp", false, func(Load) error { return nil }); !errors.Is(err, ErrConflict) {
		t.Fatalf("second reserve: %v", err)
	}
	if err := l.MarkDispatching(ctx, j.ID, "wrong-token", false); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale token moved a job: %v", err)
	}
	if err := l.MarkDispatching(ctx, j.ID, tok, true); err != nil {
		t.Fatal(err)
	}
	if err := l.SetWorkspace(ctx, j.ID, tok, "w3", "w3:p1"); err != nil {
		t.Fatal(err)
	}
	if err := l.MarkRunning(ctx, j.ID, tok, ""); err != nil {
		t.Fatal(err)
	}
	got, _ := l.Get(ctx, j.ID)
	if got.State != Running || got.Worker != "mbp" || got.PaneID != "w3:p1" || !got.Worktree || got.ReservedMB != 4000 {
		t.Fatalf("%+v", got)
	}
	if _, err := l.Cancel(ctx, j.ID, "human"); err == nil {
		t.Fatal("cancelled a running job")
	}
	ld, _ := l.LoadOf(ctx, "mbp", false)
	if ld.ActiveAgents != 1 || ld.PendingMemoryMB != 4000 || ld.RunningRepos["/src/app"] != 1 {
		t.Fatalf("%+v", ld)
	}
	if grew, err := l.Observe(ctx, j.ID, 3000, 25, 10); err != nil || grew {
		t.Fatal(grew, err)
	}
	ld, _ = l.LoadOf(ctx, "mbp", false)
	if ld.PendingMemoryMB != 1000 {
		t.Fatalf("observed use is in the OS numbers, so only 1000 MB is pending: %+v", ld)
	}
	if ld, _ := l.LoadOf(ctx, "mbp", true); ld.PendingMemoryMB != 4000 {
		t.Fatalf("a static worker counts the whole reservation: %+v", ld)
	}
	if err := l.Finish(ctx, j.ID, Finished, "agent exited"); err != nil {
		t.Fatal(err)
	}
	ld, _ = l.LoadOf(ctx, "mbp", false)
	if ld.ActiveAgents != 0 {
		t.Fatal("finished job still holds capacity")
	}
	mb, src, ok, _ := l.Estimate(ctx, "claude", "/src/app", 20)
	if !ok || mb != 3600 || src == "" {
		t.Fatalf("estimate %d %s %v", mb, src, ok)
	}
	if mb, _, ok, _ := l.Estimate(ctx, "claude", "/other", 0); !ok || mb != 3000 {
		t.Fatalf("kind-wide estimate %d %v", mb, ok)
	}
	evs, _ := l.Events(ctx, 0, 0)
	if len(evs) < 5 {
		t.Fatalf("events %d", len(evs))
	}
}

func TestOverrunRaisesTheReservationAndNeverKills(t *testing.T) {
	l, _ := open(t)
	j := submit(t, l, 2000)
	tok, _ := l.Reserve(ctx, j.ID, "mini", false, func(Load) error { return nil })
	_ = l.MarkDispatching(ctx, j.ID, tok, false)
	_ = l.MarkRunning(ctx, j.ID, tok, "w1:p1")
	grew, err := l.Observe(ctx, j.ID, 5000, 50, 10)
	if err != nil || !grew {
		t.Fatal(grew, err)
	}
	got, _ := l.Get(ctx, j.ID)
	if got.ReservedMB != 5500 || got.State != Running {
		t.Fatalf("%+v", got)
	}
}

func TestDispatchFailureRequeuesThenFails(t *testing.T) {
	l, _ := open(t)
	j, _ := l.Submit(ctx, NewJob{Kind: "codex", Repo: "/r", MemoryMB: 1000, MaxAttempts: 2})
	for attempt := 1; attempt <= 2; attempt++ {
		tok, err := l.Reserve(ctx, j.ID, "w", false, func(Load) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		state, err := l.DispatchFailed(ctx, j.ID, tok, "boom")
		if err != nil {
			t.Fatal(err)
		}
		want := Queued
		if attempt == 2 {
			want = Failed
		}
		if state != want {
			t.Fatalf("attempt %d: %s", attempt, state)
		}
		if ld, _ := l.LoadOf(ctx, "w", false); ld.ActiveAgents != 0 {
			t.Fatal("failed dispatch kept its reservation")
		}
	}
}

func TestCancelQueuedAndReserved(t *testing.T) {
	l, _ := open(t)
	a, b := submit(t, l, 1000), submit(t, l, 1000)
	if _, err := l.Reserve(ctx, b.ID, "w", false, func(Load) error { return nil }); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{a.ID, b.ID} {
		j, err := l.Cancel(ctx, id, "human")
		if err != nil || j.State != Cancelled {
			t.Fatal(j.State, err)
		}
	}
	if ld, _ := l.LoadOf(ctx, "w", false); ld.ActiveAgents != 0 {
		t.Fatal("cancelled job kept capacity")
	}
	if _, err := l.Cancel(ctx, "nope", "human"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestWorkerBackoffDoubles(t *testing.T) {
	now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	l, err := OpenWithClock(filepath.Join(t.TempDir(), "c.db"), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for i := 0; i < 4; i++ {
		_ = l.RecordFailure(ctx, "mini", "Mac mini", "unreachable", "timeout", 10*time.Second, 30*time.Second)
	}
	ws, _ := l.Workers(ctx)
	if got := ws["mini"].BackoffUntil.Sub(now); got != 30*time.Second || ws["mini"].Failures != 4 {
		t.Fatalf("%v %d", got, ws["mini"].Failures)
	}
	_ = l.RecordProbe(ctx, "mini", "Mac mini", "ready", map[string]int{"x": 1}, 12)
	ws, _ = l.Workers(ctx)
	if ws["mini"].Failures != 0 || !ws["mini"].BackoffUntil.IsZero() {
		t.Fatalf("%+v", ws["mini"])
	}
}

// Property: however many callers race, through one handle or through two
// handles on the same file (two processes), no job is reserved twice and no
// worker's capacity is exceeded, because the capacity check runs inside the
// write transaction.
func TestPropertyNoReservationIsConsumedTwice(t *testing.T) {
	for round := 0; round < 5; round++ {
		path := filepath.Join(t.TempDir(), "c.db")
		a, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		b, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		r := rand.New(rand.NewSource(int64(round)))
		const capacity = int64(20000)
		var ids []string
		for i := 0; i < 30; i++ {
			j, err := a.Submit(ctx, NewJob{Kind: "claude", Repo: "/r", MemoryMB: int64(500 + r.Intn(3000))})
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, j.ID)
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		won := map[string]int{}
		for g := 0; g < 16; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				l := a
				if g%2 == 1 {
					l = b
				}
				rr := rand.New(rand.NewSource(int64(g)))
				for _, k := range rr.Perm(len(ids)) {
					id := ids[k]
					mem := int64(0)
					if j, err := l.Get(ctx, id); err == nil {
						mem = j.MemoryMB
					}
					_, err := l.Reserve(ctx, id, "w", false, func(ld Load) error {
						if ld.PendingMemoryMB+mem > capacity {
							return fmt.Errorf("full")
						}
						return nil
					})
					if err == nil {
						mu.Lock()
						won[id]++
						mu.Unlock()
					}
				}
			}(g)
		}
		wg.Wait()
		for id, n := range won {
			if n != 1 {
				t.Fatalf("round %d: %s reserved %d times", round, id, n)
			}
		}
		ld, _ := a.LoadOf(ctx, "w", false)
		if ld.PendingMemoryMB > capacity {
			t.Fatalf("round %d: %d MB reserved of %d", round, ld.PendingMemoryMB, capacity)
		}
		if ld.ActiveAgents != len(won) {
			t.Fatalf("round %d: %d live reservations, %d winners", round, ld.ActiveAgents, len(won))
		}
		a.Close()
		b.Close()
	}
}

func TestDecisionsDeduplicateWhileWaiting(t *testing.T) {
	l, _ := open(t)
	j := submit(t, l, 1000)
	for i := 0; i < 5; i++ {
		_ = l.RecordDecision(ctx, j.ID, "", "balanced", "no worker can take this job now", map[string]int{"i": i})
	}
	_ = l.RecordDecision(ctx, j.ID, "mbp", "balanced", "placed on mbp", nil)
	ds, _ := l.Decisions(ctx, j.ID)
	if len(ds) != 2 {
		t.Fatalf("%d decisions", len(ds))
	}
}

func TestPruneKeepsTheTail(t *testing.T) {
	l, _ := open(t)
	for i := 0; i < 50; i++ {
		_ = l.AddEvent(ctx, "x", "", "", nil)
	}
	if err := l.Prune(ctx, 10, 10); err != nil {
		t.Fatal(err)
	}
	evs, _ := l.Events(ctx, 0, 0)
	if len(evs) != 10 || evs[9].ID != 50 {
		t.Fatalf("%d events, last %d", len(evs), evs[len(evs)-1].ID)
	}
}
