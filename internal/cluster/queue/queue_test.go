package queue

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/placement"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

var t0 = time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)

func worker(id string, availMB int64) placement.Worker {
	return placement.Worker{ID: id, State: placement.StateReady, Snapshot: resource.Snapshot{
		OS: "darwin", Memory: resource.Memory{TotalMB: 65536, AvailableMB: availMB, Pressure: resource.PressureNormal},
		CPU: resource.CPU{LogicalCores: 16}, Power: resource.Power{Source: resource.PowerAC},
	}, Settings: placement.Settings{MinFreeMemoryMB: 1000}}
}

func entry(id string, mem int64, prio int, at time.Duration, seq int64) Entry {
	return Entry{Job: placement.Job{ID: id, MemoryMB: mem, CPU: 0.5}, Priority: prio, SubmittedAt: t0.Add(at), Seq: seq}
}

func TestOrderingAndAgeing(t *testing.T) {
	es := []Entry{entry("low-old", 1, 0, 0, 1), entry("high-new", 1, 5, 50*time.Minute, 2), entry("low-new", 1, 0, 30*time.Minute, 3)}
	now := t0.Add(60 * time.Minute)
	got := Order(es, now, 0)
	if got[0].Job.ID != "high-new" || got[1].Job.ID != "low-old" {
		t.Fatalf("no ageing: %v", ids(got))
	}
	// With one point per ten minutes, the hour-old job reaches 6 and passes
	// the ten-minute-old high one at 6, winning on submission time.
	got = Order(es, now, 10*time.Minute)
	if got[0].Job.ID != "low-old" {
		t.Fatalf("ageing: %v", ids(got))
	}
}

func TestBackfillPlacesSmallJobsBehindABigOne(t *testing.T) {
	ws := []placement.Worker{worker("mini", 9000)}
	es := []Entry{entry("big", 20000, 0, 0, 1), entry("small1", 2000, 0, time.Second, 2), entry("small2", 2000, 0, 2*time.Second, 3)}
	p := Pass(es, ws, t0.Add(time.Minute), Config{}, nil)
	if len(p.Assignments) != 2 || p.Assignments[0].JobID != "small1" || p.Assignments[1].JobID != "small2" {
		t.Fatalf("assignments %+v", p.Assignments)
	}
	if len(p.Waiting) != 1 || p.Waiting[0].JobID != "big" {
		t.Fatalf("waiting %+v", p.Waiting)
	}
}

func TestPassAccountsForEarlierAssignments(t *testing.T) {
	ws := []placement.Worker{worker("mini", 6500)}
	es := []Entry{entry("a", 3000, 0, 0, 1), entry("b", 3000, 0, time.Second, 2)}
	p := Pass(es, ws, t0, Config{}, nil)
	if len(p.Assignments) != 1 {
		t.Fatalf("only one 3000 MB job fits into 6500 MB with a 1000 MB margin: %+v", p.Assignments)
	}
	if ws[0].PendingMemoryMB != 0 {
		t.Fatal("Pass changed the caller's workers")
	}
}

func TestStarvationGuardHoldsAWorkerAndStopsBackfill(t *testing.T) {
	ws := []placement.Worker{worker("mbp", 15000), worker("mini", 9000)}
	es := []Entry{entry("big", 20000, 0, 0, 1), entry("small", 2000, 0, time.Second, 2)}
	cfg := Config{Starvation: 10 * time.Minute}
	p := Pass(es, ws, t0.Add(11*time.Minute), cfg, nil)
	if p.Holds["mbp"] != "big" {
		t.Fatalf("holds %+v", p.Holds)
	}
	if len(p.Assignments) != 1 || p.Assignments[0].WorkerID != "mini" {
		t.Fatalf("small job must backfill onto the other worker only: %+v", p.Assignments)
	}
	// Once mbp frees up, the held job is placed and the hold drops.
	ws[0].Snapshot.Memory.AvailableMB = 40000
	p = Pass(es[:1], ws, t0.Add(12*time.Minute), cfg, p.Holds)
	if len(p.Assignments) != 1 || p.Assignments[0].WorkerID != "mbp" || len(p.Holds) != 0 {
		t.Fatalf("%+v holds %+v", p.Assignments, p.Holds)
	}
}

func TestHoldForAJobNoLongerQueuedIsDropped(t *testing.T) {
	p := Pass(nil, []placement.Worker{worker("a", 9000)}, t0, Config{}, map[string]string{"a": "gone"})
	if len(p.Holds) != 0 {
		t.Fatal(p.Holds)
	}
}

// Property: a plan never over-commits a worker's memory or assigns a job
// twice, whatever the queue and the workers.
func TestPropertyAPassNeverOvercommitsOrDuplicates(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for i := 0; i < 2000; i++ {
		var ws []placement.Worker
		for k := 0; k < 1+r.Intn(4); k++ {
			ws = append(ws, worker(fmt.Sprintf("w%d", k), int64(2000+r.Intn(60000))))
		}
		var es []Entry
		for k := 0; k < r.Intn(20); k++ {
			es = append(es, entry(fmt.Sprintf("j%d", k), int64(256*(1+r.Intn(40))), r.Intn(5), time.Duration(r.Intn(3600))*time.Second, int64(k)))
		}
		p := Pass(es, ws, t0.Add(2*time.Hour), Config{Ageing: 5 * time.Minute, Starvation: 30 * time.Minute}, nil)
		seen := map[string]bool{}
		used := map[string]int64{}
		mem := map[string]int64{}
		for _, e := range es {
			mem[e.Job.ID] = e.Job.MemoryMB
		}
		for _, a := range p.Assignments {
			if seen[a.JobID] {
				t.Fatalf("case %d: %s assigned twice", i, a.JobID)
			}
			seen[a.JobID] = true
			used[a.WorkerID] += mem[a.JobID]
		}
		for _, w := range ws {
			if w.Snapshot.Memory.AvailableMB-used[w.ID] < w.Settings.MinFreeMemoryMB {
				t.Fatalf("case %d: %s over-committed: %d used of %d", i, w.ID, used[w.ID], w.Snapshot.Memory.AvailableMB)
			}
		}
		if len(p.Assignments)+len(p.Waiting) != len(es) {
			t.Fatalf("case %d: %d jobs, %d assigned, %d waiting", i, len(es), len(p.Assignments), len(p.Waiting))
		}
	}
}

func ids(es []Entry) []string {
	var out []string
	for _, e := range es {
		out = append(out, e.Job.ID)
	}
	return out
}
