package sim

import (
	"strings"
	"testing"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/coord"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
)

func tenJobs(mem int64) []coord.SubmitRequest {
	var out []coord.SubmitRequest
	for i := 0; i < 10; i++ {
		out = append(out, coord.SubmitRequest{Kind: "claude", Repo: repo, MemoryMB: mem, CPU: 1})
	}
	return out
}

// A plan is a promise about what submit would do: submitting the same batch
// and running one pass puts the same number of agents on each worker and
// leaves the same number queued.
func TestPlanMatchesWhatSubmitThenDoes(t *testing.T) {
	r := newRig(t, nil)
	p, err := r.c.Plan(ctx, tenJobs(8192))
	if err != nil {
		t.Fatal(err)
	}
	if p.Placed+p.Queued+p.Refused != 10 || p.Refused != 0 {
		t.Fatalf("plan counts: %+v", p)
	}
	if p.Queued == 0 {
		t.Fatalf("ten 8 GB agents should not all fit on 64 + 16 GB with 2 GB kept free: %s", p.Summary)
	}
	want := map[string]int{}
	for _, j := range p.Jobs {
		if j.Outcome == coord.PlanPlaced {
			want[j.Worker]++
		} else if !strings.Contains(j.Reason, "memory") {
			t.Errorf("queued job reason should name memory: %s", j.Reason)
		}
	}
	for range 10 {
		r.submit(8192)
	}
	r.tick()
	r.check()
	got := map[string]int{}
	running, _ := r.l.List(ctx, ledger.Running, ledger.Dispatching, ledger.Reserved)
	for _, j := range running {
		got[j.Worker]++
	}
	for w, n := range want {
		if got[w] != n {
			t.Errorf("worker %s: plan said %d, a real pass placed %d (plan %v, real %v)", w, n, got[w], want, got)
		}
	}
	if n := r.states()[ledger.Queued]; n != p.Queued {
		t.Errorf("plan said %d would queue, %d did", p.Queued, n)
	}
}

// A plan writes nothing a pass would act on: no job, no reservation, no
// decision and no agent.
func TestPlanLaunchesNothing(t *testing.T) {
	r := newRig(t, nil)
	if _, err := r.c.Plan(ctx, tenJobs(4096)); err != nil {
		t.Fatal(err)
	}
	for st, n := range r.states() {
		if n != 0 {
			t.Fatalf("plan left %d %s jobs", n, st)
		}
	}
	if r.cl.Live("mbp")+r.cl.Live("mini") != 0 {
		t.Fatal("plan started an agent")
	}
	evs, _ := r.l.Events(ctx, 0, 0)
	for _, e := range evs {
		if strings.HasPrefix(e.Kind, "cluster.job") || strings.Contains(e.Kind, "reserve") || strings.Contains(e.Kind, "dispatch") {
			t.Fatalf("plan wrote %s", e.Kind)
		}
	}
}

// Jobs already waiting go first, and the plan says so.
func TestPlanQueuesBehindWhatIsAlreadyWaiting(t *testing.T) {
	r := newRig(t, nil)
	for range 9 {
		r.submit(8192)
	}
	r.tick()
	p, err := r.c.Plan(ctx, []coord.SubmitRequest{{Kind: "claude", Repo: repo, MemoryMB: 8192, CPU: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if p.Jobs[0].Outcome != coord.PlanQueue {
		t.Fatalf("a full cluster should queue the job: %+v", p.Jobs[0])
	}
	if p.AlreadyQueue == 0 || !strings.Contains(p.Summary, "already queued") {
		t.Fatalf("summary should mention the existing queue: %s", p.Summary)
	}
}

func TestPlanRefusesWhatSubmitRefuses(t *testing.T) {
	r := newRig(t, nil)
	p, err := r.c.Plan(ctx, []coord.SubmitRequest{
		{Kind: "claude", Repo: repo, MemoryMB: 200_000},
		{Kind: "nope", Repo: repo},
		{Kind: "claude", Repo: repo, MemoryMB: 1024, CPU: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if p.Jobs[0].Outcome != coord.PlanRefuse || !strings.Contains(p.Jobs[0].Reason, "no worker could ever run") {
		t.Errorf("oversized: %+v", p.Jobs[0])
	}
	if p.Jobs[1].Outcome != coord.PlanRefuse || !strings.Contains(p.Jobs[1].Reason, "unknown agent kind") {
		t.Errorf("unknown kind: %+v", p.Jobs[1])
	}
	if p.Jobs[2].Outcome != coord.PlanPlaced {
		t.Errorf("small job: %+v", p.Jobs[2])
	}
	if _, err := r.c.Plan(ctx, nil); err == nil {
		t.Error("an empty plan was accepted")
	}
}

// A small job behind a big one that does not fit is still placed: no
// head-of-line blocking, and the queue view says why the big one waits.
func TestQueueViewExplainsAndDoesNotBlockOnItsHead(t *testing.T) {
	r := newRig(t, nil)
	r.submit(8192)
	r.submit(8192)
	r.tick()
	big := r.submit(60000, func(s *coord.SubmitRequest) { s.Priority = 50 })
	small := r.submit(2048)
	r.tick()
	if r.job(big.ID).State != ledger.Queued {
		t.Fatalf("big job should wait: %+v", r.job(big.ID))
	}
	if st := r.job(small.ID).State; st == ledger.Queued {
		t.Fatalf("the small job was blocked behind the big one")
	}
	q, err := r.c.Queue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if q.Count != 1 || q.Entries[0].Job != big.ID {
		t.Fatalf("queue view: %+v", q)
	}
	if e := q.Entries[0]; e.Placeable || !strings.Contains(e.Reason, "memory") || e.Position != 1 {
		t.Errorf("big job entry: %+v", e)
	}
}

func TestStatusCountsRoom(t *testing.T) {
	r := newRig(t, func(c *coord.Config) { c.DefaultMemoryMB = 4096; c.DefaultCPU = 1 })
	st, err := r.c.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Ready != 2 || st.RoomFor == 0 {
		t.Fatalf("%+v", st)
	}
	for _, w := range st.Workers {
		// Every agent counted must leave the kept-free memory intact.
		if w.FreeMB-int64(w.RoomFor)*4096 < w.KeepFreeMB {
			t.Errorf("%s: room %d would eat into the %d MB kept free", w.ID, w.RoomFor, w.KeepFreeMB)
		}
	}
	before := st.RoomFor
	r.submit(4096)
	r.tick()
	st, _ = r.c.Status(ctx)
	if st.RoomFor >= before {
		t.Errorf("room did not shrink after a job started: %d then %d", before, st.RoomFor)
	}
}
