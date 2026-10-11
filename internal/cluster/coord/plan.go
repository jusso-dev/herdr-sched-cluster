package coord

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/placement"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/queue"
)

// MaxPlanJobs bounds one plan, so a typo in a count is not a minute of
// placement arithmetic.
const MaxPlanJobs = 200

// PlanOutcome is what a plan says about one proposed job.
const (
	// PlanPlaced: a worker has room now, after the jobs already queued and
	// the proposed jobs ahead of it.
	PlanPlaced = "would_place"
	// PlanQueue: it would be accepted and wait for capacity.
	PlanQueue = "would_queue"
	// PlanRefuse: submit would refuse it, because no worker could ever run
	// it, or the request itself is invalid.
	PlanRefuse = "would_refuse"
)

// PlannedJob is one proposed job and what would happen to it.
type PlannedJob struct {
	Index          int                 `json:"index"`
	Ref            string              `json:"ref"`
	Kind           string              `json:"kind"`
	Repo           string              `json:"repo"`
	MemoryMB       int64               `json:"memory_mb"`
	CPU            float64             `json:"cpu"`
	Priority       int                 `json:"priority"`
	EstimateSource string              `json:"estimate_source,omitempty"`
	Outcome        string              `json:"outcome"`
	Worker         string              `json:"worker,omitempty"`
	WorkerLabel    string              `json:"worker_label,omitempty"`
	Reason         string              `json:"reason"`
	Decision       *placement.Decision `json:"decision,omitempty"`
}

// PlanWorker is one worker's share of a plan.
type PlanWorker struct {
	ID            string `json:"id"`
	Label         string `json:"label"`
	State         string `json:"state"`
	FreeBeforeMB  int64  `json:"free_before_mb"`
	FreeAfterMB   int64  `json:"free_after_mb"`
	KeepFreeMB    int64  `json:"keep_free_mb"`
	PlannedAgents int    `json:"planned_agents"`
	ActiveAgents  int    `json:"active_agents"`
}

// Plan is a dry run of submitting a batch: nothing is queued, reserved or
// started, and no decision is recorded.
type Plan struct {
	Jobs         []PlannedJob `json:"jobs"`
	Workers      []PlanWorker `json:"workers"`
	AlreadyQueue int          `json:"already_queued"`
	Placed       int          `json:"would_place"`
	Queued       int          `json:"would_queue"`
	Refused      int          `json:"would_refuse"`
	Summary      string       `json:"summary"`
	Advice       []string     `json:"advice,omitempty"`
}

// Plan works out where a batch of jobs would go if it were submitted now,
// behind whatever is already queued. It reads the same probes and ledger a
// real pass reads and runs the same queue pass, then throws the result away.
// It takes no lock and writes nothing but probe readings.
func (c *Coordinator) Plan(ctx context.Context, reqs []SubmitRequest) (Plan, error) {
	var out Plan
	if len(reqs) == 0 {
		return out, fmt.Errorf("a plan needs at least one job")
	}
	if len(reqs) > MaxPlanJobs {
		return out, fmt.Errorf("a plan takes at most %d jobs, got %d", MaxPlanJobs, len(reqs))
	}
	var extra []string
	for _, r := range reqs {
		extra = append(extra, r.Repo)
	}
	workers, err := c.workersWith(ctx, false, nil, extra)
	if err != nil {
		return out, err
	}
	queued, err := c.L.List(ctx, ledger.Queued)
	if err != nil {
		return out, err
	}
	rows, err := c.L.Workers(ctx)
	if err != nil {
		return out, err
	}
	held := map[string]string{}
	for id, r := range rows {
		if r.HeldFor != "" {
			held[id] = r.HeldFor
		}
	}
	now := c.now()
	var entries []queue.Entry
	var maxSeq int64
	for _, j := range queued {
		entries = append(entries, queue.Entry{Job: c.PlacementJob(j), Priority: j.Priority, SubmittedAt: j.SubmittedAt, Seq: j.Seq})
		if j.Seq > maxSeq {
			maxSeq = j.Seq
		}
	}
	out.AlreadyQueue = len(queued)
	planned := map[string]int{}
	for i, r := range reqs {
		pj := PlannedJob{Index: i, Ref: fmt.Sprintf("plan-%d", i+1), Kind: r.Kind, Repo: r.Repo, Priority: r.Priority}
		pr, source, _, err := c.prepare(ctx, r)
		if err != nil {
			pj.Outcome, pj.Reason = PlanRefuse, err.Error()
			out.Jobs = append(out.Jobs, pj)
			continue
		}
		pj.MemoryMB, pj.CPU, pj.EstimateSource = pr.MemoryMB, pr.CPU, source
		job := placement.Job{ID: pj.Ref, Kind: pr.Kind, Repo: pr.Repo, MemoryMB: pr.MemoryMB, CPU: pr.CPU, Policy: pr.Policy,
			Machine: pr.Machine, Labels: pr.Labels, OS: pr.OS, Arch: pr.Arch}
		if job.Policy == "" {
			job.Policy = c.Cfg.Policy
		}
		if ok, why := placement.FitsAnywhereEver(job, workers); !ok {
			pj.Outcome, pj.Reason = PlanRefuse, "no worker could ever run this job: "+strings.Join(why, " | ")
			out.Jobs = append(out.Jobs, pj)
			continue
		}
		planned[pj.Ref] = len(out.Jobs)
		out.Jobs = append(out.Jobs, pj)
		// The proposed jobs queue behind everything already waiting, in the
		// order given, exactly as a burst of submits would.
		entries = append(entries, queue.Entry{Job: job, Priority: pr.Priority, SubmittedAt: now, Seq: maxSeq + int64(i) + 1})
	}
	p := queue.Pass(entries, workers, now, c.Cfg.Queue, held)
	byWorker := map[string]placement.Worker{}
	for _, w := range workers {
		byWorker[w.ID] = w
	}
	perWorker := map[string]int{}
	usedMB := map[string]int64{}
	for _, a := range p.Assignments {
		ji, ok := planned[a.JobID]
		if !ok {
			// A job already queued that this pass would place first: it
			// takes its capacity before any proposed job does.
			usedMB[a.WorkerID] += memOf(queued, a.JobID)
			continue
		}
		d := a.Decision
		pj := &out.Jobs[ji]
		pj.Outcome, pj.Worker, pj.Decision = PlanPlaced, a.WorkerID, &d
		pj.WorkerLabel = byWorker[a.WorkerID].Label
		pj.Reason = d.Summary
		perWorker[a.WorkerID]++
		usedMB[a.WorkerID] += pj.MemoryMB
	}
	for _, d := range p.Waiting {
		ji, ok := planned[d.JobID]
		if !ok {
			continue
		}
		d := d
		pj := &out.Jobs[ji]
		pj.Outcome, pj.Decision = PlanQueue, &d
		pj.Reason = waitReason(d)
	}
	for _, w := range workers {
		s := w.Effective()
		out.Workers = append(out.Workers, PlanWorker{ID: w.ID, Label: w.Label, State: w.State,
			FreeBeforeMB: w.FreeMemoryMB(), FreeAfterMB: w.FreeMemoryMB() - usedMB[w.ID], KeepFreeMB: s.MinFreeMemoryMB,
			PlannedAgents: perWorker[w.ID], ActiveAgents: w.ActiveAgents})
	}
	sort.SliceStable(out.Workers, func(a, b int) bool { return out.Workers[a].ID < out.Workers[b].ID })
	for _, j := range out.Jobs {
		switch j.Outcome {
		case PlanPlaced:
			out.Placed++
		case PlanQueue:
			out.Queued++
		default:
			out.Refused++
		}
	}
	var spread []string
	for _, w := range out.Workers {
		if w.PlannedAgents > 0 {
			spread = append(spread, fmt.Sprintf("%d on %s", w.PlannedAgents, nameOrID(w.Label, w.ID)))
		}
	}
	out.Summary = fmt.Sprintf("%d of %d would start now", out.Placed, len(reqs))
	if len(spread) > 0 {
		out.Summary += " (" + strings.Join(spread, ", ") + ")"
	}
	if out.Queued > 0 {
		out.Summary += fmt.Sprintf("; %d would wait in the queue", out.Queued)
	}
	if out.Refused > 0 {
		out.Summary += fmt.Sprintf("; %d would be refused", out.Refused)
	}
	if out.AlreadyQueue > 0 {
		out.Summary += fmt.Sprintf("; %d job(s) already queued go first", out.AlreadyQueue)
	}
	out.Advice = advise(out)
	return out, nil
}

func memOf(jobs []ledger.Job, id string) int64 {
	for _, j := range jobs {
		if j.ID == id {
			return j.MemoryMB
		}
	}
	return 0
}

func nameOrID(label, id string) string {
	if label != "" {
		return label
	}
	return id
}

// waitReason is the shortest honest answer to "why would this wait": the
// constraint that blocked the worker closest to taking it.
func waitReason(d placement.Decision) string {
	if len(d.Candidates) == 0 {
		return "no workers are registered"
	}
	var parts []string
	for _, c := range d.Candidates {
		if len(c.Reasons) > 0 {
			parts = append(parts, nameOrID(c.Label, c.WorkerID)+": "+strings.Join(c.Reasons, "; "))
		}
	}
	return "waits for capacity (" + strings.Join(parts, " | ") + ")"
}

func advise(p Plan) []string {
	var out []string
	if p.Queued > 0 {
		out = append(out, "Jobs that would queue are safe to submit: they start on their own as capacity frees. "+
			"Submit the most important ones first or give them a higher priority.")
	}
	if p.Refused > 0 {
		out = append(out, "Fix or drop the refused jobs before submitting; submit refuses them the same way.")
	}
	for _, w := range p.Workers {
		if w.State != placement.StateReady && w.State != placement.StateDraining {
			out = append(out, fmt.Sprintf("Worker %s is %s, so it takes nothing; `cluster_workers` with refresh shows why.",
				nameOrID(w.Label, w.ID), strings.ReplaceAll(w.State, "_", " ")))
		}
	}
	return out
}

// QueueEntry is one waiting job in the order the next pass tries it.
type QueueEntry struct {
	Position          int    `json:"position"`
	Job               string `json:"job"`
	Kind              string `json:"kind"`
	Repo              string `json:"repo"`
	MemoryMB          int64  `json:"memory_mb"`
	Priority          int    `json:"priority"`
	EffectivePriority int    `json:"effective_priority"`
	WaitingSeconds    int64  `json:"waiting_seconds"`
	HeldWorker        string `json:"held_worker,omitempty"`
	Placeable         bool   `json:"placeable_now"`
	Reason            string `json:"reason"`
}

// QueueView is the queue as the next pass would see it.
type QueueView struct {
	Entries []QueueEntry `json:"entries"`
	Count   int          `json:"count"`
}

// Queue lists the waiting jobs in the order the next pass tries them, with
// why each one is waiting. It dispatches nothing and records nothing.
func (c *Coordinator) Queue(ctx context.Context) (QueueView, error) {
	var v QueueView
	queued, err := c.L.List(ctx, ledger.Queued)
	if err != nil || len(queued) == 0 {
		return v, err
	}
	workers, err := c.Workers(ctx, false, nil)
	if err != nil {
		return v, err
	}
	rows, err := c.L.Workers(ctx)
	if err != nil {
		return v, err
	}
	held := map[string]string{}
	for id, r := range rows {
		if r.HeldFor != "" {
			held[id] = r.HeldFor
		}
	}
	byID := map[string]ledger.Job{}
	var entries []queue.Entry
	for _, j := range queued {
		byID[j.ID] = j
		entries = append(entries, queue.Entry{Job: c.PlacementJob(j), Priority: j.Priority, SubmittedAt: j.SubmittedAt, Seq: j.Seq})
	}
	now := c.now()
	p := queue.Pass(entries, workers, now, c.Cfg.Queue, held)
	reason := map[string]string{}
	placeable := map[string]bool{}
	for _, a := range p.Assignments {
		placeable[a.JobID] = true
		reason[a.JobID] = "fits now; the next pass starts it (" + a.Decision.Summary + ")"
	}
	for _, d := range p.Waiting {
		reason[d.JobID] = waitReason(d)
	}
	heldFor := map[string]string{}
	for w, j := range p.Holds {
		heldFor[j] = w
	}
	for i, e := range queue.Order(entries, now, c.Cfg.Queue.Ageing) {
		j := byID[e.Job.ID]
		v.Entries = append(v.Entries, QueueEntry{Position: i + 1, Job: j.ID, Kind: j.Kind, Repo: j.Repo, MemoryMB: j.MemoryMB,
			Priority: j.Priority, EffectivePriority: queue.Effective(e, now, c.Cfg.Queue.Ageing),
			WaitingSeconds: int64(math.Max(0, now.Sub(j.SubmittedAt).Seconds())), HeldWorker: heldFor[j.ID],
			Placeable: placeable[j.ID], Reason: reason[j.ID]})
	}
	v.Count = len(v.Entries)
	return v, nil
}

// StatusWorker is one worker in the status summary.
type StatusWorker struct {
	ID           string `json:"id"`
	Label        string `json:"label"`
	State        string `json:"state"`
	OS           string `json:"os,omitempty"`
	Arch         string `json:"arch,omitempty"`
	TotalMB      int64  `json:"total_mb"`
	FreeMB       int64  `json:"free_mb"`
	KeepFreeMB   int64  `json:"keep_free_mb"`
	Pressure     string `json:"pressure,omitempty"`
	ActiveAgents int    `json:"active_agents"`
	MaxAgents    int    `json:"max_agents,omitempty"`
	// RoomFor is how many more agents of the default size fit here right
	// now under every hard limit. It is a guide; plan is the real answer.
	RoomFor int `json:"room_for_default_agents"`
}

// Status is the one-call picture an agent reads before fanning out.
type Status struct {
	Workers         []StatusWorker `json:"workers"`
	Ready           int            `json:"ready_workers"`
	Jobs            map[string]int `json:"jobs"`
	Queued          int            `json:"queued"`
	Running         int            `json:"running"`
	RoomFor         int            `json:"room_for_default_agents"`
	DefaultMemoryMB int64          `json:"default_memory_mb"`
	DefaultCPU      float64        `json:"default_cpu"`
	Policy          string         `json:"policy"`
	Summary         string         `json:"summary"`
	At              time.Time      `json:"at"`
}

// Status summarises capacity and work in one answer.
func (c *Coordinator) Status(ctx context.Context) (Status, error) {
	st := Status{DefaultMemoryMB: c.Cfg.DefaultMemoryMB, DefaultCPU: c.Cfg.DefaultCPU, Policy: c.Cfg.Policy, At: c.now()}
	workers, err := c.Workers(ctx, false, nil)
	if err != nil {
		return st, err
	}
	counts, err := c.L.Counts(ctx)
	if err != nil {
		return st, err
	}
	st.Jobs = counts
	st.Queued = counts[ledger.Queued]
	st.Running = counts[ledger.Running] + counts[ledger.Dispatching] + counts[ledger.Reserved]
	for _, w := range workers {
		s := w.Effective()
		sw := StatusWorker{ID: w.ID, Label: w.Label, State: w.State, OS: w.Snapshot.OS, Arch: w.Snapshot.Arch,
			TotalMB: w.Snapshot.Memory.TotalMB, FreeMB: w.FreeMemoryMB(), KeepFreeMB: s.MinFreeMemoryMB,
			Pressure: w.Snapshot.Memory.Pressure, ActiveAgents: w.ActiveAgents, MaxAgents: s.MaxAgents}
		sw.RoomFor = roomFor(w, placement.Job{ID: "status-probe", MemoryMB: c.Cfg.DefaultMemoryMB, CPU: c.Cfg.DefaultCPU})
		if w.State == placement.StateReady {
			st.Ready++
		}
		st.RoomFor += sw.RoomFor
		st.Workers = append(st.Workers, sw)
	}
	st.Summary = fmt.Sprintf("%d of %d workers ready; room for about %d more agents of %d MB; %d running, %d queued",
		st.Ready, len(workers), st.RoomFor, c.Cfg.DefaultMemoryMB, st.Running, st.Queued)
	return st, nil
}

// roomFor counts how many copies of job fit on w one after another, each
// checked against every hard constraint. Bounded so a huge machine and a
// tiny estimate cannot spin.
func roomFor(w placement.Worker, job placement.Job) int {
	n := 0
	for n < 256 {
		if len(placement.Eligible(job, w)) > 0 {
			break
		}
		n++
		w.PendingMemoryMB += job.MemoryMB
		w.CommittedCPU += job.CPU
		w.ActiveAgents++
	}
	return n
}
