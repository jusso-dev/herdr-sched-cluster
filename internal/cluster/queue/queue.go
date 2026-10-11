// Package queue decides the order waiting jobs are tried in, and plans one
// pass over them: which job goes where, which wait, and which worker is held
// clear for a job that has waited too long.
//
// It is pure. The plan it returns is a proposal; the ledger re-checks every
// assignment inside a transaction before any capacity is actually taken.
package queue

import (
	"sort"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/placement"
)

// Entry is a waiting job.
type Entry struct {
	Job         placement.Job
	Priority    int
	SubmittedAt time.Time
	Seq         int64
}

// Config tunes ordering.
type Config struct {
	// Ageing adds one priority point per this much waiting. Zero turns
	// ageing off.
	Ageing time.Duration
	// Starvation is how long the front job may wait before a worker is held
	// clear for it. Zero turns the guard off.
	Starvation time.Duration
}

// Effective is a job's priority after ageing.
func Effective(e Entry, now time.Time, ageing time.Duration) int {
	if ageing <= 0 {
		return e.Priority
	}
	waited := now.Sub(e.SubmittedAt)
	if waited <= 0 {
		return e.Priority
	}
	return e.Priority + int(waited/ageing)
}

// Order sorts entries: higher effective priority first, then earlier
// submission, then lower sequence number. The input is not changed.
func Order(entries []Entry, now time.Time, ageing time.Duration) []Entry {
	out := append([]Entry(nil), entries...)
	sort.SliceStable(out, func(i, j int) bool {
		pi, pj := Effective(out[i], now, ageing), Effective(out[j], now, ageing)
		if pi != pj {
			return pi > pj
		}
		if !out[i].SubmittedAt.Equal(out[j].SubmittedAt) {
			return out[i].SubmittedAt.Before(out[j].SubmittedAt)
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

// Assignment is one job planned onto one worker.
type Assignment struct {
	JobID    string
	WorkerID string
	Decision placement.Decision
}

// Plan is the outcome of one pass.
type Plan struct {
	Assignments []Assignment
	// Waiting holds the decision for every job that stays queued, so the
	// operator can ask why.
	Waiting []placement.Decision
	// Holds maps a worker to the starving job it is being kept clear for.
	Holds map[string]string
}

// Pass plans one pass over the queue. Workers are copied; each assignment
// is applied to the copy so later jobs in the same pass see the capacity it
// took. held carries holds over from the previous pass.
func Pass(entries []Entry, workers []placement.Worker, now time.Time, cfg Config, held map[string]string) Plan {
	ws := make([]placement.Worker, len(workers))
	copy(ws, workers)
	// The cluster-wide repository counts are one map shared by every
	// worker, so an assignment anywhere is seen by every later job.
	cluster := map[string]int{}
	for _, w := range workers {
		for k, v := range w.ClusterRepos {
			if v > cluster[k] {
				cluster[k] = v
			}
		}
	}
	for i := range ws {
		ws[i].ClusterRepos = cluster
	}
	for i := range ws {
		if ws[i].RunningRepos != nil {
			m := make(map[string]int, len(ws[i].RunningRepos))
			for k, v := range ws[i].RunningRepos {
				m[k] = v
			}
			ws[i].RunningRepos = m
		}
	}
	plan := Plan{Holds: map[string]string{}}
	waitingIDs := map[string]bool{}
	for _, e := range entries {
		waitingIDs[e.Job.ID] = true
	}
	// A hold survives only while its job is still waiting.
	for w, j := range held {
		if waitingIDs[j] {
			plan.Holds[w] = j
		}
	}
	apply := func() {
		for i := range ws {
			ws[i].HeldFor = plan.Holds[ws[i].ID]
		}
	}
	apply()
	for _, e := range Order(entries, now, cfg.Ageing) {
		d := placement.Place(e.Job, ws)
		if d.Chosen == "" {
			plan.Waiting = append(plan.Waiting, d)
			starving := cfg.Starvation > 0 && now.Sub(e.SubmittedAt) >= cfg.Starvation
			if starving && !holdsAny(plan.Holds, e.Job.ID) {
				if w := placement.Nearest(e.Job, ws); w != "" {
					if _, taken := plan.Holds[w]; !taken {
						plan.Holds[w] = e.Job.ID
						apply()
					}
				}
			}
			continue
		}
		plan.Assignments = append(plan.Assignments, Assignment{JobID: e.Job.ID, WorkerID: d.Chosen, Decision: d})
		for i := range ws {
			if ws[i].ID != d.Chosen {
				continue
			}
			ws[i].PendingMemoryMB += e.Job.MemoryMB
			ws[i].CommittedCPU += e.Job.CPU
			ws[i].PendingCPU += e.Job.CPU
			ws[i].ActiveAgents++
			if e.Job.Repo != "" {
				if ws[i].RunningRepos == nil {
					ws[i].RunningRepos = map[string]int{}
				}
				ws[i].RunningRepos[e.Job.Repo]++
			}
		}
		if e.Job.Repo != "" {
			cluster[e.Job.Repo]++
		}
		for w, j := range plan.Holds {
			if j == e.Job.ID {
				delete(plan.Holds, w)
			}
		}
		apply()
	}
	return plan
}

func holdsAny(h map[string]string, job string) bool {
	for _, j := range h {
		if j == job {
			return true
		}
	}
	return false
}
