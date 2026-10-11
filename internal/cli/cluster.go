package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/coord"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/placement"
	"github.com/jusso-dev/herdr-sched-cluster/internal/daemon"
)

// writeCluster prints a cluster verb's answer for a person.
func writeCluster(verb string, result json.RawMessage, out io.Writer) error {
	switch verb {
	case "cluster.workers":
		var rep daemon.WorkersReport
		if err := json.Unmarshal(result, &rep); err != nil {
			return err
		}
		if rep.Count == 0 {
			fmt.Fprintln(out, "no workers: add machines with `herdr machine add`, or set cluster_include_local = true")
			return nil
		}
		fmt.Fprintf(out, "%-14s %-13s %9s %9s %9s %-9s %6s %6s %s\n", "WORKER", "STATE", "TOTAL", "FREE", "RESERVED", "PRESSURE", "CPU%", "AGENTS", "NOTES")
		for _, w := range rep.Workers {
			s := w.Snapshot
			var notes []string
			if w.LastError != "" && w.State != placement.StateReady && w.State != placement.StateDraining {
				notes = append(notes, w.LastError)
			}
			if !w.BackoffUntil.IsZero() && w.BackoffUntil.After(time.Now()) {
				notes = append(notes, "retry after "+w.BackoffUntil.Local().Format("15:04:05"))
			}
			if s.OS != "" {
				notes = append(notes, s.OS+"/"+s.Arch)
			}
			if s.Backend == "static" {
				notes = append(notes, "declared capacity")
			}
			if w.HeldFor != "" {
				notes = append(notes, "held for "+w.HeldFor)
			}
			name := w.ID
			if w.Label != "" && w.Label != w.ID {
				name = w.Label
			}
			fmt.Fprintf(out, "%-14s %-13s %8dM %8dM %8dM %-9s %6.0f %6d %s\n", name, w.State, s.Memory.TotalMB,
				w.FreeMemoryMB, w.PendingMemoryMB, orDash(s.Memory.Pressure), s.CPU.UtilPct, w.ActiveAgents, strings.Join(notes, "; "))
		}
	case "cluster.status":
		var st coord.Status
		if err := json.Unmarshal(result, &st); err != nil {
			return err
		}
		fmt.Fprintln(out, st.Summary)
		fmt.Fprintf(out, "%-14s %-13s %-13s %9s %9s %9s %-9s %6s %s\n", "WORKER", "STATE", "PLATFORM", "TOTAL", "FREE", "KEEPFREE", "PRESSURE", "AGENTS", "ROOM")
		for _, w := range st.Workers {
			plat := "-"
			if w.OS != "" {
				plat = w.OS + "/" + w.Arch
			}
			agents := fmt.Sprintf("%d", w.ActiveAgents)
			if w.MaxAgents > 0 {
				agents += fmt.Sprintf("/%d", w.MaxAgents)
			}
			fmt.Fprintf(out, "%-14s %-13s %-13s %8dM %8dM %8dM %-9s %6s %d\n", nameOr(w.Label, w.ID), w.State, plat,
				w.TotalMB, w.FreeMB, w.KeepFreeMB, orDash(w.Pressure), agents, w.RoomFor)
		}
		fmt.Fprintf(out, "ROOM counts agents of %d MB and %.1f cpu; plan a real batch with: hsched cluster plan --kind <kind> --repo <dir> --count N\n",
			st.DefaultMemoryMB, st.DefaultCPU)
	case "cluster.plan":
		var p coord.Plan
		if err := json.Unmarshal(result, &p); err != nil {
			return err
		}
		fmt.Fprintln(out, p.Summary)
		for _, j := range p.Jobs {
			where := "-"
			if j.Worker != "" {
				where = nameOr(j.WorkerLabel, j.Worker)
			}
			fmt.Fprintf(out, "%-8s %-13s %-12s %-8s %-28s %6dM  %s\n", j.Ref, j.Outcome, where, j.Kind, j.Repo, j.MemoryMB, j.Reason)
			for _, w := range j.Warnings {
				fmt.Fprintf(out, "         warning: %s\n", w)
			}
		}
		fmt.Fprintln(out, "workers after this plan:")
		for _, w := range p.Workers {
			fmt.Fprintf(out, "  %-14s %-11s +%d agents, free %dM -> %dM (keeps %dM free)\n", nameOr(w.Label, w.ID), w.State,
				w.PlannedAgents, w.FreeBeforeMB, w.FreeAfterMB, w.KeepFreeMB)
		}
		for _, a := range p.Advice {
			fmt.Fprintf(out, "note: %s\n", a)
		}
		fmt.Fprintln(out, "nothing was submitted; this was a dry run")
	case "cluster.queue":
		var q coord.QueueView
		if err := json.Unmarshal(result, &q); err != nil {
			return err
		}
		if q.Count == 0 {
			fmt.Fprintln(out, "the queue is empty")
			return nil
		}
		for _, e := range q.Entries {
			held := ""
			if e.HeldWorker != "" {
				held = " (holding " + e.HeldWorker + " clear)"
			}
			fmt.Fprintf(out, "%3d. %-6s %-8s %-28s %6dM prio %d->%d waited %s%s\n     %s\n", e.Position, e.Job, e.Kind, e.Repo,
				e.MemoryMB, e.Priority, e.EffectivePriority, (time.Duration(e.WaitingSeconds) * time.Second).String(), held, e.Reason)
		}
	case "cluster.submit":
		var rep daemon.ClusterSubmitted
		if err := json.Unmarshal(result, &rep); err != nil {
			return err
		}
		fmt.Fprintf(out, "%s queued: %s in %s, %d MB (%s), %.1f cpu\n", rep.Job.ID, rep.Job.Kind, rep.Job.Repo,
			rep.Job.MemoryMB, rep.Job.EstimateSource, rep.Job.CPU)
		for _, n := range rep.Notes {
			fmt.Fprintf(out, "  %s\n", n)
		}
		fmt.Fprintf(out, "  follow it with: hsched cluster jobs   or   hsched cluster explain %s\n", rep.Job.ID)
	case "cluster.jobs":
		var rep daemon.ClusterJobsReport
		if err := json.Unmarshal(result, &rep); err != nil {
			return err
		}
		if rep.Count == 0 {
			fmt.Fprintln(out, "no cluster jobs (hsched cluster submit <kind> <repo> --prompt ...)")
			return nil
		}
		for _, j := range rep.Jobs {
			writeClusterJob(out, j)
		}
	case "cluster.cancel", "cluster.detach":
		var j ledger.Job
		if err := json.Unmarshal(result, &j); err != nil {
			return err
		}
		writeClusterJob(out, j)
	case "cluster.explain":
		var ex coord.Explanation
		if err := json.Unmarshal(result, &ex); err != nil {
			return err
		}
		writeClusterJob(out, ex.Job)
		for _, d := range ex.Decisions {
			fmt.Fprintf(out, "  %s  %s\n", d.At.Local().Format("2006-01-02 15:04:05"), d.Summary)
		}
		if ex.Now != nil {
			fmt.Fprintf(out, "  now: %s\n", ex.Now.Summary)
			writeCandidates(out, ex.Now.Candidates)
		} else if n := len(ex.Decisions); n > 0 {
			var d placement.Decision
			if json.Unmarshal(ex.Decisions[n-1].Detail, &d) == nil && len(d.Candidates) > 0 {
				fmt.Fprintln(out, "  last decision in detail:")
				writeCandidates(out, d.Candidates)
			}
		}
	case "cluster.drain", "cluster.undrain":
		var c daemon.ClusterWorkerChange
		if err := json.Unmarshal(result, &c); err != nil {
			return err
		}
		if c.Draining {
			fmt.Fprintf(out, "%s is draining: no new agents go there; running ones carry on\n", c.Worker)
		} else {
			fmt.Fprintf(out, "%s takes new agents again\n", c.Worker)
		}
	case "cluster.events":
		var rep daemon.ClusterEventsReport
		if err := json.Unmarshal(result, &rep); err != nil {
			return err
		}
		if rep.Count == 0 {
			fmt.Fprintln(out, "no cluster events for that read")
			return nil
		}
		for _, e := range rep.Events {
			fmt.Fprintf(out, "%6d  %s  %-32s %-6s %-8s %s\n", e.ID, e.At.Local().Format("2006-01-02 15:04:05"), e.Kind,
				orDash(e.JobID), orDash(e.Worker), compact(e.Detail))
		}
	case "cluster.metrics":
		var rep daemon.MetricsReport
		if err := json.Unmarshal(result, &rep); err != nil {
			return err
		}
		fmt.Fprint(out, rep.Text)
	default:
		fmt.Fprintln(out, string(result))
	}
	return nil
}

func writeClusterJob(out io.Writer, j ledger.Job) {
	where := orDash(j.Worker)
	if j.PaneID != "" {
		where += " " + j.PaneID
	}
	if j.Worktree {
		where += " (worktree)"
	}
	if j.Disconnected {
		where += " (disconnected)"
	}
	mem := fmt.Sprintf("%dM", j.MemoryMB)
	if j.ReservedMB > 0 && j.ReservedMB != j.MemoryMB {
		mem += fmt.Sprintf(" reserved %dM", j.ReservedMB)
	}
	if j.ObservedRSSMB > 0 {
		mem += fmt.Sprintf(" using %dM", j.ObservedRSSMB)
	}
	fmt.Fprintf(out, "%-6s %-11s %-8s %-28s %-24s %s\n", j.ID, j.State, j.Kind, j.Repo, where, mem)
	if j.LastError != "" {
		fmt.Fprintf(out, "       %s\n", j.LastError)
	}
}

func writeCandidates(out io.Writer, cs []placement.Candidate) {
	for _, c := range cs {
		if c.Eligible {
			keys := make([]string, 0, len(c.Factors))
			for k := range c.Factors {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var fs []string
			for _, k := range keys {
				fs = append(fs, fmt.Sprintf("%s %.2f", k, c.Factors[k]))
			}
			fmt.Fprintf(out, "    %-12s eligible, score %.1f (%s)\n", c.WorkerID, c.Score, strings.Join(fs, ", "))
			for _, w := range c.Warnings {
				fmt.Fprintf(out, "    %-12s warning: %s\n", "", w)
			}
			continue
		}
		fmt.Fprintf(out, "    %-12s ruled out: %s\n", c.WorkerID, strings.Join(c.Reasons, "; "))
	}
}

func nameOr(label, id string) string {
	if label != "" && label != id {
		return label
	}
	return id
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func compact(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	return string(raw)
}
