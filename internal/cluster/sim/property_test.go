package sim

import (
	"math/rand"
	"testing"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/coord"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/ledger"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

// Property: across random submissions, ticks, injected Herdr failures,
// ambiguous agent starts (the start happened but the answer was lost),
// unreachable workers, pressure changes, agents exiting, clock jumps and
// coordinator restarts, no agent is ever started twice, and every live
// agent belongs to exactly one active job on the machine it runs on.
func TestPropertyNoDoubleDispatch(t *testing.T) {
	seeds := 30
	if testing.Short() {
		seeds = 8
	}
	for seed := 0; seed < seeds; seed++ {
		r := newRig(t, func(c *coord.Config) { c.DispatchGrace = 2 * time.Minute; c.MaxAttempts = 4 })
		rnd := rand.New(rand.NewSource(int64(seed)))
		machines := []string{"mbp", "mini"}
		for step := 0; step < 120; step++ {
			m := machines[rnd.Intn(2)]
			switch op := rnd.Intn(14); {
			case op < 3:
				r.submit(int64(1024 * (1 + rnd.Intn(8))))
			case op < 6:
				r.tick()
			case op == 6:
				r.cl.With(func() {
					for _, f := range []string{"workspace", "worktree", "start", "prompt", "list", "close"} {
						if rnd.Intn(3) == 0 {
							r.cl.Machines[m].Fail[f]++
						}
					}
				})
			case op == 7:
				r.cl.With(func() {
					if rnd.Intn(2) == 0 {
						r.cl.Machines[m].AmbiguousStarts++
					} else {
						r.cl.Machines[m].LostReplies++
					}
				})
			case op == 8:
				r.cl.With(func() { r.cl.Machines[m].Unreachable = !r.cl.Machines[m].Unreachable })
			case op == 9:
				r.cl.With(func() {
					if r.cl.Machines[m].Pressure == resource.PressureCritical {
						r.cl.Machines[m].Pressure = resource.PressureNormal
					} else {
						r.cl.Machines[m].Pressure = resource.PressureCritical
					}
				})
			case op == 10:
				var name string
				r.cl.With(func() {
					for n := range r.cl.Machines[m].Agents {
						name = n
						break
					}
				})
				if name != "" {
					r.cl.Exit(m, name)
				}
			case op == 11:
				r.cl.Clock.Advance(time.Duration(rnd.Intn(5)) * time.Minute)
			case op == 12:
				r.open()
			case op == 13:
				var name string
				r.cl.With(func() {
					for n := range r.cl.Machines[m].Agents {
						name = n
						break
					}
				})
				if name != "" {
					r.cl.SetRSS(m, name, int64(256+rnd.Intn(12000)))
				}
			}
			r.check()
		}
		// Heal everything and let the cluster settle; the invariant must
		// still hold and nothing may be stuck mid-dispatch.
		r.cl.With(func() {
			for _, mm := range r.cl.Machines {
				mm.Unreachable, mm.Pressure, mm.Fail, mm.AmbiguousStarts, mm.ListFails, mm.LostReplies = false, resource.PressureNormal, map[string]int{}, 0, 0, 0
			}
		})
		for i := 0; i < 6; i++ {
			r.cl.Clock.Advance(r.cfg.BackoffMax + r.cfg.DispatchGrace)
			r.tick()
		}
		n := r.states()
		if n[ledger.Reserved]+n[ledger.Dispatching] != 0 {
			t.Fatalf("seed %d: stuck %v", seed, n)
		}
		r.cl.With(func() {
			live := 0
			for _, mm := range r.cl.Machines {
				live += len(mm.Agents)
			}
			if live != n[ledger.Running] {
				t.Fatalf("seed %d: %d live agents, %d running jobs", seed, live, n[ledger.Running])
			}
		})
	}
}
