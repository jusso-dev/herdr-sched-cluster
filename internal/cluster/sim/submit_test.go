package sim

import (
	"strings"
	"testing"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/coord"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

func TestSubmitRefusesWhatCouldNeverRun(t *testing.T) {
	r := newRig(t, nil)
	cases := map[string]coord.SubmitRequest{
		"unknown agent kind":       {Kind: "nope", Repo: repo},
		"repository":               {Kind: "claude", Repo: "relative/path"},
		"characters outside":       {Kind: "claude", Repo: repo, AgentArgs: []string{"--x", "$(id)"}},
		"no worker could ever run": {Kind: "claude", Repo: repo, MemoryMB: 200_000},
		"policy":                   {Kind: "claude", Repo: repo, Policy: "fastest"},
		"isolation":                {Kind: "claude", Repo: repo, Isolation: "docker"},
		"priority must be":         {Kind: "claude", Repo: repo, Priority: 1000},
		"label":                    {Kind: "claude", Repo: repo, Labels: []string{"Bad Label"}},
		"pinned to ghost":          {Kind: "claude", Repo: repo, Machine: "ghost"},
	}
	for want, req := range cases {
		_, _, err := r.c.Submit(ctx, req)
		if err == nil {
			t.Errorf("%s: accepted", want)
			continue
		}
		if want != "pinned to ghost" && !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v", want, err)
		}
	}
	if n := r.states(); len(n) != 0 && n["queued"] != 0 {
		t.Fatalf("%v", n)
	}
}

func TestStaticWorkerIsScheduledAgainstDeclaredCapacity(t *testing.T) {
	r := newRig(t, func(c *coord.Config) {
		c.Workers["win"] = coord.WorkerConfig{Probe: coord.ProbeStatic, Static: resource.StaticSpec{OS: "windows", Arch: "amd64", MemoryMB: 32768, Cores: 8, Agents: []string{"codex"}}}
	})
	r.cl.Add("win", 32768, 8)
	r.cl.With(func() { r.cl.Machines["win"].ProbeFails = true }) // never probed live
	j := r.submit(8192, func(s *coord.SubmitRequest) { s.Kind = "codex"; s.Machine = "win" })
	r.tick()
	if got := r.job(j.ID); got.State != "running" || got.Worker != "win" {
		t.Fatalf("%+v", got)
	}
	// Declared agents are a hard constraint.
	if _, _, err := r.c.Submit(ctx, coord.SubmitRequest{Kind: "claude", Repo: repo, Machine: "win", MemoryMB: 1024}); err == nil {
		t.Fatal("placed claude on a worker declared to have only codex")
	}
}

func TestConfigValidation(t *testing.T) {
	c := coord.DefaultConfig()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Policy = "x"
	if c.Validate() == nil {
		t.Fatal("bad policy accepted")
	}
	c = coord.DefaultConfig()
	c.Workers["w"] = coord.WorkerConfig{Probe: coord.ProbeStatic}
	if c.Validate() == nil {
		t.Fatal("static worker without capacity accepted")
	}
}
