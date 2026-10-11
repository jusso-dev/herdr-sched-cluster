package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/transport"
)

// newProbeCmd is `hsched cluster probe`: read this machine and print one JSON
// document. It never talks to a daemon, so it is what a coordinator runs on a
// remote worker over SSH, and it works on a machine where no daemon has ever
// started.
func newProbeCmd() *cobra.Command {
	var (
		paths    []string
		pids     []int
		sampleMS int
		asJSON   bool
	)
	cmd := &cobra.Command{
		Use:   "probe",
		Short: "Measure this machine's memory, CPU, disk, power and agents (no daemon)",
		Long: "Reads this machine and prints one JSON document: memory and memory " +
			"pressure, swap, CPU use and load, disk, power, thermal state, which " +
			"agent CLIs are installed, whether the given paths exist, and what the " +
			"given processes and their children are using. It talks to no daemon, " +
			"which is why a coordinator can run it on a worker over SSH.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			for _, p := range paths {
				if err := transport.ValidatePath(p); err != nil {
					return err
				}
			}
			for _, pid := range pids {
				if pid <= 0 {
					return fmt.Errorf("--pid must be a positive process id, got %d", pid)
				}
			}
			if sampleMS < 0 || sampleMS > 5000 {
				return fmt.Errorf("--sample-ms must be between 0 and 5000")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			snap, err := resource.Collect(ctx, resource.Request{Paths: paths, PIDs: pids, Sample: time.Duration(sampleMS) * time.Millisecond})
			if err != nil {
				return err
			}
			enc := json.NewEncoder(os.Stdout)
			if !asJSON {
				enc.SetIndent("", "  ")
			}
			return enc.Encode(snap)
		},
	}
	f := cmd.Flags()
	f.StringArrayVar(&paths, "path", nil, "report whether this path exists here (repeatable; absolute or ~/)")
	f.IntSliceVar(&pids, "pid", nil, "sample this process and its children (repeatable)")
	f.IntVar(&sampleMS, "sample-ms", int(resource.DefaultSample/time.Millisecond), "how long to watch CPU and swap counters")
	f.BoolVar(&asJSON, "json", false, "print compact JSON (the output is JSON either way)")
	return cmd
}

// attachUnder adds sub under the top-level group named group, creating the
// group when the registry has not.
func attachUnder(root *cobra.Command, group string, sub *cobra.Command) {
	for _, c := range root.Commands() {
		if c.Name() == group {
			c.AddCommand(sub)
			return
		}
	}
	g := &cobra.Command{Use: group, Short: "The multi-machine agent scheduler"}
	g.AddCommand(sub)
	root.AddCommand(g)
}
