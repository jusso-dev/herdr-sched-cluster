package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/spf13/cobra"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/gitop"
)

// newGitOpCmd is `hsched cluster gitop`: one typed Git operation on THIS
// machine, which a coordinator runs on a worker over SSH. Like probe it talks
// to no daemon. It only knows two operations, each validated here again: delete
// a branch the scheduler made (git branch -d, which keeps unmerged work), and
// clone a repository into a path that does not exist yet.
func newGitOpCmd() *cobra.Command {
	var r gitop.Request
	cmd := &cobra.Command{
		Use:   "gitop",
		Short: "Run one scheduler Git operation on this machine (no daemon)",
		Long: "Deletes a scheduler-made branch (hsc/j<n>) with git branch -d, which refuses " +
			"unmerged work, or clones a repository into a path that does not exist yet. " +
			"Prints one JSON result. A coordinator runs it on a worker over SSH to finish " +
			"worktree cleanup and to fetch a repository a job needs.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			res := gitop.Run(context.Background(), r)
			return json.NewEncoder(os.Stdout).Encode(res)
		},
	}
	f := cmd.Flags()
	f.StringVar(&r.Op, "op", "", "branch-delete or clone")
	f.StringVar(&r.Repo, "repo", "", "repository checkout, for branch-delete")
	f.StringVar(&r.Branch, "branch", "", "the scheduler branch to delete (hsc/j<n>)")
	f.StringVar(&r.URL, "url", "", "the URL to clone (https, ssh:// or user@host:path)")
	f.StringVar(&r.Path, "path", "", "where to clone to; must not exist")
	return cmd
}
