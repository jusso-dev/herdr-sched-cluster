package gitop

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidationRefusesAnythingButTheTypedOperations(t *testing.T) {
	bad := []Request{
		{Op: "exec", Repo: "/r"},
		{Op: OpBranchDelete, Repo: "/r", Branch: "main"},
		{Op: OpBranchDelete, Repo: "/r", Branch: "hsc/j1;rm -rf /"},
		{Op: OpBranchDelete, Repo: "relative", Branch: "hsc/j1"},
		{Op: OpBranchDelete, Repo: "/r/../etc", Branch: "hsc/j1"},
		{Op: OpClone, URL: "https://user:pw@github.com/a/b", Path: "/x"},
		{Op: OpClone, URL: "https://github.com/a/b$(id)", Path: "/x"},
		{Op: OpClone, URL: "file:///etc", Path: "/x"},
		{Op: OpClone, URL: "--upload-pack=evil", Path: "/x"},
		{Op: OpClone, URL: "git@-oProxyCommand:x", Path: "/x"},
		{Op: OpClone, URL: "ssh://-oProxyCommand/x", Path: "/x"},
		{Op: OpClone, URL: "https://github.com/a/b", Path: "x'y"},
	}
	for _, r := range bad {
		if r.Validate() == nil {
			t.Errorf("accepted %+v", r)
		}
	}
	good := []Request{
		{Op: OpBranchDelete, Repo: "~/src/app", Branch: "hsc/j12"},
		{Op: OpBranchDelete, Repo: "/src/app", Branch: "hsc/j12-2"},
		{Op: OpClone, URL: "https://github.com/jusso-dev/herdr-sched-cluster.git", Path: "~/src/x"},
		{Op: OpClone, URL: "git@github.com:jusso-dev/x.git", Path: "/src/x"},
		{Op: OpClone, URL: "ssh://git@host:2222/a/b.git", Path: "/src/x"},
	}
	for _, r := range good {
		if err := r.Validate(); err != nil {
			t.Errorf("refused %+v: %v", r, err)
		}
	}
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func TestBranchDeleteOnlyRemovesMergedBranches(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	repo := t.TempDir()
	git(t, repo, "init", "-q")
	git(t, repo, "commit", "-q", "--allow-empty", "-m", "base")
	git(t, repo, "branch", "hsc/j1")
	git(t, repo, "checkout", "-q", "-b", "hsc/j2")
	if err := os.WriteFile(filepath.Join(repo, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	git(t, repo, "add", "f")
	git(t, repo, "commit", "-q", "-m", "work")
	git(t, repo, "checkout", "-q", "-")
	if r := Run(context.Background(), Request{Op: OpBranchDelete, Repo: repo, Branch: "hsc/j1"}); !r.OK {
		t.Fatalf("merged branch not deleted: %+v", r)
	}
	r := Run(context.Background(), Request{Op: OpBranchDelete, Repo: repo, Branch: "hsc/j2"})
	if r.OK || !r.Refused || !strings.Contains(r.Message, "not fully merged") {
		t.Fatalf("unmerged branch: %+v", r)
	}
}

func TestCloneNeverWritesOverAnExistingPath(t *testing.T) {
	dir := t.TempDir()
	r := Run(context.Background(), Request{Op: OpClone, URL: "https://example.invalid/a/b.git", Path: dir})
	if r.OK || !r.Refused {
		t.Fatalf("%+v", r)
	}
}
