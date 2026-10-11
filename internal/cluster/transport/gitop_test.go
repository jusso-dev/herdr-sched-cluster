package transport

import (
	"strings"
	"testing"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/gitop"
)

func TestGitOpWordsAreQuotedAndValidated(t *testing.T) {
	w, err := GitOpWords("hsched", gitop.Request{Op: gitop.OpBranchDelete, Repo: "~/src/app", Branch: "hsc/j12"})
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(w, " ")
	if !strings.Contains(got, "'cluster' 'gitop'") || !strings.Contains(got, "'hsc/j12'") {
		t.Fatal(got)
	}
	for _, bad := range []gitop.Request{
		{Op: gitop.OpBranchDelete, Repo: "/src/app", Branch: "main"},
		{Op: gitop.OpClone, URL: "https://x/y;rm -rf /", Path: "/tmp/x"},
		{Op: "push"},
	} {
		if _, err := GitOpWords("hsched", bad); err == nil {
			t.Errorf("accepted %+v", bad)
		}
	}
}
