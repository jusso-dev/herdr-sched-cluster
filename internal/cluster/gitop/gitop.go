// Package gitop is the small set of Git operations the coordinator asks a
// worker to perform: delete a scheduler-made branch once its worktree is
// gone, and clone a repository a job needs onto a worker that lacks it.
//
// Each operation is typed and validated here, on the worker, as well as on
// the coordinator. There is no way to ask for anything else: the branch must
// be one the scheduler names (hsc/j<n>, optionally with an attempt suffix),
// branch deletion uses `git branch -d`, which refuses a branch that is not
// fully merged, and a clone never writes into a path that already exists.
package gitop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

// Operations.
const (
	OpBranchDelete = "branch-delete"
	OpClone        = "clone"
)

// Request is one operation.
type Request struct {
	Op     string `json:"op"`
	Repo   string `json:"repo,omitempty"`
	Branch string `json:"branch,omitempty"`
	URL    string `json:"url,omitempty"`
	Path   string `json:"path,omitempty"`
}

// Result is what the worker answers.
type Result struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
	// Refused is true when Git declined on purpose (an unmerged branch, an
	// existing path), as opposed to failing.
	Refused bool `json:"refused,omitempty"`
}

// A host or user never starts with a dash, so neither can be read as an
// option by git or ssh.
const (
	host = `[A-Za-z0-9][A-Za-z0-9.-]*`
	user = `[A-Za-z0-9_][A-Za-z0-9._-]*`
)

var (
	branchRe = regexp.MustCompile(`^hsc/j[0-9]{1,9}(-[0-9]{1,3})?$`)
	urlRe    = regexp.MustCompile(`^(https://` + host + `(:[0-9]{1,5})?/[A-Za-z0-9._/~-]+|ssh://(` + user + `@)?` + host + `(:[0-9]{1,5})?/[A-Za-z0-9._/~-]+|` + user + `@` + host + `:[A-Za-z0-9_~][A-Za-z0-9._/~-]*)$`)
	pathRe   = regexp.MustCompile(`^(/|~/)[A-Za-z0-9 ._/+@,:=~-]*$`)
)

// ValidURL says whether u is a clone URL the scheduler accepts: https, ssh://
// or scp-style user@host:path, with no shell-active characters and no
// embedded credentials.
func ValidURL(u string) error {
	if len(u) > 512 || !urlRe.MatchString(u) || strings.Contains(u, "..") {
		return fmt.Errorf("clone url %q must be https://host/path, ssh://host/path or user@host:path, with no credentials", u)
	}
	if strings.HasPrefix(u, "https://") && strings.Contains(strings.SplitN(strings.TrimPrefix(u, "https://"), "/", 2)[0], "@") {
		return errors.New("clone url must not carry credentials; use the worker's own Git credential helper or SSH keys")
	}
	return nil
}

// Validate checks a request before it is sent or run.
func (r Request) Validate() error {
	switch r.Op {
	case OpBranchDelete:
		if !branchRe.MatchString(r.Branch) {
			return fmt.Errorf("branch %q is not one the scheduler made (hsc/j<n>)", r.Branch)
		}
		if !pathRe.MatchString(r.Repo) || strings.Contains(r.Repo, "..") {
			return fmt.Errorf("repository path %q is not an absolute or ~/ path", r.Repo)
		}
	case OpClone:
		if err := ValidURL(r.URL); err != nil {
			return err
		}
		if !pathRe.MatchString(r.Path) || strings.Contains(r.Path, "..") {
			return fmt.Errorf("clone path %q is not an absolute or ~/ path", r.Path)
		}
	default:
		return fmt.Errorf("unknown git operation %q", r.Op)
	}
	return nil
}

func expand(p string) string {
	if strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return home + p[1:]
		}
	}
	return p
}

// Run performs a request on this machine.
func Run(ctx context.Context, r Request) Result {
	if err := r.Validate(); err != nil {
		return Result{Message: err.Error(), Refused: true}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	var args []string
	switch r.Op {
	case OpBranchDelete:
		args = []string{"-C", expand(r.Repo), "branch", "-d", "--", r.Branch}
	case OpClone:
		dst := expand(r.Path)
		if _, err := os.Stat(dst); err == nil {
			return Result{Message: dst + " already exists; not cloning over it", Refused: true}
		}
		args = []string{"clone", "--quiet", "--", r.URL, dst}
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	// Never prompt: a worker without credentials for the URL fails rather
	// than hanging on a terminal nobody is watching.
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_ASKPASS=true", "SSH_ASKPASS=true")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	msg := strings.TrimSpace(out.String())
	if len(msg) > 400 {
		msg = msg[:400] + "..."
	}
	if err != nil {
		refused := strings.Contains(msg, "not fully merged") || strings.Contains(msg, "checked out at") ||
			strings.Contains(msg, "used by worktree")
		if msg == "" {
			msg = err.Error()
		}
		return Result{Message: msg, Refused: refused}
	}
	return Result{OK: true, Message: msg}
}

// Words renders a request as the remote hsched argv, before quoting.
func (r Request) Words() []string {
	w := []string{"cluster", "gitop", "--op", r.Op}
	if r.Repo != "" {
		w = append(w, "--repo", r.Repo)
	}
	if r.Branch != "" {
		w = append(w, "--branch", r.Branch)
	}
	if r.URL != "" {
		w = append(w, "--url", r.URL)
	}
	if r.Path != "" {
		w = append(w, "--path", r.Path)
	}
	return w
}
