package herdrx

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fake writes a herdr stand-in that records its argv, one argument per
// line, and answers with the given stdout, stderr and exit code.
func fake(t *testing.T, stdout, stderr string, code int) (*CLI, string) {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "argv")
	if err := os.WriteFile(filepath.Join(dir, "out"), []byte(stdout), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "err"), []byte(stderr), 0o600); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nfor a in \"$@\"; do printf '%s\\n' \"$a\" >> " + log + "; done\ncat " +
		filepath.Join(dir, "out") + "\ncat " + filepath.Join(dir, "err") + " >&2\nexit " + string(rune('0'+code)) + "\n"
	bin := filepath.Join(dir, "herdr")
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return &CLI{Bin: bin, Timeout: 10 * time.Second}, log
}

func argvOf(t *testing.T, log string) []string {
	b, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimRight(string(b), "\n"), "\n")
}

var ctx = context.Background()

func TestRemoteCallsCarryTheMachinePrefix(t *testing.T) {
	c, log := fake(t, `{"result":{"type":"workspace_created","workspace":{"workspace_id":"w_4"},"tab":{},"root_pane":{"pane_id":"w_4:p1"}}}`, "", 0)
	p, err := c.WorkspaceCreate(ctx, "mini", "/Users/j/src/app", "hsc-j1")
	if err != nil {
		t.Fatal(err)
	}
	if p.WorkspaceID != "w_4" || p.PaneID != "w_4:p1" {
		t.Fatalf("%+v", p)
	}
	want := []string{"--machine", "mini", "workspace", "create", "--cwd", "/Users/j/src/app", "--label", "hsc-j1", "--no-focus"}
	if got := argvOf(t, log); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("argv %q", got)
	}
}

func TestLocalCallsHaveNoPrefix(t *testing.T) {
	c, log := fake(t, `{"result":{"agents":[{"name":"hsc-j1","agent_status":"working","pane_id":"w1:p1","workspace_id":"w1","terminal_id":"t"}]}}`, "", 0)
	as, err := c.Agents(ctx, Local)
	if err != nil || len(as) != 1 || as[0].Name != "hsc-j1" || as[0].Status != "working" {
		t.Fatal(as, err)
	}
	if got := argvOf(t, log); got[0] != "agent" {
		t.Fatalf("argv %q", got)
	}
}

func TestPromptIsOneArgumentAndNeverInterpreted(t *testing.T) {
	c, log := fake(t, `{"result":{}}`, "", 0)
	prompt := "fix it; rm -rf ~ $(whoami) `id`\nsecond line"
	if err := c.AgentPrompt(ctx, "mini", "hsc-j1", prompt); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(argvOf(t, log), "\n")
	if !strings.HasSuffix(got, "prompt\nhsc-j1\n"+prompt) {
		t.Fatalf("argv %q", got)
	}
}

func TestAgentStartArgv(t *testing.T) {
	c, log := fake(t, `{"result":{"agent":{"name":"hsc-j2"}}}`, "", 0)
	err := c.AgentStart(ctx, "mbp", StartRequest{Name: "hsc-j2", Kind: "claude", Pane: "w_1:p1", Timeout: 5 * time.Second, Args: []string{"--model", "opus"}})
	if err != nil {
		t.Fatal(err)
	}
	want := "--machine|mbp|agent|start|hsc-j2|--kind|claude|--pane|w_1:p1|--timeout|5000|--|--model|opus"
	if got := strings.Join(argvOf(t, log), "|"); got != want {
		t.Fatalf("argv %s", got)
	}
}

func TestRefusalCarriesTheCode(t *testing.T) {
	c, _ := fake(t, "", `{"error":{"code":"timeout","message":"agent did not become ready"}}`, 1)
	err := c.AgentStart(ctx, "", StartRequest{Name: "hsc-j3", Kind: "codex", Pane: "w1:p1"})
	if Code(err) != "timeout" {
		t.Fatalf("%v", err)
	}
}

func TestBadIdentifiersNeverReachHerdr(t *testing.T) {
	c, log := fake(t, `{"result":{}}`, "", 0)
	for _, err := range []error{
		c.AgentStart(ctx, "", StartRequest{Name: "Bad Name", Kind: "claude", Pane: "p"}),
		c.AgentStart(ctx, "", StartRequest{Name: "ok", Kind: "claude;id", Pane: "p"}),
		c.AgentPrompt(ctx, "-oProxyCommand=x", "hsc-j1", "hi"),
		c.WorkspaceClose(ctx, "", "--all"),
	} {
		if err == nil {
			t.Fatal("accepted a bad identifier")
		}
	}
	if _, err := os.Stat(log); err == nil {
		t.Fatal("herdr was called")
	}
}

func TestMachineListIsABareArray(t *testing.T) {
	c, _ := fake(t, `[{"id":"m1","label":"mini","target":"justin@mini.local","session":"default","enabled":true,"selected":false}]`, "", 0)
	ms, err := c.Machines(ctx)
	if err != nil || len(ms) != 1 || ms[0].Target != "justin@mini.local" || !ms[0].Enabled {
		t.Fatal(ms, err)
	}
}

func TestProcessInfo(t *testing.T) {
	c, _ := fake(t, `{"result":{"type":"pane_process_info","process_info":{"pane_id":"w1:p1","shell_pid":4242,"foreground_processes":[{"pid":4300,"name":"claude"}]}}}`, "", 0)
	pi, err := c.ProcessInfo(ctx, "", "w1:p1")
	if err != nil || pi.ShellPID != 4242 || len(pi.Foreground) != 1 || pi.Foreground[0].PID != 4300 {
		t.Fatal(pi, err)
	}
}

// Herdr 0.9.3 takes the pane as --pane; a bare pane id is refused with
// "unknown option".
func TestProcessInfoPassesThePaneAsAFlag(t *testing.T) {
	c, log := fake(t, `{"result":{"type":"pane_process_info","process_info":{"pane_id":"w1:p1","shell_pid":1}}}`, "", 0)
	if _, err := c.ProcessInfo(ctx, "mini", "w1:p1"); err != nil {
		t.Fatal(err)
	}
	want := []string{"--machine", "mini", "pane", "process-info", "--pane", "w1:p1"}
	if got := argvOf(t, log); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("argv %q", got)
	}
}

// The answers below are copied from real Herdr 0.9.3.
func TestWorkspaceListAndWorktreeRemove(t *testing.T) {
	c, _ := fake(t, `{"id":"cli:workspace:list","result":{"type":"workspace_list","workspaces":[{"label":"hsc-j2","workspace_id":"w3","worktree":{"checkout_path":"/home/box/.herdr/worktrees/hz-repo/hsc-j2","is_linked_worktree":true}}]}}`, "", 0)
	ws, err := c.WorkspaceList(ctx, "")
	if err != nil || len(ws) != 1 || ws[0].ID != "w3" || ws[0].Label != "hsc-j2" || ws[0].Worktree == nil || !ws[0].Worktree.IsLinked {
		t.Fatal(ws, err)
	}
	c, log := fake(t, "", `{"error":{"code":"dirty_worktree_requires_force","message":"fatal: '/x' contains modified or untracked files, use --force to delete it"},"id":"cli:worktree:remove"}`, 1)
	err = c.WorktreeRemove(ctx, "mini", "w3", false)
	if Code(err) != "dirty_worktree_requires_force" {
		t.Fatal(err)
	}
	want := []string{"--machine", "mini", "worktree", "remove", "--workspace", "w3"}
	if got := argvOf(t, log); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("argv %q", got)
	}
	c, _ = fake(t, "", `{"error":{"code":"worktree_create_failed","message":"fatal: invalid reference: HEAD"},"id":"cli:worktree:create"}`, 1)
	_, err = c.WorktreeCreate(ctx, "", "/tmp/empty", "hsc/j1", "hsc-j1")
	if err == nil || !strings.Contains(err.Error(), "invalid reference: HEAD") {
		t.Fatal(err)
	}
}
