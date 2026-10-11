// Package herdrx drives the Herdr CLI for the cluster scheduler. It follows
// the pattern herdr-dispatch uses: run `herdr` with a fixed argument vector
// (never a shell string), read the {"result": ...} envelope from stdout, and
// read Herdr's JSON error body from stderr when a verb is refused.
//
// Remote machines are reached with Herdr's own `--machine <id>` prefix, so
// the SSH connection, its keys and the remote Herdr server all stay Herdr's
// business. Only the verbs Herdr allows over --machine are used here:
// workspace, worktree, agent and pane.
package herdrx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Local is the machine id this package uses for the computer the
// coordinator runs on. It is never passed to Herdr.
const Local = "local"

// Machine is one row of `herdr machine list --json`.
type Machine struct {
	ID       string `json:"id"`
	Label    string `json:"label"`
	Target   string `json:"target"`
	Session  string `json:"session"`
	Enabled  bool   `json:"enabled"`
	Selected bool   `json:"selected"`
}

// MachineStatus is one row of `herdr machine status --json` (Herdr 0.9.2+).
type MachineStatus struct {
	ID     string `json:"id"`
	Label  string `json:"label"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// Agent is the part of Herdr's AgentInfo the scheduler reads.
type Agent struct {
	Name        string `json:"name"`
	Agent       string `json:"agent"`
	Status      string `json:"agent_status"`
	PaneID      string `json:"pane_id"`
	WorkspaceID string `json:"workspace_id"`
	Cwd         string `json:"cwd"`
}

// ProcessInfo is the part of `pane process-info` the scheduler reads.
type ProcessInfo struct {
	PaneID     string `json:"pane_id"`
	ShellPID   int    `json:"shell_pid"`
	Foreground []struct {
		PID int `json:"pid"`
	} `json:"foreground_processes"`
}

// Placement is where a new workspace or worktree came up.
type Placement struct {
	WorkspaceID string
	PaneID      string
	Path        string
}

// StartRequest is one `agent start`.
type StartRequest struct {
	Name    string
	Kind    string
	Pane    string
	Timeout time.Duration
	Args    []string
}

// Herdr is everything the coordinator asks of Herdr. The CLI implements it;
// the simulated cluster implements it in memory.
type Herdr interface {
	Machines(ctx context.Context) ([]Machine, error)
	MachineStatus(ctx context.Context) ([]MachineStatus, error)
	WorkspaceCreate(ctx context.Context, machine, cwd, label string) (Placement, error)
	WorktreeCreate(ctx context.Context, machine, cwd, branch, label string) (Placement, error)
	WorkspaceClose(ctx context.Context, machine, workspace string) error
	AgentStart(ctx context.Context, machine string, req StartRequest) error
	AgentPrompt(ctx context.Context, machine, target, text string) error
	Agents(ctx context.Context, machine string) ([]Agent, error)
	ProcessInfo(ctx context.Context, machine, pane string) (ProcessInfo, error)
}

// Error is a refusal from Herdr with its code.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return fmt.Sprintf("herdr: %s: %s", e.Code, e.Message) }

// Code returns the Herdr error code inside err, if there is one.
func Code(err error) string {
	var he *Error
	if errors.As(err, &he) {
		return he.Code
	}
	return ""
}

// CLI runs the herdr binary.
type CLI struct {
	// Bin is the herdr binary. Empty means HERDR_BIN_PATH at construction,
	// then `herdr` on PATH.
	Bin string
	// Timeout bounds every call that does not carry its own. Zero means 60s.
	Timeout time.Duration
}

// New builds a CLI client from the environment Herdr gives plugins.
func New() *CLI { return &CLI{Bin: os.Getenv("HERDR_BIN_PATH")} }

func (c *CLI) bin() string {
	if c.Bin != "" {
		return c.Bin
	}
	return "herdr"
}

var (
	idRe        = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$`)
	agentNameRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
)

// ValidAgentName reports whether Herdr will accept name for `agent start`.
func ValidAgentName(name string) bool { return agentNameRe.MatchString(name) }

func checkID(what, v string) error {
	if !idRe.MatchString(v) {
		return fmt.Errorf("herdrx: %s %q is not a plain identifier", what, v)
	}
	return nil
}

// argv builds the argument vector, with --machine first for a remote one.
func argv(machine string, args ...string) ([]string, error) {
	if machine == "" || machine == Local {
		return args, nil
	}
	if err := checkID("machine", machine); err != nil {
		return nil, err
	}
	return append([]string{"--machine", machine}, args...), nil
}

const maxOutput = 4 << 20

func (c *CLI) run(ctx context.Context, timeout time.Duration, args []string) ([]byte, error) {
	if timeout <= 0 {
		timeout = c.Timeout
	}
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.bin(), args...)
	cmd.Stdin = nil
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limited{w: &stdout, n: maxOutput}
	cmd.Stderr = &limited{w: &stderr, n: 64 << 10}
	if err := cmd.Run(); err != nil {
		return nil, refusal(err, args, stderr.Bytes())
	}
	return stdout.Bytes(), nil
}

// describe names a call for an error without echoing free text (a prompt
// is never written into an error or a log).
func describe(args []string) string {
	var keep []string
	for _, a := range args {
		if len(keep) >= 4 {
			break
		}
		keep = append(keep, a)
	}
	return strings.Join(keep, " ")
}

func refusal(runErr error, args []string, stderr []byte) error {
	var body struct {
		Error *Error `json:"error"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(stderr), &body); err == nil && body.Error != nil {
		return fmt.Errorf("herdr %s: %w", describe(args), body.Error)
	}
	msg := strings.TrimSpace(string(stderr))
	if len(msg) > 400 {
		msg = msg[:400] + "..."
	}
	if msg != "" {
		return fmt.Errorf("herdr %s: %s: %w", describe(args), msg, runErr)
	}
	return fmt.Errorf("herdr %s: %w", describe(args), runErr)
}

func (c *CLI) result(ctx context.Context, timeout time.Duration, into any, machine string, args ...string) error {
	full, err := argv(machine, args...)
	if err != nil {
		return err
	}
	out, err := c.run(ctx, timeout, full)
	if err != nil || into == nil {
		return err
	}
	var body struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(out, &body); err != nil || len(body.Result) == 0 {
		return fmt.Errorf("herdr %s: unreadable response", describe(full))
	}
	if err := json.Unmarshal(body.Result, into); err != nil {
		return fmt.Errorf("herdr %s: unreadable result: %w", describe(full), err)
	}
	return nil
}

// Machines lists the machines saved in Herdr.
func (c *CLI) Machines(ctx context.Context) ([]Machine, error) {
	out, err := c.run(ctx, 0, []string{"machine", "list", "--json"})
	if err != nil {
		return nil, err
	}
	var rows []Machine
	if err := json.Unmarshal(out, &rows); err != nil {
		return nil, fmt.Errorf("herdr machine list: unreadable json: %w", err)
	}
	return rows, nil
}

// MachineStatus asks Herdr to check every saved machine over SSH.
func (c *CLI) MachineStatus(ctx context.Context) ([]MachineStatus, error) {
	out, err := c.run(ctx, 2*time.Minute, []string{"machine", "status", "--json"})
	if err != nil {
		return nil, err
	}
	var rows []MachineStatus
	if err := json.Unmarshal(out, &rows); err != nil {
		return nil, fmt.Errorf("herdr machine status: unreadable json: %w", err)
	}
	return rows, nil
}

type created struct {
	Workspace struct {
		WorkspaceID string `json:"workspace_id"`
	} `json:"workspace"`
	RootPane struct {
		PaneID string `json:"pane_id"`
	} `json:"root_pane"`
	Worktree struct {
		Path string `json:"path"`
	} `json:"worktree"`
}

func (r created) placement(verb string) (Placement, error) {
	if r.Workspace.WorkspaceID == "" || r.RootPane.PaneID == "" {
		return Placement{}, fmt.Errorf("herdr %s: no workspace or pane id in the response", verb)
	}
	return Placement{WorkspaceID: r.Workspace.WorkspaceID, PaneID: r.RootPane.PaneID, Path: r.Worktree.Path}, nil
}

// WorkspaceCreate opens a workspace at cwd without taking focus.
func (c *CLI) WorkspaceCreate(ctx context.Context, machine, cwd, label string) (Placement, error) {
	var r created
	if err := c.result(ctx, 0, &r, machine, "workspace", "create", "--cwd", cwd, "--label", label, "--no-focus"); err != nil {
		return Placement{}, err
	}
	return r.placement("workspace create")
}

// WorktreeCreate opens a new Git worktree of the repository at cwd on a new
// branch, in its own workspace, without taking focus.
func (c *CLI) WorktreeCreate(ctx context.Context, machine, cwd, branch, label string) (Placement, error) {
	var r created
	if err := c.result(ctx, 0, &r, machine, "worktree", "create", "--cwd", cwd, "--branch", branch, "--label", label, "--no-focus"); err != nil {
		return Placement{}, err
	}
	return r.placement("worktree create")
}

// WorkspaceClose closes a workspace the scheduler opened and never started
// an agent in. It is not used on a workspace with a live agent.
func (c *CLI) WorkspaceClose(ctx context.Context, machine, workspace string) error {
	if err := checkID("workspace", workspace); err != nil {
		return err
	}
	return c.result(ctx, 0, nil, machine, "workspace", "close", workspace)
}

// AgentStart starts a named agent in a pane. A "timeout" refusal means the
// agent was launched but did not report ready in time; the caller decides.
func (c *CLI) AgentStart(ctx context.Context, machine string, req StartRequest) error {
	if !ValidAgentName(req.Name) {
		return fmt.Errorf("herdrx: agent name %q does not match [a-z][a-z0-9_-]{0,31}", req.Name)
	}
	if err := checkID("agent kind", req.Kind); err != nil {
		return err
	}
	if err := checkID("pane", req.Pane); err != nil {
		return err
	}
	args := []string{"agent", "start", req.Name, "--kind", req.Kind, "--pane", req.Pane}
	if req.Timeout > 0 {
		args = append(args, "--timeout", strconv.FormatInt(req.Timeout.Milliseconds(), 10))
	}
	if len(req.Args) > 0 {
		args = append(args, "--")
		args = append(args, req.Args...)
	}
	return c.result(ctx, req.Timeout+30*time.Second, nil, machine, args...)
}

// AgentPrompt submits text to an agent. The text travels as one argv
// element (and as a JSON field over Herdr's SSH bridge), never through a
// shell.
func (c *CLI) AgentPrompt(ctx context.Context, machine, target, text string) error {
	if err := checkID("agent", target); err != nil {
		return err
	}
	return c.result(ctx, 0, nil, machine, "agent", "prompt", target, text)
}

// Agents lists the agents on a machine.
func (c *CLI) Agents(ctx context.Context, machine string) ([]Agent, error) {
	var r struct {
		Agents []Agent `json:"agents"`
	}
	if err := c.result(ctx, 0, &r, machine, "agent", "list"); err != nil {
		return nil, err
	}
	return r.Agents, nil
}

// ProcessInfo reads a pane's shell and foreground processes.
func (c *CLI) ProcessInfo(ctx context.Context, machine, pane string) (ProcessInfo, error) {
	if err := checkID("pane", pane); err != nil {
		return ProcessInfo{}, err
	}
	var r struct {
		Info ProcessInfo `json:"process_info"`
	}
	full, err := argv(machine, "pane", "process-info", pane)
	if err != nil {
		return ProcessInfo{}, err
	}
	out, err := c.run(ctx, 0, full)
	if err != nil {
		return ProcessInfo{}, err
	}
	// Accept both {"result":{"process_info":{...}}} and {"result":{...}}.
	var body struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(out, &body); err != nil {
		return ProcessInfo{}, fmt.Errorf("herdr pane process-info: unreadable response")
	}
	if err := json.Unmarshal(body.Result, &r); err == nil && r.Info.PaneID != "" {
		return r.Info, nil
	}
	var flat ProcessInfo
	if err := json.Unmarshal(body.Result, &flat); err != nil {
		return ProcessInfo{}, fmt.Errorf("herdr pane process-info: unreadable result")
	}
	return flat, nil
}

type limited struct {
	w io.Writer
	n int
}

func (l *limited) Write(p []byte) (int, error) {
	if l.n <= 0 {
		return len(p), nil
	}
	k := len(p)
	if k > l.n {
		k = l.n
	}
	l.n -= k
	if _, err := l.w.Write(p[:k]); err != nil {
		return 0, err
	}
	return len(p), nil
}
