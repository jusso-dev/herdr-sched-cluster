// Package transport runs the resource probe on a remote worker.
//
// It is the only place this project builds a command that a shell will read:
// OpenSSH joins the remote argv into one string and hands it to the remote
// user's login shell. So every word is checked against an allow-list first
// and then single-quoted, and the argv itself is fixed. Nothing the caller
// passes can become a second command.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/gitop"
	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

// MaxPathLen bounds a path the probe is asked about.
const MaxPathLen = 1024

// maxOutput bounds what a probe may print. A real answer is a few kilobytes.
const maxOutput = 1 << 20

func pathRuneOK(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	}
	return strings.ContainsRune(" ._-/+@,:=~", r)
}

// ValidatePath accepts an absolute path or one starting with ~/, made only of
// letters, digits, space and ._-/+@,:=~ . Quotes, backslashes, dollar signs,
// backticks, semicolons, newlines and every other shell-active character are
// refused outright rather than escaped.
func ValidatePath(p string) error {
	if p == "" {
		return errors.New("a path must not be empty")
	}
	if len(p) > MaxPathLen {
		return fmt.Errorf("path is longer than %d bytes", MaxPathLen)
	}
	if !(strings.HasPrefix(p, "/") || strings.HasPrefix(p, "~/") || p == "~") {
		return fmt.Errorf("path %q must be absolute or start with ~/", p)
	}
	for _, r := range p {
		if !pathRuneOK(r) {
			return fmt.Errorf("path %q contains %q, which is not allowed", p, r)
		}
	}
	return nil
}

// ValidateTarget accepts an SSH destination as Herdr saves it: host,
// user@host, a Host alias, or ssh://user@host:port.
func ValidateTarget(t string) error {
	if t == "" || len(t) > 255 {
		return errors.New("ssh target must be 1 to 255 bytes")
	}
	if strings.HasPrefix(t, "-") {
		return fmt.Errorf("ssh target %q must not start with -", t)
	}
	for _, r := range t {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case strings.ContainsRune("._-@:/[]%", r):
		default:
			return fmt.Errorf("ssh target %q contains %q, which is not allowed", t, r)
		}
	}
	return nil
}

// quote renders one word for a POSIX, zsh or fish shell. The word has already
// passed an allow-list with no quote or backslash in it, so wrapping it in
// single quotes is enough. A leading ~/ is left outside the quotes so the
// remote shell still expands it.
func quote(w string) string {
	if rest, ok := strings.CutPrefix(w, "~/"); ok {
		if rest == "" {
			return "~/"
		}
		return "~/'" + rest + "'"
	}
	if w == "~" {
		return "~"
	}
	return "'" + w + "'"
}

// RemoteWords is the remote command, one quoted word per element, ready to
// follow `ssh ... -- target`. program is the hsched binary on the worker.
func RemoteWords(program string, req resource.Request) ([]string, error) {
	if err := ValidatePath(program); err != nil && !bareProgram(program) {
		return nil, fmt.Errorf("probe program: %w", err)
	}
	words := []string{quote(program), "'cluster'", "'probe'", "'--json'"}
	if req.Sample > 0 {
		words = append(words, "'--sample-ms'", "'"+strconv.FormatInt(req.Sample.Milliseconds(), 10)+"'")
	}
	for _, p := range req.Paths {
		if err := ValidatePath(p); err != nil {
			return nil, err
		}
		words = append(words, "'--path'", quote(p))
	}
	for _, pid := range req.PIDs {
		if pid <= 0 {
			return nil, fmt.Errorf("pid %d is not a process id", pid)
		}
		words = append(words, "'--pid'", "'"+strconv.Itoa(pid)+"'")
	}
	return words, nil
}

// bareProgram is a command name looked up on the remote PATH, like "hsched".
func bareProgram(p string) bool {
	if p == "" || len(p) > 64 {
		return false
	}
	for _, r := range p {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}

// SSH runs the probe over OpenSSH.
type SSH struct {
	// Binary is the ssh client; empty means "ssh" on PATH.
	Binary string
	// ConnectTimeout bounds the TCP and SSH handshake.
	ConnectTimeout time.Duration
	// Timeout bounds the whole call.
	Timeout time.Duration
}

// Result is a probe answer and how long the round trip took beyond the
// sampling the probe itself did.
type Result struct {
	Snapshot  resource.Snapshot
	LatencyMS int64
}

// Probe runs the probe on target. BatchMode means ssh fails rather than
// asking for a password or a host-key decision: authentication is the
// operator's, set up once for Herdr.
func (s SSH) Probe(ctx context.Context, target, program string, req resource.Request) (Result, error) {
	if err := ValidateTarget(target); err != nil {
		return Result{}, err
	}
	words, err := RemoteWords(program, req)
	if err != nil {
		return Result{}, err
	}
	bin := s.Binary
	if bin == "" {
		bin = "ssh"
	}
	ct := s.ConnectTimeout
	if ct <= 0 {
		ct = 5 * time.Second
	}
	to := s.Timeout
	if to <= 0 {
		to = 20 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, to)
	defer cancel()
	argv := []string{
		"-T",
		"-o", "BatchMode=yes",
		"-o", "ConnectTimeout=" + strconv.Itoa(int(ct.Seconds())),
		"-o", "ServerAliveInterval=5",
		"-o", "ServerAliveCountMax=2",
		"--", target,
	}
	argv = append(argv, words...)
	cmd := exec.CommandContext(ctx, bin, argv...)
	var stdout, stderr limited
	stdout.max, stderr.max = maxOutput, 4096
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	start := time.Now()
	runErr := cmd.Run()
	elapsed := time.Since(start)
	if runErr != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = runErr.Error()
		}
		return Result{}, &Error{Target: target, Message: msg, Auth: authFailure(msg)}
	}
	var snap resource.Snapshot
	if err := json.Unmarshal(stdout.Bytes(), &snap); err != nil {
		return Result{}, &Error{Target: target, Message: "probe answer is not JSON: " + err.Error()}
	}
	if snap.Schema != resource.SchemaVersion {
		return Result{}, &Error{Target: target, Message: fmt.Sprintf("probe schema %d, this coordinator speaks %d; update hsched on the worker", snap.Schema, resource.SchemaVersion)}
	}
	lat := elapsed - time.Duration(snap.SampleMS)*time.Millisecond
	if lat < 0 {
		lat = 0
	}
	return Result{Snapshot: snap, LatencyMS: lat.Milliseconds()}, nil
}

// Report runs `nighthawk report` on target. The remote argv is fixed.
func (s SSH) Report(ctx context.Context, target string) ([]byte, error) {
	if err := ValidateTarget(target); err != nil {
		return nil, err
	}
	bin := s.Binary
	if bin == "" {
		bin = "ssh"
	}
	ct := s.ConnectTimeout
	if ct <= 0 {
		ct = 5 * time.Second
	}
	to := s.Timeout
	if to <= 0 {
		to = 8 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, to)
	defer cancel()
	argv := []string{
		"-T",
		"-o", "BatchMode=yes",
		"-o", "ConnectTimeout=" + strconv.Itoa(int(ct.Seconds())),
		"--", target,
		"nighthawk", "report",
	}
	cmd := exec.CommandContext(ctx, bin, argv...)
	var stdout, stderr limited
	stdout.max, stderr.max = maxOutput, 4096
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, &Error{Target: target, Message: msg, Auth: authFailure(msg)}
	}
	return stdout.Bytes(), nil
}

// GitOpWords renders a typed Git operation as the remote command, every
// word allow-listed and quoted as RemoteWords does.
func GitOpWords(program string, r gitop.Request) ([]string, error) {
	if err := ValidatePath(program); err != nil && !bareProgram(program) {
		return nil, fmt.Errorf("probe program: %w", err)
	}
	if err := r.Validate(); err != nil {
		return nil, err
	}
	words := []string{quote(program)}
	for _, w := range r.Words() {
		if strings.ContainsAny(w, "'\\\n$`;|&<>(){}*?!\"") {
			return nil, fmt.Errorf("git operation word %q has a shell-active character", w)
		}
		words = append(words, quote(w))
	}
	return words, nil
}

// GitOp runs a typed Git operation on target over SSH, with the same fixed
// options the probe uses.
func (s SSH) GitOp(ctx context.Context, target, program string, r gitop.Request) (gitop.Result, error) {
	if err := ValidateTarget(target); err != nil {
		return gitop.Result{}, err
	}
	words, err := GitOpWords(program, r)
	if err != nil {
		return gitop.Result{}, err
	}
	bin := s.Binary
	if bin == "" {
		bin = "ssh"
	}
	ctx, cancel := context.WithTimeout(ctx, 11*time.Minute)
	defer cancel()
	argv := append([]string{"-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "ServerAliveInterval=5",
		"-o", "ServerAliveCountMax=2", "--", target}, words...)
	cmd := exec.CommandContext(ctx, bin, argv...)
	var stdout, stderr limited
	stdout.max, stderr.max = 64<<10, 4096
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil && stdout.Len() == 0 {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return gitop.Result{}, &Error{Target: target, Message: msg, Auth: authFailure(msg)}
	}
	var res gitop.Result
	if err := json.Unmarshal(stdout.Bytes(), &res); err != nil {
		return gitop.Result{}, &Error{Target: target, Message: "git operation answer is not JSON: " + err.Error()}
	}
	return res, nil
}

// Error is a probe that did not answer.
type Error struct {
	Target  string
	Message string
	Auth    bool
}

func (e *Error) Error() string { return "probe " + e.Target + ": " + e.Message }

func authFailure(msg string) bool {
	m := strings.ToLower(msg)
	return strings.Contains(m, "permission denied") || strings.Contains(m, "host key verification failed") ||
		strings.Contains(m, "no more authentication methods")
}

// limited is a buffer that stops growing at max bytes.
type limited struct {
	bytes.Buffer
	max int
}

func (l *limited) Write(p []byte) (int, error) {
	room := l.max - l.Len()
	if room <= 0 {
		return len(p), nil
	}
	if len(p) > room {
		l.Buffer.Write(p[:room])
		return len(p), nil
	}
	return l.Buffer.Write(p)
}

var _ io.Writer = (*limited)(nil)
