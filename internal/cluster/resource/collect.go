package resource

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// ErrNoCollector is returned on a platform with no live collector. Such a
// worker is described with static capacity instead (see Static).
var ErrNoCollector = errors.New("no live resource collector on this platform; declare the worker with probe = \"static\"")

// Collect reads this machine. It never fails because one reading is missing:
// the reading is left at its zero value and a warning says why. It fails
// only when the platform has no collector at all.
func Collect(ctx context.Context, req Request) (Snapshot, error) {
	if req.Sample <= 0 {
		req.Sample = DefaultSample
	}
	s := Snapshot{
		Schema:      SchemaVersion,
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		CollectedAt: time.Now().UTC(),
		SampleMS:    req.Sample.Milliseconds(),
		Thermal:     ThermalUnknown,
		Power:       Power{Source: PowerUnknown, BatteryPct: -1},
	}
	s.Host, _ = os.Hostname()
	if err := collectPlatform(ctx, &s, req); err != nil {
		return s, err
	}
	home, err := os.UserHomeDir()
	if err == nil {
		s.Home = home
		s.Disk, err = diskOf(home)
	}
	if err != nil {
		s.warn("disk: %v", err)
	}
	s.Agents = findAgents(lookPathWithExtras)
	if len(req.Paths) > 0 {
		s.Paths = map[string]bool{}
		for _, p := range req.Paths {
			s.Paths[p] = pathExists(p)
		}
	}
	return s, nil
}

func (s *Snapshot) warn(format string, args ...any) {
	s.Warnings = append(s.Warnings, sprintf(format, args...))
}

// ExpandHome turns a leading ~ or ~/ into the home directory. Any other ~
// form (~user) is left alone, and the path will then not exist.
func ExpandHome(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, strings.TrimPrefix(p, "~"))
		}
	}
	return p
}

func pathExists(p string) bool {
	_, err := os.Stat(ExpandHome(p))
	return err == nil
}

// extraBinDirs are where agent CLIs are commonly installed but which a
// non-interactive SSH session often leaves off PATH.
var extraBinDirs = []string{
	"~/.local/bin", "~/bin", "~/.npm-global/bin", "~/.bun/bin", "~/.cargo/bin",
	"~/.claude/local", "~/.opencode/bin", "/opt/homebrew/bin", "/usr/local/bin",
}

func lookPathWithExtras(name string) bool {
	if _, err := exec.LookPath(name); err == nil {
		return true
	}
	for _, d := range extraBinDirs {
		fi, err := os.Stat(filepath.Join(ExpandHome(d), name))
		if err == nil && !fi.IsDir() && fi.Mode()&0o111 != 0 {
			return true
		}
	}
	return false
}

func findAgents(found func(string) bool) []string {
	out := []string{}
	for kind, exe := range AgentExecutables {
		if found(exe) {
			out = append(out, kind)
		}
	}
	sort.Strings(out)
	return out
}
