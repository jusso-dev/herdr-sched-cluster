package placement

import (
	"strings"
	"testing"
)

func TestRequireNighthawk(t *testing.T) {
	job := Job{ID: "j", Kind: "claude", MemoryMB: 1000, CPU: 0.5}
	w := mac("w", 32768, 20000, 8)
	w.Settings.RequireNighthawk = true

	w.Nighthawk = "down"
	if got := strings.Join(Eligible(job, w), "; "); !strings.Contains(got, "nighthawk is down") {
		t.Fatalf("%s", got)
	}

	w.Nighthawk, w.NighthawkMode = "ok", "attended"
	if got := strings.Join(Eligible(job, w), "; "); !strings.Contains(got, "attended-only") {
		t.Fatalf("%s", got)
	}

	w.NighthawkMode = "unattended"
	if got := Eligible(job, w); len(got) != 0 {
		t.Fatalf("unattended should be eligible: %v", got)
	}

	w.Settings.RequireNighthawk = false
	w.Nighthawk = "down"
	if got := strings.Join(Eligible(job, w), "; "); strings.Contains(got, "nighthawk") {
		t.Fatalf("check is opt-in: %s", got)
	}
}

func TestReadReport(t *testing.T) {
	h, m := ReadReport([]byte(`{"health":"ok","mode":"unattended","spend":null}`), nil)
	if h != "ok" || m != "unattended" {
		t.Fatalf("%s %s", h, m)
	}
	h, m = ReadReport(nil, errDown)
	if h != "down" || m != "" {
		t.Fatalf("%s %s", h, m)
	}
	h, _ = ReadReport([]byte(`{"health":"no"}`), nil)
	if h != "down" {
		t.Fatal(h)
	}
}

var errDown = downError{}

type downError struct{}

func (downError) Error() string { return "down" }
