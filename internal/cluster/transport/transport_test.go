package transport

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jusso-dev/herdr-sched-cluster/internal/cluster/resource"
)

func TestValidatePathRefusesShellActiveCharacters(t *testing.T) {
	good := []string{"/Users/justin/src/app", "~/src/app", "~", "/srv/a b/c+d@e,f:g=h", "/tmp/x.y_z-1"}
	for _, p := range good {
		if err := ValidatePath(p); err != nil {
			t.Errorf("%q refused: %v", p, err)
		}
	}
	bad := []string{"", "relative/path", "/x;rm -rf ~", "/x$(id)", "/x`id`", "/x'y", "/x\"y", "/x\\y",
		"/x\ny", "/x|y", "/x&y", "/x>y", "/x*", "~root/x", "/x\x00y", "/x\ty", "/x{a,b}", "/x!y", "/x#y"}
	for _, p := range bad {
		if err := ValidatePath(p); err == nil {
			t.Errorf("%q accepted", p)
		}
	}
	if ValidatePath("/"+strings.Repeat("a", MaxPathLen)) == nil {
		t.Error("over-long path accepted")
	}
}

func TestValidateTarget(t *testing.T) {
	for _, ok := range []string{"macmini", "justin@macmini.local", "ssh://justin@10.0.0.5:2222", "[fe80::1%en0]"} {
		if err := ValidateTarget(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "-oProxyCommand=evil", "host;id", "host name", "host\n", "h$(x)"} {
		if ValidateTarget(bad) == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestRemoteWordsAreFixedAndQuoted(t *testing.T) {
	words, err := RemoteWords("~/.local/bin/hsched", resource.Request{Paths: []string{"~/src/my app", "/srv/x"}, PIDs: []int{42}, Sample: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(words, " ")
	want := "~/'.local/bin/hsched' 'cluster' 'probe' '--json' '--sample-ms' '200' '--path' ~/'src/my app' '--path' '/srv/x' '--pid' '42'"
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	if _, err := RemoteWords("hsched", resource.Request{Paths: []string{"/x;id"}}); err == nil {
		t.Fatal("a hostile path reached the remote command")
	}
	if _, err := RemoteWords("hsched; id", resource.Request{}); err == nil {
		t.Fatal("a hostile program reached the remote command")
	}
	if _, err := RemoteWords("hsched", resource.Request{PIDs: []int{-1}}); err == nil {
		t.Fatal("a negative pid reached the remote command")
	}
}

// fakeSSH writes a stand-in ssh that records its argv and answers with a
// fixed document, so the whole Probe path runs without a network.
func fakeSSH(t *testing.T, answer string, exit int) (bin, argvFile string) {
	t.Helper()
	dir := t.TempDir()
	argvFile = filepath.Join(dir, "argv")
	out := filepath.Join(dir, "out")
	if err := os.WriteFile(out, []byte(answer), 0o600); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nfor a in \"$@\"; do printf '%s\\n' \"$a\"; done > " + argvFile + "\n"
	if exit != 0 {
		script += "echo 'Permission denied (publickey).' >&2\nexit 255\n"
	} else {
		script += "cat " + out + "\n"
	}
	bin = filepath.Join(dir, "ssh")
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return bin, argvFile
}

func TestProbeRunsSSHInBatchModeWithOneArgvElementPerWord(t *testing.T) {
	snap := resource.Snapshot{Schema: resource.SchemaVersion, Backend: "darwin", Memory: resource.Memory{TotalMB: 16384, AvailableMB: 9000}}
	b, _ := json.Marshal(snap)
	bin, argvFile := fakeSSH(t, string(b), 0)
	res, err := SSH{Binary: bin}.Probe(context.Background(), "macmini", "hsched", resource.Request{Paths: []string{"~/src/app"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Snapshot.Memory.TotalMB != 16384 {
		t.Fatalf("%+v", res.Snapshot)
	}
	raw, _ := os.ReadFile(argvFile)
	argv := strings.Split(strings.TrimSpace(string(raw)), "\n")
	joined := strings.Join(argv, "|")
	if !strings.Contains(joined, "-T|-o|BatchMode=yes") || !strings.Contains(joined, "--|macmini|'hsched'|'cluster'|'probe'|'--json'|'--path'|~/'src/app'") {
		t.Fatalf("argv %q", argv)
	}
}

func TestProbeReportsAuthFailure(t *testing.T) {
	bin, _ := fakeSSH(t, "", 255)
	_, err := SSH{Binary: bin}.Probe(context.Background(), "macmini", "hsched", resource.Request{})
	pe, ok := err.(*Error)
	if !ok || !pe.Auth {
		t.Fatalf("want an auth error, got %v", err)
	}
}

func TestProbeRefusesAnotherSchema(t *testing.T) {
	bin, _ := fakeSSH(t, `{"schema":99}`, 0)
	if _, err := (SSH{Binary: bin}).Probe(context.Background(), "macmini", "hsched", resource.Request{}); err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("got %v", err)
	}
	bin, _ = fakeSSH(t, `not json`, 0)
	if _, err := (SSH{Binary: bin}).Probe(context.Background(), "macmini", "hsched", resource.Request{}); err == nil {
		t.Fatal("non-JSON answer accepted")
	}
}

func TestProbeRefusesHostileTargetBeforeRunningAnything(t *testing.T) {
	bin, argvFile := fakeSSH(t, "{}", 0)
	if _, err := (SSH{Binary: bin}).Probe(context.Background(), "-oProxyCommand=id", "hsched", resource.Request{}); err == nil {
		t.Fatal("hostile target accepted")
	}
	if _, err := os.Stat(argvFile); err == nil {
		t.Fatal("ssh ran for a hostile target")
	}
}
