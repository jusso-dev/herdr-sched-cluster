package daemon

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jusso-dev/herdr-sched-cluster/internal/protocol"
)

func TestClusterOwnerChecksAndNewVerbs(t *testing.T) {
	d, _ := clusterDaemon(t)
	d.Cluster.Coord.Cfg.MaxQueueSize = 0
	// Pin to a machine that does not exist yet would be refused, so queue
	// by asking for more memory than is free right now but less than total.
	if _, err := call(t, d, "cluster.submit", map[string]any{"kind": "claude", "repo": "~/src/app", "memory_mb": float64(1024),
		"restartable": true, "depends_on": "", "machine": "mini"}); err != nil {
		t.Fatal(err)
	}
	other := protocol.Request{Verb: "cluster.cancel", Args: map[string]any{"id": "j1"}, Pane: "wOther:p9", Door: "cli"}
	if _, err := d.Handle(context.Background(), other); err == nil || !strings.Contains(err.Error(), "cluster_owner_checks") {
		t.Fatalf("another agent cancelled the job: %v", err)
	}
	raw, err := call(t, d, "cluster.estimates", nil)
	if err != nil {
		t.Fatal(err)
	}
	var est EstimatesReport
	if err := json.Unmarshal(raw, &est); err != nil || est.Window != 20 {
		t.Fatalf("%s %v", raw, err)
	}
	if _, err := call(t, d, "cluster.reregister", map[string]any{"worker": "mini"}); err != nil {
		t.Fatal(err)
	}
	if _, err := call(t, d, "cluster.attach", map[string]any{"id": "j404"}); err == nil {
		t.Fatal("attached to a job that does not exist")
	}
	raw, err = call(t, d, "cluster.metrics", nil)
	if err != nil || !strings.Contains(string(raw), "hsched_cluster_jobs_ended") {
		t.Fatalf("%s %v", raw, err)
	}
}
