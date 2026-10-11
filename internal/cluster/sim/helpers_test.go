package sim

import "github.com/jusso-dev/herdr-sched-cluster/internal/cluster/herdrx"

func herdrxStart(name, pane string) herdrx.StartRequest {
	return herdrx.StartRequest{Name: name, Kind: "claude", Pane: pane}
}
