//go:build !linux && !darwin

package resource

import (
	"context"
	"errors"
)

func collectPlatform(_ context.Context, s *Snapshot, _ Request) error {
	s.Backend = BackendStatic
	return ErrNoCollector
}

func diskOf(path string) (Disk, error) {
	return Disk{Path: path}, errors.New("disk usage is not read on this platform")
}
