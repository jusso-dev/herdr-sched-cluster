//go:build linux || darwin

package resource

import "golang.org/x/sys/unix"

func diskOf(path string) (Disk, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return Disk{Path: path}, err
	}
	bs := float64(st.Bsize)
	const gb = 1 << 30
	return Disk{
		Path:    path,
		TotalGB: round1(float64(st.Blocks) * bs / gb),
		FreeGB:  round1(float64(st.Bavail) * bs / gb),
	}, nil
}

func round1(f float64) float64 { return float64(int64(f*10+0.5)) / 10 }
