package resource

import (
	"context"
	"os/exec"
	"strings"
	"time"
)

// nvidiaGPUs asks nvidia-smi, when it is installed, for each GPU's name and
// memory. It costs nothing on a machine without one: LookPath fails first.
func nvidiaGPUs(ctx context.Context) []GPU {
	path, err := exec.LookPath("nvidia-smi")
	if err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, path,
		"--query-gpu=name,memory.total,memory.used", "--format=csv,noheader,nounits").Output()
	if err != nil {
		return nil
	}
	return parseNvidiaSMI(string(out))
}

func parseNvidiaSMI(text string) []GPU {
	var out []GPU
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		f := strings.Split(line, ",")
		if len(f) != 3 {
			continue
		}
		out = append(out, GPU{Name: strings.TrimSpace(f[0]), MemoryMB: atoi64(f[1]), UsedMB: atoi64(f[2])})
	}
	return out
}
