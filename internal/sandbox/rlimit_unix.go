//go:build linux || darwin

package sandbox

import (
	"fmt"
	"os/exec"

	"sentra-agent/internal/obs"
)

func applyRLimits(cmd *exec.Cmd, env *SandboxEnv) error {
	memoryMB := env.Manifest.Resources.MemoryMB
	if memoryMB <= 0 {
		memoryMB = env.Config.DefaultMemoryMB
	}
	if memoryMB > env.Config.MaxMemoryMB {
		memoryMB = env.Config.MaxMemoryMB
	}

	if memoryMB > 0 {
		wrapWithUlimit(cmd, memoryMB)
		obs.Info("resource limits applied via rlimit (ulimit -v)", obs.Field{
			"memory_mb": memoryMB,
			"plugin":    env.Manifest.Name,
		})
	}

	cpuSeconds := env.Manifest.Resources.CPUSeconds
	if cpuSeconds > 0 {
		obs.Info("CPU limit requested but not enforced", obs.Field{
			"cpu_seconds": cpuSeconds,
			"plugin":      env.Manifest.Name,
		})
	}

	return nil
}

func wrapWithUlimit(cmd *exec.Cmd, memoryMB int64) {
	prevArgs := cmd.Args
	shellCmd := fmt.Sprintf("ulimit -v %d 2>/dev/null; exec \"$@\"", memoryMB*1024*1024)
	cmd.Path = "/bin/sh"
	cmd.Args = append([]string{"/bin/sh", "-c", shellCmd, "--"}, prevArgs...)
}
