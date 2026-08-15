//go:build linux || darwin

package sandbox

import (
	"fmt"
	"os/exec"
	"strings"

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

	limits := []string{}
	if memoryMB > 0 {
		// ulimit -v takes KiB, so a 64 MiB cap is 64*1024 KiB, not
		// 64*1024*1024 (which would be 64 GiB — 1024x too loose).
		limits = append(limits, fmt.Sprintf("ulimit -v %d 2>/dev/null", memoryMB*1024))
	}

	cpuSeconds := env.Manifest.Resources.CPUSeconds
	if cpuSeconds > 0 {
		limits = append(limits, fmt.Sprintf("ulimit -t %d 2>/dev/null", cpuSeconds))
	}

	if len(limits) == 0 {
		return nil
	}

	wrapWithLimits(cmd, limits)

	obs.Info("resource limits applied via rlimit", obs.Field{
		"memory_mb":   memoryMB,
		"cpu_seconds": cpuSeconds,
		"plugin":      env.Manifest.Name,
	})
	return nil
}

func wrapWithLimits(cmd *exec.Cmd, limits []string) {
	prevArgs := cmd.Args
	shellCmd := strings.Join(limits, "; ") + "; exec \"$@\""
	cmd.Path = "/bin/sh"
	cmd.Args = append([]string{"/bin/sh", "-c", shellCmd, "--"}, prevArgs...)
}
