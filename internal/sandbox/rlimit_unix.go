//go:build linux || darwin

package sandbox

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

// applyCPULimit applies only the CPU time limit (RLIMIT_CPU) through the
// rlimit wrapper. It is used when a cgroup is already enforcing the memory
// cap (memory.max) and the CPU bandwidth cap (cpu.max): the per-process CPU
// TIME cap is complementary to a bandwidth throttle and must still be
// enforced, while an additional RLIMIT_AS address-space cap is intentionally
// NOT stacked on top of memory.max — script runtimes (Node/V8, Go) reserve
// large virtual address regions on Linux and a tight address-space cap would
// break them at startup.
func applyCPULimit(cmd *exec.Cmd, env *SandboxEnv) error {
	cpuSeconds := env.Manifest.Resources.CPUSeconds
	if cpuSeconds <= 0 {
		return nil
	}
	wrapWithLimits(cmd, []string{fmt.Sprintf("ulimit -t %d 2>/dev/null", cpuSeconds)})
	obs.Info("cpu time limit applied via rlimit", obs.Field{
		"cpu_seconds": cpuSeconds,
		"plugin":      env.Manifest.Name,
	})
	return nil
}

// wrapWithLimits rewrites cmd to run through /bin/sh -c '<limits>; exec "$@"'.
//
// The executable is resolved to an absolute path BEFORE the rewrite: Go's
// exec only resolves cmd.Path at Start(), so once cmd.Path becomes /bin/sh the
// original name would be re-looked-up by the child shell via its own PATH
// inside the sandbox. On hosts where the runtime lives off-PATH (e.g. arm64
// macOS CI runners with node under /opt/homebrew/bin), that lookup can fail
// with exit 127. Resolving here (parent PATH + well-known locations) gives the
// sandboxed process a direct absolute path with no PATH dependency. The first
// argv element is rewritten too, because the wrapper's `exec "$@"` runs
// argv[0], not cmd.Path.
func wrapWithLimits(cmd *exec.Cmd, limits []string) {
	if resolved := resolveExecutablePath(cmd.Path); resolved != "" && resolved != cmd.Path {
		cmd.Path = resolved
		if len(cmd.Args) > 0 {
			cmd.Args[0] = resolved
		}
	}
	prevArgs := cmd.Args
	shellCmd := strings.Join(limits, "; ") + "; exec \"$@\""
	cmd.Path = "/bin/sh"
	cmd.Args = append([]string{"/bin/sh", "-c", shellCmd, "--"}, prevArgs...)
}

// resolveExecutablePath returns an absolute path for a plugin runtime name.
// Absolute paths pass through untouched. Bare names (no path separator) are
// resolved via the parent's PATH first, then common runtime locations
// (Homebrew on arm64 macOS, /usr/local/bin on older images). It returns ""
// for relative paths that must be resolved relative to the process cwd at
// Start time, and the original name when nothing matches (the child shell
// then reports the failure, keeping behavior debuggable).
func resolveExecutablePath(name string) string {
	if name == "" || filepath.IsAbs(name) {
		return name
	}
	if strings.ContainsRune(name, os.PathSeparator) {
		return ""
	}
	if resolved, err := exec.LookPath(name); err == nil {
		return resolved
	}
	for _, candidate := range []string{
		"/opt/homebrew/bin/" + name,
		"/usr/local/bin/" + name,
		"/usr/bin/" + name,
	} {
		if fi, err := os.Stat(candidate); err == nil && !fi.IsDir() {
			return candidate
		}
	}
	obs.Warn("could not resolve plugin runtime to an absolute path; sandboxed process will rely on the child PATH", obs.Field{
		"executable": name,
	})
	return name
}
