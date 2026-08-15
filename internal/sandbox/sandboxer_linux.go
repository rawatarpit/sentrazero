//go:build linux

package sandbox

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"sentra-agent/internal/obs"
)

func newPlatformSandbox(cfg SandboxConfig) Sandboxer {
	return &linuxSandbox{cfg: cfg}
}

type linuxSandbox struct {
	cfg         SandboxConfig
	cgroupPaths []string
}

func (s *linuxSandbox) Prepare(ctx context.Context, jobID string, manifest PluginManifest, network bool) (*SandboxEnv, error) {
	workDir := filepath.Join(s.cfg.TempDir, "sentrazero", jobID)
	if err := os.MkdirAll(workDir, 0700); err != nil {
		return nil, fmt.Errorf("create work dir: %w", err)
	}

	return &SandboxEnv{
		WorkDir:  workDir,
		Config:   s.cfg,
		Manifest: manifest,
		Network:  network,
		Platform: "linux",
		Cleanup:  func() { os.RemoveAll(workDir) },
	}, nil
}

var cgroupJobCounter int64

func (s *linuxSandbox) Execute(ctx context.Context, env *SandboxEnv, cmd *exec.Cmd) error {
	if !s.cfg.LinuxNamespaces {
		return cmd.Run()
	}

	// Check if unprivileged user namespaces are supported before attempting
	namespacesSupported := isUserNamespaceSupported()
	if !namespacesSupported {
		obs.Warn("Linux namespaces requested but not supported, falling back to noop sandbox",
			obs.Field{"plugin": env.Manifest.Name})
		return cmd.Run()
	}

	// Build clone flags: isolate everything EXCEPT network when the plugin
	// has explicitly requested network access. CLONE_NEWNET creates a new
	// network namespace with NO external connectivity (only loopback, which
	// is DOWN by default). Plugins that need HTTP access must have network=true
	// in their manifest so that CLONE_NEWNET is omitted and the host network
	// is shared.
	cloneFlags := uintptr(syscall.CLONE_NEWNS |
		syscall.CLONE_NEWUTS |
		syscall.CLONE_NEWIPC |
		syscall.CLONE_NEWPID |
		syscall.CLONE_NEWUSER)
	if !env.Network {
		cloneFlags |= uintptr(syscall.CLONE_NEWNET)
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: cloneFlags,
		UidMappings: []syscall.SysProcIDMap{
			{ContainerID: 0, HostID: os.Getuid(), Size: 1},
		},
		GidMappings: []syscall.SysProcIDMap{
			{ContainerID: 0, HostID: os.Getgid(), Size: 1},
		},
	}

	caps := DetectCapabilities(s.cfg)

	memoryMB := s.resolveMemoryLimit(env)
	cgSubPath := ""
	useCgroup := memoryMB > 0 && caps.HasCgroupWrite && caps.CgroupPath != ""

	if useCgroup {
		jobID := fmt.Sprintf("job-%d-%d", time.Now().Unix(), atomic.AddInt64(&cgroupJobCounter, 1))
		cgSubPath = caps.CgroupPath + "/" + jobID
		if err := os.MkdirAll(cgSubPath, 0755); err != nil {
			obs.Warn("cgroup subdir creation failed, falling back to rlimit",
				obs.Field{"error": err.Error(), "path": cgSubPath})
			cgSubPath = ""
			useCgroup = false
		} else {
			memLimit := strconv.FormatInt(memoryMB*1024*1024, 10)
			if err := os.WriteFile(cgSubPath+"/memory.max", []byte(memLimit), 0644); err != nil {
				obs.Warn("cgroup memory.max write failed, falling back to rlimit",
					obs.Field{"error": err.Error(), "memory_mb": memoryMB})
				os.RemoveAll(cgSubPath)
				cgSubPath = ""
				useCgroup = false
			} else {
				// cpu.max enforces a bandwidth cap ("quota period" in
				// microseconds; 100000us period = 100ms). The quota is
				// derived from the manifest cpu_limit (percent) or the
				// sandbox default.
				if err := s.writeCPUMax(cgSubPath, env); err != nil {
					obs.Warn("cgroup cpu.max write failed, CPU bandwidth not capped",
						obs.Field{"error": err.Error(), "plugin": env.Manifest.Name})
				}
				s.cgroupPaths = append(s.cgroupPaths, cgSubPath)
				obs.Info("cgroup resource limit applied", obs.Field{
					"cgroup_path": cgSubPath,
					"memory_mb":   memoryMB,
				})
			}
		}
	}

	if !useCgroup && (memoryMB > 0 || env.Manifest.Resources.CPUSeconds > 0) {
		applyRLimits(cmd, env)
	} else if memoryMB <= 0 && env.Manifest.Resources.CPUSeconds <= 0 {
		obs.Info("no resource limits configured for plugin", obs.Field{
			"plugin": env.Manifest.Name,
		})
	}

	if env.Manifest.Resources.RequiresGPU {
		if err := s.enableGPU(env, cmd, caps); err != nil {
			return fmt.Errorf("gpu: %w", err)
		}
	}

	// Re-exec through the agent binary so NO_NEW_PRIVS (and, when enabled,
	// the seccomp allowlist filter) are applied to the plugin process before
	// it exec's. The clone flags / namespaces set above apply to this
	// re-exec process, and the filter is inherited by the final target.
	if err := s.maybeReexec(env, cmd, caps); err != nil {
		return fmt.Errorf("sandbox reexec: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start command: %w", err)
	}

	if cgSubPath != "" && cmd.Process != nil {
		pidStr := strconv.Itoa(cmd.Process.Pid)
		if err := os.WriteFile(cgSubPath+"/cgroup.procs", []byte(pidStr), 0644); err != nil {
			obs.Warn("failed to add process to cgroup, cleaning up",
				obs.Field{"error": err.Error(), "pid": cmd.Process.Pid})
			os.RemoveAll(cgSubPath)
		}
	}

	return cmd.Wait()
}

func (s *linuxSandbox) resolveMemoryLimit(env *SandboxEnv) int64 {
	memoryMB := env.Manifest.Resources.MemoryMB
	if memoryMB <= 0 {
		memoryMB = s.cfg.DefaultMemoryMB
	}
	if memoryMB > s.cfg.MaxMemoryMB {
		memoryMB = s.cfg.MaxMemoryMB
	}
	return memoryMB
}

// writeCPUMax writes the cpu.max bandwidth limit for a cgroup v2 subdir.
// Format: "<quota_us> <period_us>". Period is fixed at 100000us (100ms);
// quota is cpuLimit (percent, 1-100) * 1000us, or the sandbox default.
func (s *linuxSandbox) writeCPUMax(cgSubPath string, env *SandboxEnv) error {
	percent := env.Manifest.Resources.CPULimit
	if percent <= 0 {
		percent = float64(s.cfg.DefaultCPUPercent)
	}
	if percent <= 0 {
		percent = 80
	}
	if percent > 100 {
		percent = 100
	}
	quota := int64(percent * 1000)
	cpuMax := fmt.Sprintf("%d 100000", quota)
	if err := os.WriteFile(cgSubPath+"/cpu.max", []byte(cpuMax), 0644); err != nil {
		return err
	}
	obs.Info("cgroup cpu.max applied", obs.Field{
		"plugin":  env.Manifest.Name,
		"cpu_max": cpuMax,
	})
	return nil
}

// maybeReexec rewrites cmd to re-exec through the agent binary when
// NO_NEW_PRIVS and/or seccomp enforcement is enabled. The re-exec process
// applies PR_SET_NO_NEW_PRIVS (and the seccomp allowlist filter) in its own
// main() before exec'ing the original plugin command, so the filter is
// inherited by the plugin.
//
// The seccomp allowlist is x86_64-only: the syscall numbers in
// seccomp_linux.go are the x86_64 table, and ARM64 (e.g. Raspberry Pi) uses
// a completely different numbering. Applying the x86_64 filter on ARM64
// would SIGSYS-kill every plugin, so non-x86_64 builds automatically fall
// back to NO_NEW_PRIVS-only enforcement.
//
// Seccomp also requires kernel CONFIG_SECCOMP_FILTER (probed via
// caps.HasSeccomp). Kernels without it reject SECCOMP_SET_MODE_FILTER with
// EOPNOTSUPP, so the re-exec must not attempt --seccomp-exec; it falls back
// to NO_NEW_PRIVS-only, which is always available.
func (s *linuxSandbox) maybeReexec(env *SandboxEnv, cmd *exec.Cmd, caps PlatformCapabilities) error {
	seccompEnabled := env.Config.SeccompProfile != "" &&
		env.Config.SeccompProfile != "off" &&
		runtime.GOARCH == "amd64" &&
		caps.HasSeccomp
	if !seccompEnabled && !env.Config.SandboxNoNewPrivs {
		return nil
	}
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve agent executable: %w", err)
	}
	origPath := cmd.Path
	origArgs := make([]string, 0, len(cmd.Args)-1)
	if len(cmd.Args) > 0 {
		origArgs = append(origArgs, cmd.Args[1:]...)
	}
	mode := "--no-new-privs-exec"
	if seccompEnabled {
		mode = "--seccomp-exec"
	}
	obs.Info("sandbox re-exec through agent", obs.Field{
		"mode":   mode,
		"target": origPath,
		"plugin": env.Manifest.Name,
	})
	cmd.Path = self
	cmd.Args = append([]string{filepath.Base(self), mode, origPath}, origArgs...)
	return nil
}

func (s *linuxSandbox) enableGPU(env *SandboxEnv, cmd *exec.Cmd, caps PlatformCapabilities) error {
	if !caps.HasCgroupWrite || s.cfg.Cgroupsv2Path == "" {
		obs.Warn("GPU access requested but cgroup write not available, GPU isolation may be incomplete",
			obs.Field{"plugin": env.Manifest.Name})
		cmd.Env = append(cmd.Env,
			"NVIDIA_VISIBLE_DEVICES=all",
			"NVIDIA_DRIVER_CAPABILITIES=compute,utility",
		)
		return nil
	}

	nvidiaAllow := []byte("c 195:* rwm\n")
	if err := os.WriteFile(s.cfg.Cgroupsv2Path+"/device.allow", nvidiaAllow, 0644); err != nil {
		return fmt.Errorf("write device.allow: %w", err)
	}
	cmd.Env = append(cmd.Env,
		"NVIDIA_VISIBLE_DEVICES=all",
		"NVIDIA_DRIVER_CAPABILITIES=compute,utility",
	)
	return nil
}

func (s *linuxSandbox) Destroy(ctx context.Context, env *SandboxEnv) error {
	for _, cgPath := range s.cgroupPaths {
		if err := os.RemoveAll(cgPath); err != nil {
			obs.Warn("failed to remove cgroup path", obs.Field{
				"path":  cgPath,
				"error": err.Error(),
			})
		}
	}
	s.cgroupPaths = nil
	if env.Cleanup != nil {
		env.Cleanup()
	}
	return nil
}

func isUserNamespaceSupported() bool {
	return unix.Access("/proc/self/ns/user", unix.F_OK) == nil
}
