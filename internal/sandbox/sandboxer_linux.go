//go:build linux

package sandbox

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"sentra-agent/internal/obs"
	"golang.org/x/sys/unix"
)

func newPlatformSandbox(cfg SandboxConfig) Sandboxer {
	return &linuxSandbox{cfg: cfg}
}

type linuxSandbox struct {
	cfg          SandboxConfig
	cgroupPaths  []string
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
				s.cgroupPaths = append(s.cgroupPaths, cgSubPath)
				obs.Info("cgroup resource limit applied", obs.Field{
					"cgroup_path": cgSubPath,
					"memory_mb":   memoryMB,
				})
			}
		}
	}

	if !useCgroup && memoryMB > 0 {
		applyRLimits(cmd, env)
	} else if memoryMB <= 0 {
		obs.Info("no memory limit configured for plugin", obs.Field{
			"plugin": env.Manifest.Name,
		})
	}

	if env.Manifest.Resources.RequiresGPU {
		if err := s.enableGPU(env, cmd, caps); err != nil {
			return fmt.Errorf("gpu: %w", err)
		}
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

func detectSeccompAvailable() bool {
	files, err := os.ReadDir("/proc/sys/kernel/seccomp")
	if err != nil || len(files) == 0 {
		data, err := os.ReadFile("/proc/sys/kernel/seccomp/actions_avail")
		if err != nil {
			return false
		}
		return len(strings.TrimSpace(string(data))) > 0
	}
	return false
}
