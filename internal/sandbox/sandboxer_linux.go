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
	"syscall"

	"golang.org/x/sys/unix"
)

func newPlatformSandbox(cfg SandboxConfig) Sandboxer {
	return &linuxSandbox{cfg: cfg}
}

type linuxSandbox struct {
	cfg SandboxConfig
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

func (s *linuxSandbox) Execute(ctx context.Context, env *SandboxEnv, cmd *exec.Cmd) error {
	if !s.cfg.LinuxNamespaces {
		return cmd.Run()
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWNS |
			syscall.CLONE_NEWUTS |
			syscall.CLONE_NEWIPC |
			syscall.CLONE_NEWPID |
			syscall.CLONE_NEWNET |
			syscall.CLONE_NEWUSER,
		UidMappings: []syscall.SysProcIDMap{
			{ContainerID: 0, HostID: os.Getuid(), Size: 1},
		},
		GidMappings: []syscall.SysProcIDMap{
			{ContainerID: 0, HostID: os.Getgid(), Size: 1},
		},
	}

	memoryMB := env.Manifest.Resources.MemoryMB
	if memoryMB <= 0 {
		memoryMB = s.cfg.DefaultMemoryMB
	}
	if memoryMB > s.cfg.MaxMemoryMB {
		memoryMB = s.cfg.MaxMemoryMB
	}

	if memoryMB > 0 && s.cfg.Cgroupsv2Path != "" {
		if err := s.applyCgroup(cmd, memoryMB); err != nil {
			return fmt.Errorf("cgroup: %w", err)
		}
	}

	return cmd.Run()
}

func (s *linuxSandbox) Destroy(ctx context.Context, env *SandboxEnv) error {
	if env.Cleanup != nil {
		env.Cleanup()
	}
	return nil
}

func (s *linuxSandbox) applyCgroup(cmd *exec.Cmd, memoryMB int64) error {
	cgPath := s.cfg.Cgroupsv2Path + "/sentrazero"
	if err := os.MkdirAll(cgPath, 0755); err != nil {
		return fmt.Errorf("mkdir cgroup: %w", err)
	}

	memLimit := strconv.FormatInt(memoryMB*1024*1024, 10)
	if err := os.WriteFile(cgPath+"/memory.max", []byte(memLimit), 0644); err != nil {
		return fmt.Errorf("write memory.max: %w", err)
	}

	if err := os.WriteFile(cgPath+"/cgroup.procs", []byte(strconv.Itoa(cmd.Process.Pid)), 0644); err != nil {
		return fmt.Errorf("write cgroup.procs: %w", err)
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
