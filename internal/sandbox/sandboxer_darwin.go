//go:build darwin

package sandbox

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

func newPlatformSandbox(cfg SandboxConfig) Sandboxer {
	return &macSandbox{cfg: cfg}
}

type macSandbox struct {
	cfg SandboxConfig
}

func (s *macSandbox) Prepare(ctx context.Context, jobID string, manifest PluginManifest, network bool) (*SandboxEnv, error) {
	workDir := filepath.Join(s.cfg.TempDir, "sentrazero", jobID)
	if err := os.MkdirAll(workDir, 0700); err != nil {
		return nil, fmt.Errorf("create work dir: %w", err)
	}
	// Resolve symlinks so Seatbelt subpath matching works correctly
	// (e.g., /var -> /private/var on macOS)
	if resolved, err := filepath.EvalSymlinks(workDir); err == nil {
		workDir = resolved
	}

	return &SandboxEnv{
		WorkDir:  workDir,
		Config:   s.cfg,
		Manifest: manifest,
		Network:  network,
		Platform: "darwin",
		Cleanup:  func() { os.RemoveAll(workDir) },
	}, nil
}

func (s *macSandbox) Execute(ctx context.Context, env *SandboxEnv, cmd *exec.Cmd) error {
	if !s.cfg.MacOSSeatbelt {
		return cmd.Run()
	}

	memoryMB := env.Manifest.Resources.MemoryMB
	if memoryMB <= 0 {
		memoryMB = s.cfg.DefaultMemoryMB
	}
	if memoryMB > s.cfg.MaxMemoryMB {
		memoryMB = s.cfg.MaxMemoryMB
	}

	netPolicy := macSandboxNetworkAllow()
	if !env.Network {
		netPolicy = macSandboxNetworkBlock()
	}
	sbProfile := fmt.Sprintf(macSandboxProfileTpl, netPolicy, env.WorkDir)
	sbPath := filepath.Join(env.WorkDir, ".sandbox.sb")
	if err := os.WriteFile(sbPath, []byte(sbProfile), 0600); err != nil {
		return fmt.Errorf("write sandbox profile: %w", err)
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{}
	cmd.Env = append(cmd.Env, "SANDBOX_PROFILE="+sbPath)

	if err := s.applySandbox(cmd, sbPath); err != nil {
		return fmt.Errorf("seatbelt: %w", err)
	}

	return cmd.Run()
}

func (s *macSandbox) Destroy(ctx context.Context, env *SandboxEnv) error {
	if env.Cleanup != nil {
		env.Cleanup()
	}
	return nil
}

func (s *macSandbox) applySandbox(cmd *exec.Cmd, profile string) error {
	cmd.Args = append([]string{"sandbox-exec", "-f", profile, cmd.Path}, cmd.Args[1:]...)
	cmd.Path = "/usr/bin/sandbox-exec"
	return nil
}

const macSandboxProfileTpl = `(version 1)

(deny default)

; Allow reading everything (required for macOS 15+ firmlink compatibility)
(allow file-read* (subpath "/"))
(allow file-read-metadata)

; Allow process execution
(allow process-exec*)
(allow process-fork)

; Allow POSIX IPC
(allow sysctl-read)
(allow mach-lookup)
(allow ipc-posix*)

; Signal handling
(allow signal)

; Network policy
%s

; Deny writes to system paths (read-only enforcement)
(deny file-write* (subpath "/System"))
(deny file-write* (subpath "/usr"))
(deny file-write* (subpath "/bin"))
(deny file-write* (subpath "/sbin"))
(deny file-write* (subpath "/private/etc"))
(deny file-write* (subpath "/private/var"))
(deny file-write* (subpath "/etc"))
(deny file-write* (subpath "/var"))

; Allow writes to temp directories
(allow file-write* (subpath "/private/tmp"))
(allow file-write* (subpath "/tmp"))

; Allow writes to Users home directories (needed for .sentra cache etc)
(allow file-write* (subpath "/Users"))

; Allow read-write in work directory (MUST be last: overrides preceding deny rules)
(allow file-read* file-write* (subpath "%s"))
`

func macSandboxNetworkBlock() string {
	return `(deny network*)
(deny system-socket)`
}

func macSandboxNetworkAllow() string {
	return `(allow network*)
(allow system-socket)`
}

func macSandboxMemoryLimit(memoryMB int64) string {
	return "" // memory limits not supported in macOS 15+ sandbox profiles
}
