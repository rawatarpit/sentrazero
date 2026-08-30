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

	if memoryMB > 0 {
		applyRLimits(cmd, env)
	}

	netPolicy := macSandboxNetworkAllow()
	if !env.Network {
		netPolicy = macSandboxNetworkBlock()
	}
	home, _ := os.UserHomeDir()
	sbProfile := fmt.Sprintf(macSandboxProfileTpl, netPolicy, macSandboxHomeDeny(home), env.WorkDir)
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

; Allow GPU device access (Metal, IOAccelerator)
(allow device*)
(allow iokit-open (require-all
    (iokit-registry-entry-class "IOAccelerator")
    (iokit-property "MetalPluginName" "com.apple.Metal")))

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

; Allow /dev/null (required by Python runtime for subprocess etc.)
(allow file-read* file-write* (subpath "/dev"))
(allow file-read* file-write* (subpath "/dev/null"))

; Deny writes to system paths (read-only enforcement)
(deny file-write* (subpath "/System"))
(deny file-write* (subpath "/usr"))
(deny file-write* (subpath "/bin"))
(deny file-write* (subpath "/sbin"))
(deny file-write* (subpath "/private/etc"))
(deny file-write* (subpath "/private/var/db"))
(deny file-write* (subpath "/private/var/log"))
(deny file-write* (subpath "/private/var/run"))
(deny file-write* (subpath "/private/var/root"))
(deny file-write* (subpath "/private/var/select"))
(deny file-write* (subpath "/etc"))
(deny file-write* (subpath "/var"))

; Allow writes to temp directories (both /tmp and /private/var/folders for macOS temp)
(allow file-write* (subpath "/private/tmp"))
(allow file-write* (subpath "/tmp"))
(allow file-write* (subpath "/private/var/folders"))
(allow file-write* (subpath "/var/folders"))

; Allow writes to Users home directories (needed for .sentra cache etc)
(allow file-write* (subpath "/Users"))

; Home-level privacy denies (credentials + app-private data that script
; runtimes never legitimately touch). Injected by macSandboxHomeDeny when
; os.UserHomeDir() resolves; empty when it does not.
%s
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

// macSandboxHomeDeny returns Seatbelt deny rules for the user's credentials
// and app-private data. The baseline profile allows reading everything under
// "/" (required for macOS 15+ firmlink compatibility) and writing under
// "/Users" (required for ~/.sentra), so without these rules a compromised
// plugin could exfiltrate the user's Keychain database, browser cookies and
// mail. Script runtimes do not read these paths, so the denies are safe.
//
// The deny block is injected before the work-dir allow (which must remain the
// LAST rule in the profile so Seatbelt's last-match-wins semantics keep the
// work dir writable even if it ever lands under one of these paths).
func macSandboxHomeDeny(home string) string {
	if home == "" {
		return "; (no home dir resolved - home privacy denies skipped)"
	}
	return fmt.Sprintf(`; Deny credentials / app-private data under the resolved home dir
(deny file-read* file-write* (subpath "%s/Library/Keychains"))
(deny file-read* file-write* (subpath "%s/Library/HTTPStorages"))
(deny file-read* (subpath "%s/Library/Cookies"))
(deny file-read* (subpath "%s/Library/WebKit"))
(deny file-read* (subpath "%s/Library/Mail"))
(deny file-read* (subpath "%s/Library/Safari"))`,
		home, home, home, home, home, home)
}

func macSandboxMemoryLimit(memoryMB int64) string {
	return "" // memory limits not supported in macOS 15+ sandbox profiles
}
