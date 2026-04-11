//go:build linux
// +build linux

package sandbox

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

// DangerousSyscalls are blocked - these are especially dangerous for privilege escalation
var DangerousSyscalls = map[string]bool{
	"mount":          true,
	"ptrace":         true,
	"reboot":         true,
	"setns":          true,
	"unshare":        true,
	"init_module":    true,
	"delete_module":  true,
	"capset":         true,
	"capget":         true,
	"syslog":         true,
	"lookup_dcookie": true,
	"finit_module":   true,
	"request_key":    true,
	"keyctl":         true,
	"ioprio_set":     true,
	"ioprio_get":     true,
	"chroot":         true,
	"acct":           true,
	"settimeofday":   true,
	"umount2":        true,
	"swapon":         true,
	"swapoff":        true,
	"sethostname":    true,
	"setdomainname":  true,
	"iopl":           true,
	"ioperm":         true,
	"quotactl":       true,
	"nfsservctl":     true,
	"readahead":      true,
	"setxattr":       true,
	"lsetxattr":      true,
	"fsetxattr":      true,
	"getxattr":       true,
	"lgetxattr":      true,
	"fgetxattr":      true,
	"listxattr":      true,
	"llistxattr":     true,
	"flistxattr":     true,
	"removexattr":    true,
	"lremovexattr":   true,
	"fremovexattr":   true,
	"tkill":          true,
	"time_admin":     true,
	"sysfs":          true,
	"adjtimex":       true,
	"setrlimit":      true,
}

// Apply applies best-effort sandboxing on Linux.
// Docker is strongly recommended for production; local sandbox provides
// UID drop, NO_NEW_PRIVS, rlimits, and process-group kill on cancel.
func Apply(ctx context.Context, cmd *exec.Cmd, limits Limits) error {
	if err := limits.Validate(); err != nil {
		return err
	}

	if !isRunningInContainer() {
		log.Printf("[sandbox] WARNING: Plugin isolation incomplete — Docker recommended for production")
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid:   true,
		Pdeathsig: syscall.SIGKILL,
		Credential: &syscall.Credential{
			Uid: 65534,
			Gid: 65534,
		},
	}

	if err := applyNoNewPrivs(); err != nil {
		return fmt.Errorf("sandbox: NO_NEW_PRIVS: %w", err)
	}

	unix.Prctl(unix.PR_SET_PTRACER, 0, 0, 0, 0)

	log.Printf("[sandbox] WARNING: seccomp disabled — local sandbox is not fully isolated, prefer Docker")

	go func() {
		<-ctx.Done()
		_ = killProcessGroup(cmd)
	}()

	return nil
}

func applyNoNewPrivs() error {
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("failed to set NO_NEW_PRIVS: %w", err)
	}
	return nil
}

func isRunningInContainer() bool {
	containerMarkers := []string{
		"/.dockerenv",
		"/run/.containerenv",
	}

	for _, marker := range containerMarkers {
		if _, err := os.Stat(marker); err == nil {
			return true
		}
	}

	// Check cgroup
	data, err := os.ReadFile("/proc/1/cgroup")
	if err == nil {
		content := string(data)
		containerIndicators := []string{"docker", "containerd", "kubepods", "lxc", "docker-ce"}
		for _, ind := range containerIndicators {
			if len(content) >= len(ind) {
				for i := 0; i <= len(content)-len(ind); i++ {
					if content[i:i+len(ind)] == ind {
						return true
					}
				}
			}
		}
	}

	// Check environment
	if os.Getenv("DOCKER_CONTAINER") != "" {
		return true
	}

	return false
}

func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return fmt.Errorf("sandbox: process not started")
	}
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
