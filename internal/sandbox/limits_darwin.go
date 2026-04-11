//go:build darwin
// +build darwin

package sandbox

import (
	"context"
	"fmt"
	"os/exec"
	"syscall"
)

// Apply applies sandboxing on macOS.
// Parity with Linux:
// - Process group isolation (Setpgid)
// - SIGKILL on timeout via context cancellation
// NOTE: macOS does not support Pdeathsig (parent-death signal).
// We rely on the context cancellation goroutine to kill the process group.
// Child cleanup is enforced via:
// - Process group termination (kill -PGID)
// - Context timeout enforcement
func Apply(ctx context.Context, cmd *exec.Cmd, limits Limits) error {
	if err := limits.Validate(); err != nil {
		return err
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	go func() {
		<-ctx.Done()
		_ = killProcessGroup(cmd)
	}()

	return nil
}

func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return fmt.Errorf("sandbox: process not started")
	}
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
