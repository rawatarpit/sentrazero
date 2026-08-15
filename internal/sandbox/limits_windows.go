//go:build windows
// +build windows

package sandbox

import (
	"context"
	"fmt"
	"os/exec"
)

// Apply refuses execution on Windows.
// Resource limits on Windows are enforced via Job Objects in
// sandboxer_windows.go (memory + process limits, kill-on-close);
// this rlimit path is unsupported on Windows.
func Apply(_ context.Context, _ *exec.Cmd, limits Limits) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	return fmt.Errorf("sandbox: rlimits are unsupported on Windows; Job Objects in sandboxer_windows.go enforce limits instead")
}
