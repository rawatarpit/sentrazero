//go:build windows
// +build windows

package sandbox

import (
	"context"
	"fmt"
	"os/exec"
)

// Apply refuses execution on Windows.
// Proper sandboxing requires Job Objects (future work).
func Apply(_ context.Context, _ *exec.Cmd, limits Limits) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	return fmt.Errorf("sandbox: Windows execution disabled until Job Objects are implemented")
}
