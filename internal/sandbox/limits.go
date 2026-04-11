package sandbox

import (
	"fmt"
	"time"
)

// Limits defines execution constraints for a plugin.
// All fields are mandatory.
type Limits struct {
	MaxMemoryMB   int64
	MaxCPUSeconds int64
	Timeout       time.Duration
}

// Validate enforces default-deny semantics.
func (l Limits) Validate() error {
	if l.MaxMemoryMB <= 0 {
		return fmt.Errorf("sandbox: MaxMemoryMB must be > 0")
	}
	if l.MaxCPUSeconds <= 0 {
		return fmt.Errorf("sandbox: MaxCPUSeconds must be > 0")
	}
	if l.Timeout <= 0 {
		return fmt.Errorf("sandbox: Timeout must be > 0")
	}
	return nil
}
