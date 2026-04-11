package models

import "errors"

// FailureType defines the semantic class of a job failure.
// These are MACHINE-ACTIONABLE signals.
type FailureType string

const (
	// Temporary issues that MAY succeed on retry
	FailureTransient FailureType = "TRANSIENT"

	// Deterministic logic or data errors — retrying is useless
	FailureDeterministic FailureType = "DETERMINISTIC"

	// Job exceeded its execution deadline
	FailureTimeout FailureType = "TIMEOUT"

	// Sandbox, permission, or security boundary violation
	FailureSandboxViolation FailureType = "SANDBOX_VIOLATION"
)

// Failure wraps an error with an explicit failure type.
type Failure struct {
	Type    FailureType
	Message string
	Err     error
}

// Error implements the error interface.
func (f *Failure) Error() string {
	if f.Err != nil {
		return f.Message + ": " + f.Err.Error()
	}
	return f.Message
}

// Unwrap allows errors.Is / errors.As to work.
func (f *Failure) Unwrap() error {
	return f.Err
}

// NewFailure creates a typed failure.
func NewFailure(
	t FailureType,
	message string,
	err error,
) *Failure {
	return &Failure{
		Type:    t,
		Message: message,
		Err:     err,
	}
}

// -----------------------------------------------------------------------------
// Classification helpers
// -----------------------------------------------------------------------------

// IsFailure checks whether an error is a typed Failure.
func IsFailure(err error) bool {
	var f *Failure
	return errors.As(err, &f)
}

// GetFailure extracts a Failure from an error.
// Returns nil if not present.
func GetFailure(err error) *Failure {
	var f *Failure
	if errors.As(err, &f) {
		return f
	}
	return nil
}

// ClassifyFailure returns a failure type for any error.
// Safe default = DETERMINISTIC.
func ClassifyFailure(err error) FailureType {
	if err == nil {
		return ""
	}

	var f *Failure
	if errors.As(err, &f) {
		return f.Type
	}

	// Default-deny: unknown errors are deterministic
	return FailureDeterministic
}
