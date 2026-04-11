//go:build (linux || darwin) && !cgo
// +build linux darwin
// +build !cgo

package dispatcher

import (
	"context"
	"errors"
	"unsafe"
)

// openAndGetSymbols is a stub implementation used when CGO is disabled.
// It exists to preserve API parity with the CGO-enabled version.
func openAndGetSymbols(
	pluginPath string,
	expectedSHA string,
) (unsafe.Pointer, unsafe.Pointer, unsafe.Pointer, error) {

	// Explicitly mark unused parameters (intentional for nocgo builds)
	_ = pluginPath
	_ = expectedSHA

	return nil, nil, nil, errors.New(
		"native plugins require CGO (CGO_ENABLED=1)",
	)
}

// callPlugin is a stub that always fails when CGO is disabled.
func callPlugin(
	ctx context.Context,
	runSym unsafe.Pointer,
	freeSym unsafe.Pointer,
	input string,
) (string, error) {

	// Explicitly mark unused parameters (intentional for nocgo builds)
	_ = ctx
	_ = runSym
	_ = freeSym
	_ = input

	return "", errors.New(
		"native plugins require CGO (CGO_ENABLED=1)",
	)
}

// closePluginHandle is a no-op for nocgo builds.
func closePluginHandle(handle unsafe.Pointer) {
	_ = handle
	// intentionally no-op
}
