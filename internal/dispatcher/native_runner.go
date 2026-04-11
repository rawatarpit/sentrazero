//go:build (linux || darwin) && cgo
// +build linux darwin
// +build cgo

package dispatcher

import (
	"context"
	"fmt"

	"sentra-agent/internal/ffi"
	"sentra-agent/internal/plugin"
)

func nativeRunner(
	ctx context.Context,
	pluginPath string,
	checksum string,
	inputJSON string,
) (string, error) {
	handle, runSym, freeSym, err := openAndGetSymbols(pluginPath, checksum)
	if err != nil {
		return "", fmt.Errorf("native runner: open symbols: %w", err)
	}
	defer closePluginHandle(handle)

	out, err := callPlugin(ctx, runSym, freeSym, inputJSON)
	if err != nil {
		return "", fmt.Errorf("native runner: call plugin: %w", err)
	}

	return out, nil
}

func NativeRunnerFunc() plugin.NativeRunner {
	return nativeRunner
}

var _ = ffi.DLOpen
var _ = ffi.DLSym
var _ = ffi.DLClose
