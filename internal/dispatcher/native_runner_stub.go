//go:build (linux || darwin) && !cgo
// +build linux darwin
// +build !cgo

package dispatcher

import (
	"context"
	"errors"

	"sentra-agent/internal/plugin"
)

func nativeRunner(
	ctx context.Context,
	pluginPath string,
	checksum string,
	inputJSON string,
) (string, error) {
	return "", errors.New("native plugin execution requires CGO (build with CGO_ENABLED=1)")
}

func NativeRunnerFunc() plugin.NativeRunner {
	return nativeRunner
}

var _ = plugin.NativeRunner(nativeRunner)
