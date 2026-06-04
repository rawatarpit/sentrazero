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
	return "", errors.New("native plugin execution disabled (not built with CGO)")
}

func NativeRunnerFunc() plugin.NativeRunner {
	return nativeRunner
}

var _ = plugin.NativeRunner(nativeRunner)
