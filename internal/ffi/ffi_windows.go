//go:build windows
// +build windows

package ffi

import "fmt"

func RunStage(pluginName, input string) (string, error) {
	return "", fmt.Errorf("FFI plugins are not supported on Windows")
}
