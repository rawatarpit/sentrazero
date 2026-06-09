package system

import (
	"os"
	"runtime"
	"sync"
)

type ExecutionEnv struct {
	OS           string `json:"os"`
	Arch         string `json:"arch"`
	HasCGO       bool   `json:"has_cgo"`
	IsPrivileged bool   `json:"is_privileged"`
}

var (
	envOnce   sync.Once
	cachedEnv ExecutionEnv
)

func DetectExecutionEnv() ExecutionEnv {
	envOnce.Do(func() {
		cachedEnv = detect()
	})
	return cachedEnv
}

func detect() ExecutionEnv {
	env := ExecutionEnv{
		OS:           runtime.GOOS,
		Arch:         runtime.GOARCH,
		HasCGO:       isCGOEnabled(),
		IsPrivileged: os.Geteuid() == 0,
	}

	return env
}
