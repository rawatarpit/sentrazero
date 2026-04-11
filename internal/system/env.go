package system

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"time"
)

type ExecutionEnv struct {
	OS           string `json:"os"`
	Arch         string `json:"arch"`
	HasCGO       bool   `json:"has_cgo"`
	HasDocker    bool   `json:"has_docker"`
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
		OS:     runtime.GOOS,
		Arch:   runtime.GOARCH,
		HasCGO: isCGOEnabled(),
	}

	env.IsPrivileged = os.Geteuid() == 0 || isInContainer()
	env.HasDocker = dockerAvailableWithValidation()

	return env
}

func isInContainer() bool {
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	}
	if _, err := os.Stat("/run/.containerenv"); err == nil {
		return true
	}
	data, err := os.ReadFile("/proc/1/cgroup")
	if err != nil {
		return false
	}
	content := string(data)
	indicators := []string{"docker", "containerd", "kubepods", "lxc", "docker-ce"}
	for _, ind := range indicators {
		for i := 0; i <= len(content)-len(ind); i++ {
			if content[i:i+len(ind)] == ind {
				return true
			}
		}
	}
	return false
}

func dockerAvailableWithValidation() bool {
	if _, err := exec.LookPath("docker"); err != nil {
		return false
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "run", "--rm", "hello-world").Run(); err != nil {
		return false
	}
	return true
}

func IsDockerAvailable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "info").Run(); err != nil {
		return false
	}
	return true
}
