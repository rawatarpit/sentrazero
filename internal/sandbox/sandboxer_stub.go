//go:build !linux && !darwin && !windows

package sandbox

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

type platformSandbox struct {
	cfg SandboxConfig
}

func newPlatformSandbox(cfg SandboxConfig) Sandboxer {
	return &platformSandbox{cfg: cfg}
}

func (s *platformSandbox) Prepare(ctx context.Context, jobID string, manifest PluginManifest, network bool) (*SandboxEnv, error) {
	workDir := filepath.Join(s.cfg.TempDir, "sentrazero", jobID)
	if err := os.MkdirAll(workDir, 0700); err != nil {
		return nil, fmt.Errorf("create work dir: %w", err)
	}
	return &SandboxEnv{
		WorkDir:  workDir,
		Config:   s.cfg,
		Manifest: manifest,
		Network:  network,
		Platform: runtime.GOOS,
		Cleanup:  func() { os.RemoveAll(workDir) },
	}, nil
}

func (s *platformSandbox) Execute(ctx context.Context, env *SandboxEnv, cmd *exec.Cmd) error {
	return cmd.Run()
}

func (s *platformSandbox) Destroy(ctx context.Context, env *SandboxEnv) error {
	if env.Cleanup != nil {
		env.Cleanup()
	}
	return nil
}
