package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"sentra-agent/internal/obs"
	"sentra-agent/internal/sandbox"
)

func PrepareJobWorkDir(jobID string) (string, func(), error) {
	workDir := filepath.Join(sandbox.LoadConfig().TempDir, fmt.Sprintf("sentra-%s", jobID))
	if err := os.MkdirAll(workDir, 0700); err != nil {
		return "", nil, fmt.Errorf("failed to create work dir: %w", err)
	}
	cleanup := func() {
		os.RemoveAll(workDir)
	}
	return workDir, cleanup, nil
}

type SandboxResult struct {
	Output     string `json:"output"`
	ExitCode   int    `json:"exit_code"`
	DurationMs int64  `json:"duration_ms"`
	Method     string `json:"method"`
}

func RunSandboxedPlugin(
	ctx context.Context,
	pluginPath string,
	payload map[string]interface{},
	manifest Manifest,
) (*SandboxResult, error) {
	start := time.Now()
	plb, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	timeout := 5 * time.Minute
	if manifest.Resources.TimeoutSeconds > 0 {
		timeout = time.Duration(manifest.Resources.TimeoutSeconds) * time.Second
	}

	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	hasResourceLimits := manifest.Resources.MemoryMB > 0 ||
		manifest.Resources.CPUSeconds > 0 ||
		manifest.Resources.CPULimit > 0

	obs.Info(
		"sandbox start",
		obs.Field{
			"plugin_path":         pluginPath,
			"timeout_s":           timeout.Seconds(),
			"network":             manifest.Network,
			"has_resource_limits": hasResourceLimits,
		},
	)

	cfg := sandbox.LoadConfig()
	sb := sandbox.New(cfg)

	pm := sandbox.PluginManifest{
		Name:     manifest.Name,
		Language: manifest.Language,
		Network:  manifest.Network,
		Resources: sandbox.PluginResources{
			MemoryMB:       manifest.Resources.MemoryMB,
			CPUSeconds:     manifest.Resources.CPUSeconds,
			CPULimit:       manifest.Resources.CPULimit,
			TimeoutSeconds: manifest.Resources.TimeoutSeconds,
		},
	}

	env, err := sb.Prepare(dctx, manifest.Name+"-"+fmt.Sprintf("%d", start.UnixNano()), pm, manifest.Network)
	if err != nil {
		return nil, fmt.Errorf("sandbox prepare: %w", err)
	}
	defer sb.Destroy(dctx, env)

	runner := getRunnerForLanguage(manifest.Language)
	entrypoint := manifest.Filename
	if entrypoint == "" {
		entrypoint = "main.py"
	}
	scriptPath := filepath.Join(env.WorkDir, entrypoint)
	if err := os.Symlink(pluginPath, scriptPath); err != nil {
		if err := os.RemoveAll(scriptPath); err != nil {
		}
		if err := os.Rename(pluginPath, scriptPath); err != nil {
			return nil, fmt.Errorf("setup plugin: %w", err)
		}
	}

	cmd := exec.CommandContext(dctx, runner, scriptPath)
	cmd.Dir = env.WorkDir
	cmd.Stdin = bytes.NewReader(plb)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	var cmdErr error
	sbErr := sb.Execute(dctx, env, cmd)
	if sbErr != nil {
		cmdErr = sbErr
		log.Printf("[sandbox] execution failed: %v, output:\n%s", sbErr, out.String())
	} else {
		cmdErr = nil
	}

	exitCode := exitCodeFromError(cmdErr)
	duration := time.Since(start)

	return &SandboxResult{
		Output:     truncateOutput(out.String()),
		ExitCode:   exitCode,
		DurationMs: duration.Milliseconds(),
		Method:     "native_sandbox",
	}, cmdErr
}

func exitCodeFromError(err error) int {
	if err == nil {
		return 0
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return exitErr.ExitCode()
	}
	return -1
}

func truncateOutput(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 4096 {
		return s[:4096] + "... [truncated]"
	}
	return s
}

func getRunnerForLanguage(language string) string {
	switch language {
	case "python", "python3":
		return "python3"
	case "python2":
		return "python2"
	case "node", "nodejs", "javascript":
		return "node"
	case "ruby":
		return "ruby"
	case "bash", "shell":
		return "bash"
	case "go":
		return "go run"
	default:
		return "python3"
	}
}
