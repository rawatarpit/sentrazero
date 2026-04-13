package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"sentra-agent/internal/obs"
	"sentra-agent/internal/sandbox"
)

func PrepareJobWorkDir(jobID string) (string, func(), error) {
	workDir := filepath.Join("/tmp", fmt.Sprintf("sentra-%s", jobID))
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

// RunSandboxedPlugin executes a plugin under strict sandbox rules.
// Network is DISABLED by default and must be explicitly allowed in the manifest.
// Docker is REQUIRED for all script plugin execution.
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

	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf(
			"plugin %s requires Docker sandbox but Docker was not found on PATH",
			manifest.Name,
		)
	}

	return runDocker(dctx, pluginPath, plb, manifest, start)
}

// -----------------------------------------------------------------------------
// Docker sandbox (preferred, network isolated)
// -----------------------------------------------------------------------------

func runDocker(
	ctx context.Context,
	pluginPath string,
	input []byte,
	manifest Manifest,
	start time.Time,
) (*SandboxResult, error) {

	img := getRunnerImageForLanguage(manifest.Language)

	args := []string{
		"run", "--rm",
		"-v", fmt.Sprintf("%s:/mnt/plugin:ro", pluginPath),
		"-i",
	}

	// 🔒 Network isolation (default deny)
	if manifest.Network {
		obs.Info(
			"sandbox docker network enabled (explicit)",
			obs.Field{"network": true},
		)
	} else {
		args = append(args, "--network", "none")
	}

	// Resource limits
	if manifest.Resources.CPULimit > 0 {
		args = append(args, "--cpus", fmt.Sprintf("%.2f", manifest.Resources.CPULimit))
	}
	if manifest.Resources.MemoryMB > 0 {
		args = append(args, "--memory", fmt.Sprintf("%dM", manifest.Resources.MemoryMB))
	}

	runner := getRunnerForLanguage(manifest.Language)
	entrypoint := manifest.Filename
	if entrypoint == "" {
		entrypoint = "main.py"
	}
	args = append(args, img, "sh", "-c", fmt.Sprintf("%s /mnt/plugin/%s -", runner, entrypoint))

	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = bytes.NewReader(input)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	err := cmd.Run()
	exitCode := exitCodeFromError(err)
	duration := time.Since(start)

	if err != nil {
		obs.Error(
			"sandbox docker execution failed",
			obs.Field{
				"exit_code": exitCode,
				"error":     err.Error(),
			},
		)
	}

	return &SandboxResult{
		Output:     truncateOutput(out.String()),
		ExitCode:   exitCode,
		DurationMs: duration.Milliseconds(),
		Method:     "docker",
	}, err
}

// -----------------------------------------------------------------------------
// Local sandbox (NO NETWORK, strict limits)
// -----------------------------------------------------------------------------

func runLocal(
	ctx context.Context,
	pluginPath string,
	input []byte,
	manifest Manifest,
	start time.Time,
) (*SandboxResult, error) {

	if _, err := os.Stat(pluginPath); err != nil {
		return nil, fmt.Errorf("plugin not found: %s", pluginPath)
	}

	workDir, err := os.MkdirTemp("", "sentra-plugin-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(workDir)

	cmd := exec.CommandContext(ctx, pluginPath)
	cmd.Dir = workDir
	cmd.Stdin = bytes.NewReader(input)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	limits := sandbox.Limits{
		MaxMemoryMB:   manifest.Resources.MemoryMB,
		MaxCPUSeconds: manifest.Resources.CPUSeconds,
		Timeout:       time.Duration(manifest.Resources.TimeoutSeconds) * time.Second,
	}

	if err := sandbox.Apply(ctx, cmd, limits); err != nil {
		return nil, err
	}

	err = cmd.Run()
	exitCode := exitCodeFromError(err)
	duration := time.Since(start)

	if err != nil {
		obs.Error(
			"sandbox local execution failed",
			obs.Field{
				"exit_code": exitCode,
				"error":     err.Error(),
			},
		)
	}

	return &SandboxResult{
		Output:     truncateOutput(out.String()),
		ExitCode:   exitCode,
		DurationMs: duration.Milliseconds(),
		Method:     "local",
	}, err
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

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

func getRunnerImageForLanguage(language string) string {
	key := "SANDBOX_DOCKER_IMAGE_" + strings.ToUpper(language)
	if img := os.Getenv(key); img != "" {
		return img
	}

	if img := os.Getenv("SANDBOX_DOCKER_IMAGE"); img != "" {
		return img
	}

	switch language {
	case "python", "python2", "python3":
		return "sentra/python-runner:1.0.0"
	case "node", "nodejs", "javascript":
		return "sentra/node-runner:1.0.0"
	case "ruby":
		return "sentra/ruby-runner:1.0.0"
	case "bash", "shell":
		return "sentra/bash-runner:1.0.0"
	case "go":
		return "sentra/go-runner:1.0.0"
	default:
		return "sentra/python-runner:1.0.0"
	}
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
