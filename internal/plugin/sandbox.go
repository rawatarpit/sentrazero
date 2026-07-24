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
	"sentra-agent/internal/sysinfo"
)

func EnsurePluginDependencies(ctx context.Context, manifest Manifest) error {
	if len(manifest.Dependencies) == 0 {
		return nil
	}
	if manifest.Language != "python" && manifest.Language != "python3" {
		return nil
	}

	runtimeDeps := make([]string, 0, len(manifest.Dependencies))
	for _, dep := range manifest.Dependencies {
		if dep.Source != "" && dep.Source != "python" {
			continue
		}
		name := dep.Name
		if name == "" {
			continue
		}

		show := exec.CommandContext(ctx, "python3", "-m", "pip", "show", name)
		if show.Run() == nil {
			continue
		}

		if dep.Version != "" {
			ver := strings.ReplaceAll(dep.Version, "≥", ">=")
			ver = strings.ReplaceAll(ver, "≤", "<=")
			ver = strings.ReplaceAll(ver, " ", "")
			// "*" or "latest" means any version — just use the package name
			if ver == "*" || ver == "latest" {
				runtimeDeps = append(runtimeDeps, name)
			} else if strings.HasPrefix(ver, ">=") || strings.HasPrefix(ver, "<=") ||
				strings.HasPrefix(ver, "==") || strings.HasPrefix(ver, "!=") ||
				strings.HasPrefix(ver, "~=") || strings.HasPrefix(ver, ">") ||
				strings.HasPrefix(ver, "<") {
				runtimeDeps = append(runtimeDeps, name+ver)
			} else {
				runtimeDeps = append(runtimeDeps, fmt.Sprintf("%s==%s", name, ver))
			}
		} else {
			runtimeDeps = append(runtimeDeps, name)
		}
	}

	if len(runtimeDeps) == 0 {
		return nil
	}

	obs.Info("installing plugin dependencies", obs.Field{
		"plugin": manifest.Name,
		"deps":   strings.Join(runtimeDeps, ", "),
	})

	dctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-dctx.Done():
				return dctx.Err()
			case <-time.After(time.Duration(attempt*2) * time.Second):
			}
		}

		args := append([]string{"-m", "pip", "install", "--no-cache-dir"}, runtimeDeps...)
		cmd := exec.CommandContext(dctx, "python3", args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			obs.Warn("pip install attempt failed", obs.Field{
				"plugin":  manifest.Name,
				"attempt": attempt + 1,
				"error":   err.Error(),
			})
			continue
		}

		obs.Info("plugin dependencies installed", obs.Field{
			"plugin": manifest.Name,
		})
		return nil
	}

	return fmt.Errorf("failed to install dependencies for %s after 3 retries", manifest.Name)
}

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

	if manifest.Resources.RequiresGPU {
		specs := sysinfo.Detect()
		if specs.GPUModel == "" {
			return nil, fmt.Errorf("plugin %s requires GPU but device has none", manifest.Name)
		}
		if manifest.Resources.GPUMemoryMB > 0 &&
			specs.GPUMemoryFreeGB*1024 < float64(manifest.Resources.GPUMemoryMB) {
			return nil, fmt.Errorf(
				"plugin %s requires %d MB GPU memory but only %.0f MB free",
				manifest.Name, manifest.Resources.GPUMemoryMB, specs.GPUMemoryFreeGB*1024,
			)
		}
	}

	if err := EnsurePluginDependencies(ctx, manifest); err != nil {
		obs.Warn("failed to ensure plugin dependencies", obs.Field{
			"plugin": manifest.Name,
			"error":  err.Error(),
		})
	}

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
			RequiresGPU:    manifest.Resources.RequiresGPU,
			GPUMemoryMB:    manifest.Resources.GPUMemoryMB,
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

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	var cmdErr error
	sbErr := sb.Execute(dctx, env, cmd)
	if sbErr != nil {
		cmdErr = sbErr
		log.Printf("[sandbox] execution failed: %v, stdout:\n%s\nstderr:\n%s", sbErr, truncateOutput(stdoutBuf.String()), truncateOutput(stderrBuf.String()))
	} else {
		cmdErr = nil
	}

	exitCode := exitCodeFromError(cmdErr)
	duration := time.Since(start)

	output := stdoutBuf.String()
	if output == "" {
		// If stdout is empty, use stderr as fallback (plugin may have redirected)
		output = stderrBuf.String()
	}

	// Truncate the output field for memory safety, but only if it's extremely large.
	// Plugin JSON output (e.g., 200+ images with metadata) can exceed the old 4KB limit.
	// Use a generous 10MB limit so large pipeline results are preserved.
	maxOutputLen := 10 * 1024 * 1024
	if len(output) > maxOutputLen {
		output = output[:maxOutputLen] + "... [truncated at 10MB]"
	}

	return &SandboxResult{
		Output:     output,
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
