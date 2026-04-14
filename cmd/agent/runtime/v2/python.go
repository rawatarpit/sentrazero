package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"sentra-agent/internal/obs"
)

type PythonRuntime struct {
	home       string
	venvPath   string
	executable string
	version    string
	installed  bool
	platform   PlatformInfo
}

func NewPythonRuntime(workingDir string) *PythonRuntime {
	return &PythonRuntime{
		home:     workingDir,
		platform: GetCurrentPlatform(),
	}
}

func (pr *PythonRuntime) getPythonCommand() string {
	switch runtime.GOOS {
	case "windows":
		if pr.platform.Python != "" {
			return pr.platform.Python
		}
		return "python"
	case "darwin":
		return "python3"
	default:
		return "python3"
	}
}

func (pr *PythonRuntime) getVenvPython() string {
	switch runtime.GOOS {
	case "windows":
		return filepath.Join(pr.venvPath, "Scripts", "python.exe")
	default:
		return filepath.Join(pr.venvPath, "bin", "python")
	}
}

func (pr *PythonRuntime) Type() RuntimeType {
	return RuntimePython
}

func (pr *PythonRuntime) Setup(ctx context.Context, spec RuntimeSpec, envPath string) error {
	if err := os.MkdirAll(envPath, 0755); err != nil {
		return fmt.Errorf("failed to create runtime directory: %w", err)
	}

	pr.home = envPath
	pr.venvPath = filepath.Join(envPath, "venv")

	pythonCmd := pr.getPythonCommand()

	cmd := exec.CommandContext(ctx, pythonCmd, "--version")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("python not available: %w", err)
	}
	pr.version = strings.TrimSpace(out.String())

	if err := pr.createVirtualEnv(ctx, pythonCmd); err != nil {
		return fmt.Errorf("failed to create virtual environment: %w", err)
	}

	pr.executable = pr.getVenvPython()
	pr.installed = true

	return nil
}

func (pr *PythonRuntime) createVirtualEnv(ctx context.Context, pythonCmd string) error {
	if _, err := os.Stat(pr.venvPath); err == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, pythonCmd, "-m", "venv", pr.venvPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("venv creation failed: %w", err)
	}

	return nil
}

func (pr *PythonRuntime) InstallDeps(ctx context.Context, deps []Dependency, envPath string) error {
	if !pr.installed {
		return fmt.Errorf("runtime not set up")
	}

	if len(deps) == 0 {
		readyFile := filepath.Join(envPath, ".ready")
		return os.WriteFile(readyFile, []byte("ok"), 0644)
	}

	pythonCmd := pr.getVenvPython()

	pipDeps := make([]string, len(deps))
	for i, dep := range deps {
		if dep.Version != "" {
			pipDeps[i] = fmt.Sprintf("%s==%s", dep.Name, dep.Version)
		} else {
			pipDeps[i] = dep.Name
		}
	}

	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	if err := pr.upgradePip(ctx, pythonCmd); err != nil {
		obs.Warn("pip upgrade failed (non-fatal)", obs.Field{"error": err.Error()})
	}

	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt*2) * time.Second):
			}
		}

		args := []string{pythonCmd, "-m", "pip", "install", "--no-cache-dir"}
		args = append(args, pipDeps...)
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Dir = envPath

		if err := cmd.Run(); err != nil {
			lastErr = fmt.Errorf("failed to install dependencies: %w", err)
			obs.Warn("pip install attempt failed", obs.Field{
				"attempt":     attempt + 1,
				"max_retries": maxRetries,
				"error":       err.Error(),
			})
			continue
		}

		readyFile := filepath.Join(envPath, ".ready")
		if err := os.WriteFile(readyFile, []byte("ok"), 0644); err != nil {
			return fmt.Errorf("failed to write ready marker: %w", err)
		}
		return nil
	}

	return fmt.Errorf("dependency installation failed after %d retries: %w", maxRetries, lastErr)
}

func (pr *PythonRuntime) upgradePip(ctx context.Context, pythonCmd string) error {
	cmd := exec.CommandContext(ctx, pythonCmd, "-m", "pip", "install", "--upgrade", "pip")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (pr *PythonRuntime) Run(ctx context.Context, input ExecutionInput, envPath string) (*ExecutionOutput, *ExecutionMetrics, error) {
	metrics := &ExecutionMetrics{}

	if !pr.installed {
		return &ExecutionOutput{
			Success: false,
			Error:   "runtime not initialized",
		}, metrics, fmt.Errorf("runtime not initialized")
	}

	if input.PluginCode != "" {
		pluginFile := filepath.Join(envPath, "plugin.py")
		if err := os.WriteFile(pluginFile, []byte(input.PluginCode), 0644); err != nil {
			return &ExecutionOutput{
				Success: false,
				Error:   fmt.Sprintf("failed to write plugin: %v", err),
			}, metrics, err
		}
	}

	payload := map[string]interface{}{
		"input":    input.Input,
		"config":   input.Config,
		"metadata": input.Metadata,
	}

	inputFile := filepath.Join(envPath, "input.json")
	if err := writeJSONFile(inputFile, payload); err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("failed to write input: %v", err),
		}, metrics, err
	}

	outputFile := filepath.Join(envPath, "output.json")

	script := `
import json
import sys
import time
import traceback

start_time = time.time()

try:
    with open("input.json", "r") as f:
        data = json.load(f)
    
    input_data = data.get("input", {})
    config = data.get("config", {})
    metadata = data.get("metadata", {})
    
    result = {
        "success": True,
        "data": {},
        "error": None,
        "items_processed": 0,
        "duration_ms": 0
    }
    
    exec_globals = {
        "__name__": "__sentra_plugin__",
        "input": input_data,
        "config": config,
        "metadata": metadata,
    }
    
    with open("plugin.py", "r") as f:
        plugin_code = f.read()
    
    exec(plugin_code, exec_globals)
    
    if "main" in exec_globals:
        output = exec_globals["main"](input_data, config, metadata)
        if isinstance(output, dict):
            result["data"] = output
            result["items_processed"] = output.get("items_processed", 0)
        else:
            result["data"] = {"result": output}
    
    result["duration_ms"] = int((time.time() - start_time) * 1000)
    
    with open("output.json", "w") as f:
        json.dump(result, f)
        
except Exception as e:
    error_result = {
        "success": False,
        "data": {},
        "error": f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}",
        "items_processed": 0,
        "duration_ms": int((time.time() - start_time) * 1000)
    }
    with open("output.json", "w") as f:
        json.dump(error_result, f)
    sys.exit(1)
`

	scriptFile := filepath.Join(envPath, "runner.py")
	if err := os.WriteFile(scriptFile, []byte(script), 0644); err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("failed to write runner: %v", err),
		}, metrics, err
	}

	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	pythonCmd := pr.getVenvPython()
	cmd := exec.CommandContext(ctx, pythonCmd, scriptFile)
	cmd.Dir = envPath
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		metrics.ExitCode = cmd.ProcessState.ExitCode()
		if _, err := os.Stat(outputFile); err == nil {
			var output ExecutionOutput
			if data, err := os.ReadFile(outputFile); err == nil {
				if err := json.Unmarshal(data, &output); err == nil {
					return &output, metrics, nil
				}
			}
		}
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("execution failed: %v", err),
		}, metrics, err
	}

	data, err := os.ReadFile(outputFile)
	if err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("failed to read output: %v", err),
		}, metrics, err
	}

	var output ExecutionOutput
	if err := json.Unmarshal(data, &output); err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("invalid output format: %v", err),
		}, metrics, err
	}

	if output.Data == nil {
		output.Data = make(map[string]interface{})
	}

	if output.ItemsProcessed > 0 {
		output.Data["items_processed"] = output.ItemsProcessed
	}
	if output.DurationMs > 0 {
		output.Data["duration_ms"] = output.DurationMs
	}

	return &output, metrics, nil
}

func (pr *PythonRuntime) Cleanup(ctx context.Context, envPath string) error {
	if envPath == "" {
		return nil
	}
	tempFiles := []string{"runner.py", "plugin.py", "input.json", "output.json"}
	for _, f := range tempFiles {
		os.Remove(filepath.Join(envPath, f))
	}
	return nil
}

func (pr *PythonRuntime) IsInstalled(ctx context.Context, envPath string) bool {
	if !pr.installed {
		return false
	}
	pythonPath := pr.getVenvPython()
	if _, err := os.Stat(pythonPath); err != nil {
		return false
	}
	return true
}

func (pr *PythonRuntime) Version(ctx context.Context) (string, error) {
	if pr.version != "" {
		return pr.version, nil
	}

	cmd := exec.CommandContext(ctx, pr.getPythonCommand(), "--version")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}
	pr.version = strings.TrimSpace(out.String())
	return pr.version, nil
}

func writeJSONFile(path string, data interface{}) error {
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, content, 0644)
}
