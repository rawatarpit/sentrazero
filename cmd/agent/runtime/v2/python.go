package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	cfgPath := filepath.Join(pr.venvPath, "pyvenv.cfg")

	if _, err := os.Stat(cfgPath); err == nil {
		data, err := os.ReadFile(cfgPath)
		if err == nil && bytes.Contains(data, []byte("include-system-site-packages = true")) {
			return nil
		}
		os.RemoveAll(pr.venvPath)
	}

	if err := os.MkdirAll(pr.venvPath, 0755); err != nil {
		return fmt.Errorf("failed to create venv directory: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, pythonCmd, "-m", "venv", "--system-site-packages", "--without-pip", pr.venvPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		os.RemoveAll(pr.venvPath)
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
			ver := dep.Version
			ver = strings.ReplaceAll(ver, "≥", ">=")
			ver = strings.ReplaceAll(ver, "≤", "<=")
			ver = strings.ReplaceAll(ver, " ", "")
			if strings.HasPrefix(ver, ">=") || strings.HasPrefix(ver, "<=") ||
				strings.HasPrefix(ver, "==") || strings.HasPrefix(ver, "!=") ||
				strings.HasPrefix(ver, "~=") || strings.HasPrefix(ver, ">") ||
				strings.HasPrefix(ver, "<") {
				pipDeps[i] = dep.Name + ver
			} else {
				pipDeps[i] = fmt.Sprintf("%s==%s", dep.Name, ver)
			}
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

	// Extract chunk index for file naming (used throughout)
	chunkIdx := 0
	if ci, ok := input.Input["chunk_index"].(float64); ok {
		chunkIdx = int(ci)
	}

	// Save original output_path before remapping (for copy-back later)
	originalOutputPath := ""
	if op, ok := input.Input["output_path"].(string); ok {
		originalOutputPath = op
	}

	// Copy input data file to envPath so it's accessible to the plugin
	if inputPath, ok := input.Input["input_path"].(string); ok && inputPath != "" {
		localInputPath := filepath.Join(envPath, fmt.Sprintf("chunk_%d%s", chunkIdx, filepath.Ext(inputPath)))
		if _, err := os.Stat(inputPath); err == nil {
			if copyErr := copyFile(inputPath, localInputPath); copyErr == nil {
				input.Input["input_path"] = localInputPath
				payload["input"] = input.Input
			}
		}
		if outputPath, ok := input.Input["output_path"].(string); ok && outputPath != "" {
			localOutputPath := filepath.Join(envPath, fmt.Sprintf("chunk_%d.out", chunkIdx))
			input.Input["output_path"] = localOutputPath
			payload["input"] = input.Input
		}
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
import io

start_time = time.time()

result = {
    "success": True,
    "data": {},
    "error": None,
    "items_processed": 0,
    "duration_ms": 0
}

try:
    with open("input.json", "r") as f:
        data = json.load(f)

    input_data = data.get("input", {})
    config = data.get("config", {})
    metadata = data.get("metadata", {})

    # Pipe input as stdin for Docker-style plugins
    stdin_payload = {"payload": input_data} if input_data else {"payload": config}
    sys.stdin = io.StringIO(json.dumps(stdin_payload))

    exec_globals = {
        "__name__": "__sentra_plugin__",
        "input": input_data,
        "config": config,
        "metadata": metadata,
    }

    with open("plugin.py", "r") as f:
        plugin_code = f.read()

    # Capture stdout so plugins using output_result() + sys.exit work
    stdout_capture = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = stdout_capture

    try:
        exec(plugin_code, exec_globals)
        if "main" in exec_globals:
            try:
                output = exec_globals["main"]()
            except TypeError:
                output = exec_globals["main"](input_data, config, metadata)
            if isinstance(output, dict):
                result["data"] = output
                result["items_processed"] = output.get("items_processed", 0)
            elif output is not None:
                result["data"] = {"result": output}
    except SystemExit as e:
        if e.code not in (None, 0):
            raise
    finally:
        sys.stdout = old_stdout

    captured = stdout_capture.getvalue()
    if not result["data"] and captured:
        try:
            result["data"] = json.loads(captured)
        except json.JSONDecodeError:
            result["data"] = {"raw_output": captured}

    result["duration_ms"] = int((time.time() - start_time) * 1000)

    with open("output.json", "w") as f:
        json.dump(result, f)

except SystemExit as e:
    code = e.code if e.code is not None else 0
    if code == 0:
        result["duration_ms"] = int((time.time() - start_time) * 1000)
        with open("output.json", "w") as f:
            json.dump(result, f)
    else:
        error_result = {
            "success": False,
            "data": {},
            "error": f"plugin exited with code {code}",
            "items_processed": 0,
            "duration_ms": int((time.time() - start_time) * 1000)
        }
        with open("output.json", "w") as f:
            json.dump(error_result, f)
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

	// Copy output file back from sandbox path to original output_path
	if originalOutputPath != "" {
		localOutputPath := filepath.Join(envPath, fmt.Sprintf("chunk_%d.out", chunkIdx))
		if _, statErr := os.Stat(localOutputPath); statErr == nil {
			if copyErr := copyFile(localOutputPath, originalOutputPath); copyErr == nil {
				obs.Info("v2 runtime: copied output file back", obs.Field{
					"from": localOutputPath,
					"to":   originalOutputPath,
				})
			} else {
				obs.Warn("v2 runtime: failed to copy output file back", obs.Field{
					"from":  localOutputPath,
					"to":    originalOutputPath,
					"error": copyErr.Error(),
				})
			}
		}
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

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
