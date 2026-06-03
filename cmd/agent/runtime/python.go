package runtime

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
)

type PythonRuntime struct {
	home       string
	venvPath   string
	executable string
	version    string
	installed  bool
}

func NewPythonRuntime(workingDir string) *PythonRuntime {
	return &PythonRuntime{
		home:       workingDir,
		venvPath:   filepath.Join(workingDir, "venv"),
		executable: "",
		version:    "",
		installed:  false,
	}
}

func writeJSONFile(path string, data interface{}) error {
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, content, 0644)
}

func (pr *PythonRuntime) Type() RuntimeType {
	return RuntimePython
}

func (pr *PythonRuntime) Setup(ctx context.Context, spec RuntimeSpec) error {
	if err := os.MkdirAll(pr.home, 0755); err != nil {
		return fmt.Errorf("failed to create runtime directory: %w", err)
	}

	cmd := exec.CommandContext(ctx, "python3", "--version")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("python3 not available: %w", err)
	}
	pr.version = strings.TrimSpace(out.String())

	if err := pr.createVirtualEnv(ctx); err != nil {
		return fmt.Errorf("failed to create virtual environment: %w", err)
	}

	pr.executable = filepath.Join(pr.venvPath, "bin", "python")
	pr.installed = true

	return nil
}

func (pr *PythonRuntime) createVirtualEnv(ctx context.Context) error {
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

	cmd := exec.CommandContext(ctx, "python3", "-m", "venv", "--system-site-packages", "--without-pip", pr.venvPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		os.RemoveAll(pr.venvPath)
		return fmt.Errorf("venv creation failed: %w", err)
	}

	return nil
}

func (pr *PythonRuntime) InstallDeps(ctx context.Context, deps []Dependency) error {
	if !pr.installed {
		return fmt.Errorf("runtime not set up")
	}

	for _, dep := range deps {
		var pkg string
		if dep.Version != "" {
			pkg = fmt.Sprintf("%s==%s", dep.Name, dep.Version)
		} else {
			pkg = dep.Name
		}

		ctx, cancel := context.WithTimeout(ctx, 180*time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, pr.executable, "-m", "pip", "install", pkg, "--quiet")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to install %s: %w", pkg, err)
		}
	}

	return nil
}

func (pr *PythonRuntime) Run(ctx context.Context, input ExecutionInput) (*ExecutionOutput, error) {
	if !pr.installed {
		return &ExecutionOutput{
			Success: false,
			Error:   "runtime not initialized",
		}, fmt.Errorf("runtime not initialized")
	}

	payload := map[string]interface{}{
		"input":    input.Input,
		"config":   input.Config,
		"metadata": input.Metadata,
	}

	inputFile := filepath.Join(pr.home, "input.json")
	if err := writeJSONFile(inputFile, payload); err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("failed to write input: %v", err),
		}, err
	}

	outputFile := filepath.Join(pr.home, "output.json")

	script := `
import json
import sys
import traceback

try:
    with open("input.json", "r") as f:
        data = json.load(f)
    
    input_data = data.get("input", {})
    config = data.get("config", {})
    metadata = data.get("metadata", {})
    
    result = {
        "success": True,
        "data": {},
        "error": None
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
        result["data"] = exec_globals["main"](input_data, config, metadata)
    
    with open("output.json", "w") as f:
        json.dump(result, f)
        
except Exception as e:
    error_result = {
        "success": False,
        "data": {},
        "error": f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
    }
    with open("output.json", "w") as f:
        json.dump(error_result, f)
    sys.exit(1)
`

	scriptFile := filepath.Join(pr.home, "runner.py")
	if err := os.WriteFile(scriptFile, []byte(script), 0644); err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("failed to write runner: %v", err),
		}, err
	}

	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, pr.executable, scriptFile)
	cmd.Dir = pr.home
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if _, err := os.Stat(outputFile); err == nil {
			var output ExecutionOutput
			if data, err := os.ReadFile(outputFile); err == nil {
				if err := json.Unmarshal(data, &output); err == nil {
					return &output, nil
				}
			}
		}
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("execution failed: %v", err),
		}, err
	}

	data, err := os.ReadFile(outputFile)
	if err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("failed to read output: %v", err),
		}, err
	}

	var output ExecutionOutput
	if err := json.Unmarshal(data, &output); err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("invalid output format: %v", err),
		}, err
	}

	return &output, nil
}

func (pr *PythonRuntime) Cleanup(ctx context.Context) error {
	if pr.home != "" {
		return os.RemoveAll(pr.home)
	}
	return nil
}

func (pr *PythonRuntime) IsInstalled(ctx context.Context) bool {
	return pr.installed
}

func (pr *PythonRuntime) Version(ctx context.Context) (string, error) {
	if pr.version != "" {
		return pr.version, nil
	}

	cmd := exec.CommandContext(ctx, "python3", "--version")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}
	pr.version = strings.TrimSpace(out.String())
	return pr.version, nil
}
