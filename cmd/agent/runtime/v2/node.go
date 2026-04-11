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

type NodeRuntime struct {
	home      string
	nodePath  string
	npmPath   string
	version   string
	installed bool
	packages  map[string]bool
	platform  PlatformInfo
}

func NewNodeRuntime(workingDir string) *NodeRuntime {
	return &NodeRuntime{
		home:     workingDir,
		platform: GetCurrentPlatform(),
		nodePath: "node",
		npmPath:  "npm",
		packages: make(map[string]bool),
	}
}

func (nr *NodeRuntime) getNodeCommand() string {
	switch runtime.GOOS {
	case "windows":
		return "node"
	default:
		return "node"
	}
}

func (nr *NodeRuntime) getNPMCommand() string {
	switch runtime.GOOS {
	case "windows":
		return "npm"
	default:
		return "npm"
	}
}

func (nr *NodeRuntime) Type() RuntimeType {
	return RuntimeNode
}

func (nr *NodeRuntime) Setup(ctx context.Context, spec RuntimeSpec, envPath string) error {
	if err := os.MkdirAll(envPath, 0755); err != nil {
		return fmt.Errorf("failed to create runtime directory: %w", err)
	}

	nr.home = envPath

	nodeCmd := nr.getNodeCommand()
	npmCmd := nr.getNPMCommand()

	nodeCheck := exec.CommandContext(ctx, nodeCmd, "--version")
	var out bytes.Buffer
	nodeCheck.Stdout = &out
	nodeCheck.Stderr = &out
	if err := nodeCheck.Run(); err != nil {
		return fmt.Errorf("node not available: %w", err)
	}
	nr.nodePath = nodeCmd
	nr.version = strings.TrimSpace(out.String())

	npmCheck := exec.CommandContext(ctx, npmCmd, "--version")
	npmCheck.Stdout = &out
	npmCheck.Stderr = &out
	if err := npmCheck.Run(); err != nil {
		return fmt.Errorf("npm not available: %w", err)
	}
	nr.npmPath = npmCmd

	if err := nr.initPackage(ctx, envPath); err != nil {
		return fmt.Errorf("failed to initialize package: %w", err)
	}

	nr.installed = true
	return nil
}

func (nr *NodeRuntime) initPackage(ctx context.Context, envPath string) error {
	packageFile := filepath.Join(envPath, "package.json")
	if _, err := os.Stat(packageFile); err == nil {
		return nil
	}

	packageJson := map[string]interface{}{
		"name":    "sentra-plugin",
		"version": "1.0.0",
		"type":    "module",
	}

	data, err := json.MarshalIndent(packageJson, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(packageFile, data, 0644)
}

func (nr *NodeRuntime) InstallDeps(ctx context.Context, deps []Dependency, envPath string) error {
	if !nr.installed {
		return fmt.Errorf("runtime not set up")
	}

	if len(deps) == 0 {
		readyFile := filepath.Join(envPath, ".ready")
		return os.WriteFile(readyFile, []byte("ok"), 0644)
	}

	var pkgSpecs []string
	for _, dep := range deps {
		if nr.packages[dep.Name] {
			continue
		}
		pkg := dep.Name
		if dep.Version != "" {
			pkg = fmt.Sprintf("%s@%s", dep.Name, dep.Version)
		}
		pkgSpecs = append(pkgSpecs, pkg)
	}

	if len(pkgSpecs) == 0 {
		readyFile := filepath.Join(envPath, ".ready")
		return os.WriteFile(readyFile, []byte("ok"), 0644)
	}

	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

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

		args := []string{nr.npmPath, "install", "--no-audit", "--no-fund"}
		args = append(args, pkgSpecs...)
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		cmd.Dir = envPath
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			lastErr = fmt.Errorf("failed to install dependencies: %w", err)
			obs.Warn("npm install attempt failed", obs.Field{
				"attempt":     attempt + 1,
				"max_retries": maxRetries,
				"error":       err.Error(),
			})
			continue
		}

		for _, dep := range deps {
			nr.packages[dep.Name] = true
		}

		readyFile := filepath.Join(envPath, ".ready")
		if err := os.WriteFile(readyFile, []byte("ok"), 0644); err != nil {
			return fmt.Errorf("failed to write ready marker: %w", err)
		}
		return nil
	}

	return fmt.Errorf("dependency installation failed after %d retries: %w", maxRetries, lastErr)
}

func (nr *NodeRuntime) Run(ctx context.Context, input ExecutionInput, envPath string) (*ExecutionOutput, *ExecutionMetrics, error) {
	metrics := &ExecutionMetrics{}

	if !nr.installed {
		return &ExecutionOutput{
			Success: false,
			Error:   "runtime not initialized",
		}, metrics, fmt.Errorf("runtime not initialized")
	}

	if input.PluginCode != "" {
		pluginFile := filepath.Join(envPath, "plugin.js")
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
	content, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("failed to marshal input: %v", err),
		}, metrics, err
	}
	if err := os.WriteFile(inputFile, content, 0644); err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("failed to write input: %v", err),
		}, metrics, err
	}

	outputFile := filepath.Join(envPath, "output.json")

	script := `
import { readFileSync, writeFileSync } from 'fs';

const input = JSON.parse(readFileSync('input.json', 'utf8'));
const { input: inputData, config, metadata } = input;

const result = {
    success: true,
    data: {},
    error: null
};

try {
    const pluginCode = readFileSync('plugin.js', 'utf8');
    
    const pluginFn = new Function('input', 'config', 'metadata', pluginCode);
    result.data = pluginFn(inputData, config, metadata) || {};
    
} catch (e) {
    result.success = false;
    result.error = e.message || String(e);
}

writeFileSync('output.json', JSON.stringify(result));
`

	scriptFile := filepath.Join(envPath, "runner.mjs")
	if err := os.WriteFile(scriptFile, []byte(script), 0644); err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("failed to write runner: %v", err),
		}, metrics, err
	}

	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, nr.nodePath, scriptFile)
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

	return &output, metrics, nil
}

func (nr *NodeRuntime) Cleanup(ctx context.Context, envPath string) error {
	if envPath != "" {
		return os.RemoveAll(envPath)
	}
	return nil
}

func (nr *NodeRuntime) IsInstalled(ctx context.Context, envPath string) bool {
	if !nr.installed {
		return false
	}
	nodePath, err := exec.LookPath(nr.nodePath)
	if err != nil {
		return false
	}
	if _, err := os.Stat(nodePath); err != nil {
		return false
	}
	return true
}

func (nr *NodeRuntime) Version(ctx context.Context) (string, error) {
	if nr.version != "" {
		return nr.version, nil
	}

	cmd := exec.CommandContext(ctx, nr.nodePath, "--version")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}
	nr.version = strings.TrimSpace(out.String())
	return nr.version, nil
}
