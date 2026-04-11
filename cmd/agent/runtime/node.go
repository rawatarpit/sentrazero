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

type NodeRuntime struct {
	home      string
	nodePath  string
	npmPath   string
	version   string
	installed bool
	packages  map[string]bool
}

func NewNodeRuntime(workingDir string) *NodeRuntime {
	return &NodeRuntime{
		home:      workingDir,
		nodePath:  "",
		npmPath:   "",
		version:   "",
		installed: false,
		packages:  make(map[string]bool),
	}
}

func (nr *NodeRuntime) Type() RuntimeType {
	return RuntimeNode
}

func (nr *NodeRuntime) Setup(ctx context.Context, spec RuntimeSpec) error {
	if err := os.MkdirAll(nr.home, 0755); err != nil {
		return fmt.Errorf("failed to create runtime directory: %w", err)
	}

	nodeCmd := exec.CommandContext(ctx, "node", "--version")
	var out bytes.Buffer
	nodeCmd.Stdout = &out
	if err := nodeCmd.Run(); err != nil {
		return fmt.Errorf("node not available: %w", err)
	}
	nr.nodePath = "node"
	nr.version = strings.TrimSpace(out.String())

	npmCmd := exec.CommandContext(ctx, "npm", "--version")
	npmCmd.Stdout = &out
	if err := npmCmd.Run(); err != nil {
		return fmt.Errorf("npm not available: %w", err)
	}
	nr.npmPath = "npm"

	if err := nr.initPackage(ctx); err != nil {
		return fmt.Errorf("failed to initialize package: %w", err)
	}

	nr.installed = true
	return nil
}

func (nr *NodeRuntime) initPackage(ctx context.Context) error {
	packageFile := filepath.Join(nr.home, "package.json")
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

func (nr *NodeRuntime) InstallDeps(ctx context.Context, deps []Dependency) error {
	if !nr.installed {
		return fmt.Errorf("runtime not set up")
	}

	for _, dep := range deps {
		if nr.packages[dep.Name] {
			continue
		}

		ctx, cancel := context.WithTimeout(ctx, 180*time.Second)
		defer cancel()

		pkg := dep.Name
		if dep.Version != "" {
			pkg = fmt.Sprintf("%s@%s", dep.Name, dep.Version)
		}

		cmd := exec.CommandContext(ctx, nr.npmPath, "install", pkg, "--save", "--silent")
		cmd.Dir = nr.home
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to install %s: %w", pkg, err)
		}

		nr.packages[dep.Name] = true
	}

	return nil
}

func (nr *NodeRuntime) Run(ctx context.Context, input ExecutionInput) (*ExecutionOutput, error) {
	if !nr.installed {
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

	inputFile := filepath.Join(nr.home, "input.json")
	content, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("failed to marshal input: %v", err),
		}, err
	}
	if err := os.WriteFile(inputFile, content, 0644); err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("failed to write input: %v", err),
		}, err
	}

	outputFile := filepath.Join(nr.home, "output.json")

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

	scriptFile := filepath.Join(nr.home, "runner.mjs")
	if err := os.WriteFile(scriptFile, []byte(script), 0644); err != nil {
		return &ExecutionOutput{
			Success: false,
			Error:   fmt.Sprintf("failed to write runner: %v", err),
		}, err
	}

	ctx, cancel := context.WithTimeout(ctx, 300*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, nr.nodePath, scriptFile)
	cmd.Dir = nr.home
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

func (nr *NodeRuntime) Cleanup(ctx context.Context) error {
	if nr.home != "" {
		return os.RemoveAll(nr.home)
	}
	return nil
}

func (nr *NodeRuntime) IsInstalled(ctx context.Context) bool {
	return nr.installed
}

func (nr *NodeRuntime) Version(ctx context.Context) (string, error) {
	if nr.version != "" {
		return nr.version, nil
	}

	cmd := exec.CommandContext(ctx, "node", "--version")
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}
	nr.version = strings.TrimSpace(out.String())
	return nr.version, nil
}
