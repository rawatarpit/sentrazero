package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"sentra-agent/internal/obs"
	"sentra-agent/internal/system"
)

const defaultExecutionTimeout = 30 * time.Second

type ExecutionResult struct {
	Output         string `json:"output"`
	Method         string `json:"method"`
	DurationMs     int64  `json:"duration_ms"`
	FallbackReason string `json:"fallback_reason,omitempty"`
}

type NativeRunner func(ctx context.Context, pluginPath, checksum, inputJSON string) (string, error)

var (
	ErrUnsupportedLanguage = errors.New("unsupported plugin language")
	ErrNoExecutionPath     = errors.New("no execution path available")
	ErrPluginTypeMismatch  = errors.New("plugin type mismatch: file extension does not match declared language")
	ErrCGORequired         = errors.New("native plugin requires CGO (CGO_ENABLED=1)")
)

func Execute(
	ctx context.Context,
	pluginPath string,
	manifest Manifest,
	inputJSON string,
	env system.ExecutionEnv,
	nativeRunner NativeRunner,
) (*ExecutionResult, error) {
	start := time.Now()

	lang := manifest.Language
	if lang == "" {
		lang = detectLanguageFromPath(pluginPath)
	}

	timeout := time.Duration(manifest.Resources.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = defaultExecutionTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := validatePluginTypeMismatch(pluginPath, lang); err != nil {
		obs.Warn("plugin type validation failed", obs.Field{
			"plugin":   manifest.Name,
			"path":     pluginPath,
			"language": lang,
			"error":    err.Error(),
		})
		return nil, err
	}

	obs.Info("plugin execution started", obs.Field{
		"plugin":       manifest.Name,
		"language":     lang,
		"os":           env.OS,
		"arch":         env.Arch,
		"has_cgo":      env.HasCGO,
		"has_docker":   env.HasDocker,
		"privileged":   env.IsPrivileged,
		"timeout_secs": timeout.Seconds(),
		"memory_mb":    manifest.Resources.MemoryMB,
		"cpu_seconds":  manifest.Resources.CPUSeconds,
		"cpu_limit":    manifest.Resources.CPULimit,
	})

	payload := parseInputJSON(inputJSON)

	switch lang {
	case "go", "rust", "native", "c", "cpp", "":
		return executeNative(ctx, pluginPath, manifest, payload, env, nativeRunner, start)

	case "python", "python3", "python2", "node", "nodejs",
		"javascript", "ruby", "bash", "shell", "typescript":
		return executeScript(ctx, pluginPath, manifest, payload, env, start)

	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedLanguage, lang)
	}
}

func executeNative(
	ctx context.Context,
	pluginPath string,
	manifest Manifest,
	payload map[string]interface{},
	env system.ExecutionEnv,
	nativeRunner NativeRunner,
	start time.Time,
) (*ExecutionResult, error) {
	if !env.HasCGO || nativeRunner == nil {
		obs.Error("native plugin cannot execute — CGO unavailable", obs.Field{
			"plugin":   manifest.Name,
			"language": manifest.Language,
			"has_cgo":  env.HasCGO,
			"os":       env.OS,
			"arch":     env.Arch,
		})
		return nil, fmt.Errorf(
			"plugin %q is a native plugin and requires CGO to execute, but this agent is running without CGO support: %w",
			manifest.Name, ErrCGORequired,
		)
	}

	inputJSON, _ := json.Marshal(payload)
	out, err := nativeRunner(ctx, pluginPath, manifest.Checksum, string(inputJSON))
	duration := time.Since(start)

	if err != nil {
		obs.Error("native plugin execution failed", obs.Field{
			"plugin":      manifest.Name,
			"language":    manifest.Language,
			"method":      "native_cgo",
			"duration_ms": duration.Milliseconds(),
			"error":       err.Error(),
		})
		return nil, fmt.Errorf("native plugin %q execution failed: %w", manifest.Name, err)
	}

	obs.Info("plugin executed", obs.Field{
		"plugin":          manifest.Name,
		"language":        manifest.Language,
		"method":          "native_cgo",
		"duration_ms":     duration.Milliseconds(),
		"fallback_reason": "",
		"os":              env.OS,
		"arch":            env.Arch,
	})

	return &ExecutionResult{
		Output:         out,
		Method:         "native_cgo",
		DurationMs:     duration.Milliseconds(),
		FallbackReason: "",
	}, nil
}

func executeScript(
	ctx context.Context,
	pluginPath string,
	manifest Manifest,
	payload map[string]interface{},
	env system.ExecutionEnv,
	start time.Time,
) (*ExecutionResult, error) {
	if env.HasDocker {
		out, err := RunSandboxedPlugin(ctx, pluginPath, payload, manifest)
		duration := time.Since(start)

		if err != nil {
			obs.Error("docker sandbox execution failed", obs.Field{
				"plugin":      manifest.Name,
				"language":    manifest.Language,
				"method":      "docker",
				"duration_ms": duration.Milliseconds(),
				"error":       err.Error(),
			})
			return nil, fmt.Errorf("docker sandbox execution failed for plugin %q: %w", manifest.Name, err)
		}

		obs.Info("plugin executed", obs.Field{
			"plugin":          manifest.Name,
			"language":        manifest.Language,
			"method":          out.Method,
			"duration_ms":     duration.Milliseconds(),
			"fallback_reason": "",
			"os":              env.OS,
			"arch":            env.Arch,
		})

		return &ExecutionResult{
			Output:         out.Output,
			Method:         out.Method,
			DurationMs:     duration.Milliseconds(),
			FallbackReason: "",
		}, nil
	}

	// Local execution is disabled for security - require Docker or RuntimeManager
	obs.Error("script plugin requires Docker but Docker is unavailable", obs.Field{
		"plugin":   manifest.Name,
		"language": manifest.Language,
	})
	return nil, fmt.Errorf(
		"plugin %q requires Docker sandbox but Docker is not available on this device",
		manifest.Name,
	)
}

func validatePluginTypeMismatch(pluginPath string, lang string) error {
	ext := strings.ToLower(filepath.Ext(pluginPath))

	if ext == "" {
		switch lang {
		case "go", "rust", "native", "c", "cpp", "python", "node", "javascript", "typescript", "bash", "shell", "python3", "python2", "nodejs", "ruby":
			return nil
		default:
			return fmt.Errorf("%w: extensionless file for language %q",
				ErrPluginTypeMismatch, lang)
		}
	}

	if ext == ".wasm" {
		return nil
	}

	var expectedExts map[string][]string
	switch lang {
	case "go", "rust", "native", "c", "cpp":
		expectedExts = map[string][]string{".so": {}, ".a": {}}
	case "python", "python3", "python2":
		expectedExts = map[string][]string{".py": {}}
	case "node", "nodejs", "javascript", "typescript":
		expectedExts = map[string][]string{
			".js":  {},
			".mjs": {},
			".ts":  {},
		}
	case "bash", "shell":
		expectedExts = map[string][]string{".sh": {}}
	}

	if len(expectedExts) == 0 {
		return nil
	}

	matches := false
	for expectedExt := range expectedExts {
		if ext == expectedExt {
			matches = true
			break
		}
	}

	if !matches {
		return fmt.Errorf(
			"%w: file %q has extension %q but declared language is %q",
			ErrPluginTypeMismatch, pluginPath, ext, lang,
		)
	}

	return nil
}

func detectLanguageFromPath(pluginPath string) string {
	ext := strings.ToLower(filepath.Ext(pluginPath))
	switch ext {
	case ".so", ".a":
		return "native"
	case ".py":
		return "python"
	case ".js", ".mjs":
		return "javascript"
	case ".ts":
		return "typescript"
	case ".sh":
		return "bash"
	default:
		return ""
	}
}

func parseInputJSON(inputJSON string) map[string]interface{} {
	if inputJSON == "" {
		return nil
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(inputJSON), &m); err != nil {
		return nil
	}
	return m
}


