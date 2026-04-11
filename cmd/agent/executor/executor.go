package executor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"sentra-agent/cmd/agent/runtime"
)

type ExecutionMode string

const (
	ModeNative  ExecutionMode = "native"
	ModeRuntime ExecutionMode = "runtime"
	ModeDocker  ExecutionMode = "docker"
)

type Job struct {
	ID                string                 `json:"id"`
	Type              string                 `json:"job_type"`
	Payload           map[string]interface{} `json:"payload"`
	RuntimeType       string                 `json:"runtime_type,omitempty"`
	RuntimeDeps       []runtime.Dependency   `json:"runtime_dependencies,omitempty"`
	EntryPoint        string                 `json:"entrypoint,omitempty"`
	ExecutionMode     string                 `json:"execution_mode,omitempty"`
	EnvironmentStrict bool                   `json:"environment_strict"`
	TimeoutSeconds    int                    `json:"execution_timeout_seconds"`
	PluginID          string                 `json:"plugin_id,omitempty"`
	PluginVersion     string                 `json:"plugin_version,omitempty"`
	ExecutionID       string                 `json:"execution_id,omitempty"`
	ExecutionStepID   string                 `json:"execution_step_id,omitempty"`
}

type Result struct {
	Success    bool                   `json:"success"`
	Data       map[string]interface{} `json:"data"`
	Error      string                 `json:"error,omitempty"`
	DurationMs int64                  `json:"duration_ms"`
	Throughput float64                `json:"throughput,omitempty"`
}

type Executor struct {
	runtimeMgr     *runtime.RuntimeManager
	sandboxBase    string
	defaultTimeout time.Duration
}

func NewExecutor(sandboxBase string, defaultTimeout time.Duration) *Executor {
	mgr := runtime.NewRuntimeManager(sandboxBase, defaultTimeout)
	mgr.RegisterRuntime(runtime.NewPythonRuntime(""))

	return &Executor{
		runtimeMgr:     mgr,
		sandboxBase:    sandboxBase,
		defaultTimeout: defaultTimeout,
	}
}

func (e *Executor) ExecuteJob(ctx context.Context, job Job, pluginCode string) (*Result, error) {
	sandboxPath := e.getSandboxPath(job.ID)

	timeout := time.Duration(job.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = e.defaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	executionMode := ExecutionMode(job.ExecutionMode)
	if executionMode == "" {
		executionMode = ModeNative
	}

	switch executionMode {
	case ModeRuntime:
		return e.executeRuntime(ctx, job, pluginCode, sandboxPath)
	case ModeDocker:
		return e.executeDocker(ctx, job, pluginCode, sandboxPath)
	default:
		return e.executeNative(ctx, job, pluginCode)
	}
}

func (e *Executor) executeRuntime(ctx context.Context, job Job, pluginCode string, sandboxPath string) (*Result, error) {
	rtType := runtime.RuntimeType(job.RuntimeType)
	if rtType == "" {
		rtType = runtime.RuntimePython
	}

	workingDir := filepath.Join(e.sandboxBase, "job-"+job.ID)
	if err := os.MkdirAll(workingDir, 0755); err != nil {
		return &Result{
			Success: false,
			Error:   fmt.Sprintf("failed to create sandbox: %v", err),
		}, err
	}

	var rtInstance *runtime.PythonRuntime
	switch rtType {
	case runtime.RuntimePython:
		rtInstance = runtime.NewPythonRuntime(workingDir)
	default:
		return &Result{
			Success: false,
			Error:   fmt.Sprintf("unsupported runtime type: %s", rtType),
		}, fmt.Errorf("unsupported runtime: %s", rtType)
	}

	spec := runtime.RuntimeSpec{
		Type:         rtType,
		Version:      "3.x",
		Dependencies: job.RuntimeDeps,
		EntryPoint:   job.EntryPoint,
		Environment: map[string]bool{
			"strict": job.EnvironmentStrict,
		},
	}

	if err := rtInstance.Setup(ctx, spec); err != nil {
		return &Result{
			Success: false,
			Error:   fmt.Sprintf("runtime setup failed: %v", err),
		}, err
	}

	if len(job.RuntimeDeps) > 0 {
		if err := rtInstance.InstallDeps(ctx, job.RuntimeDeps); err != nil {
			return &Result{
				Success: false,
				Error:   fmt.Sprintf("dependency installation failed: %v", err),
			}, err
		}
	}

	pluginFile := filepath.Join(workingDir, "plugin.py")
	if err := os.WriteFile(pluginFile, []byte(pluginCode), 0644); err != nil {
		return &Result{
			Success: false,
			Error:   fmt.Sprintf("failed to write plugin: %v", err),
		}, err
	}

	input := runtime.ExecutionInput{
		Input:  job.Payload,
		Config: map[string]interface{}{},
		Metadata: map[string]interface{}{
			"job_id":     job.ID,
			"job_type":   job.Type,
			"plugin_id":  job.PluginID,
			"plugin_ver": job.PluginVersion,
		},
	}

	output, err := rtInstance.Run(ctx, input)
	if err != nil {
		return &Result{
			Success: false,
			Error:   fmt.Sprintf("execution failed: %v", err),
		}, err
	}

	rtInstance.Cleanup(ctx)

	result := &Result{
		Success: output.Success,
		Data:    output.Data,
		Error:   output.Error,
	}

	return result, nil
}

func (e *Executor) executeDocker(ctx context.Context, job Job, pluginCode string, sandboxPath string) (*Result, error) {
	return &Result{
		Success: false,
		Error:   "docker execution not yet implemented",
	}, fmt.Errorf("docker execution mode not implemented")
}

func (e *Executor) executeNative(ctx context.Context, job Job, pluginCode string) (*Result, error) {
	return &Result{
		Success: false,
		Error:   "native execution not yet implemented - use runtime mode",
	}, fmt.Errorf("native execution mode deprecated - use runtime mode")
}

func (e *Executor) getSandboxPath(jobID string) string {
	return filepath.Join(e.sandboxBase, "job-"+jobID)
}

func (e *Executor) CleanupSandbox(jobID string) error {
	path := e.getSandboxPath(jobID)
	return os.RemoveAll(path)
}

func (e *Executor) SupportedRuntimes() []runtime.RuntimeType {
	return e.runtimeMgr.SupportedRuntimes()
}
