package executor

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"sentra-agent/cmd/agent/runtime/v2"
	"sentra-agent/cmd/agent/sandbox"
)

type ExecutionMode string

const (
	ModeNative  ExecutionMode = "native"
	ModeRuntime ExecutionMode = "runtime"
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
	PluginCode        string                 `json:"plugin_code,omitempty"`
	ExecutionID       string                 `json:"execution_id,omitempty"`
	ExecutionStepID   string                 `json:"execution_step_id,omitempty"`
	RunID             string                 `json:"run_id,omitempty"`
	AttemptNumber     int                    `json:"attempt_number"`
	OrgID             string                 `json:"org_id,omitempty"`
	DependencyHash    string                 `json:"dependency_hash,omitempty"`
	EnvironmentID     string                 `json:"environment_id,omitempty"`
	ExecutionPolicy   *ExecutionPolicy       `json:"execution_policy,omitempty"`
	Trusted           bool                   `json:"trusted,omitempty"`
}

type ExecutionPolicy struct {
	MaxRetries             int      `json:"max_retries"`
	RetryBackoffSeconds    int      `json:"retry_backoff_seconds"`
	RetryBackoffMultiplier float64  `json:"retry_backoff_multiplier"`
	MaxRetryDelaySeconds   int      `json:"max_retry_delay_seconds"`
	DefaultTimeoutSeconds  int      `json:"default_timeout_seconds"`
	HardTimeoutSeconds     int      `json:"hard_timeout_seconds"`
	RetryableErrors        []string `json:"retryable_errors"`
	FatalErrors            []string `json:"fatal_errors"`
}

type Result struct {
	Success             bool                   `json:"success"`
	Data                map[string]interface{} `json:"data"`
	Error               string                 `json:"error,omitempty"`
	ErrorClassification string                 `json:"error_classification,omitempty"`
	DurationMs          int64                  `json:"duration_ms"`
	Throughput          float64                `json:"throughput,omitempty"`
	ExitCode            int                    `json:"exit_code,omitempty"`
	Signal              string                 `json:"signal,omitempty"`
	OutputSizeBytes     int64                  `json:"output_size_bytes"`
	LogSizeBytes        int64                  `json:"log_size_bytes"`
	MaxMemoryBytes      int64                  `json:"max_memory_bytes"`
	RunID               string                 `json:"run_id"`
	AttemptNumber       int                    `json:"attempt_number"`
	ExecutionPolicy     *ExecutionPolicy       `json:"execution_policy,omitempty"`
}

type ExecutionState struct {
	mu            sync.Mutex
	runningJobs   map[string]*Job
	completedJobs map[string]*Result
}

func NewExecutionState() *ExecutionState {
	return &ExecutionState{
		runningJobs:   make(map[string]*Job),
		completedJobs: make(map[string]*Result),
	}
}

type Executor struct {
	runtimeMgr     *runtime.RuntimeManager
	sandboxBase    string
	defaultTimeout time.Duration
	state          *ExecutionState
	sandboxPool    *sync.Pool
}

func NewExecutor(sandboxBase string, defaultTimeout time.Duration) *Executor {
	mgr := runtime.NewRuntimeManager(sandboxBase, defaultTimeout)

	return &Executor{
		runtimeMgr:     mgr,
		sandboxBase:    sandboxBase,
		defaultTimeout: defaultTimeout,
		state:          NewExecutionState(),
		sandboxPool: &sync.Pool{
			New: func() interface{} {
				return sandbox.NewSandbox("", nil)
			},
		},
	}
}

func (e *Executor) GetRuntimeManager() *runtime.RuntimeManager {
	return e.runtimeMgr
}

func selectExecutionModes(executionMode string) []ExecutionMode {
	switch ExecutionMode(executionMode) {
	case ModeRuntime:
		return []ExecutionMode{ModeRuntime}
	case ModeNative:
		return []ExecutionMode{ModeNative}
	default:
		return []ExecutionMode{ModeRuntime}
	}
}

func (e *Executor) ExecuteJob(ctx context.Context, job Job) (*Result, error) {
	startTime := time.Now()

	runID := job.RunID
	if runID == "" {
		runID = generateRunID()
	}

	attemptNumber := job.AttemptNumber
	if attemptNumber == 0 {
		attemptNumber = 1
	}

	if !job.Trusted {
		return &Result{
			Success:             false,
			Error:               fmt.Sprintf("plugin %s is not trusted and cannot be executed", job.PluginID),
			ErrorClassification: "system_error",
		}, fmt.Errorf("plugin %s is not trusted", job.PluginID)
	}

	if job.PluginCode == "" {
		return &Result{
			Success:             false,
			Error:               "plugin_code is empty — no code to execute in v2 runtime",
			ErrorClassification: "system_error",
		}, fmt.Errorf("plugin_code is empty — pipeline jobs should use native handler, not v2 executor")
	}

	e.state.mu.Lock()
	e.state.runningJobs[job.ID] = &job
	e.state.mu.Unlock()

	defer func() {
		e.state.mu.Lock()
		delete(e.state.runningJobs, job.ID)
		e.state.mu.Unlock()
	}()

	policy := job.ExecutionPolicy
	if policy == nil {
		policy = &ExecutionPolicy{
			DefaultTimeoutSeconds: 300,
			HardTimeoutSeconds:    600,
			MaxRetries:            3,
			RetryBackoffSeconds:   5,
			MaxRetryDelaySeconds:  300,
		}
	}

	var result *Result
	var err error

	modes := selectExecutionModes(job.ExecutionMode)

	for i, mode := range modes {
		result, err = e.executeRuntimeWithRetry(ctx, job, policy, mode)
		if result != nil && result.Success && err == nil {
			break
		}

		if i < len(modes)-1 {
			continue
		}
	}

	result.RunID = runID
	result.AttemptNumber = attemptNumber
	result.DurationMs = time.Since(startTime).Milliseconds()
	result.ExecutionPolicy = policy

	e.state.mu.Lock()
	if len(e.state.completedJobs) >= 1000 {
		var oldestKey string
		var oldestDuration int64
		for k, r := range e.state.completedJobs {
			if oldestKey == "" || r.DurationMs < oldestDuration {
				oldestKey = k
				oldestDuration = r.DurationMs
			}
		}
		if oldestKey != "" {
			delete(e.state.completedJobs, oldestKey)
		}
	}
	e.state.completedJobs[job.ID] = result
	e.state.mu.Unlock()

	return result, err
}

func (e *Executor) executeRuntime(ctx context.Context, job Job, policy *ExecutionPolicy) (*Result, error) {
	timeout := time.Duration(policy.DefaultTimeoutSeconds) * time.Second
	if job.TimeoutSeconds > 0 {
		timeout = time.Duration(job.TimeoutSeconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	workDir := filepath.Join(e.sandboxBase, "job-"+job.ID)
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return &Result{
			Success:             false,
			Error:               fmt.Sprintf("failed to create sandbox: %v", err),
			ErrorClassification: "fatal",
		}, err
	}

	sandbox := sandbox.NewSandbox(workDir, &sandbox.Config{
		MaxMemoryBytes:    512 * 1024 * 1024,
		Timeout:           timeout,
		EnvironmentStrict: job.EnvironmentStrict,
		BlockedEnvVars:    []string{"SENTRA_API_KEY", "SENTRA_SECRET", "AWS_"},
	})

	if err := sandbox.Prepare(ctx); err != nil {
		return &Result{
			Success:             false,
			Error:               fmt.Sprintf("sandbox preparation failed: %v", err),
			ErrorClassification: "fatal",
		}, err
	}

	defer sandbox.Cleanup()

	rtType := runtime.RuntimeType(job.RuntimeType)
	if rtType == "" {
		rtType = runtime.RuntimePython
	}

	runtimeVersion := job.RuntimeType
	if runtimeVersion == "" {
		runtimeVersion = "3.x"
	}

	spec := runtime.RuntimeSpec{
		Type:               rtType,
		Version:            runtimeVersion,
		DependencyLockHash: job.DependencyHash,
		Dependencies: func() []runtime.Dependency {
			deps := make([]runtime.Dependency, len(job.RuntimeDeps))
			for i, d := range job.RuntimeDeps {
				deps[i] = runtime.Dependency{Name: d.Name, Version: d.Version, Source: d.Source}
			}
			return deps
		}(),
		EntryPoint: job.EntryPoint,
		Environment: map[string]bool{
			"strict": job.EnvironmentStrict,
		},
		Strict: job.EnvironmentStrict,
	}

	input := runtime.ExecutionInput{
		Input: job.Payload,
		Config: map[string]interface{}{
			"job_id":       job.ID,
			"job_type":     job.Type,
			"plugin_id":    job.PluginID,
			"plugin_ver":   job.PluginVersion,
			"execution_id": job.ExecutionID,
			"step_id":      job.ExecutionStepID,
		},
		Metadata: map[string]interface{}{
			"org_id":          job.OrgID,
			"run_id":          job.RunID,
			"attempt":         job.AttemptNumber,
			"dependency_hash": job.DependencyHash,
		},
		PluginCode: job.PluginCode,
	}

	execResult, err := e.runtimeMgr.ExecuteWithMetrics(ctx, spec, input, job.OrgID)

	result := &Result{
		Success:             execResult.Output.Success,
		Data:                execResult.Output.Data,
		Error:               execResult.Output.Error,
		ErrorClassification: classifyError(execResult.Output.Error),
		MaxMemoryBytes:      execResult.Metrics.MaxMemoryBytes,
		ExitCode:            execResult.Metrics.ExitCode,
		Signal:              execResult.Metrics.Signal,
		Throughput:          execResult.Throughput,
	}

	if execResult.Output.Error != "" {
		result.ErrorClassification = classifyError(execResult.Output.Error)
	} else if execResult.Metrics.OOMKilled {
		result.ErrorClassification = "fatal"
		result.Error = "OOM killed"
	}

	outputSize, _ := measureDirectory(filepath.Join(workDir, "output"))
	result.OutputSizeBytes = outputSize

	logSize, _ := measureDirectory(filepath.Join(workDir, "logs"))
	result.LogSizeBytes = logSize

	return result, err
}

func (e *Executor) GetJobStatus(jobID string) (running bool, result *Result) {
	e.state.mu.Lock()
	defer e.state.mu.Unlock()

	if _, ok := e.state.runningJobs[jobID]; ok {
		return true, nil
	}

	if res, ok := e.state.completedJobs[jobID]; ok {
		return false, res
	}

	return false, nil
}

func (e *Executor) GetRunningJobs() []*Job {
	e.state.mu.Lock()
	defer e.state.mu.Unlock()

	jobs := make([]*Job, 0, len(e.state.runningJobs))
	for _, job := range e.state.runningJobs {
		jobs = append(jobs, job)
	}

	return jobs
}

func (e *Executor) executeRuntimeWithRetry(ctx context.Context, job Job, policy *ExecutionPolicy, mode ExecutionMode) (*Result, error) {
	if policy.MaxRetries <= 0 {
		if mode == ModeRuntime {
			return e.executeRuntime(ctx, job, policy)
		}
		return &Result{
			Success:             false,
			Error:               "unsupported execution mode",
			ErrorClassification: "fatal",
		}, fmt.Errorf("unsupported execution mode")
	}

	backoffMultiplier := policy.RetryBackoffMultiplier
	if backoffMultiplier <= 0 {
		backoffMultiplier = 2.0
	}

	maxDelay := time.Duration(policy.MaxRetryDelaySeconds) * time.Second
	if maxDelay <= 0 {
		maxDelay = 300 * time.Second
	}

	var lastResult *Result
	var lastErr error

	for attempt := 0; attempt <= policy.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(policy.RetryBackoffSeconds) * time.Duration(backoffMultiplier*float64(attempt-1))
			if delay > maxDelay {
				delay = maxDelay
			}

			select {
			case <-ctx.Done():
				return lastResult, lastErr
			case <-time.After(delay):
			}
		}

		var result *Result
		result, lastErr = e.executeRuntime(ctx, job, policy)

		lastResult = result

		if lastErr == nil && result.Success {
			return result, nil
		}

		errClass := result.ErrorClassification

		if errClass != "retryable" {
			return result, lastErr
		}
	}

	return lastResult, lastErr
}

func generateRunID() string {
	buf := make([]byte, 16)
	_, err := rand.Read(buf)
	if err != nil {
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}
	return fmt.Sprintf("%x", buf)
}

func getPluginFilename(rt runtime.RuntimeType) string {
	switch rt {
	case runtime.RuntimePython:
		return "plugin.py"
	case runtime.RuntimeNode:
		return "plugin.js"
	default:
		return "plugin.py"
	}
}

func measureDirectory(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func classifyError(errMsg string) string {
	errLower := strings.ToLower(errMsg)

	retryable := []string{
		"timeout",
		"connection",
		"network",
		"io error",
		"disk full",
		"no module named",
		"cannot find module",
		"install failed",
		"out of memory",
	}

	pluginErrors := []string{
		"syntax",
		"indentation",
		"type error",
		"undefined",
		"import error",
		"attribute error",
		"name error",
		"value error",
		"runtime error",
		"exception",
	}

	infraErrors := []string{
		"docker",
		"container",
		"no space left",
		"cannot create",
		"permission denied",
		"access denied",
		"failed to mount",
		"failed to start",
		"not found",
		"exec format error",
	}

	systemErrors := []string{
		"untrusted",
		"security",
		"panic",
		"forbidden",
		"invalid signature",
		"checksum mismatch",
		"not trusted",
	}

	for _, pattern := range systemErrors {
		if strings.Contains(errLower, pattern) {
			return "system_error"
		}
	}

	for _, pattern := range infraErrors {
		if strings.Contains(errLower, pattern) {
			return "infra_error"
		}
	}

	for _, pattern := range pluginErrors {
		if strings.Contains(errLower, pattern) {
			return "plugin_error"
		}
	}

	for _, pattern := range retryable {
		if strings.Contains(errLower, pattern) {
			return "infra_error"
		}
	}

	return "unknown"
}

func writeJSONFile(path string, data interface{}) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0644)
}

func readFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	return string(data), err
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
