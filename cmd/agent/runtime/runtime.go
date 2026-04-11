package runtime

import (
	"context"
	"fmt"
	"time"
)

type RuntimeType string

const (
	RuntimePython RuntimeType = "python"
	RuntimeNode   RuntimeType = "node"
	RuntimeNative RuntimeType = "native"
)

type Dependency struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
	Source  string `json:"source,omitempty"`
}

type RuntimeSpec struct {
	Type         RuntimeType     `json:"type"`
	Version      string          `json:"version"`
	Dependencies []Dependency    `json:"dependencies"`
	EntryPoint   string          `json:"entry_point"`
	Environment  map[string]bool `json:"environment"`
}

type ExecutionInput struct {
	Input    map[string]interface{} `json:"input"`
	Config   map[string]interface{} `json:"config"`
	Metadata map[string]interface{} `json:"metadata"`
}

type ExecutionOutput struct {
	Success bool                   `json:"success"`
	Data    map[string]interface{} `json:"data"`
	Error   string                 `json:"error,omitempty"`
}

type ExecutionResult struct {
	Output     ExecutionOutput
	Duration   time.Duration
	Throughput float64
	CleanedUp  bool
}

type Runtime interface {
	Type() RuntimeType
	Setup(ctx context.Context, spec RuntimeSpec) error
	InstallDeps(ctx context.Context, deps []Dependency) error
	Run(ctx context.Context, input ExecutionInput) (*ExecutionOutput, error)
	Cleanup(ctx context.Context) error
	IsInstalled(ctx context.Context) bool
	Version(ctx context.Context) (string, error)
}

type RuntimeManager struct {
	runtimes    map[RuntimeType]Runtime
	sandboxPath string
	timeout     time.Duration
}

func NewRuntimeManager(sandboxPath string, defaultTimeout time.Duration) *RuntimeManager {
	mgr := &RuntimeManager{
		runtimes:    make(map[RuntimeType]Runtime),
		sandboxPath: sandboxPath,
		timeout:     defaultTimeout,
	}
	mgr.runtimes[RuntimePython] = NewPythonRuntime("")
	mgr.runtimes[RuntimeNode] = NewNodeRuntime("")
	return mgr
}

func (rm *RuntimeManager) RegisterRuntime(rt Runtime) {
	rm.runtimes[rt.Type()] = rt
}

func (rm *RuntimeManager) GetRuntime(t RuntimeType) (Runtime, error) {
	rt, ok := rm.runtimes[t]
	if !ok {
		return nil, fmt.Errorf("runtime type %s not registered", t)
	}
	return rt, nil
}

func (rm *RuntimeManager) Execute(ctx context.Context, spec RuntimeSpec, input ExecutionInput) (*ExecutionResult, error) {
	startTime := time.Now()

	rt, err := rm.GetRuntime(spec.Type)
	if err != nil {
		return &ExecutionResult{
			Output: ExecutionOutput{
				Success: false,
				Error:   err.Error(),
			},
		}, err
	}

	if err := rt.Setup(ctx, spec); err != nil {
		return &ExecutionResult{
			Output: ExecutionOutput{
				Success: false,
				Error:   fmt.Sprintf("setup failed: %v", err),
			},
		}, err
	}

	if len(spec.Dependencies) > 0 {
		if err := rt.InstallDeps(ctx, spec.Dependencies); err != nil {
			return &ExecutionResult{
				Output: ExecutionOutput{
					Success: false,
					Error:   fmt.Sprintf("dependency installation failed: %v", err),
				},
			}, err
		}
	}

	output, err := rt.Run(ctx, input)
	duration := time.Since(startTime)

	result := &ExecutionResult{
		Output:   *output,
		Duration: duration,
	}

	if output.Success && output.Data != nil {
		if items, ok := output.Data["items_processed"].(float64); ok && duration.Seconds() > 0 {
			result.Throughput = items / duration.Seconds()
		}
	}

	cleanupErr := rt.Cleanup(ctx)
	result.CleanedUp = cleanupErr == nil

	if err != nil {
		return result, err
	}

	return result, nil
}

func (rm *RuntimeManager) SupportedRuntimes() []RuntimeType {
	types := make([]RuntimeType, 0, len(rm.runtimes))
	for t := range rm.runtimes {
		types = append(types, t)
	}
	return types
}
