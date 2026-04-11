package sandbox

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	MaxMemoryBytes     int64
	MaxCPUPercent      int
	MaxFileSizeBytes   int64
	MaxFiles           int
	MaxProcesses       int
	MaxFileDescriptors int
	Timeout            time.Duration
	EnvironmentStrict  bool
	AllowedEnvVars     []string
	BlockedEnvVars     []string
	NetworkRestricted  bool
	WorkingDirectory   string
	RunAsUser          string
}

var DefaultConfig = Config{
	MaxMemoryBytes:     512 * 1024 * 1024,
	MaxCPUPercent:      80,
	MaxFileSizeBytes:   100 * 1024 * 1024,
	MaxFiles:           100,
	MaxProcesses:       10,
	MaxFileDescriptors: 50,
	Timeout:            300 * time.Second,
	EnvironmentStrict:  false,
	AllowedEnvVars:     []string{"PATH", "HOME", "USER", "LANG"},
	BlockedEnvVars:     []string{"SENTRA_API_KEY", "SENTRA_SECRET", "AWS_ACCESS_KEY_ID"},
	NetworkRestricted:  true,
	WorkingDirectory:   "/tmp/sentra",
	RunAsUser:          "",
}

type ExecutionResult struct {
	ExitCode        int
	Signal          string
	Duration        time.Duration
	MemoryBytes     int64
	ContextSwitches int64
	StdoutPath      string
	StderrPath      string
	TimedOut        bool
	Killed          bool
}

func (er ExecutionResult) Success() bool {
	return er.ExitCode == 0 && !er.TimedOut && !er.Killed
}

func (er ExecutionResult) Classification() string {
	if er.Killed {
		return "killed"
	}
	if er.TimedOut {
		return "timeout"
	}
	if er.ExitCode != 0 {
		return "failed"
	}
	return "success"
}

type Sandbox struct {
	mu      sync.Mutex
	config  Config
	workDir string
	cmd     *exec.Cmd
}

func NewSandbox(workDir string, config *Config) *Sandbox {
	cfg := DefaultConfig
	if config != nil {
		cfg = *config
	}
	return &Sandbox{
		config:  cfg,
		workDir: workDir,
	}
}

func (s *Sandbox) Prepare(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.workDir, 0755); err != nil {
		return fmt.Errorf("failed to create sandbox directory: %w", err)
	}

	for _, subdir := range []string{"input", "output", "tmp"} {
		if err := os.MkdirAll(filepath.Join(s.workDir, subdir), 0755); err != nil {
			return fmt.Errorf("failed to create %s directory: %w", subdir, err)
		}
	}

	return nil
}

func (s *Sandbox) Execute(ctx context.Context, executable string, args []string, env []string) (ExecutionResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.workDir == "" {
		return ExecutionResult{}, fmt.Errorf("sandbox not prepared")
	}

	cmd := exec.CommandContext(ctx, executable, args...)
	s.cmd = cmd

	cmd.Dir = s.workDir

	if s.config.EnvironmentStrict {
		cmd.Env = s.filterEnvironment(env)
	} else {
		cmd.Env = append(os.Environ(), env...)
	}

	stdout, err := os.Create(filepath.Join(s.workDir, "output", "stdout.log"))
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("failed to create stdout file: %w", err)
	}
	defer stdout.Close()

	stderr, err := os.Create(filepath.Join(s.workDir, "output", "stderr.log"))
	if err != nil {
		return ExecutionResult{}, fmt.Errorf("failed to create stderr file: %w", err)
	}
	defer stderr.Close()

	cmd.Stdout = io.MultiWriter(stdout, os.Stdout)
	cmd.Stderr = io.MultiWriter(stderr, os.Stderr)

	startTime := time.Now()

	if err := cmd.Start(); err != nil {
		return ExecutionResult{}, fmt.Errorf("failed to start process: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		<-done
		return ExecutionResult{TimedOut: true}, ctx.Err()
	case err = <-done:
	}

	duration := time.Since(startTime)

	result := ExecutionResult{
		ExitCode:   cmd.ProcessState.ExitCode(),
		Duration:   duration,
		StdoutPath: filepath.Join(s.workDir, "output", "stdout.log"),
		StderrPath: filepath.Join(s.workDir, "output", "stderr.log"),
		TimedOut:   false,
	}

	if err != nil {
		result.Signal = "signal"
	}

	if err := s.gatherResourceUsage(cmd, &result); err != nil {
	}

	return result, nil
}

func (s *Sandbox) filterEnvironment(env []string) []string {
	blocked := make(map[string]bool)
	for _, v := range s.config.BlockedEnvVars {
		blocked[v] = true
	}

	allowed := make(map[string]bool)
	for _, v := range s.config.AllowedEnvVars {
		allowed[v] = true
	}

	var filtered []string
	for _, e := range env {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := parts[0]

		if blocked[key] {
			continue
		}

		if len(s.config.AllowedEnvVars) > 0 && !allowed[key] {
			continue
		}

		filtered = append(filtered, e)
	}

	filtered = append(filtered, "HOME="+s.workDir)
	filtered = append(filtered, "TMPDIR="+filepath.Join(s.workDir, "tmp"))

	return filtered
}

func (s *Sandbox) gatherResourceUsage(cmd *exec.Cmd, result *ExecutionResult) error {
	if cmd.Process == nil {
		return nil
	}

	pid := cmd.Process.Pid
	procPath := filepath.Join("/proc", strconv.Itoa(pid), "status")

	data, err := os.ReadFile(procPath)
	if err != nil {
		return nil
	}

	lines := bytes.Split(data, []byte("\n"))
	for _, line := range lines {
		fields := bytes.SplitN(line, []byte(":"), 2)
		if len(fields) != 2 {
			continue
		}

		key := string(bytes.TrimSpace(fields[0]))
		value := string(bytes.TrimSpace(fields[1]))

		switch key {
		case "VmRSS":
			if idx := strings.Index(value, "kB"); idx > 0 {
				mem, err := strconv.ParseInt(strings.TrimSpace(value[:idx]), 10, 64)
				if err == nil {
					result.MemoryBytes = mem * 1024
				}
			}
		case "voluntary_ctxt_switches":
			parts := strings.Fields(value)
			if len(parts) > 0 {
				switches, err := strconv.ParseInt(parts[0], 10, 64)
				if err == nil {
					result.ContextSwitches = switches
				}
			}
		}
	}

	return nil
}

func (s *Sandbox) Cleanup() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.workDir == "" {
		return nil
	}

	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}

	return os.RemoveAll(s.workDir)
}

func (s *Sandbox) WorkDir() string {
	return s.workDir
}
