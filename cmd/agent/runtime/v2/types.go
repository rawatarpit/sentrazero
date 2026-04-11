package runtime

import (
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
	Type               RuntimeType     `json:"type"`
	Version            string          `json:"version"`
	DependencyLockHash string          `json:"dependency_lock_hash"`
	Dependencies       []Dependency    `json:"dependencies"`
	EntryPoint         string          `json:"entry_point"`
	Environment        map[string]bool `json:"environment"`
	Strict             bool            `json:"strict"`
}

type ExecutionInput struct {
	Input      map[string]interface{} `json:"input"`
	Config     map[string]interface{} `json:"config"`
	Metadata   map[string]interface{} `json:"metadata"`
	PluginCode string                 `json:"plugin_code,omitempty"`
}

type ExecutionOutput struct {
	Success        bool                   `json:"success"`
	Data           map[string]interface{} `json:"data"`
	Error          string                 `json:"error,omitempty"`
	ItemsProcessed int64                  `json:"items_processed,omitempty"`
	DurationMs     int64                  `json:"duration_ms,omitempty"`
}

type ExecutionMetrics struct {
	SetupTimeMs         int64   `json:"setup_time_ms"`
	DependencyInstallMs int64   `json:"dependency_install_ms"`
	ExecutionTimeMs     int64   `json:"execution_time_ms"`
	CleanupTimeMs       int64   `json:"cleanup_time_ms"`
	TotalTimeMs         int64   `json:"total_time_ms"`
	MaxMemoryBytes      int64   `json:"max_memory_bytes"`
	AvgCPUPercent       float64 `json:"avg_cpu_percent"`
	PeakCPUPercent      float64 `json:"peak_cpu_percent"`
	ExitCode            int     `json:"exit_code"`
	Signal              string  `json:"signal,omitempty"`
	OOMKilled           bool    `json:"oom_killed"`
	EnvReuse            bool    `json:"env_reuse"`
	EnvCreationTimeMs   int64   `json:"env_creation_time_ms"`
	CacheHit            bool    `json:"cache_hit"`
	ColdStartCount      int64   `json:"cold_start_count"`
	EnvKey              string  `json:"env_key"`
	FailureType         string  `json:"failure_type"`
}

type ExecutionResult struct {
	Output     ExecutionOutput
	Metrics    ExecutionMetrics
	Duration   time.Duration
	Throughput float64
	CleanedUp  bool
}
