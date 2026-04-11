package models

import (
	"encoding/json"
	"time"
)

// -----------------------------------------------------------------------------
// DeviceMetrics describes live metrics from each device.
// -----------------------------------------------------------------------------
type DeviceMetrics struct {
	DeviceID          string  `json:"device_id"`
	OrgID             string  `json:"org_id,omitempty"`
	CPUFree           float64 `json:"cpu_free"`
	MemoryFreeGB      float64 `json:"memory_free_gb"`
	TotalMemoryGB     float64 `json:"total_memory_gb,omitempty"`
	GPUAvailable      bool    `json:"gpu_available"`
	GPUMemoryFreeGB   float64 `json:"gpu_memory_free_gb,omitempty"`
	GPUMemoryTotalGB  float64 `json:"gpu_memory_total_gb,omitempty"`
	IOThroughputMBps  float64 `json:"io_throughput_mb_s,omitempty"`
	NetworkLatencyMs  float64 `json:"network_latency_ms"`
	CPUUsagePercent   float64 `json:"cpu_usage_percent,omitempty"`
	ActiveWorkers     int     `json:"active_workers,omitempty"`
	MaxConcurrency    int     `json:"max_concurrency,omitempty"`
	LastUpdated       string  `json:"last_updated,omitempty"`
	FirmwareVersion   string  `json:"firmware_version,omitempty"`
	RecommendedWorker int     `json:"recommended_worker,omitempty"`
}

// -----------------------------------------------------------------------------
// Job describes a logical unit of work payload (plugin-level).
// -----------------------------------------------------------------------------
type Job struct {
	ID                 string  `json:"id"`
	DatasetID          string  `json:"dataset_id,omitempty"`
	ChunkID            string  `json:"chunk_id,omitempty"`
	ComplexityScore    float64 `json:"complexity_score,omitempty"`
	RequiresModel      bool    `json:"requires_model,omitempty"`
	PreferredPrecision string  `json:"preferred_precision,omitempty"`
	DataSizeMB         float64 `json:"data_size_mb,omitempty"`
	JobType            string  `json:"job_type"`
	OrgID              string  `json:"org_id,omitempty"`
	DeviceID           string  `json:"device_id,omitempty"`
	AssignedAt         string  `json:"assigned_at,omitempty"`
	DeadlineAt         string  `json:"deadline_at,omitempty"`
}

// -----------------------------------------------------------------------------
// AgentJob maps EXACTLY to public.agent_jobs (Supabase realtime-safe).
// -----------------------------------------------------------------------------
type AgentJob struct {
	ID        string          `json:"id"`
	AgentID   string          `json:"agent_id"`
	OrgID     string          `json:"org_id"`
	JobType   string          `json:"job_type"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"`
	Completed bool            `json:"completed"`

	AssignedAt *time.Time `json:"assigned_at"`
	StartedAt  *time.Time `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// -----------------------------------------------------------------------------
// AgentPolicyResponse represents /agent_health_policy output.
// -----------------------------------------------------------------------------
type AgentPolicyResponse struct {
	Concurrency int     `json:"concurrency"`
	LoadFactor  float64 `json:"load_factor,omitempty"`
	Notes       string  `json:"notes,omitempty"`
}
