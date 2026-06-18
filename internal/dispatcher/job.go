package dispatcher

import "encoding/json"

// Job payload (backend-owned schema)
// Core Rule: Paths are never stored - agents derive them locally using dataset_id + chunk_index
type Job struct {
	ID              string          `json:"job_id"`  // FIX: was "id,omitempty"
	Type            string          `json:"job_type"`  // FIX: was "type"
	OrgID           string          `json:"org_id,omitempty"`
	DatasetID       string          `json:"dataset_id,omitempty"`
	ChunkIndex      int             `json:"chunk_index,omitempty"`
	ChunkID         string          `json:"chunk_id,omitempty"`
	BatchID         string          `json:"batch_id,omitempty"`
	ExecutionID     string          `json:"execution_id,omitempty"`
	ExecutionStepID string          `json:"execution_step_id,omitempty"`
	StepIndex       int             `json:"step_index,omitempty"`
	StepConfig      json.RawMessage `json:"config,omitempty"`
	Rows            int             `json:"rows,omitempty"`
	Checksum        string          `json:"checksum,omitempty"`
	StorageMode     string          `json:"storage_mode,omitempty"`
	StorageType     string          `json:"storage_type,omitempty"`
	PluginName      string          `json:"plugin_name,omitempty"`
	PluginID        string          `json:"plugin_id,omitempty"`
	RetryAttempt    int             `json:"retry_attempt,omitempty"`
	TotalSteps      int             `json:"total_steps,omitempty"`
	InputPath       string          `json:"input_path,omitempty"`
	OutputPath      string          `json:"output_path,omitempty"`
	Payload         json.RawMessage `json:"payload"`
	SourcePath      string          `json:"source_path,omitempty"`
	StorageConfigID string          `json:"storage_config_id,omitempty"`
}

// PluginContext is the canonical input shape for all plugin binaries
type PluginContext struct {
	JobID       string          `json:"job_id"`
	OrgID       string          `json:"org_id"`
	DatasetID   string          `json:"dataset_id"`
	ExecutionID string          `json:"execution_id,omitempty"`
	StepIndex   int             `json:"step_index,omitempty"`
	ChunkID     string          `json:"chunk_id"`
	ChunkIndex  int             `json:"chunk_index"`
	InputPath   string          `json:"input_path"`
	OutputPath  string          `json:"output_path"`
	Config      json.RawMessage `json:"config,omitempty"`
}
