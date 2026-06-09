package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"sentra-agent/cmd/agent/executor/v2"
	runtimev2 "sentra-agent/cmd/agent/runtime/v2"
	"sentra-agent/internal/obs"
)

// ---------------------------------------------------------------------
// Job metadata (comes from backend payload)
// ---------------------------------------------------------------------

type JobMeta struct {
	JobType   string `json:"job_type"`   // process | process_dataset | merge_dataset | scan_dataset
	ChunkID   string `json:"chunk_id"`   // required for chunk-level jobs (process)
	DatasetID string `json:"dataset_id"` // required for dataset-level jobs (process_dataset, merge_dataset)
}

// ---------------------------------------------------------------------
// Metadata extraction & validation
// ---------------------------------------------------------------------

func extractJobMeta(payload json.RawMessage, envelopeJobType ...string) (*JobMeta, error) {

	var meta JobMeta

	// ============================================================
	// HARDENED VALIDATION: Check payload before parsing
	// ============================================================
	
	// Payload must not be empty
	if len(payload) == 0 {
		return nil, errors.New("empty payload - job cannot be processed")
	}
	
	// Must be valid JSON
	if !json.Valid(payload) {
		return nil, errors.New("invalid JSON payload - job cannot be parsed")
	}

	// Parse payload into meta to extract ChunkID and DatasetID
	if err := json.Unmarshal(payload, &meta); err != nil {
		return nil, fmt.Errorf("failed to parse job payload: %w", err)
	}

	if meta.JobType == "" {
		if len(envelopeJobType) > 0 && envelopeJobType[0] != "" {
			meta.JobType = envelopeJobType[0]
		} else {
			return nil, errors.New("job_type is required")
		}
	}

	// Normalize job types
	normalizedType := meta.JobType
	if normalizedType == "process_dataset" {
		normalizedType = "process"
		meta.JobType = "process"
	}

	switch meta.JobType {

	case "process":
		// chunk-level job - chunk_id is required
		if meta.ChunkID == "" && meta.DatasetID == "" {
			return nil, errors.New("process job missing chunk_id or dataset_id")
		}

	case "merge_dataset":
		if meta.DatasetID == "" {
			return nil, errors.New("merge_dataset job missing dataset_id")
		}

	case "preprocess":
		// preprocess jobs are handled below in ExecuteJob

	case "scan_dataset":
		// scan_dataset jobs allowed without chunk_id

	default:
		return nil, fmt.Errorf("unknown job_type: %s", meta.JobType)
	}

	return &meta, nil
}

var executorInstance *executor.Executor

func SetExecutor(e *executor.Executor) {
	executorInstance = e
	if e != nil && e.GetRuntimeManager() != nil {
		SetRuntimeManager(e.GetRuntimeManager())
	}
}

func init() {
	sandboxBase := os.Getenv("SENTRA_SANDBOX_BASE")
	if sandboxBase == "" {
		sandboxBase = filepath.Join(os.Getenv("HOME"), ".sentra", "sandbox")
	}
	executorInstance = executor.NewExecutor(sandboxBase, 10*time.Minute)
	if executorInstance.GetRuntimeManager() != nil {
		SetRuntimeManager(executorInstance.GetRuntimeManager())
	}
}

// ---------------------------------------------------------------------
// ExecuteJob — SINGLE public entry point
// ---------------------------------------------------------------------

func ExecuteJob(
	ctx context.Context,
	jobType string,
	payload json.RawMessage,
	executionID string,
) error {

	// -------------------------------------------------------------
	// Context cancellation check
	// -------------------------------------------------------------

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// -------------------------------------------------------------
	// Inject execution_id into context for handlers
	// -------------------------------------------------------------
	ctx = obs.WithExecutionID(ctx, executionID)

	// -------------------------------------------------------------
	// Validate & extract metadata
	// -------------------------------------------------------------

	meta, err := extractJobMeta(payload, jobType)
	if err != nil {
		return err
	}

	// Defensive check: envelope vs payload
	if jobType != "" {
		normalizedEnvelopeType := jobType
		if normalizedEnvelopeType == "process_dataset" {
			normalizedEnvelopeType = "process"
		}
		if normalizedEnvelopeType != meta.JobType {
			return fmt.Errorf(
				"job_type mismatch: envelope=%s payload=%s",
				jobType,
				meta.JobType,
			)
		}
	}

	// -------------------------------------------------------------
	// Parse full job payload for v2 executor
	// -------------------------------------------------------------

	var fullJob struct {
		ID              string `json:"id"`
		OrgID           string `json:"org_id"`
		ExecutionID     string `json:"execution_id"`
		ExecutionStepID string `json:"execution_step_id"`
		RunID           string `json:"run_id"`
		AttemptNumber   int    `json:"attempt_number"`
		RuntimeType     string `json:"runtime_type"`
		RuntimeDeps     []struct {
			Name    string `json:"name"`
			Version string `json:"version"`
			Source  string `json:"source"`
		} `json:"runtime_dependencies"`
		ExecutionMode    string `json:"execution_mode"`
		PluginID         string `json:"plugin_id"`
		PluginCode       string `json:"plugin_code"`
		ExecutionTimeout int    `json:"execution_timeout_seconds"`
		DependencyHash   string `json:"dependency_hash"`
		EnvironmentID    string `json:"environment_id"`
		Trusted          bool   `json:"trusted,omitempty"`
		ExecutionPolicy  *struct {
			MaxRetries            int `json:"max_retries"`
			RetryBackoffSeconds   int `json:"retry_backoff_seconds"`
			DefaultTimeoutSeconds int `json:"default_timeout_seconds"`
			HardTimeoutSeconds    int `json:"hard_timeout_seconds"`
		} `json:"execution_policy"`
	}

	if err := json.Unmarshal(payload, &fullJob); err != nil {
		return fmt.Errorf("failed to parse full job payload: %w", err)
	}

	execJob := executor.Job{
		Type:            meta.JobType,
		Payload:         make(map[string]interface{}),
		RuntimeType:     fullJob.RuntimeType,
		ExecutionMode:   fullJob.ExecutionMode,
		TimeoutSeconds:  fullJob.ExecutionTimeout,
		PluginID:        fullJob.PluginID,
		PluginCode:      fullJob.PluginCode,
		ExecutionID:     fullJob.ExecutionID,
		ExecutionStepID: fullJob.ExecutionStepID,
		RunID:           fullJob.RunID,
		AttemptNumber:   fullJob.AttemptNumber,
		OrgID:           fullJob.OrgID,
		DependencyHash:  fullJob.DependencyHash,
		EnvironmentID:   fullJob.EnvironmentID,
		Trusted:         fullJob.Trusted,
	}

	if fullJob.ID != "" {
		execJob.ID = fullJob.ID
	} else if fullJob.ExecutionID != "" {
		execJob.ID = fullJob.ExecutionID
	} else {
		execJob.ID = meta.ChunkID
		if execJob.ID == "" {
			execJob.ID = meta.DatasetID
		}
	}

	if fullJob.RuntimeDeps != nil {
		execJob.RuntimeDeps = make([]runtimev2.Dependency, len(fullJob.RuntimeDeps))
		for i, d := range fullJob.RuntimeDeps {
			execJob.RuntimeDeps[i] = runtimev2.Dependency{Name: d.Name, Version: d.Version, Source: d.Source}
		}
	}

	if fullJob.ExecutionPolicy != nil {
		execJob.ExecutionPolicy = &executor.ExecutionPolicy{
			MaxRetries:            fullJob.ExecutionPolicy.MaxRetries,
			RetryBackoffSeconds:   fullJob.ExecutionPolicy.RetryBackoffSeconds,
			DefaultTimeoutSeconds: fullJob.ExecutionPolicy.DefaultTimeoutSeconds,
			HardTimeoutSeconds:    fullJob.ExecutionPolicy.HardTimeoutSeconds,
		}
	}

	// Preprocess jobs are completed immediately (marker jobs)
	if meta.JobType == "preprocess" {
		obs.Info("preprocess job — completing immediately", obs.Field{
			"chunk_id":   meta.ChunkID,
			"dataset_id": meta.DatasetID,
		})
		return nil
	}

	// Route scan_dataset through built-in handler (no v2 executor needed)
	if meta.JobType == "scan_dataset" {
		obs.Info("routing scan_dataset to built-in handler", obs.Field{
			"job_type":   meta.JobType,
			"dataset_id": meta.DatasetID,
		})
		return executeScanDataset(ctx, payload)
	}

	// Route merge_dataset through built-in handler (no v2 executor needed)
	if meta.JobType == "merge_dataset" {
		obs.Info("routing merge_dataset to built-in handler", obs.Field{
			"job_type":   meta.JobType,
			"dataset_id": meta.DatasetID,
		})
		return executeMergeDataset(ctx, payload)
	}

	// Route chunk-level process jobs through native handler when they have
	// chunk-level fields and no inline plugin_code
	if meta.JobType == "process" && fullJob.PluginCode == "" && meta.ChunkID != "" {
		obs.Info("routing chunk-level process to native handler", obs.Field{
			"chunk_id":   meta.ChunkID,
			"dataset_id": meta.DatasetID,
			"plugin_id":  fullJob.PluginID,
		})
		// Inject job_id into payload so native handler can use it
		if fullJob.ID != "" {
			var payloadMap map[string]interface{}
			if err := json.Unmarshal(payload, &payloadMap); err == nil {
				payloadMap["job_id"] = fullJob.ID
				if injected, err := json.Marshal(payloadMap); err == nil {
					return executeProcessChunk(ctx, injected)
				}
			}
		}
		return executeProcessChunk(ctx, payload)
	}

	// Step-level process_dataset jobs (no chunk_id) — they are coordination markers.
	// The actual chunk work is done via batch_chunks trigger → chunk-level agent_jobs.
	// Completing immediately lets advance_pipeline proceed to plan_dataset_chunks.
	if meta.JobType == "process" && meta.ChunkID == "" && meta.DatasetID != "" {
		obs.Info("step-level process_dataset job — no chunk data, completing immediately", obs.Field{
			"dataset_id": meta.DatasetID,
			"plugin_id":  fullJob.PluginID,
		})
		return nil
	}

	result, err := executorInstance.ExecuteJob(ctx, execJob)
	if err != nil {
		return err
	}
	if !result.Success {
		errMsg := result.Error
		if errMsg == "" {
			errMsg = "plugin execution returned failure without error details"
		}
		return fmt.Errorf("%s: %s", result.ErrorClassification, errMsg)
	}
	return nil
}
