package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"sentra-agent/internal/httpclient"
	"sentra-agent/internal/sanitize"
)

type APIResponse struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type ExecutionClient struct {
	baseURL     string
	anonKey     string
	deviceToken string
	deviceID    string
	orgID       string
	http        *http.Client
	httpc       *httpclient.Client
}

func NewExecutionClient(baseURL, anonKey, deviceToken, deviceID, orgID string) *ExecutionClient {
	return &ExecutionClient{
		baseURL:     baseURL,
		anonKey:     anonKey,
		deviceToken: deviceToken,
		deviceID:    deviceID,
		orgID:       orgID,
		http: &http.Client{
			Timeout: 10 * time.Second,
		},
		httpc: httpclient.NewClient(baseURL, anonKey, deviceToken),
	}
}

func (c *ExecutionClient) post(ctx context.Context, path string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := c.httpc.PostWithHeaders(ctx, path, body, func(r *http.Request) {
		r.Header.Set("x-device-id", c.deviceID)
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	log.Printf("[EXEC-CLIENT] POST %s → HTTP %d | body: %s", path, resp.StatusCode, string(respBody))

	if resp.StatusCode >= 300 {
		var apiResp APIResponse
		if json.Unmarshal(respBody, &apiResp) == nil {
			if apiResp.Error != "" {
				return fmt.Errorf("API error: %s", apiResp.Error)
			}
		}
		return fmt.Errorf("POST %s failed: HTTP %d: %s", path, resp.StatusCode, string(respBody))
	}

	return nil
}

type AssignedJob struct {
	JobID       string          `json:"job_id"`
	JobType    string          `json:"job_type"`
	Payload    json.RawMessage `json:"payload"`
	LeaseTTL   int             `json:"lease_ttl_seconds"`
	ExecutionID string          `json:"execution_id,omitempty"`
}

func (c *ExecutionClient) RequestJobAssignment(ctx context.Context) (*AssignedJob, error) {
	payload := map[string]any{
		"org_id":       c.orgID,
		"requested_at": time.Now().UTC(),
	}

	body, _ := json.Marshal(payload)

	resp, err := c.httpc.PostWithHeaders(ctx, "/functions/v1/assign_agent_job", body, func(r *http.Request) {
		r.Header.Set("x-device-id", c.deviceID)
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	log.Printf("[EXEC-CLIENT] assign_agent_job → HTTP %d | body: %s", resp.StatusCode, string(respBody))

	if resp.StatusCode >= 300 {
		var apiResp APIResponse
		if json.Unmarshal(respBody, &apiResp) == nil {
			if apiResp.Error != "" {
				return nil, fmt.Errorf("assign_agent_job error: %s", apiResp.Error)
			}
		}
		return nil, fmt.Errorf("assign_agent_job failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		Ok      bool          `json:"ok"`
		Result  AssignedJob   `json:"result"`
		Error   string        `json:"error,omitempty"`
		JobID   string        `json:"assigned_job_id"`
		JobType string        `json:"job_type"`
		Payload json.RawMessage `json:"payload"`
		ExecutionID string    `json:"execution_id"`
	}

	// Try parsing response
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("invalid JSON from assign_agent_job: %w | body=%s", err, string(respBody))
	}

	// Handle both wrapped (result) and flat (top-level) response
	var assignedJob AssignedJob
	if result.Result.JobID != "" {
		assignedJob = result.Result
	} else if result.JobID != "" {
		assignedJob = AssignedJob{
			JobID:       result.JobID,
			JobType:    result.JobType,
			Payload:   result.Payload,
			ExecutionID: result.ExecutionID,
		}
	} else if !result.Ok && result.Error != "" {
		return nil, fmt.Errorf("assign_agent_job rejected: %s", result.Error)
	} else if !result.Ok {
		return nil, fmt.Errorf("assign_agent_job failed: no job assigned")
	}

	return &assignedJob, nil
}

type JobLeaseStatus struct {
	JobID   string `json:"job_id"`
	Status  string `json:"status"`
	AgentID string `json:"agent_id"`
	IsValid bool   `json:"is_valid"`
}

func (c *ExecutionClient) VerifyJobLease(ctx context.Context, jobID string) (*JobLeaseStatus, error) {
	payload := map[string]any{
		"job_id":   jobID,
		"agent_id": c.deviceID,
	}

	body, _ := json.Marshal(payload)

	resp, err := c.httpc.PostWithHeaders(ctx, "/functions/v1/verify_job_lease", body, func(r *http.Request) {
		r.Header.Set("x-device-id", c.deviceID)
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return &JobLeaseStatus{
			JobID:   jobID,
			IsValid: false,
		}, nil
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	log.Printf("[EXEC-CLIENT] verify_job_lease → HTTP %d | body: %s", resp.StatusCode, string(respBody))

	if resp.StatusCode >= 300 {
		var apiResp APIResponse
		if json.Unmarshal(respBody, &apiResp) == nil {
			if apiResp.Error != "" {
				return nil, fmt.Errorf("verify_job_lease error: %s", apiResp.Error)
			}
		}
		return nil, fmt.Errorf("verify_job_lease failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var status JobLeaseStatus
	if err := json.Unmarshal(respBody, &status); err != nil {
		return nil, fmt.Errorf("invalid JSON from verify_job_lease: %w | body=%s", err, string(respBody))
	}

	return &status, nil
}

type DeviceHeartbeat struct {
	DeviceID         string  `json:"device_id"`
	TotalCPUCores    int     `json:"total_cpu_cores"`
	CPUCoresFree     int     `json:"cpu_cores_free"`
	TotalMemoryGB    float64 `json:"total_memory_gb"`
	MemoryFreeGB     float64 `json:"memory_free_gb"`
	NetworkLatencyMs float64 `json:"network_latency_ms"`
	GPUAvailable     bool    `json:"gpu_available"`
	CPUUsagePercent  float64 `json:"cpu_usage_percent"`
	IncomingWorkload int     `json:"incoming_workload_weight"`
	ActiveWorkers    int     `json:"active_workers"`
	PythonVersion    string  `json:"python_version,omitempty"`
	NodeVersion      string  `json:"node_version,omitempty"`
	DockerAvailable  bool    `json:"docker_available"`
	RuntimeSupported bool    `json:"runtime_supported"`
}

type HealthPolicyResult struct {
	Ok          bool    `json:"ok"`
	Concurrency int     `json:"concurrency"`
	LoadFactor  float64 `json:"load_factor,omitempty"`
	Error       string  `json:"error,omitempty"`
}

func (c *ExecutionClient) SendDeviceHeartbeat(ctx context.Context, hb DeviceHeartbeat) (HealthPolicyResult, error) {
	body, err := json.Marshal(hb)
	if err != nil {
		return HealthPolicyResult{}, err
	}

	resp, err := c.httpc.PostWithHeaders(ctx, "/functions/v1/agent_health_policy", body, func(r *http.Request) {
		r.Header.Set("x-device-id", c.deviceID)
	})
	if err != nil {
		return HealthPolicyResult{}, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return HealthPolicyResult{}, fmt.Errorf("failed to read response: %w", err)
	}

	log.Printf("[EXEC-CLIENT] agent_health_policy → HTTP %d | body: %s", resp.StatusCode, string(respBody))

	if resp.StatusCode >= 300 {
		var apiResp APIResponse
		if json.Unmarshal(respBody, &apiResp) == nil {
			if apiResp.Error != "" {
				return HealthPolicyResult{}, fmt.Errorf("agent_health_policy error: %s", apiResp.Error)
			}
		}
		return HealthPolicyResult{}, fmt.Errorf("agent_health_policy failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var result HealthPolicyResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return HealthPolicyResult{}, fmt.Errorf("invalid JSON from agent_health_policy: %w | body=%s", err, string(respBody))
	}

	if !result.Ok && result.Error != "" {
		return HealthPolicyResult{}, fmt.Errorf("agent_health_policy rejected: %s", result.Error)
	}

	return result, nil
}

type RelayJobEvent struct {
	Channel string `json:"channel"`
	Data    any    `json:"data"`
}

type JobStart struct {
	Type      string    `json:"type"`
	JobID     string    `json:"job_id"`
	StartedAt time.Time `json:"started_at"`
}

type JobHeartbeat struct {
	Type      string    `json:"type"`
	JobID     string    `json:"job_id"`
	Timestamp time.Time `json:"timestamp"`
}

type JobComplete struct {
	Type       string    `json:"type"`
	JobID      string    `json:"job_id"`
	DurationMs int64     `json:"duration_ms"`
	FinishedAt time.Time `json:"finished_at"`
	Result     any       `json:"result,omitempty"`
}

type JobFail struct {
	Type         string    `json:"type"`
	JobID        string    `json:"job_id"`
	ErrorMessage string    `json:"error_message"`
	FinishedAt   time.Time `json:"finished_at"`
}

func (c *ExecutionClient) ReportJobStart(ctx context.Context, jobID string) error {
	return c.post(ctx, "/functions/v1/relay_job_event", RelayJobEvent{
		Channel: "agent-" + c.deviceID,
		Data: JobStart{
			Type:      "job_started",
			JobID:     jobID,
			StartedAt: time.Now().UTC(),
		},
	})
}

func (c *ExecutionClient) SendJobExecutionHeartbeat(ctx context.Context, jobID string) error {
	return c.post(ctx, "/functions/v1/relay_job_event", RelayJobEvent{
		Channel: "agent-" + c.deviceID,
		Data: JobHeartbeat{
			Type:      "job_heartbeat",
			JobID:     jobID,
			Timestamp: time.Now().UTC(),
		},
	})
}

func (c *ExecutionClient) ReportJobComplete(ctx context.Context, jobID string, durationMs int64, result any) error {
	return c.post(ctx, "/functions/v1/relay_job_event", RelayJobEvent{
		Channel: "agent-" + c.deviceID,
		Data: JobComplete{
			Type:       "job_completed",
			JobID:      jobID,
			DurationMs: durationMs,
			FinishedAt: time.Now().UTC(),
			Result:     result,
		},
	})
}

func (c *ExecutionClient) ReportJobFailure(ctx context.Context, jobID string, err error) error {
	sanitizedError := sanitize.ErrorMessage(err.Error())
	return c.post(ctx, "/functions/v1/relay_job_event", RelayJobEvent{
		Channel: "agent-" + c.deviceID,
		Data: JobFail{
			Type:         "job_failed",
			JobID:        jobID,
			ErrorMessage: sanitizedError,
			FinishedAt:   time.Now().UTC(),
		},
	})
}

func (c *ExecutionClient) MarkChunkRunning(ctx context.Context, chunkID string) error {
	return c.post(ctx, "/functions/v1/relay_job_event", RelayJobEvent{
		Channel: "agent-" + c.deviceID,
		Data: map[string]any{
			"type":     "chunk_running",
			"chunk_id": chunkID,
		},
	})
}

func (c *ExecutionClient) MarkChunkDone(ctx context.Context, chunkID string) error {
	return c.post(ctx, "/functions/v1/relay_job_event", RelayJobEvent{
		Channel: "agent-" + c.deviceID,
		Data: map[string]any{
			"type":     "chunk_done",
			"chunk_id": chunkID,
		},
	})
}

func (c *ExecutionClient) MarkChunkFailed(ctx context.Context, chunkID string, err error) error {
	return c.post(ctx, "/functions/v1/relay_job_event", RelayJobEvent{
		Channel: "agent-" + c.deviceID,
		Data: map[string]any{
			"type":     "chunk_failed",
			"chunk_id": chunkID,
			"error":    err.Error(),
		},
	})
}

func (c *ExecutionClient) EmitEvent(ctx context.Context, eventType string, data any) error {
	return c.post(ctx, "/functions/v1/relay_job_event", RelayJobEvent{
		Channel: "agent-" + c.deviceID,
		Data: map[string]any{
			"type": eventType,
			"data": data,
		},
	})
}

func (c *ExecutionClient) RecordPluginExecutionStart(ctx context.Context, orgID, pluginID, jobID, deviceID string) (string, error) {
	type StartRequest struct {
		OrgID    string `json:"p_org_id"`
		PluginID string `json:"p_plugin_id"`
		JobID    string `json:"p_job_id"`
		DeviceID string `json:"p_device_id"`
	}

	body, _ := json.Marshal(StartRequest{
		OrgID:    orgID,
		PluginID: pluginID,
		JobID:    jobID,
		DeviceID: deviceID,
	})

	resp, err := c.httpc.PostWithHeaders(ctx, "/rest/v1/rpc/record_plugin_execution_start", body, func(r *http.Request) {
		r.Header.Set("x-device-id", c.deviceID)
		r.Header.Set("apikey", c.anonKey)
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}

	log.Printf("[EXEC-CLIENT] record_plugin_execution_start → HTTP %d | body: %s", resp.StatusCode, string(respBody))

	if resp.StatusCode >= 300 {
		var apiResp APIResponse
		if json.Unmarshal(respBody, &apiResp) == nil {
			if apiResp.Error != "" {
				return "", fmt.Errorf("record_plugin_execution_start error: %s", apiResp.Error)
			}
		}
		return "", fmt.Errorf("record_plugin_execution_start failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var result struct {
		ExecutionID string `json:"execution_id"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("invalid JSON from record_plugin_execution_start: %w | body=%s", err, string(respBody))
	}

	return result.ExecutionID, nil
}

func (c *ExecutionClient) RecordPluginExecutionEnd(ctx context.Context, executionID, status, errMsg string) error {
	type EndRequest struct {
		ExecutionID string `json:"p_execution_id"`
		Status      string `json:"p_status"`
		Error       string `json:"p_error"`
	}

	body, _ := json.Marshal(EndRequest{
		ExecutionID: executionID,
		Status:      status,
		Error:       errMsg,
	})

	resp, err := c.httpc.PostWithHeaders(ctx, "/rest/v1/rpc/record_plugin_execution_end", body, func(r *http.Request) {
		r.Header.Set("x-device-id", c.deviceID)
		r.Header.Set("apikey", c.anonKey)
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	log.Printf("[EXEC-CLIENT] record_plugin_execution_end → HTTP %d | body: %s", resp.StatusCode, string(respBody))

	if resp.StatusCode >= 300 {
		var apiResp APIResponse
		if json.Unmarshal(respBody, &apiResp) == nil {
			if apiResp.Error != "" {
				return fmt.Errorf("record_plugin_execution_end error: %s", apiResp.Error)
			}
		}
		return fmt.Errorf("record_plugin_execution_end failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// CompleteJob calls the complete_job Edge Function to update job status in DB
// This validates lease at completion time to prevent expired lease holders from overwriting
type execCompleteJobRequest struct {
	ExecutionID string          `json:"execution_id"`
	Status      string          `json:"status"`
	DurationMs  int64           `json:"duration_ms,omitempty"`
	Output      json.RawMessage `json:"output,omitempty"`
	Error       string          `json:"error,omitempty"`
}

type execCompleteJobResponse struct {
	Ok         bool   `json:"ok"`
	JobID      string `json:"job_id"`
	Status     string `json:"status"`
	Idempotent bool   `json:"idempotent"`
	Error      string `json:"error,omitempty"`
}

// CompleteJobResult is a wrapper that can indicate lease expiration specifically
type CompleteJobResult struct {
	Response        *execCompleteJobResponse
	IsLeaseExpired  bool
	IsAlreadyDone   bool
	IsConcurrentMod bool // State transition failed
	Err             error
}

// IsStaleExecution returns true if this result indicates the job was re-claimed
// and should NOT be retried
func (r *CompleteJobResult) IsStaleExecution() bool {
	return r.IsLeaseExpired || r.IsAlreadyDone
}

// IsRetryable returns true if the error can be safely retried
func (r *CompleteJobResult) IsRetryable() bool {
	if r.Err == nil {
		return false
	}
	// These are NOT retryable
	if r.IsLeaseExpired {
		return false
	}
	if r.IsAlreadyDone {
		return false
	}
	if r.IsConcurrentMod {
		return false // Concurrent modification means another process updated - check state first
	}
	return true
}

// ErrorCode represents specific error types
type ErrorCode string

const (
	ErrCodeLeaseExpired      ErrorCode = "LEASE_EXPIRED"
	ErrCodeAlreadyDone       ErrorCode = "ALREADY_COMPLETE"
	ErrCodeConcurrentMod     ErrorCode = "CONCURRENT_MODIFICATION"
	ErrCodeAuthorizationFail ErrorCode = "AUTHORIZATION_FAILURE"
	ErrCodeJobNotFound       ErrorCode = "JOB_NOT_FOUND"
	ErrCodeInternalError     ErrorCode = "INTERNAL_ERROR"
)

func (c *ExecutionClient) CompleteJob(ctx context.Context, executionID string, status string, durationMs int64, resultData any) *CompleteJobResult {
	reqBody := execCompleteJobRequest{
		ExecutionID: executionID,
		Status:      status,
		DurationMs:  durationMs,
		// Also include empty values - complete_job edge function can accept job_id as alternative
	}

	// Also pass via context headers for edge function to use as fallback
	if executionID == "" {
		// Try to get from elsewhere - but for now pass empty
	}

	if resultData != nil {
		resultJSON, err := json.Marshal(resultData)
		if err != nil {
			return &CompleteJobResult{Err: err}
		}
		reqBody.Output = resultJSON
	}

	body, _ := json.Marshal(reqBody)

	resp, err := c.httpc.PostWithHeaders(ctx, "/functions/v1/complete_job", body, func(r *http.Request) {
		r.Header.Set("x-device-id", c.deviceID)
	})
	if err != nil {
		return &CompleteJobResult{Err: err}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return &CompleteJobResult{Err: fmt.Errorf("failed to read response: %w", err)}
	}

	log.Printf("[EXEC-CLIENT] complete_job → HTTP %d | body: %s", resp.StatusCode, string(respBody))

	// Parse response body for error details
	var apiResp struct {
		Ok    bool   `json:"ok"`
		Error string `json:"error,omitempty"`
		Code  string `json:"code,omitempty"`
	}
	var completeResult execCompleteJobResponse
	json.Unmarshal(respBody, &apiResp)
	json.Unmarshal(respBody, &completeResult)

	// Handle based on HTTP status code
	if resp.StatusCode == 409 {
		// HTTP 409 Conflict - check error code
		errMsg := apiResp.Error
		if errMsg == "" {
			errMsg = completeResult.Error
		}

		switch ErrorCode(apiResp.Code) {
		case ErrCodeAlreadyDone:
			// Job already completed - treat as success
			return &CompleteJobResult{
				IsAlreadyDone: true,
				Err:           fmt.Errorf("job already in terminal state: %s", errMsg),
			}
		case ErrCodeConcurrentMod:
			// Concurrent modification
			return &CompleteJobResult{
				IsConcurrentMod: true,
				Err:             fmt.Errorf("state transition failed - concurrent modification: %s", errMsg),
			}
		case ErrCodeLeaseExpired:
			// Lease expired
			return &CompleteJobResult{
				IsLeaseExpired: true,
				Err:            fmt.Errorf("lease expired: %s", errMsg),
			}
		default:
			// Fallback: check error message content
			if strings.Contains(errMsg, "terminal state") ||
				strings.Contains(errMsg, "already completed") ||
				strings.Contains(errMsg, "already dead") {
				return &CompleteJobResult{
					IsAlreadyDone: true,
					Err:           fmt.Errorf("job already in terminal state: %s", errMsg),
				}
			}
			return &CompleteJobResult{
				IsLeaseExpired: true,
				Err:            fmt.Errorf("completion rejected: %s", errMsg),
			}
		}
	}

	if resp.StatusCode == 403 {
		return &CompleteJobResult{
			Err: fmt.Errorf("authorization failed: %s", apiResp.Error),
		}
	}

	if resp.StatusCode == 404 {
		return &CompleteJobResult{
			Err: fmt.Errorf("job not found: %s", apiResp.Error),
		}
	}

	if resp.StatusCode >= 300 {
		return &CompleteJobResult{Err: fmt.Errorf("complete_job failed: HTTP %d: %s", resp.StatusCode, apiResp.Error)}
	}

	// Success case
	return &CompleteJobResult{Response: &completeResult}
}
