package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"sentra-agent/internal/httpclient"
	"sentra-agent/internal/obs"
	"sentra-agent/internal/sanitize"
	"sentra-agent/internal/sysinfo"
)

const (
	FunctionClaimDevice           = "/functions/v1/claim_device"
	FunctionAgentHealthPolicy     = "/functions/v1/agent_health_policy"
	FunctionAssignAgentJob        = "/functions/v1/assign_agent_job"
	FunctionClaimJobsForDevice    = "/functions/v1/claim_jobs_for_device"
	FunctionCompleteJob           = "/functions/v1/complete_job"
	FunctionReportJobError        = "/functions/v1/report_job_error"
	FunctionCleanupStuckJobs      = "/functions/v1/cleanup_stuck_jobs"
	FunctionRelayJobEvent         = "/functions/v1/relay_job_event"
	FunctionNotifyAvailableDevice = "/functions/v1/notify_available_device"
	FunctionGetStorageConfig      = "/functions/v1/get_storage_config"
)

const (
	TraceIDHeader = "x-trace-id"
)

type Client struct {
	baseURL     string
	anonKey     string
	deviceToken string
	deviceID    string
	orgID       string
	http        *http.Client
	httpc       *httpclient.Client
}

func NewClient(baseURL, anonKey, deviceToken, deviceID, orgID string) *Client {
	return &Client{
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

func (c *Client) post(ctx context.Context, path string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := c.httpc.PostWithHeaders(ctx, path, body, func(req *http.Request) {
		if traceID := obs.TraceID(ctx); traceID != "" {
			req.Header.Set(TraceIDHeader, traceID)
		}
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	log.Printf("[BACKEND-CLIENT] POST %s → HTTP %d | body: %s", path, resp.StatusCode, string(respBody))

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

func (c *Client) postWithDeviceID(ctx context.Context, path string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := c.httpc.PostWithHeaders(ctx, path, body, func(req *http.Request) {
		req.Header.Set("x-device-id", c.deviceID)
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	log.Printf("[BACKEND-CLIENT] POST %s → HTTP %d | body: %s", path, resp.StatusCode, string(respBody))

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

type HealthPolicyRequest struct {
	DeviceID         string  `json:"device_id"`
	TotalCPUCores    int     `json:"total_cpu_cores"`
	CPUCoresFree     int     `json:"cpu_cores_free"`
	TotalMemoryGB    float64 `json:"total_memory_gb"`
	MemoryFreeGB     float64 `json:"memory_free_gb"`
	NetworkLatencyMs float64 `json:"network_latency_ms"`
	GPUAvailable     bool    `json:"gpu_available"`
	CPUUsagePercent  float64 `json:"cpu_usage_percent"`
	IncomingWorkload int     `json:"incoming_workload_weight"`
}

type HealthPolicyResponse struct {
	Ok          bool    `json:"ok"`
	Concurrency int     `json:"concurrency"`
	LoadFactor  float64 `json:"load_factor,omitempty"`
	Error       string  `json:"error,omitempty"`
}

func (c *Client) SendHealthPolicy(ctx context.Context, req HealthPolicyRequest) (*HealthPolicyResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpc.PostWithHeaders(ctx, FunctionAgentHealthPolicy, body, func(r *http.Request) {
		r.Header.Set("x-device-id", c.deviceID)
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	log.Printf("[BACKEND-CLIENT] agent_health_policy → HTTP %d | body: %s", resp.StatusCode, string(respBody))

	if resp.StatusCode >= 300 {
		var apiResp APIResponse
		if json.Unmarshal(respBody, &apiResp) == nil {
			if apiResp.Error != "" {
				return nil, fmt.Errorf("agent_health_policy error: %s", apiResp.Error)
			}
		}
		return nil, fmt.Errorf("agent_health_policy failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var result HealthPolicyResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("invalid JSON from agent_health_policy: %w | body=%s", err, string(respBody))
	}

	if !result.Ok && result.Error != "" {
		return nil, fmt.Errorf("agent_health_policy rejected: %s", result.Error)
	}

	return &result, nil
}

type AssignJobRequest struct {
	OrgID       string    `json:"org_id"`
	RequestedAt time.Time `json:"requested_at"`
}

type AssignJobResponse struct {
	Ok       bool            `json:"ok"`
	JobID    string          `json:"job_id"`
	JobType  string          `json:"job_type"`
	Payload  json.RawMessage `json:"payload"`
	LeaseTTL int             `json:"lease_ttl_seconds"`
	Error    string          `json:"error,omitempty"`
}

func (c *Client) AssignJob(ctx context.Context) (*AssignJobResponse, error) {
	payload := AssignJobRequest{
		OrgID:       c.orgID,
		RequestedAt: time.Now().UTC(),
	}

	body, _ := json.Marshal(payload)

	resp, err := c.httpc.PostWithHeaders(ctx, FunctionAssignAgentJob, body, func(r *http.Request) {
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

	log.Printf("[BACKEND-CLIENT] assign_agent_job → HTTP %d | body: %s", resp.StatusCode, string(respBody))

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
		Ok     bool              `json:"ok"`
		Result AssignJobResponse `json:"result"`
		Error  string            `json:"error,omitempty"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("invalid JSON from assign_agent_job: %w | body=%s", err, string(respBody))
	}

	if !result.Ok && result.Error != "" {
		return nil, fmt.Errorf("assign_agent_job rejected: %s", result.Error)
	}

	return &result.Result, nil
}

type RelayEventRequest struct {
	Channel string `json:"channel"`
	Data    any    `json:"data"`
}

func (c *Client) EmitEvent(ctx context.Context, eventType string, data any) error {
	return c.post(ctx, FunctionRelayJobEvent, RelayEventRequest{
		Channel: "agent-" + c.deviceID,
		Data: map[string]any{
			"type": eventType,
			"data": data,
		},
	})
}

func (c *Client) ReportJobStart(ctx context.Context, jobID string) error {
	return c.post(ctx, FunctionRelayJobEvent, RelayEventRequest{
		Channel: "agent-" + c.deviceID,
		Data: map[string]any{
			"type":       "job_started",
			"job_id":     jobID,
			"started_at": time.Now().UTC(),
		},
	})
}

func (c *Client) ReportJobComplete(ctx context.Context, jobID string, durationMs int64, result any) error {
	return c.post(ctx, FunctionRelayJobEvent, RelayEventRequest{
		Channel: "agent-" + c.deviceID,
		Data: map[string]any{
			"type":        "job_completed",
			"job_id":      jobID,
			"duration_ms": durationMs,
			"finished_at": time.Now().UTC(),
			"result":      result,
		},
	})
}

func (c *Client) ReportJobFailure(ctx context.Context, jobID string, err error) error {
	sanitizedError := sanitize.ErrorMessage(err.Error())
	return c.post(ctx, FunctionRelayJobEvent, RelayEventRequest{
		Channel: "agent-" + c.deviceID,
		Data: map[string]any{
			"type":          "job_failed",
			"job_id":        jobID,
			"error_message": sanitizedError,
			"finished_at":   time.Now().UTC(),
		},
	})
}

type NotifyAvailableRequest struct {
	DeviceID      string         `json:"device_id"`
	Metrics       map[string]any `json:"metrics"`
	ActiveWorkers int            `json:"active_workers"`
}

func (c *Client) NotifyAvailable(ctx context.Context, maxWorkers, activeWorkers int, specs sysinfo.Specs) error {
	return c.post(ctx, FunctionNotifyAvailableDevice, NotifyAvailableRequest{
		DeviceID: c.deviceID,
		Metrics: map[string]any{
			"total_cpu_cores":    specs.CPUCores,
			"cpu_cores_free":     specs.AvailableCPUCores,
			"total_memory_gb":    specs.TotalMemoryGB,
			"memory_free_gb":     specs.AvailableMemoryGB,
			"cpu_usage_percent":  specs.CPUUsagePercent,
			"io_bandwidth_mb_s":  specs.IOThroughputMBps,
			"network_latency_ms": specs.NetworkLatency,
			"gpu_available":      specs.GPUModel != "",
		},
		ActiveWorkers: activeWorkers,
	})
}

type ClaimedJob struct {
	JobID        string          `json:"job_id"`
	JobType      string          `json:"job_type"`
	Payload      json.RawMessage `json:"payload"`
	DatasetID    string          `json:"dataset_id"`
	ChunkIndex   int             `json:"chunk_index"`
	ExecutionID  string          `json:"execution_id"`
	StepIndex    int             `json:"step_index"`
	LeaseExpires string          `json:"lease_expires_at"`
	IsReclaimed  bool            `json:"is_reclaimed"`
	RetryCount   int             `json:"retry_count"`
}

type ClaimJobsRequest struct {
	Limit           int `json:"limit"`
	LeaseTtlSeconds int `json:"lease_ttl_seconds"`
}

type ClaimJobsResponse struct {
	Ok    bool         `json:"ok"`
	Jobs  []ClaimedJob `json:"jobs"`
	Error string       `json:"error,omitempty"`
}

func (c *Client) ClaimJobsForDevice(ctx context.Context, limit int) (*ClaimJobsResponse, error) {
	payload := ClaimJobsRequest{
		Limit: limit,
	}

	body, _ := json.Marshal(payload)

	resp, err := c.httpc.PostWithHeaders(ctx, FunctionClaimJobsForDevice, body, func(r *http.Request) {
		r.Header.Set("x-device-id", c.deviceID)
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	log.Printf("[BACKEND-CLIENT] claim_jobs_for_device → HTTP %d | body: %s", resp.StatusCode, string(respBody))

	if resp.StatusCode >= 300 {
		var apiResp APIResponse
		if json.Unmarshal(respBody, &apiResp) == nil {
			if apiResp.Error != "" {
				return nil, fmt.Errorf("claim_jobs_for_device error: %s", apiResp.Error)
			}
		}
		return nil, fmt.Errorf("claim_jobs_for_device failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var result ClaimJobsResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("invalid JSON from claim_jobs_for_device: %w | body=%s", err, string(respBody))
	}

	if !result.Ok && result.Error != "" {
		return nil, fmt.Errorf("claim_jobs_for_device rejected: %s", result.Error)
	}

	return &result, nil
}

type StorageConfig struct {
	Ok            bool        `json:"ok"`
	StorageMode   string      `json:"storage_mode"`
	Provider      string      `json:"provider"`
	BucketName    string      `json:"bucket_name"`
	Region        string      `json:"region"`
	Endpoint      string      `json:"endpoint"`
	MountBasePath string      `json:"mount_base_path"`
	Credentials   interface{} `json:"credentials"`
	Error         string      `json:"error,omitempty"`
}

func (c *Client) GetStorageConfig(ctx context.Context) (*StorageConfig, error) {
	resp, err := c.httpc.Get(ctx, FunctionGetStorageConfig)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	log.Printf("[BACKEND-CLIENT] get_storage_config → HTTP %d | body: %s", resp.StatusCode, string(respBody))

	if resp.StatusCode >= 300 {
		var apiResp APIResponse
		if json.Unmarshal(respBody, &apiResp) == nil {
			if apiResp.Error != "" {
				return nil, fmt.Errorf("get_storage_config error: %s", apiResp.Error)
			}
		}
		return nil, fmt.Errorf("get_storage_config failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var result StorageConfig
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("invalid JSON from get_storage_config: %w | body=%s", err, string(respBody))
	}

	if !result.Ok && result.Error != "" {
		return nil, fmt.Errorf("get_storage_config rejected: %s", result.Error)
	}

	return &result, nil
}

type CompleteJobRequest struct {
	JobID      string          `json:"job_id"`
	Status     string          `json:"status"`
	DurationMs int64           `json:"duration_ms,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
}

type CompleteJobResponse struct {
	Ok         bool   `json:"ok"`
	JobID      string `json:"job_id"`
	Status     string `json:"status"`
	Idempotent bool   `json:"idempotent"`
	Error      string `json:"error,omitempty"`
}

func (c *Client) CompleteJob(ctx context.Context, jobID string, status string, durationMs int64, resultData any) (*CompleteJobResponse, error) {
	reqBody := CompleteJobRequest{
		JobID:      jobID,
		Status:     status,
		DurationMs: durationMs,
	}

	if resultData != nil {
		resultJSON, err := json.Marshal(resultData)
		if err != nil {
			return nil, err
		}
		reqBody.Result = resultJSON
	}

	body, _ := json.Marshal(reqBody)

	resp, err := c.httpc.PostWithHeaders(ctx, FunctionCompleteJob, body, func(r *http.Request) {
		r.Header.Set("x-device-id", c.deviceID)
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	log.Printf("[BACKEND-CLIENT] complete_job → HTTP %d | body: %s", resp.StatusCode, string(respBody))

	if resp.StatusCode >= 300 {
		var apiResp APIResponse
		if json.Unmarshal(respBody, &apiResp) == nil {
			if apiResp.Error != "" {
				return nil, fmt.Errorf("complete_job error: %s", apiResp.Error)
			}
		}
		return nil, fmt.Errorf("complete_job failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var completeResult CompleteJobResponse
	if err := json.Unmarshal(respBody, &completeResult); err != nil {
		return nil, fmt.Errorf("invalid JSON from complete_job: %w | body=%s", err, string(respBody))
	}

	if !completeResult.Ok && completeResult.Error != "" {
		return nil, fmt.Errorf("complete_job rejected: %s", completeResult.Error)
	}

	return &completeResult, nil
}

type ReportJobErrorRequest struct {
	JobID           string `json:"job_id"`
	ErrorMessage    string `json:"error_message"`
	ForceDeadLetter bool   `json:"force_dead_letter,omitempty"`
}

type ReportJobErrorResponse struct {
	Ok         bool   `json:"ok"`
	JobID      string `json:"job_id"`
	Action     string `json:"action"`
	RetryCount int    `json:"retry_count"`
	MaxRetries int    `json:"max_retries"`
	CanRetry   bool   `json:"can_retry"`
	Error      string `json:"error,omitempty"`
}

func (c *Client) ReportJobError(ctx context.Context, jobID string, errMsg string, forceDeadLetter bool) (*ReportJobErrorResponse, error) {
	sanitizedError := sanitize.ErrorMessage(errMsg)

	reqBody := ReportJobErrorRequest{
		JobID:           jobID,
		ErrorMessage:    sanitizedError,
		ForceDeadLetter: forceDeadLetter,
	}

	body, _ := json.Marshal(reqBody)

	resp, err := c.httpc.PostWithHeaders(ctx, FunctionReportJobError, body, func(r *http.Request) {
		r.Header.Set("x-device-id", c.deviceID)
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	log.Printf("[BACKEND-CLIENT] report_job_error → HTTP %d | body: %s", resp.StatusCode, string(respBody))

	if resp.StatusCode >= 300 {
		var apiResp APIResponse
		if json.Unmarshal(respBody, &apiResp) == nil {
			if apiResp.Error != "" {
				return nil, fmt.Errorf("report_job_error error: %s", apiResp.Error)
			}
		}
		return nil, fmt.Errorf("report_job_error failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var errorResult ReportJobErrorResponse
	if err := json.Unmarshal(respBody, &errorResult); err != nil {
		return nil, fmt.Errorf("invalid JSON from report_job_error: %w | body=%s", err, string(respBody))
	}

	if !errorResult.Ok && errorResult.Error != "" {
		return nil, fmt.Errorf("report_job_error rejected: %s", errorResult.Error)
	}

	return &errorResult, nil
}
