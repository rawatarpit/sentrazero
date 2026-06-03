package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"sentra-agent/internal/auth"
	"sentra-agent/internal/config"
	"sentra-agent/internal/dispatcher"
	"sentra-agent/internal/obs"
	agentredis "sentra-agent/internal/redis"
)

type RealtimeJobPayload struct {
	ID               string          `json:"id"`
	JobID            string          `json:"job_id"` // Alternative field name from some endpoints
	JobType          string          `json:"job_type"`
	Payload          json.RawMessage `json:"payload"`
	JobPayload       json.RawMessage `json:"job_payload"` // Alternative field name
	Status           string          `json:"status"`
	ExecutionStepID  string          `json:"execution_step_id"`
	ExecutionID      string          `json:"execution_id,omitempty"`
	ExecID           string          `json:"exec_id,omitempty"` // Alternative field name
	OrgID            string          `json:"org_id"`
	DatasetID        string          `json:"dataset_id"`
	ChunkIndex       int             `json:"chunk_index"`
	StepIndex        int             `json:"step_index"`
	RunID            string          `json:"run_id"`
	AttemptNumber    int             `json:"attempt_number"`
	RuntimeType      string          `json:"runtime_type"`
	RuntimeDeps      json.RawMessage `json:"runtime_dependencies"`
	ExecutionMode    string          `json:"execution_mode"`
	ExecutionTimeout int             `json:"execution_timeout_seconds"`
}

func (j *RealtimeJobPayload) getJobID() string {
	if j.ID != "" {
		return j.ID
	}
	return j.JobID
}

func (j *RealtimeJobPayload) getExecutionID() string {
	if j.ExecutionID != "" {
		return j.ExecutionID
	}
	return j.ExecID
}

func (j *RealtimeJobPayload) getPayload() json.RawMessage {
	if len(j.Payload) > 0 {
		return j.Payload
	}
	return j.JobPayload
}

// ClaimJobResponse handles the new column names from claim_jobs_for_device (job_id, job_payload, exec_id)
type ClaimJobResponse struct {
	JobID       string          `json:"job_id"`
	JobType     string          `json:"job_type"`
	JobPayload json.RawMessage `json:"job_payload"`
	ExecID      string          `json:"exec_id"`
}

type PollingClient struct {
	client      *http.Client
	device      auth.Device
	cfg         *config.Config
	redisClient *agentredis.Client
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.RWMutex
	running     atomic.Bool
	baseURL     string
	anonKey     string
	deviceToken string
	sentJobs    sync.Map
}

var (
	pollingClient *PollingClient
	pollingOnce   sync.Once
	pollInterval  = 5 * time.Second
)

func RunPollingClient(
	ctx context.Context,
	device auth.Device,
	cfg *config.Config,
	deviceToken string,
	redisClient *agentredis.Client,
) {
	pollingOnce.Do(func() {
		pollingClient = &PollingClient{
			device:      device,
			cfg:         cfg,
			redisClient: redisClient,
			baseURL:     cfg.BackendURL,
			anonKey:     cfg.BackendAnonKey,
			deviceToken: deviceToken,
			client: &http.Client{
				Timeout: 10 * time.Second,
			},
		}
		pollingClient.start(ctx)
	})
}

func (p *PollingClient) start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	p.ctx = ctx
	p.cancel = cancel

	p.running.Store(true)

	log.Printf("[realtime] starting low-latency polling for device %s", p.device.ID)

	go p.pollLoop()
}

func (p *PollingClient) pollLoop() {

	// immediate first poll
	p.fetchNewJobs()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {

		case <-p.ctx.Done():
			p.running.Store(false)
			log.Printf("[realtime] polling stopped for device %s", p.device.ID)
			return

		case <-ticker.C:
			p.fetchNewJobs()

		}
	}
}

func (p *PollingClient) fetchNewJobs() {

	if p.ctx.Err() != nil {
		return
	}

	url := fmt.Sprintf("%s/functions/v1/claim_jobs_for_device", p.baseURL)

	body, _ := json.Marshal(map[string]any{
		"limit":             10,
		"lease_ttl_seconds": 1800,
	})

	req, err := http.NewRequestWithContext(p.ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		log.Printf("[realtime] failed to create request: %v", err)
		return
	}

	req.Header.Set("Authorization", "Bearer "+p.anonKey)
	req.Header.Set("x-agent-token", p.deviceToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		log.Printf("[realtime] request failed: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		log.Printf("[realtime] unexpected status: %d body=%s", resp.StatusCode, string(respBody))
		return
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[realtime] failed to read response body: %v", err)
		return
	}

	var result struct {
		Ok   bool                 `json:"ok"`
		Jobs []RealtimeJobPayload `json:"jobs"`
	}

	if err := json.Unmarshal(respBody, &result); err != nil {
		log.Printf("[realtime] failed to decode response: %v", err)
		return
	}

	log.Printf("[realtime] poll response: jobs=%d, body_len=%d", len(result.Jobs), len(respBody))

	// DEBUG: Log all raw jobs from first parse
	for i, j := range result.Jobs {
		log.Printf("[DEBUG] first_parse[%d]: job_id=%q job_type=%q execution_id=%q", i, j.getJobID(), j.JobType, j.getExecutionID())
	}

	// First, check if we have valid jobs from first parse using getter methods
	hasValidJobs := false
	for _, j := range result.Jobs {
		if j.getJobID() != "" && j.getExecutionID() != "" {
			hasValidJobs = true
			break
		}
	}

	// If no valid jobs, try the alternative format with new column names
	if !hasValidJobs {
		var altResult struct {
			Ok   bool               `json:"ok"`
			Jobs []ClaimJobResponse `json:"jobs"`
		}
		if altErr := json.Unmarshal(respBody, &altResult); altErr == nil && altResult.Ok && len(altResult.Jobs) > 0 {
			log.Printf("[realtime] alt response: altJobs=%d", len(altResult.Jobs))
			for _, altJob := range altResult.Jobs {
				if altJob.JobID == "" {
					log.Printf("[realtime] skipping empty altJob")
					continue
				}
				log.Printf("[realtime] mapping altJob: job_id=%s job_type=%s exec_id=%s", altJob.JobID, altJob.JobType, altJob.ExecID)
				job := RealtimeJobPayload{
					ID:           altJob.JobID,
					JobType:      altJob.JobType,
					Payload:     altJob.JobPayload,
					ExecutionID: altJob.ExecID,
				}
				result.Jobs = append(result.Jobs, job)
			}
		}
	}

	if !result.Ok {
		return
	}

	for _, job := range result.Jobs {

		// ============================================================
		// MANDATORY VALIDATION: Drop malformed jobs
		// ============================================================
		
		// Get canonical job_id and execution_id using getter methods
		jobID := job.getJobID()
		executionID := job.getExecutionID()
		payload := job.getPayload()
		
		// Validate required fields - job_id is the primary key
		if jobID == "" {
			log.Printf("[realtime] ⚠️ dropped malformed job: missing job_id")
			continue
		}
		
		// job_type is required
		if job.JobType == "" {
			log.Printf("[realtime] ⚠️ dropped malformed job: missing job_type job_id=%s", jobID)
			continue
		}
		
		// execution_id is CRITICAL - required for completion tracking
		if executionID == "" {
			log.Printf("[realtime] ⚠️ dropped malformed job: missing execution_id job_id=%s job_type=%s", jobID, job.JobType)
			continue
		}
		
		// Validate payload if present
		if len(payload) > 0 && !json.Valid(payload) {
			log.Printf("[realtime] ⚠️ dropped malformed job: invalid JSON payload job_id=%s", jobID)
			continue
		}

		// ============================================================
		// Duplicate detection (Redis-backed when available)
		// ============================================================
		
		if p.redisClient != nil {
			added, err := p.redisClient.MarkJobProcessed(p.ctx, jobID, sentJobsTTL)
			if err != nil || !added {
				continue
			}
		} else {
			if _, exists := p.sentJobs.Load(jobID); exists {
				continue
			}
			p.sentJobs.Store(jobID, time.Now())
		}

		log.Printf("[realtime] new job received: %s (type: %s)", jobID, job.JobType)

		obs.Info(
			"job received via realtime polling",
			obs.Field{
				"job_id":            jobID,
				"job_type":          job.JobType,
				"execution_id":      executionID,
				"execution_step_id": job.ExecutionStepID,
				"source":            "low_latency_poll",
			},
		)

		traceID := obs.NewTraceID()

		// Merge top-level runtime_dependencies into the inner payload so the
		// v2 executor can find them when looking for "runtime_dependencies".
		if len(job.RuntimeDeps) > 0 {
			var payloadMap map[string]interface{}
			if err := json.Unmarshal(payload, &payloadMap); err == nil {
				if _, exists := payloadMap["runtime_dependencies"]; !exists {
					var deps interface{}
					if err := json.Unmarshal(job.RuntimeDeps, &deps); err == nil {
						payloadMap["runtime_dependencies"] = deps
						if merged, err := json.Marshal(payloadMap); err == nil {
							payload = merged
						}
					}
				}
			}
		}

		// DEBUG: Log exact values being sent to dispatcher
		log.Printf("[DEBUG] BEFORE dispatch → job_id=%s execution_id=%s job_type=%s payload_len=%d",
			jobID, executionID, job.JobType, len(payload))

		if err := dispatcher.SubmitJobWithMeta(
			job.JobType,
			payload,
			jobID,
			job.OrgID,
			traceID,
			job.ExecutionStepID,
			executionID,
		); err != nil {

			log.Printf("[realtime] dispatch failed for job %s: %v", jobID, err)
		}
	}
}

func (p *PollingClient) IsRunning() bool {
	return p.running.Load()
}

func StopPollingClient() {
	if pollingClient != nil && pollingClient.cancel != nil {
		pollingClient.cancel()
	}
}

const sentJobsTTL = 10 * time.Minute

func CleanupOldJobs() {
	if pollingClient == nil {
		return
	}

	if pollingClient.redisClient != nil {
		return
	}

	now := time.Now()

	pollingClient.sentJobs.Range(func(key, value interface{}) bool {
		jobID, ok := key.(string)
		if !ok {
			pollingClient.sentJobs.Delete(key)
			return true
		}

		storedTime, ok := value.(time.Time)
		if !ok {
			pollingClient.sentJobs.Delete(jobID)
			return true
		}

		if now.Sub(storedTime) > sentJobsTTL {
			pollingClient.sentJobs.Delete(jobID)
		}

		return true
	})
}
