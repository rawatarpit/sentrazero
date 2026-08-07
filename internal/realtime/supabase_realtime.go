package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"sentra-agent/internal/auth"
	"sentra-agent/internal/config"
	"sentra-agent/internal/dispatcher"
	"sentra-agent/internal/obs"
	agentredis "sentra-agent/internal/redis"
	"sentra-agent/internal/sysinfo"
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

	currentInterval atomic.Int64 // nanoseconds, for adaptive backoff
	redisWakeupCh  chan struct{} // signaled when Redis publishes a new job notification

	// lastMetricsJSON is the canonical serialization of the last metrics payload
	// sent to the control plane. Metrics are only re-sent (and re-inserted into
	// agent_metrics) when the values actually change, which cuts telemetry
	// storage and edge-function write volume dramatically while idle.
	lastMetricsJSON string
	metricsMu       sync.Mutex

	// lastClaimedJobs is the number of jobs returned by the most recent
	// successful poll (i.e. chunks claimed in the last claim cycle). It is
	// reported in the next poll's metrics so we can measure the claim-burst vs
	// worker-slot occupancy gap (claims/poll vs MaxConcurrency). Because it
	// changes whenever a batch is claimed, it also force-re-sends telemetry at
	// claim time, which is exactly when we want an observation point.
	lastClaimedJobs atomic.Int32
}

var (
	pollingClient    *PollingClient
	pollingOnce      sync.Once
	pollInterval     = 30 * time.Second
	minPollInterval  = 5 * time.Second
	maxPollInterval  = 300 * time.Second
	backoffMultiplier = 2.0
	intervalLoadOnce sync.Once
)

func loadIntervalsFromEnv() {
	intervalLoadOnce.Do(func() {
		if v := os.Getenv("SENTRA_POLL_INTERVAL"); v != "" {
			if d, err := time.ParseDuration(v); err == nil && d > 0 {
				pollInterval = d
			}
		}
		if v := os.Getenv("SENTRA_MIN_POLL_INTERVAL"); v != "" {
			if d, err := time.ParseDuration(v); err == nil && d > 0 {
				minPollInterval = d
			}
		}
		if v := os.Getenv("SENTRA_MAX_POLL_INTERVAL"); v != "" {
			if d, err := time.ParseDuration(v); err == nil && d > 0 {
				maxPollInterval = d
			}
		}
		if v := os.Getenv("SENTRA_POLL_BACKOFF_MULTIPLIER"); v != "" {
			if m, err := strconv.ParseFloat(v, 64); err == nil && m > 1.0 {
				backoffMultiplier = m
			}
		}
	})
}

func RunPollingClient(
	ctx context.Context,
	device auth.Device,
	cfg *config.Config,
	deviceToken string,
	redisClient *agentredis.Client,
) {
	loadIntervalsFromEnv()
	pollingOnce.Do(func() {
		pollingClient = &PollingClient{
			device:        device,
			cfg:           cfg,
			redisClient:   redisClient,
			baseURL:       cfg.BackendURL,
			anonKey:       cfg.BackendAnonKey,
			deviceToken:   deviceToken,
			redisWakeupCh: make(chan struct{}, 64),
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
	p.currentInterval.Store(int64(minPollInterval))

	log.Printf("[realtime] starting adaptive polling for device %s (min=%s max=%s)", p.device.ID, minPollInterval, maxPollInterval)

	if p.redisClient != nil && p.cfg.OrgID != "" {
		go p.listenForRedisJobs()
	}

	go p.pollLoopAdaptive()
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

func (p *PollingClient) pollLoopAdaptive() {

	// immediate first poll
	jobCount := p.fetchNewJobs()
	current := time.Duration(p.currentInterval.Load())
	if jobCount > 0 {
		current = minPollInterval
	} else {
		next := time.Duration(float64(current) * backoffMultiplier)
		if next > maxPollInterval {
			next = maxPollInterval
		}
		current = next
	}
	p.currentInterval.Store(int64(current))

	ticker := time.NewTicker(current)
	defer ticker.Stop()

	for {
		select {

		case <-p.ctx.Done():
			p.running.Store(false)
			log.Printf("[realtime] adaptive polling stopped for device %s", p.device.ID)
			return

		case <-ticker.C:
			ticker.Stop()

			jobCount := p.fetchNewJobs()
			current := time.Duration(p.currentInterval.Load())
			if jobCount > 0 {
				current = minPollInterval
			} else {
				next := time.Duration(float64(current) * backoffMultiplier)
				if next > maxPollInterval {
					next = maxPollInterval
				}
				current = next
			}
			p.currentInterval.Store(int64(current))

			ticker = time.NewTicker(current)
			log.Printf("[realtime] adaptive poll: got %d jobs, next interval=%s", jobCount, current)

		case <-p.redisWakeupCh:
			ticker.Stop()
			p.fetchNewJobs()
			p.currentInterval.Store(int64(minPollInterval))
			ticker = time.NewTicker(minPollInterval)
			log.Printf("[realtime] redis wakeup: immediate poll, next interval=%s", minPollInterval)
		}
	}
}

// listenForRedisJobs subscribes to the sentra:newjob:{org_id} Redis Pub/Sub channel.
// When a job notification arrives, it signals the polling loop to wake up immediately.
func (p *PollingClient) listenForRedisJobs() {
	channel := fmt.Sprintf("sentra:newjob:%s", p.cfg.OrgID)
	pubsub := p.redisClient.Subscribe(p.ctx, channel)
	defer pubsub.Close()

	log.Printf("[realtime] subscribed to Redis channel %s for instant job wake-up", channel)

	for {
		msg, err := pubsub.ReceiveMessage(p.ctx)
		if err != nil {
			log.Printf("[realtime] Redis subscription error: %v (falling back to polling only)", err)
			return
		}

		// Non-blocking send to the wakeup channel (don't pile up if polling is already busy)
		select {
		case p.redisWakeupCh <- struct{}{}:
		default:
		}

		_ = msg // message payload is informational only — the actual claim happens via HTTP
	}
}

// currentMetrics returns the live device metrics map, but only when the values
// have changed since the last poll. Returns nil when nothing meaningful changed,
// in which case the poll proceeds without a metrics payload. A 5-minute absolute
// freshness cap forces a re-send even if values are unchanged, so the control
// plane always sees current telemetry at least once per cap.
func (p *PollingClient) currentMetrics() map[string]any {
	sys := sysinfo.Detect()
	m := map[string]any{
		"active_workers":    dispatcher.CurrentWorkerCount(),
		"total_cpu_cores":   runtime.NumCPU(),
		"cpu_cores_free":    int(dispatcher.MaxWorkersCount()) - dispatcher.CurrentWorkerCount(),
		"memory_free_gb":    sys.AvailableMemoryGB,
		"total_memory_gb":   sys.TotalMemoryGB,
		"cpu_usage_percent": sys.CPUUsagePercent,
		"network_latency_ms": sys.NetworkLatency,
		"gpu_available":     sys.GPUModel != "",
		"io_bandwidth_mb_s": 0,
		// Chunks claimed in the last poll (0 when idle). This is the direct
		// signal for claim-burst vs slot occupancy — if it is consistently >1
		// while active_workers == MaxConcurrency, chunks are queued ahead of
		// the worker pool (granularity mismatch).
		"claims_returned": p.lastClaimedJobs.Load(),
	}
	raw, _ := json.Marshal(m)
	key := string(raw)

	p.metricsMu.Lock()
	defer p.metricsMu.Unlock()

	if p.lastMetricsJSON == key {
		return nil
	}
	p.lastMetricsJSON = key
	return m
}

func (p *PollingClient) fetchNewJobs() int {

	if p.ctx.Err() != nil {
		return 0
	}

	// Consolidated poll_state endpoint replaces claim_jobs_for_device + agent_health_policy
	url := fmt.Sprintf("%s/functions/v1/poll_state", p.baseURL)

	// Build metrics and only include them when they actually changed. This still
	// polls (for job claiming + heartbeat), but avoids inserting a fresh
	// agent_metrics row on every poll — the primary driver of telemetry storage.
	body := map[string]any{
		"limit":             10,
		"lease_ttl_seconds": 1800,
		"heartbeat":         true,
	}
	if m := p.currentMetrics(); m != nil {
		body["metrics"] = m
	}

	bodyBytes, _ := json.Marshal(body)

	req, err := http.NewRequestWithContext(p.ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		log.Printf("[realtime] failed to create request: %v", err)
		return 0
	}

	req.Header.Set("Authorization", "Bearer "+p.anonKey)
	req.Header.Set("x-agent-token", p.deviceToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		log.Printf("[realtime] request failed: %v", err)
		return 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		log.Printf("[realtime] unexpected status: %d body=%s", resp.StatusCode, string(respBody))
		return 0
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[realtime] failed to read response body: %v", err)
		return 0
	}

	var result struct {
		Ok   bool                 `json:"ok"`
		Jobs []RealtimeJobPayload `json:"jobs"`
	}

	if err := json.Unmarshal(respBody, &result); err != nil {
		log.Printf("[realtime] failed to decode response: %v", err)
		return 0
	}

	jobCount := len(result.Jobs)
	log.Printf("[realtime] poll response: jobs=%d, body_len=%d", jobCount, len(respBody))

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
		log.Printf("[realtime] ⚠️ result.Ok is false, dropping %d jobs", len(result.Jobs))
		return 0
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
		
		// execution_id is needed for execution-scoped jobs but not for
		// background tasks like scan_dataset. Log a warning so we spot
		// unexpected cases, but don't drop the job.
		if executionID == "" {
			log.Printf("[realtime] ⚠️ job without execution_id (ok for background tasks): job_id=%s job_type=%s", jobID, job.JobType)
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
			added, err := p.redisClient.MarkJobProcessed(p.ctx, p.device.ID, jobID, sentJobsTTL)
			if err != nil {
				log.Printf("[realtime] ⚠️ Redis dedup error for job %s: %v", jobID, err)
				continue
			}
			if !added {
				log.Printf("[realtime] ⚠️ Redis dedup rejected job %s (already processed within TTL)", jobID)
				continue
			}
		} else {
			if _, exists := p.sentJobs.Load(jobID); exists {
				log.Printf("[realtime] ⚠️ in-memory dedup rejected job %s (already processed)", jobID)
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
	p.lastClaimedJobs.Store(int32(len(result.Jobs)))
	return len(result.Jobs)
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
