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
)

type RealtimeJobPayload struct {
	ID              string          `json:"id"`
	JobType         string          `json:"job_type"`
	Payload         json.RawMessage `json:"payload"`
	Status          string          `json:"status"`
	ExecutionStepID string          `json:"execution_step_id"`
	OrgID           string          `json:"org_id"`
}

type PollingClient struct {
	client      *http.Client
	device      auth.Device
	cfg         *config.Config
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
) {
	pollingOnce.Do(func() {
		pollingClient = &PollingClient{
			device:      device,
			cfg:         cfg,
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

	var result struct {
		Ok   bool                 `json:"ok"`
		Jobs []RealtimeJobPayload `json:"jobs"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Printf("[realtime] failed to decode response: %v", err)
		return
	}

	if !result.Ok {
		return
	}

	for _, job := range result.Jobs {

		if _, exists := p.sentJobs.Load(job.ID); exists {
			continue
		}

		p.sentJobs.Store(job.ID, time.Now())

		log.Printf("[realtime] new job received: %s (type: %s)", job.ID, job.JobType)

		obs.Info(
			"job received via realtime polling",
			obs.Field{
				"job_id":            job.ID,
				"job_type":          job.JobType,
				"execution_step_id": job.ExecutionStepID,
				"source":            "low_latency_poll",
			},
		)

		traceID := obs.NewTraceID()

		var executionStepID string
		if len(job.Payload) > 0 {
			var payload struct {
				ExecutionStepID string `json:"execution_step_id"`
			}
			if err := json.Unmarshal(job.Payload, &payload); err == nil {
				executionStepID = payload.ExecutionStepID
			}
		}

		if err := dispatcher.SubmitJobWithMeta(
			job.JobType,
			job.Payload,
			job.ID,
			"",
			traceID,
			executionStepID,
		); err != nil {

			log.Printf("[realtime] dispatch failed for job %s: %v", job.ID, err)
		}
	}
}

func (p *PollingClient) IsRunning() bool {
	return p.running.Load()
}

func (p *PollingClient) MarkJobSent(jobID string) {
	p.sentJobs.Store(jobID, time.Now())
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

type RealtimeClientConfig struct {
	DeviceID     string
	OrgID        string
	BaseURL      string
	AnonKey      string
	PollInterval time.Duration
}

func DefaultRealtimeClientConfig(device auth.Device, cfg *config.Config) RealtimeClientConfig {

	return RealtimeClientConfig{
		DeviceID:     device.ID,
		OrgID:        device.OrgID,
		BaseURL:      cfg.BackendURL,
		AnonKey:      cfg.BackendAnonKey,
		PollInterval: 5 * time.Second,
	}
}
