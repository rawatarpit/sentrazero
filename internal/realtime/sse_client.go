package realtime

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sentra-agent/internal/auth"
	"sentra-agent/internal/config"
	"sentra-agent/internal/dispatcher"
	"sentra-agent/internal/obs"
)

const (
	FunctionAgentStream    = "/functions/v1/agent_stream"
	FunctionReconcileAgent = "/functions/v1/reconcile_agent"
)

type CircuitBreakerState int

const (
	CircuitClosed CircuitBreakerState = iota
	CircuitOpen
	CircuitHalfOpen
)

type CircuitBreaker struct {
	state           CircuitBreakerState
	failures        int
	successes       int
	maxFailures     int
	resetTimeout    time.Duration
	lastFailureTime time.Time
	mu              sync.Mutex
}

func NewCircuitBreaker(maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:        CircuitClosed,
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
	}
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.successes++
	if cb.state == CircuitHalfOpen && cb.successes >= 3 {
		cb.state = CircuitClosed
		cb.failures = 0
		cb.successes = 0
		log.Println("[circuit_breaker] circuit closed")
	}
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailureTime = time.Now()

	if cb.state == CircuitClosed && cb.failures >= cb.maxFailures {
		cb.state = CircuitOpen
		log.Printf("[circuit_breaker] circuit opened after %d failures", cb.failures)
	}
}

func (cb *CircuitBreaker) AllowRequest() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.lastFailureTime) > cb.resetTimeout {
			cb.state = CircuitHalfOpen
			cb.successes = 0
			log.Println("[circuit_breaker] circuit half-open, allowing test request")
			return true
		}
		return false
	case CircuitHalfOpen:
		return true
	}
	return false
}

type SSEJobEvent struct {
	ID      string          `json:"id"`
	AgentID string          `json:"agent_id"`
	JobType string          `json:"job_type"`
	Payload json.RawMessage `json:"payload"`
	Status  string          `json:"status"`
}

var (
	seenJobs   = make(map[string]time.Time)
	seenJobsMu sync.Mutex
	seenTTL    = 10 * time.Minute

	circuitBreaker *CircuitBreaker
)

var acceptingJobs int32 = 1

func seenRecently(jobID string) bool {
	seenJobsMu.Lock()
	defer seenJobsMu.Unlock()

	now := time.Now()
	dedupKey := jobID

	if t, ok := seenJobs[dedupKey]; ok && now.Sub(t) < seenTTL {
		return true
	}

	seenJobs[dedupKey] = now

	for id, ts := range seenJobs {
		if now.Sub(ts) > seenTTL {
			delete(seenJobs, id)
		}
	}

	return false
}

func RunSSEClient(
	ctx context.Context,
	device auth.Device,
	cfg *config.Config,
) {
	circuitBreaker = NewCircuitBreaker(5, 30*time.Second)

	backoff := 1 * time.Second
	const maxBackoff = 30 * time.Second

	log.Printf("[sse] connecting to agent_stream")

	// Start low-latency polling client as primary
	// This uses REST API polling which is more reliable than SSE
	go RunPollingClient(ctx, device, cfg, device.Token)

	// Wait for polling client to be ready (with timeout)
	pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pollCancel()
	for {
		select {
		case <-pollCtx.Done():
			log.Printf("[sse] warning: polling client may not be ready")
			break
		default:
			if pollingClient != nil && pollingClient.IsRunning() {
				log.Printf("[sse] polling client ready")
				goto PollingReady
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
PollingReady:

	// If realtime is connected, reduce SSE polling frequency
	// SSE becomes a fallback mechanism
	for {
		select {
		case <-ctx.Done():
			log.Println("[sse] shutting down")
			return
		default:
		}

		// Circuit breaker check - don't reconnect if circuit is open
		if !circuitBreaker.AllowRequest() {
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			reconnectTime := backoff + jitter
			log.Printf("[sse] circuit open, waiting %s before retry", reconnectTime)
			select {
			case <-ctx.Done():
				log.Println("[sse] shutting down")
				return
			case <-time.After(reconnectTime):
			}
			continue
		}

		// Reset job acceptance flag on reconnect
		atomic.StoreInt32(&acceptingJobs, 1)
		log.Println("[sse] connection established, accepting jobs")

		err := connectAndConsume(ctx, device, cfg)
		if err != nil {
			circuitBreaker.RecordFailure()
			log.Printf("[sse] connection error: %v", err)
		} else {
			circuitBreaker.RecordSuccess()
		}

		jitter := time.Duration(rand.Int63n(int64(backoff)))
		reconnectTime := backoff + jitter
		log.Printf("[sse] reconnecting in %s", reconnectTime)

		select {
		case <-ctx.Done():
			log.Println("[sse] shutting down")
			return
		case <-time.After(reconnectTime):
		}

		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func connectAndConsume(
	ctx context.Context,
	device auth.Device,
	cfg *config.Config,
) error {
	streamURL := fmt.Sprintf("%s%s?device_id=%s", cfg.BackendURL, FunctionAgentStream, url.QueryEscape(device.ID))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Authorization", "Bearer "+cfg.BackendAnonKey)
	req.Header.Set("x-agent-token", device.Token)

	client := &http.Client{Timeout: 120 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return fmt.Errorf("unexpected SSE status: %d", resp.StatusCode)
	}

	log.Println("[sse] connected")
	defer resp.Body.Close()

	// Check if low-latency polling is available
	if pollingClient != nil && pollingClient.IsRunning() {
		log.Printf("[sse] low-latency polling active, using as primary job delivery")
	} else {
		log.Printf("[sse] polling not available, using SSE fallback")
	}

	reader := bufio.NewReader(resp.Body)

	var (
		eventType string
		dataBuf   strings.Builder
	)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}

		line = strings.TrimSpace(line)

		if line == "" {
			if eventType != "" {
				handleEvent(ctx, eventType, dataBuf.String())
			}
			eventType = ""
			dataBuf.Reset()
			continue
		}

		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}

		if strings.HasPrefix(line, "data:") {
			dataBuf.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
			continue
		}
	}
}

func handleEvent(ctx context.Context, eventType, data string) {
	switch eventType {

	case "hello":
		var helloMsg struct {
			DeviceID string `json:"device_id"`
			OrgID    string `json:"org_id"`
			Message  string `json:"message"`
		}
		if err := json.Unmarshal([]byte(data), &helloMsg); err == nil {
			log.Printf("[sse] session established | device=%s org=%s", helloMsg.DeviceID, helloMsg.OrgID)
		}
		return

	case "heartbeat":
		return

	case "job":
		handleJobEvent(ctx, data)

	case "drain":
		log.Println("[sse] drain requested — stopping job acceptance")
		atomic.StoreInt32(&acceptingJobs, 0)

	case "shutdown":
		log.Println("[sse] shutdown requested — stopping worker pool")
		atomic.StoreInt32(&acceptingJobs, 0)

		shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		dispatcher.StopWorkerPool(shutdownCtx)

	default:
		obs.Debug("[sse] unknown event", obs.Field{"event_type": eventType, "data": data})
	}
}

func handleJobEvent(ctx context.Context, data string) {
	if ctx.Err() != nil {
		log.Printf("[sse] job ignored — context cancelled")
		return
	}

	if atomic.LoadInt32(&acceptingJobs) == 0 {
		log.Printf("[sse] job ignored — agent draining")
		return
	}

	var job SSEJobEvent
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		log.Printf("[sse] job decode failed: %v", err)
		return
	}

	if seenRecently(job.ID) {
		log.Printf("[sse] duplicate job %s ignored", job.ID)
		return
	}

	log.Printf("[sse] received job %s", job.ID)

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

	obs.Info(
		"job received",
		obs.Field{
			"job_id":            job.ID,
			"job_type":          job.JobType,
			"trace_id":          traceID,
			"execution_step_id": executionStepID,
		},
	)

	if err := dispatcher.SubmitJobWithMeta(
		job.JobType,
		job.Payload,
		job.ID,
		"",
		traceID,
		executionStepID,
		"",
	); err != nil {
		log.Printf("[sse] dispatch failed for job %s: %v", job.ID, err)
	}
}

type ReconcileResponse struct {
	OK           bool `json:"ok"`
	RestoredJobs int  `json:"restored_jobs"`
}

func ReconcileAgent(ctx context.Context, cfg *config.Config) error {
	reconcileURL := fmt.Sprintf("%s%s", cfg.BackendURL, FunctionReconcileAgent)

	deviceID := cfg.DeviceID
	deviceToken, err := auth.GetToken()
	if err != nil {
		log.Printf("[reconcile] cannot get device token: %v", err)
		return err
	}

	payload := map[string]string{
		"device_id": deviceID,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reconcileURL, strings.NewReader(string(body)))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.BackendAnonKey)
	req.Header.Set("x-agent-token", deviceToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[reconcile] endpoint not available, skipping")
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		log.Printf("[reconcile] endpoint not available, skipping")
		return nil
	}

	if resp.StatusCode >= 300 {
		return fmt.Errorf("reconcile_agent failed: HTTP %d", resp.StatusCode)
	}

	var result ReconcileResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}

	if result.OK {
		log.Printf("[reconcile] restored_jobs=%d", result.RestoredJobs)
	}

	return nil
}
