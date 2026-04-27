package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"sentra-agent/internal/backend"
	"sentra-agent/internal/dataset"
	"sentra-agent/internal/obs"
	"sentra-agent/internal/reporter"
	"sentra-agent/internal/storage"
)

// ---------------------------------------------------------------------
// State
// ---------------------------------------------------------------------

var (
	jobQueue       chan jobRequest
	activeWorkers  int32
	maxWorkers     int32
	totalProcessed int64
	totalFailed    int64
	wgWorkers      sync.WaitGroup
	muResize       sync.Mutex
	shutdownOnce   sync.Once
	shutdownSignal = make(chan struct{})
	resizeSignal   = make(chan struct{}, 0)

	execClient *backend.ExecutionClient

	activeJobsMutex sync.Mutex
	activeJobs      = make(map[string]struct{})
	runningJobs     = make(map[string]struct{})
)

var (
	DefaultMaxWorkers          = 4
	DefaultQueueSize           = 128
	HighWaterMarkPercent       = 0.8
	LowWaterMarkPercent        = 0.5
	backpressureEnabled  int32 = 1
	pauseSSE             func() bool
)

// ---------------------------------------------------------------------
// Job definition
// ---------------------------------------------------------------------

func SetPauseSSE(fn func() bool) {
	pauseSSE = fn
}

// Job definition
// ---------------------------------------------------------------------

type jobRequest struct {
	jobType         string
	payload         json.RawMessage
	jobID           string
	orgID           string
	traceID         string
	executionStepID string
	executionID     string
}

// ---------------------------------------------------------------------
// Dependency injection
// ---------------------------------------------------------------------

var runtimeManagerInstance interface{}

func SetExecutionClient(c *backend.ExecutionClient) {
	execClient = c
}

func SetRuntimeManager(mgr interface{}) {
	runtimeManagerInstance = mgr
}

func GetRuntimeManager() interface{} {
	return runtimeManagerInstance
}

var storageBackend storage.StorageBackend

func SetStorageBackend(backend storage.StorageBackend) {
	storageBackend = backend
}

func GetStorageBackend() storage.StorageBackend {
	if storageBackend == nil {
		panic("StorageBackend: accessed before initialization - call SetStorageBackend first")
	}
	return storageBackend
}

// ---------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------

func InitWorkerPool(n int) {
	if n <= 0 {
		n = DefaultMaxWorkers
	}

	maxAllowed := runtime.NumCPU() * 2
	if maxAllowed < 1 {
		maxAllowed = 1
	}
	if n > maxAllowed {
		log.Printf("[dispatcher] capping workers from %d to CPU limit %d", n, maxAllowed)
		n = maxAllowed
	}

	jobQueue = make(chan jobRequest, DefaultQueueSize)
	atomic.StoreInt32(&maxWorkers, int32(n))

	for i := 0; i < n; i++ {
		startWorker(i)
	}

	log.Printf(
		"[dispatcher] initialized with %d workers (queue=%d)",
		n,
		DefaultQueueSize,
	)
}

// ---------------------------------------------------------------------
// Worker lifecycle
// ---------------------------------------------------------------------

func startWorker(id int) {
	wgWorkers.Add(1)

	go func(workerID int) {
		atomic.AddInt32(&activeWorkers, 1)
		defer func() {
			atomic.AddInt32(&activeWorkers, -1)
			wgWorkers.Done()

			if r := recover(); r != nil {
				log.Printf(
					"⚠️ [dispatcher.worker-%d] recovered from panic: %v",
					workerID,
					r,
				)
			}
		}()

		log.Printf("🧵 [dispatcher.worker-%d] started", workerID)

		for {
			select {
			case <-shutdownSignal:
				return

			case <-resizeSignal:
				return

			case req, ok := <-jobQueue:
				if !ok {
					return
				}
				executeJobSafe(workerID, req)
			}
		}
	}(id)
}

// ---------------------------------------------------------------------
// Execution wrapper
// ---------------------------------------------------------------------

func executeJobSafe(id int, req jobRequest) {
	_ = id

	// DEBUG: Log worker received job
	log.Printf("[DEBUG] WORKER START → job_id=%s execution_id=%s job_type=%s", req.jobID, req.executionID, req.jobType)

	defer func() {
		if req.executionStepID != "" {
			activeJobsMutex.Lock()
			delete(activeJobs, req.executionStepID)
			activeJobsMutex.Unlock()
		}
		if req.jobID != "" {
			activeJobsMutex.Lock()
			delete(runningJobs, req.jobID)
			activeJobsMutex.Unlock()
		}
	}()

	start := time.Now()
	ctx := obs.WithTrace(context.Background(), req.traceID)

	// ---- Verify lease before execution ----
	if execClient != nil && req.jobID != "" {
		leaseStatus, err := execClient.VerifyJobLease(ctx, req.jobID)
		if err != nil {
			obs.Error("lease verification failed, aborting job", obs.Field{
				"job_id": req.jobID,
				"error":  err.Error(),
			})
			atomic.AddInt64(&totalFailed, 1)
			if repErr := execClient.ReportJobFailure(
				context.Background(),
				req.jobID,
				fmt.Errorf("lease verification failed: %w", err),
			); repErr != nil {
				obs.Warn("failed to report job failure to backend", obs.Field{
					"job_id": req.jobID,
					"error":  repErr.Error(),
				})
			}
			return
		}
		if !leaseStatus.IsValid {
			obs.Warn("lease invalid - job may be claimed by another agent", obs.Field{
				"job_id":     req.jobID,
				"agent_id":   leaseStatus.AgentID,
				"job_status": leaseStatus.Status,
			})
			atomic.AddInt64(&totalFailed, 1)
			if repErr := execClient.ReportJobFailure(
				context.Background(),
				req.jobID,
				fmt.Errorf("lease invalid - job claimed by another agent"),
			); repErr != nil {
				obs.Warn("failed to report job failure to backend", obs.Field{
					"job_id": req.jobID,
					"error":  repErr.Error(),
				})
			}
			return
		}
	}

	// ---- Job start (DB-enforced) ----
	if execClient != nil && req.jobID != "" {
		// Signal job start to backend (for observability)
		if err := execClient.ReportJobStart(ctx, req.jobID); err != nil {
			log.Printf("[dispatcher] ⚠️ job start rejected: %v", err)
		}
		// Transition job to running state in DB
		if startResult, err := execClient.StartJob(ctx, req.jobID); err != nil {
			log.Printf("[dispatcher] ⚠️ start_job failed: %v", err)
		} else if !startResult.Success {
			log.Printf("[dispatcher] ⚠️ start_job returned false: %s", startResult.Error)
		} else {
			log.Printf("[dispatcher] ✅ job started: %s status=%s", req.jobID, startResult.Status)
		}
	}

	// ---- Start execution heartbeat ----
	var stopHeartbeat func()
	if execClient != nil && req.jobID != "" {
		stopHeartbeat = reporter.StartExecutionHeartbeat(
			req.jobID,
			execClient,
		)
	}

	// ---- Record plugin execution start ----
	var pluginExecID string
	var pluginExecErr error
	if execClient != nil && req.orgID != "" && req.jobID != "" {
		pluginExecID, pluginExecErr = execClient.RecordPluginExecutionStart(
			ctx, req.orgID, req.jobID, req.jobID, "",
		)
		_ = pluginExecID
		_ = pluginExecErr
		if pluginExecErr != nil {
			obs.Warn("failed to record plugin execution start", obs.Field{
				"job_id": req.jobID,
				"error":  pluginExecErr.Error(),
			})
		}
	}

	// ---- Extract execution_id from payload if not provided ----
	// NOTE: This is a fallback - execution_id should already be validated at enqueue
	if req.executionID == "" && len(req.payload) > 0 {
		var payload struct {
			ExecutionID string `json:"execution_id"`
		}
		if err := json.Unmarshal(req.payload, &payload); err == nil && payload.ExecutionID != "" {
			log.Printf("[DEBUG] extracted execution_id from payload: %s", payload.ExecutionID)
			req.executionID = payload.ExecutionID
			obs.Debug("extracted execution_id from payload", obs.Field{
				"job_id":       req.jobID,
				"execution_id": req.executionID,
			})
		} else {
			log.Printf("[DEBUG] failed to extract execution_id from payload or empty - payload=%s", string(req.payload))
		}
	}

	// ============================================================
	// STRICT VALIDATION: execution_id MUST be present
	// ============================================================
	if req.executionID == "" {
		obs.Error("dispatcher: FATAL - execution_id missing at execution start", obs.Field{
			"job_id":   req.jobID,
			"job_type": req.jobType,
		})
		atomic.AddInt64(&totalFailed, 1)
		if execClient != nil && req.jobID != "" {
			execClient.ReportJobFailure(
				context.Background(),
				req.jobID,
				errors.New("dispatcher: execution_id missing at execution - cannot complete job"),
			)
		}
		return
	}

	// ---- Execute job payload ----
	// DEBUG: Log exact values passed to handler
	log.Printf("[DEBUG] BEFORE ExecuteJob → job_id=%s execution_id=%s job_type=%s payload_len=%d",
		req.jobID, req.executionID, req.jobType, len(req.payload))
	
	err := ExecuteJob(
		ctx,
		req.jobType,
		req.payload,
		req.executionID,
	)

	// ---- Record plugin execution end ----
	if execClient != nil && pluginExecID != "" {
		status := "completed"
		errMsg := ""
		if err != nil {
			status = "failed"
			errMsg = err.Error()
		}
		if endErr := execClient.RecordPluginExecutionEnd(ctx, pluginExecID, status, errMsg); endErr != nil {
			obs.Warn("failed to record plugin execution end", obs.Field{
				"execution_id": pluginExecID,
				"error":        endErr.Error(),
			})
		}
	}

	// ---- Stop heartbeat ----
	if stopHeartbeat != nil {
		stopHeartbeat()
	}

	duration := time.Since(start)

	// ---- Failure path ----
	if err != nil {
		atomic.AddInt64(&totalFailed, 1)

		obs.Error("dispatcher: job execution failed", obs.Field{
			"job_id":       req.jobID,
			"execution_id": req.executionID,
			"error":        err.Error(),
		})

		if execClient != nil && req.jobID != "" {
			if repErr := execClient.ReportJobFailure(
				context.Background(),
				req.jobID,
				err,
			); repErr != nil {
				obs.Warn("failed to report job failure", obs.Field{
					"job_id": req.jobID,
					"error":  repErr.Error(),
				})
			}

			result := execClient.CompleteJob(ctx, req.executionID, req.jobID, "failed", duration.Milliseconds(), nil)
			if result.IsStaleExecution() {
				obs.Warn("job failure reported but completion rejected - stale execution", obs.Field{
					"job_id":        req.jobID,
					"lease_expired": result.IsLeaseExpired,
					"already_done":  result.IsAlreadyDone,
				})
			} else if result.Err != nil {
				obs.Error("job failure reported but DB update failed", obs.Field{
					"job_id": req.jobID,
					"error":  result.Err.Error(),
				})
			}
		}

		log.Printf(
			"[dispatcher] ❌ job failed id=%s type=%s err=%v dur=%dms",
			req.jobID,
			req.jobType,
			err,
			duration.Milliseconds(),
		)
		return
	}

	// ---- Success path ----
	atomic.AddInt64(&totalProcessed, 1)

	if execClient != nil && (req.executionID != "" || req.jobID != "") {
		obs.Info("dispatcher: calling complete_job", obs.Field{
			"job_id":       req.jobID,
			"execution_id": req.executionID,
		})
		
		// DEBUG: Log exact values before complete_job
		log.Printf("[DEBUG] BEFORE complete_job → job_id=%s execution_id=%s",
			req.jobID, req.executionID)
		
		result := execClient.CompleteJob(ctx, req.executionID, req.jobID, "completed", duration.Milliseconds(), nil)

		if result.IsStaleExecution() {
			obs.Warn("job execution succeeded but completion rejected - stale execution", obs.Field{
				"job_id":        req.jobID,
				"execution_id":  req.executionID,
				"lease_expired": result.IsLeaseExpired,
				"already_done":  result.IsAlreadyDone,
				"duration_ms":   duration.Milliseconds(),
			})
			if repErr := execClient.ReportJobComplete(context.Background(), req.jobID, duration.Milliseconds(), nil); repErr != nil {
				obs.Warn("failed to report job completion after stale execution", obs.Field{
					"job_id": req.jobID,
					"error":  repErr.Error(),
				})
			}
		} else if result.Err != nil {
			obs.Error("job completed but DB update failed", obs.Field{
				"job_id": req.jobID,
				"error":  result.Err.Error(),
			})
			if repErr := execClient.ReportJobComplete(context.Background(), req.jobID, duration.Milliseconds(), nil); repErr != nil {
				obs.Warn("failed to report job completion after DB failure", obs.Field{
					"job_id": req.jobID,
					"error":  repErr.Error(),
				})
			}
		} else {
			obs.Info("job completed and persisted", obs.Field{
				"job_id":      req.jobID,
				"duration_ms": duration.Milliseconds(),
			})
		}
	}

	log.Printf(
		"[dispatcher] ✅ job done id=%s type=%s dur=%dms",
		req.jobID,
		req.jobType,
		duration.Milliseconds(),
	)
}

// ---------------------------------------------------------------------
// Job submission
// ---------------------------------------------------------------------

func SubmitJobWithMeta(
	jobType string,
	payload json.RawMessage,
	jobID string,
	orgID string,
	traceID string,
	executionStepID string,
	executionID string,
) error {
	if jobQueue == nil {
		return errors.New("dispatcher: pool not initialized")
	}

	// ============================================================
	// MANDATORY VALIDATION: Drop malformed jobs at enqueue
	// ============================================================
	
	// job_id is required
	if jobID == "" {
		obs.Error("dispatcher: rejecting job with empty job_id", obs.Field{
			"job_type": jobType,
		})
		return errors.New("dispatcher: job_id is required")
	}
	
	// execution_id is CRITICAL - required for completion
	if executionID == "" {
		obs.Error("dispatcher: rejecting job with empty execution_id", obs.Field{
			"job_id":   jobID,
			"job_type": jobType,
		})
		return errors.New("dispatcher: execution_id is required for completion")
	}
	
	// job_type is required
	if jobType == "" {
		obs.Error("dispatcher: rejecting job with empty job_type", obs.Field{
			"job_id": jobID,
		})
		return errors.New("dispatcher: job_type is required")
	}

	obs.Info("dispatcher: enqueueing job", obs.Field{
		"job_id":        jobID,
		"job_type":      jobType,
		"execution_id":  executionID,
		"payload_len":   len(payload),
	})

	// DEBUG: Log enqueue point
	log.Printf("[DEBUG] ENQUEUE → job_id=%s execution_id=%s job_type=%s", jobID, executionID, jobType)

	if jobID != "" {
		activeJobsMutex.Lock()
		if _, exists := runningJobs[jobID]; exists {
			activeJobsMutex.Unlock()
			obs.Warn("dispatcher: job already running", obs.Field{
				"job_id":   jobID,
				"job_type": jobType,
			})
			return errors.New("dispatcher: duplicate job rejected - job already running")
		}
		runningJobs[jobID] = struct{}{}
		activeJobsMutex.Unlock()
	}

	if executionStepID != "" {
		activeJobsMutex.Lock()
		if _, exists := activeJobs[executionStepID]; exists {
			activeJobsMutex.Unlock()
			obs.Warn("dispatcher: job already active for execution_step", obs.Field{
				"job_id":            jobID,
				"execution_step_id": executionStepID,
			})
			if jobID != "" {
				activeJobsMutex.Lock()
				delete(runningJobs, jobID)
				activeJobsMutex.Unlock()
			}
			return errors.New("dispatcher: duplicate job rejected - one active job per execution step")
		}
		activeJobs[executionStepID] = struct{}{}
		activeJobsMutex.Unlock()
	}

	req := jobRequest{
		jobType:         jobType,
		payload:         payload,
		jobID:           jobID,
		orgID:           orgID,
		traceID:         traceID,
		executionStepID: executionStepID,
		executionID:     executionID,
	}

	queueLen := len(jobQueue)
	queueCap := cap(jobQueue)
	queueUtil := float64(queueLen) / float64(queueCap)

	// Backpressure: if queue is above high water mark, signal pause
	if queueUtil > HighWaterMarkPercent && atomic.LoadInt32(&backpressureEnabled) == 1 {
		obs.Warn("queue backpressure: high utilization, signaling SSE pause", obs.Field{
			"job_id":      jobID,
			"job_type":    jobType,
			"queue_len":   queueLen,
			"queue_cap":   queueCap,
			"utilization": queueUtil,
		})

		// Call pause function if registered
		if pauseSSE != nil {
			pauseSSE()
		}
	}

	select {
	case jobQueue <- req:
		if queueUtil > HighWaterMarkPercent {
			obs.Warn("queue utilization high after enqueue", obs.Field{
				"job_id":      jobID,
				"job_type":    jobType,
				"queue_len":   queueLen,
				"queue_cap":   queueCap,
				"utilization": queueUtil,
			})
		}
		return nil
	default:
		if executionStepID != "" {
			activeJobsMutex.Lock()
			delete(activeJobs, executionStepID)
			activeJobsMutex.Unlock()
		}
		if jobID != "" {
			activeJobsMutex.Lock()
			delete(runningJobs, jobID)
			activeJobsMutex.Unlock()
		}
		obs.Warn("dispatcher: queue full, dropping job", obs.Field{
			"job_id":      jobID,
			"job_type":    jobType,
			"queue_len":   queueLen,
			"queue_cap":   queueCap,
			"utilization": queueUtil,
		})
		atomic.AddInt64(&totalFailed, 1)

		if execClient != nil && jobID != "" {
			err := execClient.ReportJobFailure(
				context.Background(),
				jobID,
				errors.New("dispatcher: queue full, job dropped"),
			)
			if err != nil {
				obs.Error("failed to report queue-full job failure to backend", obs.Field{
					"job_id": jobID,
					"error":  err.Error(),
				})
			}
		}

		return errors.New("dispatcher: queue full, job dropped")
	}
}

// ---------------------------------------------------------------------
// Dynamic resize
// ---------------------------------------------------------------------

func SetMaxWorkers(newCount int) {
	if newCount <= 0 {
		newCount = 1
	}

	muResize.Lock()
	defer muResize.Unlock()

	current := int(atomic.LoadInt32(&maxWorkers))
	if newCount == current {
		return
	}

	if newCount > current {
		for i := current; i < newCount; i++ {
			startWorker(i)
		}
	} else {
		diff := current - newCount
		for i := 0; i < diff; i++ {
			select {
			case resizeSignal <- struct{}{}:
			default:
			}
		}
	}

	atomic.StoreInt32(&maxWorkers, int32(newCount))
}

// ---------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------

func StopWorkerPool(ctx context.Context) {
	shutdownOnce.Do(func() {
		close(shutdownSignal)
	})

	done := make(chan struct{})
	go func() {
		wgWorkers.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// ---------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------

func MaxWorkersCount() int32 {
	return atomic.LoadInt32(&maxWorkers)
}

// QueueUtilization returns the current queue utilization as a percentage (0.0-1.0)
func QueueUtilization() float64 {
	if jobQueue == nil {
		return 0
	}
	queueLen := len(jobQueue)
	queueCap := cap(jobQueue)
	if queueCap == 0 {
		return 0
	}
	return float64(queueLen) / float64(queueCap)
}

// SetBackpressure enables or disables queue backpressure
func SetBackpressure(enabled bool) {
	if enabled {
		atomic.StoreInt32(&backpressureEnabled, 1)
	} else {
		atomic.StoreInt32(&backpressureEnabled, 0)
	}
}

// ShouldResumeSSE returns true if SSE should resume sending jobs
// based on queue utilization being below the low water mark
func ShouldResumeSSE() bool {
	return QueueUtilization() < LowWaterMarkPercent
}

var jobProcessingPaused int32

func PauseJobProcessing() {
	atomic.StoreInt32(&jobProcessingPaused, 1)
}

func ResumeJobProcessing() {
	atomic.StoreInt32(&jobProcessingPaused, 0)
}

func IsJobProcessingPaused() bool {
	return atomic.LoadInt32(&jobProcessingPaused) == 1
}

func ActiveJobsCount() int {
	activeJobsMutex.Lock()
	defer activeJobsMutex.Unlock()
	count := len(runningJobs)
	return count
}

func IsJobActive(jobID string) bool {
	activeJobsMutex.Lock()
	defer activeJobsMutex.Unlock()
	_, inActive := activeJobs[jobID]
	_, inRunning := runningJobs[jobID]
	return inActive || inRunning
}

func ReleaseAllMergeLocks() {
	lockMgr := dataset.GetMergeLockManager()
	if lockMgr == nil {
		log.Println("[dispatcher] ReleaseAllMergeLocks: lock manager not initialized")
		return
	}

	activeJobsMutex.Lock()
	jobIDsToRelease := make([]string, 0, len(activeJobs)+len(runningJobs))
	for jobID := range activeJobs {
		jobIDsToRelease = append(jobIDsToRelease, jobID)
	}
	for jobID := range runningJobs {
		jobIDsToRelease = append(jobIDsToRelease, jobID)
	}
	activeJobsMutex.Unlock()

	for _, jobID := range jobIDsToRelease {
		if err := lockMgr.ReleaseMergeLock(jobID); err != nil {
			log.Printf("[dispatcher] failed to release merge lock for job %s: %v", jobID, err)
		}
	}

	lockMgr.Stop()
	log.Println("[dispatcher] ReleaseAllMergeLocks: merge locks released for in-flight jobs")
}

func ReportInFlightFailures(client interface{}) {
	activeJobsMutex.Lock()
	defer activeJobsMutex.Unlock()

	execClient, ok := client.(*backend.ExecutionClient)
	if !ok {
		for jobID := range activeJobs {
			log.Printf("[dispatcher] Reporting active job failed: %s", jobID)
		}
		for jobID := range runningJobs {
			log.Printf("[dispatcher] Reporting in-flight job failed: %s", jobID)
		}
		return
	}

	for jobID := range activeJobs {
		log.Printf("[dispatcher] Reporting active job failed: %s", jobID)
		if execClient != nil {
			if repErr := execClient.ReportJobFailure(context.Background(), jobID, errors.New("agent shutdown during graceful drain")); repErr != nil {
				obs.Warn("failed to report in-flight job failure", obs.Field{
					"job_id": jobID,
					"error":  repErr.Error(),
				})
			}
		}
	}
	for jobID := range runningJobs {
		log.Printf("[dispatcher] Reporting in-flight job failed: %s", jobID)
		if execClient != nil {
			if repErr := execClient.ReportJobFailure(context.Background(), jobID, errors.New("agent shutdown during graceful drain")); repErr != nil {
				obs.Warn("failed to report in-flight job failure", obs.Field{
					"job_id": jobID,
					"error":  repErr.Error(),
				})
			}
		}
	}
}
