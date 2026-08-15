package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sentra-agent/internal/backend"
)

func resetPoolState() {
	jobQueue = nil
	atomic.StoreInt32(&activeWorkers, 0)
	atomic.StoreInt32(&maxWorkers, 0)
	atomic.StoreInt64(&totalProcessed, 0)
	atomic.StoreInt64(&totalFailed, 0)
	runningJobs = make(map[string]struct{})
	activeJobs = make(map[string]struct{})
	execClient = nil
	// Recreate the shutdown machinery so each test starts with live workers
	// regardless of which tests ran before (previously shutdownSignal stayed
	// closed after the first StopWorkerPool, silently killing workers in
	// later tests and making queue/dedup tests order-dependent).
	shutdownOnce = sync.Once{}
	shutdownSignal = make(chan struct{})
}

// blockingExecutionClient is a deterministic fake backend client used by the
// queue-full and execution-step dedup tests. VerifyJobLease blocks until
// release() is called, which pins the worker inside executeJobSafe: the queue
// is not drained and dedup entries are not removed until the test releases
// the worker. This makes those behaviors deterministic instead of racing the
// worker goroutine (previously SetExecutionClient(nil) let workers finish
// each "test" job in microseconds — fast enough to empty the queue or drop
// the dedup entry before the test could observe it).
type blockingExecutionClient struct {
	blocked     chan struct{} // receives once a worker has entered VerifyJobLease
	released    chan struct{} // closed by release() to unblock the worker
	releaseOnce sync.Once     // release() is idempotent (defer + explicit call)
}

func newBlockingExecutionClient() *blockingExecutionClient {
	return &blockingExecutionClient{
		blocked:  make(chan struct{}, 1),
		released: make(chan struct{}),
	}
}

// blockedCh returns a channel that receives once the worker has consumed the
// pinned job and is inside VerifyJobLease (i.e., the queue slot is free and
// the dedup entry is guaranteed to persist).
func (c *blockingExecutionClient) blockedCh() <-chan struct{} { return c.blocked }

// release unblocks the pinned worker so it can finish and StopWorkerPool can
// drain cleanly.
func (c *blockingExecutionClient) release() {
	c.releaseOnce.Do(func() { close(c.released) })
}

func (c *blockingExecutionClient) GetDeviceID() string { return "test-device" }

func (c *blockingExecutionClient) VerifyJobLease(ctx context.Context, jobID string) (*backend.JobLeaseStatus, error) {
	select {
	case c.blocked <- struct{}{}:
	default:
	}
	select {
	case <-c.released:
		return &backend.JobLeaseStatus{JobID: jobID, Status: "running", AgentID: "test-device", IsValid: true}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *blockingExecutionClient) ReportJobFailure(ctx context.Context, jobID string, err error) error {
	return nil
}
func (c *blockingExecutionClient) ReportJobStart(ctx context.Context, jobID string) error { return nil }
func (c *blockingExecutionClient) StartJob(ctx context.Context, jobID string) (*backend.StartJobResult, error) {
	return &backend.StartJobResult{Success: true, Ok: true, JobID: jobID, Status: "running"}, nil
}
func (c *blockingExecutionClient) RecordPluginExecutionStart(ctx context.Context, orgID, pluginID, jobID, deviceID string) (string, error) {
	return "test-exec", nil
}
func (c *blockingExecutionClient) RecordPluginExecutionEnd(ctx context.Context, executionID, status, errMsg string) error {
	return nil
}
func (c *blockingExecutionClient) CompleteJob(ctx context.Context, executionID, jobID, status string, durationMs int64, resultData any, isLocal bool, isLastStep bool) *backend.CompleteJobResult {
	return &backend.CompleteJobResult{}
}
func (c *blockingExecutionClient) BufferRelayEvent(ev backend.RelayJobEvent) error { return nil }
func (c *blockingExecutionClient) SendJobExecutionHeartbeat(ctx context.Context, jobID string) error {
	return nil
}

func TestInitWorkerPool_DefaultWorkers(t *testing.T) {
	defer resetPoolState()

	InitWorkerPool(2)
	defer StopWorkerPool(context.Background())

	if atomic.LoadInt32(&maxWorkers) != 2 {
		t.Errorf("expected maxWorkers=2, got %d", atomic.LoadInt32(&maxWorkers))
	}
}

func TestInitWorkerPool_ZeroWorkers(t *testing.T) {
	defer resetPoolState()

	InitWorkerPool(0)
	defer StopWorkerPool(context.Background())

	if atomic.LoadInt32(&maxWorkers) != int32(DefaultMaxWorkers) {
		t.Errorf("expected maxWorkers=%d (default), got %d", DefaultMaxWorkers, atomic.LoadInt32(&maxWorkers))
	}
}

func TestInitWorkerPool_NegativeWorkers(t *testing.T) {
	defer resetPoolState()

	InitWorkerPool(-5)
	defer StopWorkerPool(context.Background())

	if atomic.LoadInt32(&maxWorkers) != int32(DefaultMaxWorkers) {
		t.Errorf("expected maxWorkers=%d (default), got %d", DefaultMaxWorkers, atomic.LoadInt32(&maxWorkers))
	}
}

func TestSubmitJobWithMeta_PoolNotInitialized(t *testing.T) {
	defer resetPoolState()

	payload, _ := json.Marshal(map[string]string{"test": "data"})
	err := SubmitJobWithMeta("test", payload, "job-1", "org-1", "trace-1", "", "")

	if err == nil {
		t.Error("expected error when pool not initialized")
	}
	if err.Error() != "dispatcher: pool not initialized" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSubmitJobWithMeta_DuplicateJobRejected(t *testing.T) {
	defer resetPoolState()

	client := newBlockingExecutionClient()
	defer client.release()

	InitWorkerPool(2)
	SetExecutionClient(client)
	SetBackpressure(false)

	payload, _ := json.Marshal(map[string]string{"test": "data"})

	err := SubmitJobWithMeta("test", payload, "job-duplicate", "org-1", "trace-1", "", "exec-duplicate")
	if err != nil {
		t.Fatalf("first submission failed: %v", err)
	}
	// Wait until a worker has consumed the job and is pinned inside
	// VerifyJobLease, so runningJobs[jobID] is guaranteed to still exist.
	<-client.blockedCh()

	err = SubmitJobWithMeta("test", payload, "job-duplicate", "org-1", "trace-1", "", "exec-duplicate")
	if err == nil {
		t.Error("expected error for duplicate job")
	}

	client.release()
	StopWorkerPool(context.Background())
}

func TestQueueUtilization(t *testing.T) {
	defer resetPoolState()

	util := QueueUtilization()
	if util != 0 {
		t.Errorf("expected utilization=0 when pool nil, got %f", util)
	}
}

func TestSetMaxWorkers(t *testing.T) {
	defer resetPoolState()

	InitWorkerPool(2)

	SetMaxWorkers(4)
	if atomic.LoadInt32(&maxWorkers) != 4 {
		t.Errorf("expected maxWorkers=4, got %d", atomic.LoadInt32(&maxWorkers))
	}

	SetMaxWorkers(1)
	if atomic.LoadInt32(&maxWorkers) != 1 {
		t.Errorf("expected maxWorkers=1, got %d", atomic.LoadInt32(&maxWorkers))
	}

	StopWorkerPool(context.Background())
}

func TestSetMaxWorkers_Zero(t *testing.T) {
	defer resetPoolState()

	InitWorkerPool(4)

	SetMaxWorkers(0)
	if atomic.LoadInt32(&maxWorkers) != 1 {
		t.Errorf("expected maxWorkers=1 (minimum), got %d", atomic.LoadInt32(&maxWorkers))
	}

	StopWorkerPool(context.Background())
}

func TestSetBackpressure(t *testing.T) {
	defer resetPoolState()

	SetBackpressure(true)
	if atomic.LoadInt32(&backpressureEnabled) != 1 {
		t.Error("expected backpressure enabled")
	}

	SetBackpressure(false)
	if atomic.LoadInt32(&backpressureEnabled) != 0 {
		t.Error("expected backpressure disabled")
	}
}

func TestShouldResumeSSE_EmptyQueue(t *testing.T) {
	defer resetPoolState()

	InitWorkerPool(1)
	defer StopWorkerPool(context.Background())

	if !ShouldResumeSSE() {
		t.Error("expected SSE resume when queue empty")
	}
}

func TestJobRequest_Structure(t *testing.T) {
	req := jobRequest{
		jobType:         "test",
		payload:         json.RawMessage(`{"key":"value"}`),
		jobID:           "job-123",
		orgID:           "org-456",
		traceID:         "trace-789",
		executionStepID: "step-abc",
	}

	if req.jobType != "test" {
		t.Errorf("expected jobType=test, got %s", req.jobType)
	}
	if req.jobID != "job-123" {
		t.Errorf("expected jobID=job-123, got %s", req.jobID)
	}
}

func TestSubmitJobWithMeta_QueueFullDrop(t *testing.T) {
	defer resetPoolState()

	client := newBlockingExecutionClient()
	defer client.release()

	InitWorkerPool(1)
	SetExecutionClient(client)
	SetBackpressure(false)

	// Pin the single worker inside VerifyJobLease first. This frees the queue
	// slot for the seed job AND guarantees no worker will drain the jobs we
	// are about to enqueue — the queue can only fill up.
	seedPayload, _ := json.Marshal(map[string]string{"seed": "true"})
	if err := SubmitJobWithMeta("test", seedPayload, "seed-job", "org-1", "", "", "exec-queue"); err != nil {
		t.Fatalf("seed submission failed: %v", err)
	}
	<-client.blockedCh()

	for i := 0; i < DefaultQueueSize; i++ {
		payload, _ := json.Marshal(map[string]int{"index": i})
		err := SubmitJobWithMeta("test", payload, fmt.Sprintf("job-%d", i), "org-1", "", "", "exec-queue")
		if err != nil {
			t.Fatalf("submission %d failed: %v", i, err)
		}
	}

	payload, _ := json.Marshal(map[string]string{"overflow": "true"})
	err := SubmitJobWithMeta("test", payload, "overflow-job", "org-1", "", "", "exec-queue")
	if err == nil {
		t.Error("expected error when queue full")
	}

	client.release()
	StopWorkerPool(context.Background())
}

func TestStopWorkerPool_Context(t *testing.T) {
	defer resetPoolState()

	InitWorkerPool(2)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	StopWorkerPool(ctx)
}

func TestSetPauseSSE(t *testing.T) {
	pauseCalled := false
	SetPauseSSE(func() bool {
		pauseCalled = true
		return true
	})

	if pauseSSE == nil {
		t.Error("pauseSSE not set")
	}

	pauseSSE()

	if !pauseCalled {
		t.Error("pauseSSE callback not called")
	}
}

func TestMetrics(t *testing.T) {
	defer resetPoolState()

	InitWorkerPool(2)
	defer StopWorkerPool(context.Background())

	if MaxWorkersCount() != 2 {
		t.Errorf("expected MaxWorkersCount=2, got %d", MaxWorkersCount())
	}
}

func TestExecutionStepDedup(t *testing.T) {
	defer resetPoolState()

	client := newBlockingExecutionClient()
	defer client.release()

	InitWorkerPool(2)
	SetExecutionClient(client)
	SetBackpressure(false)

	payload, _ := json.Marshal(map[string]string{"test": "data"})

	err := SubmitJobWithMeta("test", payload, "step-job-1", "org-1", "trace-1", "step-dedup", "exec-dedup")
	if err != nil {
		t.Fatalf("first submission failed: %v", err)
	}
	// Wait until a worker has consumed the job and is pinned inside
	// VerifyJobLease, so the activeJobs[step-dedup] entry is guaranteed to
	// still exist (the deferred cleanup only runs after executeJobSafe returns).
	<-client.blockedCh()

	err = SubmitJobWithMeta("test", payload, "step-job-2", "org-1", "trace-2", "step-dedup", "exec-dedup")
	if err == nil {
		t.Error("expected error for duplicate execution step")
	}

	client.release()
	StopWorkerPool(context.Background())
}
