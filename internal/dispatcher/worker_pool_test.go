package dispatcher

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

func resetPoolState() {
	jobQueue = nil
	atomic.StoreInt32(&activeWorkers, 0)
	atomic.StoreInt32(&maxWorkers, 0)
	atomic.StoreInt64(&totalProcessed, 0)
	atomic.StoreInt64(&totalFailed, 0)
	runningJobs = make(map[string]struct{})
	activeJobs = make(map[string]struct{})
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

	InitWorkerPool(2)
	SetExecutionClient(nil)
	SetBackpressure(false)

	payload, _ := json.Marshal(map[string]string{"test": "data"})

	err := SubmitJobWithMeta("test", payload, "job-duplicate", "org-1", "trace-1", "", "")
	if err != nil {
		t.Fatalf("first submission failed: %v", err)
	}

	err = SubmitJobWithMeta("test", payload, "job-duplicate", "org-1", "trace-1", "", "")
	if err == nil {
		t.Error("expected error for duplicate job")
	}

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

	InitWorkerPool(1)
	SetExecutionClient(nil)
	SetBackpressure(false)

	for i := 0; i < DefaultQueueSize; i++ {
		payload, _ := json.Marshal(map[string]int{"index": i})
		err := SubmitJobWithMeta("test", payload, "", "org-1", "", "", "")
		if err != nil {
			t.Fatalf("submission %d failed: %v", i, err)
		}
	}

	payload, _ := json.Marshal(map[string]string{"overflow": "true"})
	err := SubmitJobWithMeta("test", payload, "overflow-job", "org-1", "", "", "")
	if err == nil {
		t.Error("expected error when queue full")
	}

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

	InitWorkerPool(2)
	SetExecutionClient(nil)
	SetBackpressure(false)

	payload, _ := json.Marshal(map[string]string{"test": "data"})

	err := SubmitJobWithMeta("test", payload, "step-job-1", "org-1", "trace-1", "step-dedup", "")
	if err != nil {
		t.Fatalf("first submission failed: %v", err)
	}

	err = SubmitJobWithMeta("test", payload, "step-job-2", "org-1", "trace-2", "step-dedup", "")
	if err == nil {
		t.Error("expected error for duplicate execution step")
	}

	StopWorkerPool(context.Background())
}
