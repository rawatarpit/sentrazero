package reporter

import (
	"context"
	"log"
	"os"
	"sync"
	"time"
)

var jobHeartbeatInterval = 60 * time.Second

func init() {
	if v := os.Getenv("SENTRA_JOB_HEARTBEAT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			jobHeartbeatInterval = d
		}
	}
}

// ExecutionClient is the minimal contract required by the reporter.
// This intentionally mirrors backend.ExecutionClient behavior.
type ExecutionClient interface {
	SendJobExecutionHeartbeat(ctx context.Context, jobID string) error
}

var (
	hbMu sync.Mutex
	hbs  = make(map[string]context.CancelFunc)
)

// StartExecutionHeartbeat starts periodic job execution heartbeat.
// Returns a stop() function that MUST be called on job completion/failure.
func StartExecutionHeartbeat(
	jobID string,
	client ExecutionClient,
) func() {
	hbMu.Lock()
	defer hbMu.Unlock()

	// Prevent duplicate heartbeats per job
	if _, exists := hbs[jobID]; exists {
		return func() {}
	}

	ctx, cancel := context.WithCancel(context.Background())
	hbs[jobID] = cancel

	go func() {
		ticker := time.NewTicker(jobHeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				err := client.SendJobExecutionHeartbeat(context.Background(), jobID)
				if err != nil {
					log.Printf(
						"[heartbeat] ⚠️ job %s heartbeat rejected: %v",
						jobID,
						err,
					)
				}
			}
		}
	}()

	return func() {
		hbMu.Lock()
		defer hbMu.Unlock()

		if cancel, ok := hbs[jobID]; ok {
			cancel()
			delete(hbs, jobID)
		}
	}
}
