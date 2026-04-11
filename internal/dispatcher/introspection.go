package dispatcher

import "sync/atomic"

// CurrentWorkerCount returns the number of active workers.
// Safe for concurrent use.
func CurrentWorkerCount() int {
	return int(atomic.LoadInt32(&activeWorkers))
}

// QueueLength returns the current job queue length.
// Safe even if dispatcher is not initialized.
func QueueLength() int {
	if jobQueue == nil {
		return 0
	}
	return len(jobQueue)
}

// MaxWorkerLimit returns the configured max workers.
func MaxWorkerLimit() int {
	return int(atomic.LoadInt32(&maxWorkers))
}

// TotalProcessed returns total successfully processed jobs.
func TotalProcessed() int64 {
	return atomic.LoadInt64(&totalProcessed)
}

// TotalFailed returns total failed jobs.
func TotalFailed() int64 {
	return atomic.LoadInt64(&totalFailed)
}
