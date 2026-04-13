package sandbox

import (
	"fmt"
	"sync"
	"time"
)

// Limits defines execution constraints for a plugin.
// All fields are mandatory.
type Limits struct {
	MaxMemoryMB   int64
	MaxCPUSeconds int64
	Timeout       time.Duration
}

// Validate enforces default-deny semantics.
func (l Limits) Validate() error {
	if l.MaxMemoryMB <= 0 {
		return fmt.Errorf("sandbox: MaxMemoryMB must be > 0")
	}
	if l.MaxCPUSeconds <= 0 {
		return fmt.Errorf("sandbox: MaxCPUSeconds must be > 0")
	}
	if l.Timeout <= 0 {
		return fmt.Errorf("sandbox: Timeout must be > 0")
	}
	return nil
}

// contextWatcher tracks goroutines that watch context for cancellation
type contextWatcher struct {
	cancel func()
	done   chan struct{}
}

var (
	watcherMu   sync.Mutex
	watchers    = make(map[*contextWatcher]struct{})
	watcherOnce sync.Once
)

func initWatcherRegistry() {
	watcherOnce.Do(func() {
		go cleanupWatchers()
	})
}

func registerWatcher(w *contextWatcher) {
	watcherMu.Lock()
	defer watcherMu.Unlock()
	watchers[w] = struct{}{}
}

func unregisterWatcher(w *contextWatcher) {
	watcherMu.Lock()
	defer watcherMu.Unlock()
	delete(watchers, w)
	close(w.done)
}

func cleanupWatchers() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		watcherMu.Lock()
		for w := range watchers {
			select {
			case <-w.done:
				delete(watchers, w)
			default:
			}
		}
		watcherMu.Unlock()
	}
}

// StopAllWatchers stops all registered context watchers
func StopAllWatchers() {
	watcherMu.Lock()
	defer watcherMu.Unlock()

	for w := range watchers {
		w.cancel()
	}
	watchers = make(map[*contextWatcher]struct{})
}
