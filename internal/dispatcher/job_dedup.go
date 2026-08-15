package dispatcher

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// JobDeduplicationStore provides persistent deduplication of job IDs.
// Uses a simple file-based storage with TTL support.
// Security: Prevents processing of duplicate jobs across agent restarts.
type JobDeduplicationStore struct {
	mu           sync.RWMutex
	saveMu       sync.Mutex
	filePath     string
	ttl          time.Duration
	maxJobs      int
	jobs         map[string]time.Time
	cleanupTimer *time.Timer
	snapshotCh   chan map[string]time.Time
}

const (
	defaultDedupTTL     = 60 * time.Minute
	defaultDedupMaxJobs = 1000
	dedupFileName       = "job_dedup.json"
)

// newJobDeduplicationStore creates a new deduplication store.
func newJobDeduplicationStore(dataDir string, ttl time.Duration) (*JobDeduplicationStore, error) {
	if ttl <= 0 {
		ttl = defaultDedupTTL
	}

	maxJobs := defaultDedupMaxJobs
	if v := os.Getenv("SENTRA_DEDUP_MAX_JOBS"); v != "" {
		var parsed int
		if fmt.Sscanf(v, "%d", &parsed); parsed > 0 && parsed > maxJobs {
			maxJobs = parsed
		}
	}

	store := &JobDeduplicationStore{
		filePath:   filepath.Join(dataDir, dedupFileName),
		ttl:        ttl,
		maxJobs:    maxJobs,
		jobs:       make(map[string]time.Time),
		snapshotCh: make(chan map[string]time.Time, 1),
	}

	// Load existing jobs from disk
	if err := store.load(); err != nil {
		// Log but continue - we'll start fresh
		fmt.Printf("[dedup] warning: failed to load existing jobs: %v\n", err)
	}

	// Start periodic cleanup
	store.startCleanup()

	// Start snapshot worker
	go store.snapshotWorker()

	return store, nil
}

// NewJobDeduplicationStore is the exported constructor for the persistent
// file-backed dedup store. ttl <= 0 selects the default (60 minutes).
func NewJobDeduplicationStore(dataDir string, ttl time.Duration) (*JobDeduplicationStore, error) {
	return newJobDeduplicationStore(dataDir, ttl)
}

// globalDedupStore is the package-level file-backed dedup store, installed by
// SetDedupStore from the agent entrypoint. It is consulted in addition to the
// in-memory runningJobs/activeJobs maps so duplicates survive restarts.
var globalDedupStore *JobDeduplicationStore

// SetDedupStore installs the persistent dedup store used by SubmitJobWithMeta.
func SetDedupStore(store *JobDeduplicationStore) {
	globalDedupStore = store
	if store != nil {
		fmt.Printf("[dedup] file-backed dedup store enabled: %s (ttl=%v, max=%d)\n",
			store.filePath, store.ttl, store.maxJobs)
	}
}

// load loads job IDs from the persistent storage file.
func (s *JobDeduplicationStore) load() error {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No file yet - fresh start
		}
		return err
	}

	var jobs map[string]time.Time
	if err := json.Unmarshal(data, &jobs); err != nil {
		return err
	}

	// Filter out expired jobs
	now := time.Now()
	validJobs := make(map[string]time.Time)
	for jobID, expiry := range jobs {
		if expiry.After(now) {
			validJobs[jobID] = expiry
		}
	}

	s.jobs = validJobs
	return nil
}

// save persists job IDs to disk.
func (s *JobDeduplicationStore) save() error {
	data, err := json.Marshal(s.jobs)
	if err != nil {
		return err
	}

	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}

	tmpPath := s.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}

	return os.Rename(tmpPath, s.filePath)
}

// Contains checks if a job ID exists in the deduplication store.
// Returns true if the job was seen within the TTL window.
func (s *JobDeduplicationStore) Contains(jobID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	expiry, exists := s.jobs[jobID]
	if !exists {
		return false
	}

	// Check if expired
	if time.Now().After(expiry) {
		return false
	}

	return true
}

// Add adds a job ID to the deduplication store with TTL.
// Thread-safe - can be called concurrently.
func (s *JobDeduplicationStore) Add(jobID string) {
	s.mu.Lock()
	s.jobs[jobID] = time.Now().Add(s.ttl)

	var snapshot map[string]time.Time
	if len(s.jobs)%10 == 0 {
		snapshot = make(map[string]time.Time, len(s.jobs))
		for k, v := range s.jobs {
			snapshot[k] = v
		}
	}
	s.mu.Unlock()

	if snapshot != nil {
		select {
		case s.snapshotCh <- snapshot:
		default:
		}
	}
}

func (s *JobDeduplicationStore) snapshotWorker() {
	for snapshot := range s.snapshotCh {
		s.saveMu.Lock()
		if err := s.saveSnapshot(snapshot); err != nil {
			fmt.Printf("[dedup] error saving: %v\n", err)
		}
		s.saveMu.Unlock()
	}
}

// saveSnapshot marshals a pre-copied snapshot — no lock needed.
func (s *JobDeduplicationStore) saveSnapshot(snapshot map[string]time.Time) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	tmpPath := s.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmpPath, s.filePath)
}

// Remove removes a job ID from the deduplication store.
func (s *JobDeduplicationStore) Remove(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.jobs, jobID)
}

// Count returns the number of tracked job IDs.
func (s *JobDeduplicationStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Filter expired
	now := time.Now()
	count := 0
	for _, expiry := range s.jobs {
		if expiry.After(now) {
			count++
		}
	}
	return count
}

// startCleanup runs periodic cleanup of expired job IDs.
func (s *JobDeduplicationStore) startCleanup() {
	s.cleanupTimer = time.AfterFunc(2*time.Minute, func() {
		s.cleanup()
		// Reschedule without holding mutex - start a new timer
		go func() {
			time.Sleep(2 * time.Minute)
			s.mu.Lock()
			if s.cleanupTimer != nil {
				s.cleanupTimer.Reset(2 * time.Minute)
			}
			s.mu.Unlock()
		}()
	})
}

// cleanup removes all expired job IDs from the store.
func (s *JobDeduplicationStore) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	changed := false

	// Collect expired keys first to avoid mutating during iteration
	var expiredKeys []string
	for jobID, expiry := range s.jobs {
		if now.After(expiry) {
			expiredKeys = append(expiredKeys, jobID)
		}
	}

	// Remove expired entries
	for _, jobID := range expiredKeys {
		delete(s.jobs, jobID)
		changed = true
	}

	// Enforce max size - precompute eviction count
	if len(s.jobs) > defaultDedupMaxJobs {
		evictCount := len(s.jobs) - defaultDedupMaxJobs

		// Collect oldest entries for eviction
		type jobEntry struct {
			jobID  string
			expiry time.Time
		}

		entries := make([]jobEntry, 0, len(s.jobs))
		for jobID, expiry := range s.jobs {
			entries = append(entries, jobEntry{jobID: jobID, expiry: expiry})
		}

		// Sort by expiry (oldest first)
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].expiry.Before(entries[j].expiry)
		})

		// Evict oldest entries
		for i := 0; i < evictCount && i < len(entries); i++ {
			delete(s.jobs, entries[i].jobID)
		}
		changed = true

		// Emit warning for eviction
		fmt.Printf("[dedup] ⚠️ WARNING: evicted %d entries due to max size limit (%d/%d)\n",
			evictCount, len(s.jobs), defaultDedupMaxJobs)
	}

	if changed {
		if err := s.save(); err != nil {
			fmt.Printf("[dedup] error saving after cleanup: %v\n", err)
		}
	}
}

// Close stops the cleanup timer and persists the store.
func (s *JobDeduplicationStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cleanupTimer != nil {
		s.cleanupTimer.Stop()
		s.cleanupTimer = nil
	}

	return s.save()
}
