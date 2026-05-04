package dataset

import (
	"context"
	"fmt"
	"sync"
	"time"

	"sentra-agent/internal/obs"
)

const (
	MergeLockDuration = 30 * time.Minute
	HeartbeatInterval = 5 * time.Minute
	LockRetryInterval = 10 * time.Second
	MaxLockRetries    = 3
)

type MergeLockManager struct {
	mu         sync.RWMutex
	localLocks map[string]*MergeLockState
	backend    MergeLockBackend
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

type MergeLockState struct {
	DatasetID       string
	LockID          string
	AcquiredAt      time.Time
	HeartbeatTicker *time.Ticker
	Context         context.Context
	Cancel          context.CancelFunc
}

type MergeLockBackend interface {
	AcquireLock(ctx context.Context, datasetID, agentID, deviceID string, duration time.Duration) (lockID string, expiresAt time.Time, err error)
	ReleaseLock(ctx context.Context, lockID string) error
	UpdateHeartbeat(ctx context.Context, lockID string) error
	CleanupExpired(ctx context.Context) error
}

var (
	lockManager     *MergeLockManager
	lockManagerOnce sync.Once
)

func InitMergeLockManager(backend MergeLockBackend) *MergeLockManager {
	lockManagerOnce.Do(func() {
		lockManager = &MergeLockManager{
			localLocks: make(map[string]*MergeLockState),
			backend:    backend,
			stopCh:     make(chan struct{}),
		}
		lockManager.startCleanupWorker()
	})
	return lockManager
}

func GetMergeLockManager() *MergeLockManager {
	return lockManager
}

func (m *MergeLockManager) AcquireMergeLock(ctx context.Context, datasetID, agentID, deviceID string) (*MergeLockState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.localLocks[datasetID]; ok {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := m.backend.UpdateHeartbeat(ctx, existing.LockID); err != nil {
			obs.Warn("stale local lock detected, removing and re-acquiring", obs.Field{
				"dataset_id": datasetID,
				"lock_id":    existing.LockID,
				"error":      err.Error(),
			})
			delete(m.localLocks, datasetID)
		} else {
			return existing, nil
		}
	}

	var lastErr error
	for i := 0; i < MaxLockRetries; i++ {
		lockID, expiresAt, err := m.backend.AcquireLock(ctx, datasetID, agentID, deviceID, MergeLockDuration)
		if err == nil {
			ctx, cancel := context.WithCancel(context.Background())
			state := &MergeLockState{
				DatasetID:  datasetID,
				LockID:     lockID,
				AcquiredAt: time.Now(),
				Context:    ctx,
				Cancel:     cancel,
			}
			state.HeartbeatTicker = time.NewTicker(HeartbeatInterval)

			m.wg.Add(1)
			go m.heartbeatLoop(state)

			m.localLocks[datasetID] = state

			obs.Info("acquired merge lock", obs.Field{
				"dataset_id": datasetID,
				"agent_id":   agentID,
				"device_id":  deviceID,
				"lock_id":    lockID,
				"expires_at": expiresAt,
			})

			return state, nil
		}

		lastErr = err
		obs.Warn("failed to acquire merge lock, retrying", obs.Field{
			"dataset_id": datasetID,
			"attempt":    i + 1,
			"error":      err.Error(),
		})

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(LockRetryInterval):
		}
	}

	return nil, fmt.Errorf("failed to acquire merge lock after %d attempts: %w", MaxLockRetries, lastErr)
}

func (m *MergeLockManager) ReleaseMergeLock(datasetID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.localLocks[datasetID]
	if !ok {
		return nil
	}

	state.Cancel()
	if state.HeartbeatTicker != nil {
		state.HeartbeatTicker.Stop()
	}

	if err := m.backend.ReleaseLock(context.Background(), state.LockID); err != nil {
		obs.Warn("failed to release merge lock", obs.Field{
			"dataset_id": datasetID,
			"lock_id":    state.LockID,
			"error":      err.Error(),
		})
	}

	delete(m.localLocks, datasetID)

	obs.Info("released merge lock", obs.Field{
		"dataset_id": datasetID,
		"lock_id":    state.LockID,
	})

	return nil
}

func (m *MergeLockManager) heartbeatLoop(state *MergeLockState) {
	defer m.wg.Done()

	for {
		select {
		case <-state.Context.Done():
			return
		case <-state.HeartbeatTicker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := m.backend.UpdateHeartbeat(ctx, state.LockID); err != nil {
				obs.Warn("failed to update merge lock heartbeat", obs.Field{
					"dataset_id": state.DatasetID,
					"lock_id":    state.LockID,
					"error":      err.Error(),
				})
			}
			cancel()
		}
	}
}

func (m *MergeLockManager) startCleanupWorker() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-m.stopCh:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := m.backend.CleanupExpired(ctx); err != nil {
					obs.Warn("failed to cleanup expired merge locks", obs.Field{
						"error": err.Error(),
					})
				}
				cancel()
			}
		}
	}()
}

func (m *MergeLockManager) Stop() {
	close(m.stopCh)
	m.wg.Wait()

	m.mu.Lock()
	defer m.mu.Unlock()
	for datasetID, state := range m.localLocks {
		state.Cancel()
		if state.HeartbeatTicker != nil {
			state.HeartbeatTicker.Stop()
		}
		delete(m.localLocks, datasetID)
	}
}

type InMemoryMergeLockBackend struct {
	mu    sync.Mutex
	locks map[string]*inMemoryLock
}

type inMemoryLock struct {
	DatasetID  string
	AgentID    string
	DeviceID   string
	LockID     string
	AcquiredAt time.Time
	ExpiresAt  time.Time
	Status     string
}

func NewInMemoryMergeLockBackend() *InMemoryMergeLockBackend {
	return &InMemoryMergeLockBackend{
		locks: make(map[string]*inMemoryLock),
	}
}

func (b *InMemoryMergeLockBackend) AcquireLock(ctx context.Context, datasetID, agentID, deviceID string, duration time.Duration) (string, time.Time, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	expiresAt := now.Add(duration)

	// Bug #23 fix: Check if lock already exists and is active
	if existing, ok := b.locks[datasetID]; ok {
		if existing.Status == "active" && existing.ExpiresAt.After(now) {
			return "", time.Time{}, fmt.Errorf("lock already held for dataset %s", datasetID)
		}
		// Clean up expired lock
		if existing.ExpiresAt.Before(now) || existing.Status == "expired" {
			delete(b.locks, datasetID)
		}
	}

	lockID := fmt.Sprintf("lock_%s_%d", datasetID, now.UnixNano())
	b.locks[datasetID] = &inMemoryLock{
		DatasetID:  datasetID,
		AgentID:    agentID,
		DeviceID:   deviceID,
		LockID:     lockID,
		AcquiredAt: now,
		ExpiresAt:  expiresAt,
		Status:     "active",
	}

	return lockID, expiresAt, nil
}

func (b *InMemoryMergeLockBackend) ReleaseLock(ctx context.Context, lockID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for datasetID, lock := range b.locks {
		if lock.LockID == lockID {
			lock.Status = "released"
			delete(b.locks, datasetID)
			return nil
		}
	}

	return fmt.Errorf("lock not found")
}

func (b *InMemoryMergeLockBackend) UpdateHeartbeat(ctx context.Context, lockID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, lock := range b.locks {
		if lock.LockID == lockID {
			lock.ExpiresAt = time.Now().Add(MergeLockDuration)
			return nil
		}
	}

	return fmt.Errorf("lock not found")
}

func (b *InMemoryMergeLockBackend) CleanupExpired(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	for datasetID, lock := range b.locks {
		if lock.ExpiresAt.Before(now) {
			lock.Status = "expired"
			delete(b.locks, datasetID)
		}
	}

	return nil
}
