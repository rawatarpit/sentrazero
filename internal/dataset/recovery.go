package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sentra-agent/internal/obs"
)

type MergeRecoveryState struct {
	DatasetID     string    `json:"dataset_id"`
	OutputPath    string    `json:"output_path"`
	ChunksMerged  []string  `json:"chunks_merged"`
	ChunksPending []string  `json:"chunks_pending"`
	StartedAt     time.Time `json:"started_at"`
	LastUpdatedAt time.Time `json:"last_updated_at"`
	CheckpointMs  int64     `json:"checkpoint_ms"`
}

func getRecoveryStateDir() string {
	homeDir, _ := os.UserHomeDir()
	return filepath.Join(homeDir, ".sentra", "recovery")
}

func (s *MergeRecoveryState) Save() error {
	dir := getRecoveryStateDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create recovery state directory: %w", err)
	}

	statePath := filepath.Join(dir, fmt.Sprintf("%s.recovery.json", s.DatasetID))
	s.LastUpdatedAt = time.Now()

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal recovery state: %w", err)
	}

	tmpPath := statePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write recovery state: %w", err)
	}

	return os.Rename(tmpPath, statePath)
}

func LoadMergeRecoveryState(datasetID string) (*MergeRecoveryState, error) {
	statePath := filepath.Join(getRecoveryStateDir(), fmt.Sprintf("%s.recovery.json", datasetID))

	data, err := os.ReadFile(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read recovery state: %w", err)
	}

	var state MergeRecoveryState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse recovery state: %w", err)
	}

	return &state, nil
}

func ClearMergeRecoveryState(datasetID string) error {
	statePath := filepath.Join(getRecoveryStateDir(), fmt.Sprintf("%s.recovery.json", datasetID))
	return os.Remove(statePath)
}

func FindIncompleteMerges(ctx context.Context) ([]*MergeRecoveryState, error) {
	dir := getRecoveryStateDir()

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read recovery directory: %w", err)
	}

	var states []*MergeRecoveryState
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".recovery.json") {
			continue
		}

		datasetID := strings.TrimSuffix(entry.Name(), ".recovery.json")
		state, err := LoadMergeRecoveryState(datasetID)
		if err != nil {
			obs.Warn("failed to load recovery state", obs.Field{
				"dataset_id": datasetID,
				"error":      err.Error(),
			})
			continue
		}

		if state != nil && len(state.ChunksPending) > 0 {
			states = append(states, state)
		}
	}

	return states, nil
}

func ResumeMergeFromRecovery(ctx context.Context, config MergeConfig) (*MergeResult, error) {
	state, err := LoadMergeRecoveryState(config.DatasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to load recovery state: %w", err)
	}

	if state == nil {
		return StreamMerge(ctx, config)
	}

	obs.Info("resuming merge from recovery state", obs.Field{
		"dataset_id":     config.DatasetID,
		"chunks_merged":  len(state.ChunksMerged),
		"chunks_pending": len(state.ChunksPending),
	})

	chunksToMerge := make([]ChunkInfo, 0)
	mergedChunkIDs := make(map[string]bool)
	for _, id := range state.ChunksMerged {
		mergedChunkIDs[id] = true
	}

	for _, chunk := range config.Chunks {
		if !mergedChunkIDs[chunk.ChunkID] && !chunk.IsSkipped {
			chunksToMerge = append(chunksToMerge, chunk)
		}
	}

	outputPath := getOutputPath(config.DeviceOutput, config.DatasetID)

	if len(chunksToMerge) == 0 {
		obs.Info("all chunks already merged, verifying output", obs.Field{
			"dataset_id":  config.DatasetID,
			"output_path": outputPath,
		})

		if _, err := os.Stat(outputPath); err == nil {
			checksum, size, _ := computeChecksumDirect(outputPath)
			datasetChecksum, _ := computeDatasetChecksum(config.Chunks)
			if datasetChecksum != "" {
				checksum = datasetChecksum
			}
			return &MergeResult{
				DatasetID:        config.DatasetID,
				AffinityDeviceID: config.AffinityDeviceID,
				DeviceOutput:     config.DeviceOutput,
				Checksum:         checksum,
				FileSize:         size,
			}, nil
		}
	}

	config.Chunks = chunksToMerge

	result, err := StreamMerge(ctx, config)
	if err != nil {
		return nil, err
	}

	ClearMergeRecoveryState(config.DatasetID)

	return result, nil
}

func CleanupStaleRecoveryStates(maxAge time.Duration) error {
	dir := getRecoveryStateDir()

	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read recovery directory: %w", err)
	}

	now := time.Now()
	cleaned := 0

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".recovery.json") {
			continue
		}

		datasetID := strings.TrimSuffix(entry.Name(), ".recovery.json")
		state, err := LoadMergeRecoveryState(datasetID)
		if err != nil {
			continue
		}

		if state != nil && now.Sub(state.LastUpdatedAt) > maxAge {
			if err := ClearMergeRecoveryState(datasetID); err != nil {
				obs.Warn("failed to cleanup stale recovery state", obs.Field{
					"dataset_id": datasetID,
					"error":      err.Error(),
				})
				continue
			}
			cleaned++
		}
	}

	if cleaned > 0 {
		obs.Info("cleaned up stale recovery states", obs.Field{
			"count": cleaned,
		})
	}

	return nil
}

func DetectAndRecoverFromCrashes(ctx context.Context) error {
	states, err := FindIncompleteMerges(ctx)
	if err != nil {
		return fmt.Errorf("failed to find incomplete merges: %w", err)
	}

	if len(states) == 0 {
		return nil
	}

	obs.Info("found incomplete merges to recover", obs.Field{
		"count": len(states),
	})

	for _, state := range states {
		obs.Info("attempting to recover merge", obs.Field{
			"dataset_id":     state.DatasetID,
			"chunks_pending": len(state.ChunksPending),
		})
	}

	return nil
}

func init() {
	dir := getRecoveryStateDir()
	os.MkdirAll(dir, 0755)
}
