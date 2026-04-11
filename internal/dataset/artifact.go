package dataset

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"sentra-agent/internal/obs"
)

const (
	ArtifactTableName = "artifacts"
)

type Artifact struct {
	ID         string            `json:"id"`
	DatasetID  string            `json:"dataset_id"`
	AgentID    string            `json:"agent_id"`
	Checksum   string            `json:"checksum"`
	FileSize   int64             `json:"file_size"`
	LocalPath  string            `json:"local_path"`
	CreatedAt  time.Time         `json:"created_at"`
	Metadata   map[string]string `json:"metadata,omitempty"`
	StageIndex int               `json:"stage_index,omitempty"`
	IsPartial  bool              `json:"is_partial,omitempty"`
}

func RegisterArtifact(ctx context.Context, artifact Artifact) error {
	if artifact.ID == "" {
		return fmt.Errorf("artifact id is required")
	}
	if artifact.DatasetID == "" {
		return fmt.Errorf("dataset_id is required")
	}
	if artifact.LocalPath == "" {
		return fmt.Errorf("local_path is required")
	}

	if artifact.CreatedAt.IsZero() {
		artifact.CreatedAt = time.Now()
	}

	artifactDir := filepath.Dir(artifact.LocalPath)
	if err := os.MkdirAll(artifactDir, 0755); err != nil {
		return fmt.Errorf("failed to create artifact directory: %w", err)
	}

	metadataPath := artifact.LocalPath + ".metadata.json"
	metadataBytes, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal artifact metadata: %w", err)
	}

	if err := os.WriteFile(metadataPath, metadataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write artifact metadata: %w", err)
	}

	obs.Info("artifact registered", obs.Field{
		"artifact_id": artifact.ID,
		"dataset_id":  artifact.DatasetID,
		"agent_id":    artifact.AgentID,
		"local_path":  artifact.LocalPath,
		"file_size":   artifact.FileSize,
		"checksum":    artifact.Checksum,
		"stage_index": artifact.StageIndex,
		"is_partial":  artifact.IsPartial,
	})

	return nil
}

func LoadArtifactMetadata(ctx context.Context, localPath string) (*Artifact, error) {
	metadataPath := localPath + ".metadata.json"

	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read artifact metadata: %w", err)
	}

	var artifact Artifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		return nil, fmt.Errorf("failed to parse artifact metadata: %w", err)
	}

	return &artifact, nil
}

func EmitMergeCompletedEvent(ctx context.Context, result *MergeResult, agentID string) error {
	deviceOutput := result.DeviceOutput
	if deviceOutput == nil {
		deviceOutput = &DeviceOutput{
			DeviceID: result.AffinityDeviceID,
		}
	}

	artifact := Artifact{
		ID:        fmt.Sprintf("artifact_%s", result.DatasetID),
		DatasetID: result.DatasetID,
		AgentID:   agentID,
		Checksum:  result.Checksum,
		FileSize:  result.FileSize,
		LocalPath: filepath.Join(deviceOutput.MountPath, deviceOutput.File),
		CreatedAt: time.Now(),
		Metadata: map[string]string{
			"merged_chunks":  fmt.Sprintf("%d", result.MergedChunks),
			"skipped_chunks": fmt.Sprintf("%d", result.SkippedChunks),
			"deleted_chunks": fmt.Sprintf("%d", result.DeletedChunks),
			"duration_ms":    fmt.Sprintf("%d", result.DurationMs),
			"device_id":      result.AffinityDeviceID,
		},
	}

	if err := RegisterArtifact(ctx, artifact); err != nil {
		return fmt.Errorf("failed to register artifact: %w", err)
	}

	obs.Info("merge_completed event emitted", obs.Field{
		"dataset_id":  result.DatasetID,
		"artifact_id": artifact.ID,
		"checksum":    result.Checksum,
		"file_size":   result.FileSize,
		"device_id":   result.AffinityDeviceID,
	})

	return nil
}

func EmitMergeFailedEvent(ctx context.Context, datasetID, agentID string, err error) error {
	obs.Error("merge_failed event emitted", obs.Field{
		"dataset_id": datasetID,
		"agent_id":   agentID,
		"error":      err.Error(),
	})

	return nil
}
