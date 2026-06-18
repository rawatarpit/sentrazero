package dataset

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"sentra-agent/internal/obs"
)

const (
	DiskSafetyMarginPercent = 15
	ChunkCountThreshold     = 200
	DatasetSizeThresholdGB  = 50
	MaxChunksPerPass        = 64

	LockFileDir          = "~/.sentra/locks"
	MergeLockTableName   = "dataset_merge_locks"
	CheckpointFileSuffix = ".checkpoint.json"
	PartialFileSuffix    = ".partial"
)

type CompressionType string

const (
	CompressionNone CompressionType = ""
	CompressionGZ   CompressionType = ".gz"
	CompressionZST  CompressionType = ".zst"
)

type DeviceOutput struct {
	DeviceID  string `json:"device_id"`
	MountPath string `json:"mount_path"`
	File      string `json:"file"`
}

type ChunkInfo struct {
	ChunkID    string `json:"chunk_id"`
	Path       string `json:"path"`
	Index      int    `json:"chunk_index"`
	SizeBytes  int64  `json:"size_bytes"`
	Compressed bool   `json:"compressed"`
	Checksum   string `json:"checksum,omitempty"`
	IsSkipped  bool   `json:"is_skipped,omitempty"`
}

type MergeConfig struct {
	DatasetID         string        `json:"dataset_id"`
	DatasetSlug       string        `json:"dataset_slug,omitempty"`
	AffinityDeviceID  string        `json:"affinity_device_id"`
	DeviceOutput      *DeviceOutput `json:"device_output"`
	Chunks            []ChunkInfo   `json:"chunks"`
	Strategy          string        `json:"strategy"`
	MountPath         string        `json:"mount_path,omitempty"`
	DeleteChunksAfter bool          `json:"delete_chunks_after"`
	Compressed        bool          `json:"compressed"`
	ChecksumRequired  bool          `json:"checksum_required"`
	UseTreeMerge      bool          `json:"use_tree_merge"`
	AgentID           string        `json:"agent_id"`
	OrgID             string        `json:"org_id"`
}

type MergeResult struct {
	DatasetID        string           `json:"dataset_id"`
	AffinityDeviceID string           `json:"affinity_device_id"`
	DeviceOutput     *DeviceOutput    `json:"device_output"`
	Checksum         string           `json:"checksum"`
	FileSize         int64            `json:"file_size"`
	MergedChunks     int              `json:"merged_chunks"`
	SkippedChunks    int              `json:"skipped_chunks"`
	DeletedChunks    int              `json:"deleted_chunks"`
	DurationMs       int64            `json:"duration_ms"`
	DiskUsageBefore  int64            `json:"disk_usage_before"`
	DiskUsageAfter   int64            `json:"disk_usage_after"`
	Metrics          map[string]int64 `json:"metrics"`
}

type MergeCheckpoint struct {
	DatasetID      string    `json:"dataset_id"`
	OutputPath     string    `json:"output_path"`
	LastChunkIndex int       `json:"last_chunk_index"`
	BytesWritten   int64     `json:"bytes_written"`
	ChunksMerged   []string  `json:"chunks_merged"`
	StartedAt      time.Time `json:"started_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type MergeLock struct {
	ID          string    `json:"id"`
	DatasetID   string    `json:"dataset_id"`
	AgentID     string    `json:"agent_id"`
	DeviceID    string    `json:"device_id"`
	AcquiredAt  time.Time `json:"acquired_at"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	Status      string    `json:"status"`
}

var (
	mergeLocks     sync.Map
	mergeLockDir   string
	mergeMetricsMu sync.RWMutex
	mergeMetrics   = make(map[string]MergeMetrics)
)

type MergeMetrics struct {
	MergeDuration       int64 `json:"merge_duration_ms"`
	BytesMerged         int64 `json:"bytes_merged"`
	DiskUsageBefore     int64 `json:"disk_usage_before_bytes"`
	DiskUsageAfter      int64 `json:"disk_usage_after_bytes"`
	DeliveryDuration    int64 `json:"delivery_duration_ms"`
	ChunkProcessingTime int64 `json:"chunk_processing_time_ms"`
	RecoveryEvents      int64 `json:"recovery_events"`
	ChecksumFailures    int64 `json:"checksum_failures"`
}

func init() {
	homeDir, _ := os.UserHomeDir()
	mergeLockDir = filepath.Join(homeDir, ".sentra", "locks")
}

func getCompressionType(path string) CompressionType {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".gz", ".gzip":
		return CompressionGZ
	case ".zst":
		return CompressionZST
	default:
		return CompressionNone
	}
}

func IsCompressed(path string) bool {
	return getCompressionType(path) != CompressionNone
}

func calculateRequiredSpace(chunks []ChunkInfo) int64 {
	var total int64
	for _, chunk := range chunks {
		if !chunk.IsSkipped {
			total += chunk.SizeBytes
		}
	}
	margin := float64(total) * DiskSafetyMarginPercent / 100
	return total + int64(margin)
}

func checkDiskSpace(path string, required int64) (bool, int64, error) {
	available, err := getAvailableDiskSpace(path)
	if err != nil {
		return false, 0, err
	}
	return available >= required, available, nil
}

func resolveChunkPath(path, mountPath string, strategy string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}

	switch strategy {
	case "affinity":
		return "", errors.New("affinity strategy requires absolute path")
	case "shared_mount":
		if mountPath != "" {
			return filepath.Join(mountPath, path), nil
		}
		return path, nil
	default:
		return path, nil
	}
}

func computeChecksumDirect(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return "", 0, err
	}

	hash := sha256.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		return "", 0, err
	}

	return hex.EncodeToString(hash.Sum(nil)), stat.Size(), nil
}

func computeChunkChecksum(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()

	hash := sha256.New()
	compType := getCompressionType(path)

	if compType == CompressionGZ {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return "", 0, err
		}
		defer gzReader.Close()
		_, err = io.Copy(hash, gzReader)
		if err != nil {
			return "", 0, err
		}
	} else {
		_, err = io.Copy(hash, file)
		if err != nil {
			return "", 0, err
		}
	}

	stat, err := file.Stat()
	if err != nil {
		return "", 0, err
	}

	return hex.EncodeToString(hash.Sum(nil)), stat.Size(), nil
}

func computeDatasetChecksum(chunks []ChunkInfo) (string, error) {
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Index < chunks[j].Index
	})

	hash := sha256.New()
	for _, chunk := range chunks {
		if chunk.IsSkipped || chunk.Checksum == "" {
			continue
		}
		hash.Write([]byte(chunk.Checksum))
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func verifyMergedFile(outputPath, expectedChecksum string) (bool, error) {
	if expectedChecksum == "" {
		return true, nil
	}

	actualChecksum, _, err := computeChecksumDirect(outputPath)
	if err != nil {
		return false, err
	}

	return actualChecksum == expectedChecksum, nil
}

func shouldUseTreeMerge(chunks []ChunkInfo, totalSizeGB float64) bool {
	activeChunks := 0
	for _, c := range chunks {
		if !c.IsSkipped {
			activeChunks++
		}
	}
	return float64(activeChunks) > ChunkCountThreshold || totalSizeGB > DatasetSizeThresholdGB
}

func planTreeMerge(chunks []ChunkInfo, outputDir string) [][]ChunkInfo {
	activeChunks := make([]ChunkInfo, 0)
	for _, c := range chunks {
		if !c.IsSkipped {
			activeChunks = append(activeChunks, c)
		}
	}

	sort.Slice(activeChunks, func(i, j int) bool {
		return activeChunks[i].Index < activeChunks[j].Index
	})

	var stages [][]ChunkInfo
	currentStage := make([]ChunkInfo, 0)

	for i, chunk := range activeChunks {
		currentStage = append(currentStage, chunk)

		if len(currentStage) >= MaxChunksPerPass || i == len(activeChunks)-1 {
			stageCopy := make([]ChunkInfo, len(currentStage))
			copy(stageCopy, currentStage)
			stages = append(stages, stageCopy)
			currentStage = make([]ChunkInfo, 0)
		}
	}

	return stages
}

func getOutputPath(deviceOutput *DeviceOutput, datasetID, datasetSlug string) string {
	if deviceOutput != nil && deviceOutput.File != "" {
		return filepath.Join(deviceOutput.MountPath, deviceOutput.File)
	}
	name := datasetID
	if datasetSlug != "" {
		name = datasetSlug
	}
	filename := fmt.Sprintf("%s_merged.csv", name)
	if deviceOutput != nil && deviceOutput.MountPath != "" {
		return filepath.Join(deviceOutput.MountPath, filename)
	}
	return filename
}

func getPartialPath(outputPath string) string {
	return outputPath + PartialFileSuffix
}

func getCheckpointPath(outputPath string) string {
	return outputPath + CheckpointFileSuffix
}

func saveCheckpoint(checkpoint *MergeCheckpoint) error {
	checkpoint.UpdatedAt = time.Now()
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint: %w", err)
	}

	dir := filepath.Dir(checkpoint.OutputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create checkpoint directory: %w", err)
	}

	tmpPath := getCheckpointPath(checkpoint.OutputPath) + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}

	return os.Rename(tmpPath, getCheckpointPath(checkpoint.OutputPath))
}

func loadCheckpoint(outputPath string) (*MergeCheckpoint, error) {
	checkpointPath := getCheckpointPath(outputPath)
	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read checkpoint: %w", err)
	}

	var checkpoint MergeCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to parse checkpoint: %w", err)
	}

	return &checkpoint, nil
}

func clearCheckpoint(outputPath string) error {
	checkpointPath := getCheckpointPath(outputPath)
	if err := os.Remove(checkpointPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to clear checkpoint: %w", err)
	}
	return nil
}

func detectPartialMerge(outputPath string) bool {
	partialPath := getPartialPath(outputPath)
	if _, err := os.Stat(partialPath); err == nil {
		return true
	}
	return false
}

func cleanupChunks(chunks []ChunkInfo) int {
	deletedCount := 0
	seenDirs := make(map[string]bool)

	for _, chunk := range chunks {
		if chunk.IsSkipped {
			continue
		}
		resolvedPath := chunk.Path
		if err := os.Remove(resolvedPath); err == nil {
			deletedCount++
			// Remove .metadata.json if it exists
			metaPath := resolvedPath + ".metadata.json"
			os.Remove(metaPath)

			// Clean up parent directory if empty
			dir := filepath.Dir(resolvedPath)
			if !seenDirs[dir] {
				seenDirs[dir] = true
				entries, err := os.ReadDir(dir)
				if err == nil && len(entries) == 0 {
					os.Remove(dir)
					// Also clean up parent of chunks/ (e.g. {slug}/)
					parent := filepath.Dir(dir)
					parentEntries, err := os.ReadDir(parent)
					if err == nil && len(parentEntries) == 0 {
						os.Remove(parent)
					}
				}
			}
		}
	}
	return deletedCount
}

func StreamMerge(ctx context.Context, config MergeConfig) (*MergeResult, error) {
	startTime := time.Now()

	if config.DatasetID == "" {
		return nil, errors.New("dataset_id is required")
	}
	if config.AgentID == "" {
		return nil, errors.New("agent_id is required")
	}

	activeChunks := make([]ChunkInfo, 0)
	var totalSize int64
	for _, c := range config.Chunks {
		if !c.IsSkipped {
			if c.Checksum == "" {
				checksum, size, err := computeChunkChecksum(c.Path)
				if err != nil {
					obs.Warn("failed to compute chunk checksum", obs.Field{
						"chunk_id": c.ChunkID,
						"error":    err.Error(),
					})
				} else {
					c.Checksum = checksum
					c.SizeBytes = size
				}
			}
			activeChunks = append(activeChunks, c)
			totalSize += c.SizeBytes
		}
	}

	sort.Slice(activeChunks, func(i, j int) bool {
		return activeChunks[i].Index < activeChunks[j].Index
	})

	outputPath := getOutputPath(config.DeviceOutput, config.DatasetID, config.DatasetSlug)
	outputDir := filepath.Dir(outputPath)

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	enoughSpace, availableSpace, err := checkDiskSpace(outputDir, calculateRequiredSpace(activeChunks))
	if err != nil {
		obs.Warn("failed to check disk space, proceeding anyway", obs.Field{
			"output_dir": outputDir,
			"error":      err.Error(),
		})
	} else if !enoughSpace {
		return nil, fmt.Errorf("insufficient disk space: need %d bytes, have %d bytes",
			calculateRequiredSpace(activeChunks), availableSpace)
	}

	partialPath := getPartialPath(outputPath)

	checkpoint, err := loadCheckpoint(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	if checkpoint != nil {
		obs.Info("resuming merge from checkpoint", obs.Field{
			"dataset_id":       config.DatasetID,
			"last_chunk_index": checkpoint.LastChunkIndex,
			"bytes_written":    checkpoint.BytesWritten,
		})
		recordRecoveryEvent(config.DatasetID)
	}

	resumeIndex := 0
	if checkpoint != nil {
		resumeIndex = checkpoint.LastChunkIndex + 1
	}

	mergedCount := 0
	skippedCount := 0
	deletedCount := 0
	var bytesWritten int64

	outFile, err := os.OpenFile(partialPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open partial output: %w", err)
	}
	defer outFile.Close()

	if checkpoint != nil {
		if _, err := outFile.Seek(checkpoint.BytesWritten, 0); err != nil {
			return nil, fmt.Errorf("failed to seek to checkpoint: %w", err)
		}
		bytesWritten = checkpoint.BytesWritten
	}

	currentCheckpoint := &MergeCheckpoint{
		DatasetID:      config.DatasetID,
		OutputPath:     outputPath,
		LastChunkIndex: -1,
		BytesWritten:   0,
		ChunksMerged:   []string{},
		StartedAt:      startTime,
	}

	for i, chunk := range activeChunks {
		if i < resumeIndex {
			mergedCount++
			continue
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		resolvedPath, err := resolveChunkPath(chunk.Path, config.MountPath, config.Strategy)
		if err != nil {
			obs.Warn("could not resolve chunk path, skipping", obs.Field{
				"chunk_id": chunk.ChunkID,
				"error":    err.Error(),
			})
			skippedCount++
			continue
		}

		info, err := os.Stat(resolvedPath)
		if err != nil {
			if os.IsNotExist(err) {
				obs.Warn("chunk file missing", obs.Field{
					"chunk_id": chunk.ChunkID,
					"path":     resolvedPath,
				})
				skippedCount++
				continue
			}
			return nil, fmt.Errorf("failed to access chunk file %s: %w", resolvedPath, err)
		}

		if info.IsDir() {
			return nil, fmt.Errorf("chunk path is a directory: %s", resolvedPath)
		}

		inFile, err := os.Open(resolvedPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open chunk %s: %w", resolvedPath, err)
		}

		var writer io.Writer = outFile
		compType := getCompressionType(resolvedPath)
		if compType == CompressionGZ && config.Compressed {
			gzWriter := gzip.NewWriter(outFile)
			defer gzWriter.Close()
			writer = gzWriter
		}

		written, err := io.Copy(writer, inFile)
		inFile.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to write chunk %s: %w", resolvedPath, err)
		}

		if written == 0 {
			obs.Warn("chunk file is empty", obs.Field{
				"chunk_id": chunk.ChunkID,
				"path":     resolvedPath,
			})
			skippedCount++
			continue
		}

		bytesWritten += written
		mergedCount++

		if err := outFile.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync output: %w", err)
		}

		currentCheckpoint.LastChunkIndex = chunk.Index
		currentCheckpoint.BytesWritten = bytesWritten
		currentCheckpoint.ChunksMerged = append(currentCheckpoint.ChunksMerged, chunk.ChunkID)

		if err := saveCheckpoint(currentCheckpoint); err != nil {
			obs.Warn("failed to save checkpoint", obs.Field{
				"error": err.Error(),
			})
		}

		obs.Debug("chunk merged", obs.Field{
			"chunk_id":    chunk.ChunkID,
			"bytesritten": written,
			"total_bytes": bytesWritten,
		})
	}

	deletedCount = cleanupChunks(activeChunks)

	if err := outFile.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync final output: %w", err)
	}
	outFile.Close()

	if err := os.Rename(partialPath, outputPath); err != nil {
		return nil, fmt.Errorf("failed to atomic rename output: %w", err)
	}

	if err := clearCheckpoint(outputPath); err != nil {
		obs.Warn("failed to clear checkpoint", obs.Field{
			"error": err.Error(),
		})
	}

	var checksum string
	if config.ChecksumRequired {
		datasetChecksum, err := computeDatasetChecksum(activeChunks)
		if err == nil {
			checksum = datasetChecksum
		} else {
			directChecksum, _, err := computeChecksumDirect(outputPath)
			if err == nil {
				checksum = directChecksum
			}
		}
	}

	finalInfo, _ := os.Stat(outputPath)
	diskUsageAfter, _ := getAvailableDiskSpace(outputDir)
	durationMs := time.Since(startTime).Milliseconds()

	result := &MergeResult{
		DatasetID:        config.DatasetID,
		AffinityDeviceID: config.AffinityDeviceID,
		DeviceOutput: &DeviceOutput{
			DeviceID:  config.AffinityDeviceID,
			MountPath: filepath.Dir(outputPath),
			File:      filepath.Base(outputPath),
		},
		Checksum:        checksum,
		FileSize:        finalInfo.Size(),
		MergedChunks:    mergedCount,
		SkippedChunks:   skippedCount,
		DeletedChunks:   deletedCount,
		DurationMs:      durationMs,
		DiskUsageBefore: availableSpace,
		DiskUsageAfter:  diskUsageAfter,
		Metrics: map[string]int64{
			"bytes_merged":        bytesWritten,
			"chunk_count":         int64(len(activeChunks)),
			"merge_throughput_mb": bytesWritten / durationMs / 1000,
		},
	}

	recordMergeMetrics(config.DatasetID, *result)

	// Clean up empty directories and stale metadata files
	outputDirEntries, _ := os.ReadDir(outputDir)
	for _, entry := range outputDirEntries {
		if entry.IsDir() {
			subEntries, _ := os.ReadDir(filepath.Join(outputDir, entry.Name()))
			if len(subEntries) == 0 {
				os.Remove(filepath.Join(outputDir, entry.Name()))
			}
		}
	}

	obs.Info("merge completed", obs.Field{
		"dataset_id":      config.DatasetID,
		"affinity_device": config.AffinityDeviceID,
		"merged_count":    mergedCount,
		"skipped_count":   skippedCount,
		"deleted_count":   deletedCount,
		"output_path":     outputPath,
		"file_size":       finalInfo.Size(),
		"checksum":        checksum,
		"duration_ms":     durationMs,
	})

	return result, nil
}

func StreamMergeTree(ctx context.Context, config MergeConfig) (*MergeResult, error) {
	startTime := time.Now()

	activeChunks := make([]ChunkInfo, 0)
	var totalSize int64
	for _, c := range config.Chunks {
		if !c.IsSkipped {
			activeChunks = append(activeChunks, c)
			totalSize += c.SizeBytes
		}
	}

	totalSizeGB := float64(totalSize) / (1024 * 1024 * 1024)
	if !shouldUseTreeMerge(activeChunks, totalSizeGB) {
		return StreamMerge(ctx, config)
	}

	obs.Info("using tree merge strategy", obs.Field{
		"dataset_id":    config.DatasetID,
		"chunk_count":   len(activeChunks),
		"total_size_gb": totalSizeGB,
	})

	outputDir := filepath.Dir(getOutputPath(config.DeviceOutput, config.DatasetID, config.DatasetSlug))
	stages := planTreeMerge(config.Chunks, outputDir)

	if len(stages) == 0 {
		return nil, errors.New("no merge stages planned")
	}

	var finalOutput string
	intermediateOutputs := make([]string, 0)

	for stageIdx, stageChunks := range stages {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		stageOutput := filepath.Join(outputDir, fmt.Sprintf("stage_%d_%s.partial", stageIdx, config.DatasetID))

		stageSlug := config.DatasetID
		if config.DatasetSlug != "" {
			stageSlug = config.DatasetSlug
		}
		stageConfig := MergeConfig{
			DatasetID:        fmt.Sprintf("%s_stage_%d", config.DatasetID, stageIdx),
			DatasetSlug:      fmt.Sprintf("%s_stage_%d", stageSlug, stageIdx),
			AffinityDeviceID: config.AffinityDeviceID,
			DeviceOutput: &DeviceOutput{
				DeviceID:  config.AffinityDeviceID,
				MountPath: outputDir,
				File:      filepath.Base(stageOutput),
			},
			Chunks:            stageChunks,
			Strategy:          config.Strategy,
			MountPath:         config.MountPath,
			DeleteChunksAfter: stageIdx > 0,
			Compressed:        config.Compressed,
			ChecksumRequired:  config.ChecksumRequired,
			AgentID:           config.AgentID,
			OrgID:             config.OrgID,
		}

		obs.Info("executing merge stage", obs.Field{
			"stage_index": stageIdx,
			"chunk_count": len(stageChunks),
			"output_path": stageOutput,
		})

		stageResult, err := StreamMerge(ctx, stageConfig)
		if err != nil {
			return nil, fmt.Errorf("stage %d failed: %w", stageIdx, err)
		}

		intermediateOutputs = append(intermediateOutputs, stageResult.DeviceOutput.File)
		finalOutput = stageResult.DeviceOutput.File
	}

	if len(intermediateOutputs) > 1 {
		stageConfig := MergeConfig{
			DatasetID:        config.DatasetID,
			DatasetSlug:      config.DatasetSlug,
			AffinityDeviceID: config.AffinityDeviceID,
			DeviceOutput:     config.DeviceOutput,
			Chunks: func() []ChunkInfo {
				chunks := make([]ChunkInfo, len(intermediateOutputs))
				for i, path := range intermediateOutputs {
					info, _ := os.Stat(filepath.Join(outputDir, path))
					size := int64(0)
					if info != nil {
						size = info.Size()
					}
					chunks[i] = ChunkInfo{
						ChunkID:   fmt.Sprintf("intermediate_%d", i),
						Path:      filepath.Join(outputDir, path),
						Index:     i,
						SizeBytes: size,
					}
				}
				return chunks
			}(),
			Strategy:          config.Strategy,
			MountPath:         outputDir,
			DeleteChunksAfter: true,
			Compressed:        config.Compressed,
			ChecksumRequired:  config.ChecksumRequired,
			AgentID:           config.AgentID,
			OrgID:             config.OrgID,
		}

		finalResult, err := StreamMerge(ctx, stageConfig)
		if err != nil {
			return nil, fmt.Errorf("final stage failed: %w", err)
		}
		finalOutput = finalResult.DeviceOutput.File
	}

	outputPath := getOutputPath(config.DeviceOutput, config.DatasetID, config.DatasetSlug)
	if finalOutput != outputPath {
		finalPath := filepath.Join(outputDir, finalOutput)
		if err := os.Rename(finalPath, outputPath); err != nil {
			return nil, fmt.Errorf("failed to rename final output: %w", err)
		}
	}

	checksum, fileSize, _ := computeChecksumDirect(outputPath)
	datasetChecksum, _ := computeDatasetChecksum(activeChunks)
	if datasetChecksum != "" {
		checksum = datasetChecksum
	}

	durationMs := time.Since(startTime).Milliseconds()

	result := &MergeResult{
		DatasetID:        config.DatasetID,
		AffinityDeviceID: config.AffinityDeviceID,
		DeviceOutput: &DeviceOutput{
			DeviceID:  config.AffinityDeviceID,
			MountPath: outputDir,
			File:      filepath.Base(outputPath),
		},
		Checksum:     checksum,
		FileSize:     fileSize,
		MergedChunks: len(activeChunks),
		DurationMs:   durationMs,
		Metrics: map[string]int64{
			"bytes_merged":      totalSize,
			"tree_merge_stages": int64(len(stages)),
		},
	}

	recordMergeMetrics(config.DatasetID, *result)

	// Clean up intermediate stage files
	for _, intermediate := range intermediateOutputs {
		intermediatePath := filepath.Join(outputDir, intermediate)
		if err := os.Remove(intermediatePath); err == nil {
			obs.Debug("cleaned up intermediate stage output", obs.Field{
				"path": intermediatePath,
			})
		}
	}

	// Clean up empty stage directories in output
	outputDirEntries, _ := os.ReadDir(outputDir)
	for _, entry := range outputDirEntries {
		if entry.IsDir() {
			subEntries, _ := os.ReadDir(filepath.Join(outputDir, entry.Name()))
			if len(subEntries) == 0 {
				os.Remove(filepath.Join(outputDir, entry.Name()))
			}
		}
	}

	obs.Info("tree merge completed", obs.Field{
		"dataset_id":  config.DatasetID,
		"stages":      len(stages),
		"output_path": outputPath,
		"file_size":   fileSize,
		"duration_ms": durationMs,
	})

	return result, nil
}

func recordMergeMetrics(datasetID string, result MergeResult) {
	mergeMetricsMu.Lock()
	defer mergeMetricsMu.Unlock()

	mergeMetrics[datasetID] = MergeMetrics{
		MergeDuration:   result.DurationMs,
		BytesMerged:     result.FileSize,
		DiskUsageBefore: result.DiskUsageBefore,
		DiskUsageAfter:  result.DiskUsageAfter,
	}
}

func recordRecoveryEvent(datasetID string) {
	mergeMetricsMu.Lock()
	defer mergeMetricsMu.Unlock()

	if m, ok := mergeMetrics[datasetID]; ok {
		m.RecoveryEvents++
		mergeMetrics[datasetID] = m
	}
}

func recordChecksumFailure(datasetID string) {
	mergeMetricsMu.Lock()
	defer mergeMetricsMu.Unlock()

	if m, ok := mergeMetrics[datasetID]; ok {
		m.ChecksumFailures++
		mergeMetrics[datasetID] = m
	}
}

func GetMergeMetrics(datasetID string) (MergeMetrics, bool) {
	mergeMetricsMu.RLock()
	defer mergeMetricsMu.RUnlock()

	metrics, ok := mergeMetrics[datasetID]
	return metrics, ok
}

func GetAllMergeMetrics() map[string]MergeMetrics {
	mergeMetricsMu.RLock()
	defer mergeMetricsMu.RUnlock()

	result := make(map[string]MergeMetrics)
	for k, v := range mergeMetrics {
		result[k] = v
	}
	return result
}

type MergeReadersResult struct {
	Reader io.Reader
	Size   int64
}

func MergeReaders(ctx context.Context, readers []io.Reader) (io.Reader, error) {
	if len(readers) == 0 {
		return nil, errors.New("no readers provided")
	}

	if len(readers) == 1 {
		return readers[0], nil
	}

	pr, pw := io.Pipe()
	var totalSize int64

	go func() {
		for _, reader := range readers {
			select {
			case <-ctx.Done():
				pw.CloseWithError(ctx.Err())
				return
			default:
			}

			written, err := io.Copy(pw, reader)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("failed to copy reader: %w", err))
				return
			}
			totalSize += written
		}
		pw.Close()
	}()

	return pr, nil
}
