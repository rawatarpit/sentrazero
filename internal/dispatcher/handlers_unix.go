//go:build linux || darwin
// +build linux darwin

package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"sentra-agent/internal/dataset"
	"sentra-agent/internal/obs"
	"sentra-agent/internal/plugin"
	"sentra-agent/internal/storage"
	"sentra-agent/internal/system"
)

var (
	supabaseBaseURL string
	supabaseToken   string
	supabaseAnonKey string
)

func init() {
	supabaseBaseURL = os.Getenv("SUPABASE_URL")
	supabaseToken = os.Getenv("SUPABASE_SERVICE_ROLE_KEY")
	supabaseAnonKey = os.Getenv("SUPABASE_ANON_KEY")
}

// -----------------------------------------------------------------------------
// Merge Types
// -----------------------------------------------------------------------------

type MergeStrategy string

const (
	StrategyAffinity    MergeStrategy = "affinity"
	StrategySharedMount MergeStrategy = "shared_mount"
)

type MergeChunk struct {
	ChunkID    string `json:"chunk_id"`
	ChunkIndex int    `json:"chunk_index,omitempty"`
	Path       string `json:"path"`
	IsSkipped  bool   `json:"is_skipped,omitempty"`
}

type MergePayload struct {
	DatasetID         string                `json:"dataset_id"`
	AffinityDeviceID  string                `json:"affinity_device_id"`
	DeviceOutput      *dataset.DeviceOutput `json:"device_output"`
	Strategy          MergeStrategy         `json:"strategy"`
	Chunks            []MergeChunk          `json:"chunks"`
	MountPath         string                `json:"mount_path,omitempty"`
	IsPartial         bool                  `json:"is_partial,omitempty"`
	DeleteChunksAfter bool                  `json:"delete_chunks_after,omitempty"`
	Compressed        bool                  `json:"compressed,omitempty"`
	ChecksumRequired  bool                  `json:"checksum_required,omitempty"`
	UseTreeMerge      bool                  `json:"use_tree_merge,omitempty"`
}

type MergeResult struct {
	MergedCount  int    `json:"merged_count"`
	SkippedCount int    `json:"skipped_count"`
	IsPartial    bool   `json:"is_partial"`
	OutputPath   string `json:"output_path"`
	Checksum     string `json:"checksum,omitempty"`
	FileSize     int64  `json:"file_size,omitempty"`
}

// -----------------------------------------------------------------------------
// Job handlers (NO CGO, NO ffi here)
// -----------------------------------------------------------------------------

func executeScanDataset(ctx context.Context, payload json.RawMessage) error {
	var job Job
	if err := json.Unmarshal(payload, &job); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	obs.Info("executeScanDataset received job", obs.Field{
		"job_id":       job.ID,
		"dataset_id":   job.DatasetID,
		"storage_mode": job.StorageMode,
		"storage_type": job.StorageType,
		"source_path":  job.SourcePath,
		"input_path":   job.InputPath,
	})

	if job.DatasetID == "" {
		return errors.New("missing dataset_id")
	}

	storageMode := job.StorageMode
	if storageMode == "" {
		storageMode = job.StorageType
	}
	if storageMode == "" {
		storageMode = "shared_mount"
	}

	obs.Info("executeScanDataset: storage config", obs.Field{
		"storage_mode":      storageMode,
		"dataset_id":        job.DatasetID,
		"storage_config_id": job.StorageConfigID,
		"backend_type":      fmt.Sprintf("%T", GetStorageBackend()),
	})

	var backend storage.StorageBackend

	if job.StorageConfigID != "" && (storageMode == "s3" || storageMode == "aws_s3" || storageMode == "gcs" || storageMode == "azure_blob") {
		cfg, err := storage.GetConfigByID(job.StorageConfigID)
		if err != nil {
			obs.Warn("executeScanDataset: failed to get storage config by ID", obs.Field{
				"error":             err.Error(),
				"storage_config_id": job.StorageConfigID,
			})
		} else {
			newBackend, err := storage.NewBackend(cfg)
			if err != nil {
				obs.Warn("executeScanDataset: failed to create backend from config", obs.Field{
					"error":             err.Error(),
					"storage_config_id": job.StorageConfigID,
				})
			} else {
				backend = newBackend
				obs.Info("executeScanDataset: created backend from storage_config_id", obs.Field{
					"backend_type":      fmt.Sprintf("%T", backend),
					"storage_config_id": job.StorageConfigID,
				})
			}
		}
	}

	if backend == nil {
		backend = GetStorageBackend()
	}
	if backend == nil {
		return errors.New("storage backend not initialized")
	}

	var remotePath string
	if job.SourcePath != "" {
		remotePath = job.SourcePath
	} else {
		remotePath = storage.GetRemotePath(job.DatasetID, 0, "source")
	}

	obs.Info("executeScanDataset: listing remote objects", obs.Field{
		"remote_path": remotePath,
	})

	objects, err := backend.ListObjects(ctx, remotePath)
	if err != nil {
		return fmt.Errorf("failed to list source objects: %w", err)
	}

	var totalSize int64
	for _, obj := range objects {
		totalSize += obj.Size
	}

	workDir, cleanup, err := plugin.PrepareJobWorkDir(job.ID)
	if err != nil {
		return fmt.Errorf("failed to prepare work dir: %w", err)
	}
	defer cleanup()

	inputPath := job.InputPath
	if inputPath == "" && storageMode == "shared_mount" {
		mountBasePath := deriveMountBasePath(storageMode)
		inputPath = filepath.Join(mountBasePath, "datasets", job.DatasetID, "source")
	}

	if len(objects) > 0 {
		sampleObj := objects[0]
		objReader, err := backend.ReadObject(ctx, sampleObj.Key)
		if err != nil {
			return fmt.Errorf("failed to read sample object: %w", err)
		}
		defer objReader.Close()

		samplePath := filepath.Join(workDir, "sample"+filepath.Ext(sampleObj.Key))
		sampleFile, err := os.Create(samplePath)
		if err != nil {
			return fmt.Errorf("failed to create sample file: %w", err)
		}
		_, err = io.Copy(sampleFile, objReader)
		sampleFile.Close()
		if err != nil {
			return fmt.Errorf("failed to write sample file: %w", err)
		}

		inputPath = samplePath
	}

	pluginName := job.PluginName
	if pluginName == "" {
		pluginName = "plugin_scan_metadata"
	}

	pluginPath, manifest, err := plugin.LoadAndUpdatePlugin(ctx, pluginName)
	if err != nil {
		return err
	}

	outputPath := filepath.Join(workDir, "scan_result.json")

	scanCtx := PluginContext{
		JobID:      job.ID,
		OrgID:      job.OrgID,
		DatasetID:  job.DatasetID,
		ChunkID:    job.ChunkID,
		InputPath:  inputPath,
		OutputPath: outputPath,
		Config:     job.StepConfig,
	}

	inJSON, _ := json.Marshal(scanCtx)

	env := system.DetectExecutionEnv()
	result, err := plugin.Execute(ctx, pluginPath, manifest, string(inJSON), env, NativeRunnerFunc())
	if err != nil {
		return fmt.Errorf("scan plugin execution failed: %w", err)
	}

	var summary map[string]any
	if err := json.Unmarshal([]byte(result.Output), &summary); err != nil {
		return err
	}

	if summary == nil {
		return errors.New("scan plugin returned nil output")
	}

	obs.Info("scan completed", obs.Field{
		"job_id":       job.ID,
		"dataset_id":   job.DatasetID,
		"summary_keys": len(summary),
	})

	if err := reportDatasetScan(ctx, job.DatasetID, job.OrgID, storageMode, summary); err != nil {
		obs.Warn("failed to report dataset scan", obs.Field{
			"job_id":     job.ID,
			"dataset_id": job.DatasetID,
			"error":      err.Error(),
		})
		return err
	}

	return nil
}

func reportDatasetScan(ctx context.Context, datasetID, orgID, storageType string, summary map[string]any) error {
	if supabaseBaseURL == "" {
		obs.Warn("report_dataset_scan skipped: SUPABASE_URL not set", nil)
		return nil
	}

	reportPayload := map[string]interface{}{
		"dataset_id":   datasetID,
		"org_id":       orgID,
		"storage_type": storageType,
		"summary":      summary,
	}

	payloadBytes, err := json.Marshal(reportPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal report payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", supabaseBaseURL+"/functions/v1/report_dataset_scan", bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+supabaseAnonKey)
	if supabaseToken != "" {
		req.Header.Set("apikey", supabaseToken)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to call report_dataset_scan: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("report_dataset_scan returned %d: %s", resp.StatusCode, string(body))
	}

	obs.Info("dataset scan reported", obs.Field{
		"dataset_id": datasetID,
		"status":     resp.StatusCode,
	})

	return nil
}

func executeMergeDataset(ctx context.Context, payload json.RawMessage) error {
	agentID := getAgentID()

	var job Job
	if err := json.Unmarshal(payload, &job); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	var mergePayload MergePayload
	if err := json.Unmarshal(payload, &mergePayload); err != nil {
		return fmt.Errorf("invalid merge payload: %w", err)
	}

	if mergePayload.DatasetID == "" {
		return errors.New("missing dataset_id")
	}
	if mergePayload.AffinityDeviceID == "" && mergePayload.DeviceOutput == nil {
		return errors.New("missing affinity_device_id or device_output")
	}
	if len(mergePayload.Chunks) == 0 {
		return errors.New("no chunks provided")
	}

	storageMode := string(mergePayload.Strategy)
	backend := GetStorageBackend()

	chunks := make([]dataset.ChunkInfo, 0, len(mergePayload.Chunks))
	for _, chunk := range mergePayload.Chunks {
		resolvedPath := chunk.Path

		switch mergePayload.Strategy {
		case StrategyAffinity:
			if !filepath.IsAbs(resolvedPath) {
				return fmt.Errorf("affinity strategy requires absolute path for chunk %s", chunk.ChunkID)
			}
		case StrategySharedMount:
			if !filepath.IsAbs(resolvedPath) && mergePayload.MountPath != "" {
				resolvedPath = filepath.Join(mergePayload.MountPath, resolvedPath)
			}
		}

		info, err := os.Stat(resolvedPath)
		if err != nil {
			if os.IsNotExist(err) {
				if mergePayload.IsPartial {
					chunks = append(chunks, dataset.ChunkInfo{
						ChunkID:   chunk.ChunkID,
						Path:      resolvedPath,
						Index:     len(chunks),
						IsSkipped: true,
					})
					continue
				}
				return fmt.Errorf("missing chunk file (not declared as skipped): %s", resolvedPath)
			}
			return fmt.Errorf("failed to access chunk file %s: %w", resolvedPath, err)
		}
		if info.IsDir() {
			return fmt.Errorf("chunk path is a directory, expected file: %s", resolvedPath)
		}

		compressed := dataset.IsCompressed(resolvedPath)
		chunks = append(chunks, dataset.ChunkInfo{
			ChunkID:    chunk.ChunkID,
			Path:       resolvedPath,
			Index:      len(chunks),
			SizeBytes:  info.Size(),
			Compressed: compressed,
			IsSkipped:  chunk.IsSkipped,
		})
	}

	if backend != nil && storageMode != "shared_mount" && storageMode != "affinity" {
		chunkReaders := make([]io.Reader, 0, len(mergePayload.Chunks))
		for _, chunk := range mergePayload.Chunks {
			chunkKey := storage.GetRemotePath(mergePayload.DatasetID, chunk.ChunkIndex, "result")
			objReader, err := backend.ReadObject(ctx, chunkKey)
			if err != nil {
				return fmt.Errorf("failed to read chunk %s: %w", chunk.ChunkID, err)
			}
			defer objReader.Close()
			chunkReaders = append(chunkReaders, objReader)
		}

		mergedKey := storage.GetRemotePath(mergePayload.DatasetID, 0, "merged")
		mergedReader, err := dataset.MergeReaders(ctx, chunkReaders)
		if err != nil {
			return fmt.Errorf("failed to merge readers: %w", err)
		}

		if err := backend.WriteObject(ctx, mergedKey, mergedReader); err != nil {
			return fmt.Errorf("failed to write merged result: %w", err)
		}

		return nil
	}

	deviceOutput := mergePayload.DeviceOutput
	if deviceOutput == nil {
		mountPath := mergePayload.MountPath
		if mountPath == "" {
			mountPath = deriveMountBasePath(string(mergePayload.Strategy))
		}
		deviceOutput = &dataset.DeviceOutput{
			DeviceID:  mergePayload.AffinityDeviceID,
			MountPath: mountPath,
		}
	}

	mergeConfig := dataset.MergeConfig{
		DatasetID:         mergePayload.DatasetID,
		AffinityDeviceID:  mergePayload.AffinityDeviceID,
		DeviceOutput:      deviceOutput,
		Chunks:            chunks,
		Strategy:          string(mergePayload.Strategy),
		MountPath:         mergePayload.MountPath,
		DeleteChunksAfter: mergePayload.DeleteChunksAfter,
		Compressed:        mergePayload.Compressed,
		ChecksumRequired:  mergePayload.ChecksumRequired,
		UseTreeMerge:      mergePayload.UseTreeMerge,
		AgentID:           agentID,
	}

	var mergeResult *dataset.MergeResult
	var mergeErr error

	if mergePayload.UseTreeMerge {
		mergeResult, mergeErr = dataset.StreamMergeTree(ctx, mergeConfig)
	} else {
		mergeResult, mergeErr = dataset.StreamMerge(ctx, mergeConfig)
	}

	if mergeErr != nil {
		dataset.EmitMergeFailedEvent(ctx, mergePayload.DatasetID, agentID, mergeErr)
		return fmt.Errorf("merge failed: %w", mergeErr)
	}

	if err := dataset.EmitMergeCompletedEvent(ctx, mergeResult, agentID); err != nil {
		obs.Warn("failed to emit merge completed event", obs.Field{
			"error": err.Error(),
		})
	}

	outputPath := ""
	if mergeResult.DeviceOutput != nil {
		outputPath = filepath.Join(mergeResult.DeviceOutput.MountPath, mergeResult.DeviceOutput.File)
	}

	obs.Info("merge completed", obs.Field{
		"dataset_id":      mergePayload.DatasetID,
		"affinity_device": mergeResult.AffinityDeviceID,
		"merged_count":    mergeResult.MergedChunks,
		"skipped_count":   mergeResult.SkippedChunks,
		"deleted_count":   mergeResult.DeletedChunks,
		"output_path":     outputPath,
		"checksum":        mergeResult.Checksum,
		"file_size":       mergeResult.FileSize,
		"duration_ms":     mergeResult.DurationMs,
	})

	return nil
}

type ProcessPayload struct {
	ChunkID     string          `json:"chunk_id"`
	InputPath   string          `json:"input_path,omitempty"`
	OutputPath  string          `json:"output_path,omitempty"`
	DatasetID   string          `json:"dataset_id"`
	ChunkIndex  int             `json:"chunk_index,omitempty"`
	StorageMode string          `json:"storage_mode,omitempty"`
	Rows        int             `json:"rows,omitempty"`
	Checksum    string          `json:"checksum,omitempty"`
	Config      json.RawMessage `json:"config,omitempty"`
}

func executeProcessChunk(ctx context.Context, payload json.RawMessage) error {
	var job Job
	if err := json.Unmarshal(payload, &job); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	var processPayload ProcessPayload
	if err := json.Unmarshal(payload, &processPayload); err != nil {
		return fmt.Errorf("invalid process payload: %w", err)
	}

	if processPayload.ChunkID == "" && processPayload.DatasetID == "" {
		return errors.New("missing chunk_id or dataset_id")
	}

	storageMode := processPayload.StorageMode
	if storageMode == "" {
		storageMode = job.StorageMode
	}
	if storageMode == "" {
		storageMode = "shared_mount"
	}

	backend := GetStorageBackend()
	if backend == nil {
		return errors.New("storage backend not initialized")
	}

	datasetID := processPayload.DatasetID
	chunkIndex := processPayload.ChunkIndex

	chunkKey := storage.GetRemotePath(datasetID, chunkIndex, "chunk")
	resultKey := storage.GetRemotePath(datasetID, chunkIndex, "result")

	workDir, cleanup, err := plugin.PrepareJobWorkDir(job.ID)
	if err != nil {
		return fmt.Errorf("failed to prepare work dir: %w", err)
	}
	defer cleanup()

	inputPath := processPayload.InputPath
	outputPath := processPayload.OutputPath

	if storageMode == "shared_mount" {
		mountBasePath := deriveMountBasePath(storageMode)
		if inputPath == "" && datasetID != "" {
			inputPath = filepath.Join(mountBasePath, "datasets", datasetID, "chunks", fmt.Sprintf("chunk_%d.bin", chunkIndex))
		}
		if outputPath == "" && datasetID != "" {
			outputPath = filepath.Join(mountBasePath, "datasets", datasetID, "results", fmt.Sprintf("chunk_%d.out", chunkIndex))
		}
	} else {
		objReader, err := backend.ReadObject(ctx, chunkKey)
		if err != nil {
			return fmt.Errorf("failed to read chunk: %w", err)
		}
		defer objReader.Close()

		inputPath = filepath.Join(workDir, fmt.Sprintf("chunk_%d.bin", chunkIndex))
		inputFile, err := os.Create(inputPath)
		if err != nil {
			return fmt.Errorf("failed to create input file: %w", err)
		}
		_, err = io.Copy(inputFile, objReader)
		inputFile.Close()
		if err != nil {
			return fmt.Errorf("failed to write input file: %w", err)
		}

		outputPath = filepath.Join(workDir, fmt.Sprintf("chunk_%d.out", chunkIndex))
	}

	if inputPath == "" {
		return errors.New("could not derive input_path")
	}
	if outputPath == "" {
		return errors.New("could not derive output_path")
	}

	pluginName := job.PluginName
	if pluginName == "" {
		pluginName = "plugin_process_chunk"
	}

	pluginPath, manifest, err := plugin.LoadAndUpdatePlugin(ctx, pluginName)
	if err != nil {
		return err
	}

	effectiveConfig := job.StepConfig
	if len(processPayload.Config) > 0 {
		effectiveConfig = processPayload.Config
	}

	pluginCtx := PluginContext{
		JobID:       job.ID,
		OrgID:       job.OrgID,
		DatasetID:   processPayload.DatasetID,
		ExecutionID: job.ExecutionID,
		StepIndex:   job.StepIndex,
		ChunkID:     processPayload.ChunkID,
		ChunkIndex:  processPayload.ChunkIndex,
		InputPath:   inputPath,
		OutputPath:  outputPath,
		Config:      effectiveConfig,
	}

	inJSON, _ := json.Marshal(pluginCtx)

	env := system.DetectExecutionEnv()
	execResult, err := plugin.Execute(ctx, pluginPath, manifest, string(inJSON), env, NativeRunnerFunc())
	if err != nil {
		return fmt.Errorf("process plugin failed: %w", err)
	}

	var result map[string]any
	if err := json.Unmarshal([]byte(execResult.Output), &result); err != nil {
		return fmt.Errorf("failed to parse process plugin output: %w", err)
	}

	if storageMode != "shared_mount" {
		outputFile, err := os.Open(outputPath)
		if err != nil {
			return fmt.Errorf("failed to open output file: %w", err)
		}
		defer outputFile.Close()

		if err := backend.WriteObject(ctx, resultKey, outputFile); err != nil {
			return fmt.Errorf("failed to write result: %w", err)
		}
	}

	obs.Info("chunk processed", obs.Field{
		"job_id":      pluginCtx.JobID,
		"chunk_id":    pluginCtx.ChunkID,
		"output_path": pluginCtx.OutputPath,
	})

	return nil
}

type IngestPayload struct {
	DatasetID   string `json:"dataset_id"`
	StorageMode string `json:"storage_mode"`
	FileCount   int    `json:"file_count"`
	FileType    string `json:"file_type"`
	SizeGB      int    `json:"size_gb"`
	SourcePath  string `json:"source_path,omitempty"`
}

type IngestResult struct {
	DatasetID string `json:"dataset_id"`
	FileCount int    `json:"file_count"`
	FileType  string `json:"file_type"`
	SizeGB    int    `json:"size_gb"`
	Checksum  string `json:"checksum,omitempty"`
}

func deriveDatasetPaths(datasetID, storageMode, mountBasePath string) (sourcePath, chunksPath, resultsPath, mergedPath string) {
	basePath := mountBasePath
	if storageMode == "shared_mount" {
		if basePath == "" {
			homeDir, _ := os.UserHomeDir()
			basePath = filepath.Join(homeDir, "sentra", "data")
		}
		sourcePath = filepath.Join(basePath, "datasets", datasetID, "source")
		chunksPath = filepath.Join(basePath, "datasets", datasetID, "chunks")
		resultsPath = filepath.Join(basePath, "datasets", datasetID, "results")
		mergedPath = filepath.Join(basePath, "datasets", datasetID, "merged")
	}
	return sourcePath, chunksPath, resultsPath, mergedPath
}

func deriveChunkPath(datasetID string, chunkIndex int, pathType string, storageMode, mountBasePath string) string {
	if storageMode == "shared_mount" {
		if mountBasePath == "" {
			mountBasePath = deriveMountBasePath(storageMode)
		}
		basePath := filepath.Join(mountBasePath, "datasets", datasetID)
		switch pathType {
		case "source":
			return filepath.Join(basePath, "source")
		case "chunk":
			return filepath.Join(basePath, "chunks", "chunk_"+strconv.Itoa(chunkIndex)+".bin")
		case "result":
			return filepath.Join(basePath, "results", "chunk_"+strconv.Itoa(chunkIndex)+".out")
		case "merged":
			return filepath.Join(basePath, "merged", "dataset.parquet")
		}
	}
	return ""
}

func deriveMountBasePath(storageMode string) string {
	if storageMode != "shared_mount" {
		return ""
	}

	if mountPath := os.Getenv("SENTRA_MOUNT_PATH"); mountPath != "" {
		return mountPath
	}

	homeDir, _ := os.UserHomeDir()
	return filepath.Join(homeDir, "sentra", "data")
}

func executeIngestDataset(ctx context.Context, payload json.RawMessage) error {
	var job Job
	if err := json.Unmarshal(payload, &job); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	var ingestPayload IngestPayload
	if err := json.Unmarshal(payload, &ingestPayload); err != nil {
		return fmt.Errorf("invalid ingest payload: %w", err)
	}

	if ingestPayload.DatasetID == "" {
		return errors.New("missing dataset_id")
	}

	storageMode := ingestPayload.StorageMode
	if storageMode == "" {
		storageMode = "shared_mount"
	}

	mountBasePath := deriveMountBasePath(storageMode)

	backend := GetStorageBackend()

	sourcePath, chunksPath, resultsPath, mergedPath := deriveDatasetPaths(
		ingestPayload.DatasetID,
		storageMode,
		mountBasePath,
	)

	if ingestPayload.SourcePath != "" {
		sourcePath = ingestPayload.SourcePath
	}

	if backend != nil && storageMode != "shared_mount" {
		stagingPrefix := ingestPayload.SourcePath
		if stagingPrefix != "" {
			objects, err := backend.ListObjects(ctx, stagingPrefix)
			if err != nil {
				return fmt.Errorf("failed to list staging objects: %w", err)
			}

			for _, obj := range objects {
				objReader, err := backend.ReadObject(ctx, obj.Key)
				if err != nil {
					return fmt.Errorf("failed to read object %s: %w", obj.Key, err)
				}
				defer objReader.Close()

				destKey := storage.GetRemotePath(ingestPayload.DatasetID, 0, "source") + "/" + filepath.Base(obj.Key)
				if err := backend.WriteObject(ctx, destKey, objReader); err != nil {
					return fmt.Errorf("failed to write object %s: %w", destKey, err)
				}
			}
		}

		sourcePath = storage.GetRemotePath(ingestPayload.DatasetID, 0, "source")
		chunksPath = storage.GetRemotePath(ingestPayload.DatasetID, 0, "chunk")
		resultsPath = storage.GetRemotePath(ingestPayload.DatasetID, 0, "result")
		mergedPath = storage.GetRemotePath(ingestPayload.DatasetID, 0, "merged")
	}

	for _, dirPath := range []string{sourcePath, chunksPath, resultsPath, mergedPath} {
		if dirPath != "" && storageMode == "shared_mount" {
			if err := os.MkdirAll(dirPath, 0700); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", dirPath, err)
			}
		}
	}

	pluginName := job.PluginName
	if pluginName == "" {
		pluginName = "plugin_ingest_dataset"
	}

	pluginPath, manifest, err := plugin.LoadAndUpdatePlugin(ctx, pluginName)
	if err != nil {
		return err
	}

	ingestCtx := PluginContext{
		JobID:       job.ID,
		OrgID:       job.OrgID,
		DatasetID:   ingestPayload.DatasetID,
		ExecutionID: job.ExecutionID,
		StepIndex:   job.StepIndex,
		InputPath:   sourcePath,
		OutputPath:  mergedPath,
		Config:      job.StepConfig,
	}

	ingestPayloadForPlugin := map[string]any{
		"job_id":       ingestCtx.JobID,
		"org_id":       ingestCtx.OrgID,
		"dataset_id":   ingestCtx.DatasetID,
		"execution_id": ingestCtx.ExecutionID,
		"step_index":   ingestCtx.StepIndex,
		"source_path":  sourcePath,
		"chunks_path":  chunksPath,
		"results_path": resultsPath,
		"merged_path":  mergedPath,
		"storage_mode": storageMode,
		"file_count":   ingestPayload.FileCount,
		"file_type":    ingestPayload.FileType,
		"size_gb":      ingestPayload.SizeGB,
		"config":       ingestCtx.Config,
	}

	inJSON, _ := json.Marshal(ingestPayloadForPlugin)

	env := system.DetectExecutionEnv()
	execResult, err := plugin.Execute(ctx, pluginPath, manifest, string(inJSON), env, NativeRunnerFunc())
	if err != nil {
		return fmt.Errorf("ingest plugin failed: %w", err)
	}

	var result IngestResult
	if err := json.Unmarshal([]byte(execResult.Output), &result); err != nil {
		return fmt.Errorf("failed to parse ingest plugin output: %w", err)
	}

	obs.Info("dataset ingested", obs.Field{
		"dataset_id":  ingestPayload.DatasetID,
		"source_path": sourcePath,
		"file_count":  ingestPayload.FileCount,
		"file_type":   ingestPayload.FileType,
	})

	return nil
}
