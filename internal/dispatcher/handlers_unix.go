//go:build linux || darwin
// +build linux darwin

package dispatcher

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	executorv2 "sentra-agent/cmd/agent/executor/v2"
	runtimev2 "sentra-agent/cmd/agent/runtime/v2"
	"sentra-agent/internal/auth"
	"sentra-agent/internal/config"
	"sentra-agent/internal/dataset"
	"sentra-agent/internal/httpclient"
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
	supabaseBaseURL = firstNonEmpty(
		os.Getenv("SUPABASE_URL"),
		config.DefaultBackendURL,
	)
	supabaseToken = firstNonEmpty(
		os.Getenv("SUPABASE_SERVICE_ROLE_KEY"),
	)
	supabaseAnonKey = firstNonEmpty(
		os.Getenv("SUPABASE_ANON_KEY"),
		config.DefaultAnonKey,
	)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
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
	DatasetSlug       string                `json:"dataset_slug,omitempty"`
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

	// Get execution_id from context (injected by ExecuteJob)
	executionID := obs.ExecutionID(ctx)
	
	// If not in context, fall back to payload
	if executionID == "" {
		executionID = job.ExecutionID
	}
	
	// VALIDATION: execution_id is REQUIRED for completion
	if executionID == "" {
		obs.Error("executeScanDataset: missing execution_id - job will fail at completion", obs.Field{
			"job_id":     job.ID,
			"dataset_id": job.DatasetID,
		})
		return errors.New("missing execution_id - aborting execution")
	}
	
	// Use executionID from context, inject into job for downstream
	job.ExecutionID = executionID

	obs.Info("executeScanDataset received job", obs.Field{
		"job_id":        job.ID,
		"execution_id": executionID,
		"dataset_id":   job.DatasetID,
		"storage_mode":  job.StorageMode,
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
	storageMode = normalizeStorageModeForJob(storageMode)

	obs.Info("executeScanDataset: storage config", obs.Field{
		"storage_mode":      storageMode,
		"dataset_id":        job.DatasetID,
		"storage_config_id": job.StorageConfigID,
		"backend_type":      fmt.Sprintf("%T", GetStorageBackend()),
	})

	var backend storage.StorageBackend

	if job.StorageConfigID != "" && (storageMode == "s3" || storageMode == "aws_s3" || storageMode == "object_storage" || storageMode == "gcs" || storageMode == "google_cloud_storage" || storageMode == "azure_blob") {
		cfg, err := storage.GetConfigByID(job.StorageConfigID)
		if err != nil {
			return fmt.Errorf("failed to get storage config %s: %w", job.StorageConfigID, err)
		}
		obs.Info("executeScanDataset: got storage config", obs.Field{
			"storage_mode":      cfg.StorageMode,
			"provider":          cfg.Provider,
			"bucket_name":       cfg.BucketName,
			"endpoint":          cfg.Endpoint,
			"storage_config_id": job.StorageConfigID,
		})
		cfgCopy := &storage.StorageConfig{
			StorageMode:   storageMode,
			Provider:      cfg.Provider,
			BucketName:    cfg.BucketName,
			Region:        cfg.Region,
			Endpoint:      cfg.Endpoint,
			MountBasePath: cfg.MountBasePath,
			Credentials:   cfg.Credentials,
		}
		newBackend, err := storage.NewBackend(cfgCopy)
		if err != nil {
			return fmt.Errorf("failed to initialize storage backend for %s: %w", job.StorageConfigID, err)
		}
		backend = newBackend
		obs.Info("executeScanDataset: created backend from storage_config_id", obs.Field{
			"backend_type":      fmt.Sprintf("%T", backend),
			"storage_config_id": job.StorageConfigID,
		})
	}

	if backend == nil {
		backend = GetStorageBackend()
	}
	if backend == nil {
		return errors.New("storage backend not initialized")
	}

	var remotePath string
	if job.SourcePath != "" {
		if strings.Contains(job.SourcePath, "http") {
			return fmt.Errorf("invalid source_path: contains URL %q (must be S3 object key only, not full URL)", job.SourcePath)
		}
		if strings.HasPrefix(job.SourcePath, "/") || filepath.IsAbs(job.SourcePath) {
			return fmt.Errorf("invalid source_path: is absolute path %q (must be S3 object key relative to bucket)", job.SourcePath)
		}
		remotePath = job.SourcePath
		// Strip bucket prefix if present - S3 key should NOT include bucket name
		// e.g., "datasets/test-data.csv" -> "test-data.csv"
		for _, bucket := range []string{"datasets", "data", "public"} {
			remotePath = strings.TrimPrefix(remotePath, bucket+"/")
		}
		// Validate after stripping - warn if still has path separators (might be nested)
		if strings.Contains(remotePath, "/") {
			obs.Warn("source_path contains subdirectory - verify this is intentional", obs.Field{
				"source_path": job.SourcePath,
				"remote_path": remotePath,
			})
		}
	} else {
		remotePath = storage.GetRemotePath(job.DatasetID, 0, "source")
	}

	obs.Info("executeScanDataset: listing remote objects", obs.Field{
		"remote_path": remotePath,
		"source_path_provided": job.SourcePath != "",
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

	summary, err := scanDatasetBuiltin(ctx, inputPath, workDir)
	if err != nil {
		return fmt.Errorf("built-in scan failed: %w", err)
	}

	if summary == nil {
		return errors.New("scan returned nil output")
	}

	obs.Info("scan completed", obs.Field{
		"job_id":        job.ID,
		"dataset_id":    job.DatasetID,
		"summary_keys": len(summary),
	})

	// Report scan results to backend - non-critical, don't fail job on error
	if err := reportDatasetScan(ctx, job.DatasetID, job.OrgID, storageMode, summary); err != nil {
		obs.Warn("failed to report dataset scan", obs.Field{
			"job_id":     job.ID,
			"dataset_id": job.DatasetID,
			"error":      err.Error(),
		})
		// Don't return error - reporting is non-critical
	}

	return nil
}

func reportDatasetScan(ctx context.Context, datasetID, orgID, storageType string, summary map[string]any) error {
	if supabaseBaseURL == "" {
		obs.Warn("report_dataset_scan skipped: SUPABASE_URL not set", nil)
		return nil
	}

	deviceToken, err := auth.GetToken()
	if err != nil {
		obs.Warn("report_dataset_scan skipped: device token not available", obs.Field{"error": err.Error()})
		return nil
	}

	reportPayload := map[string]interface{}{
		"dataset_id":   datasetID,
		"org_id":      orgID,
		"storage_type": storageType,
		"summary":     summary,
	}

	payloadBytes, err := json.Marshal(reportPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal report payload: %w", err)
	}

	httpc := httpclient.NewClient(supabaseBaseURL, supabaseAnonKey, deviceToken)
	resp, err := httpc.DoWithReq(ctx, "POST", "/functions/v1/report_dataset_scan", payloadBytes, func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+supabaseAnonKey)
		r.Header.Set("x-agent-token", deviceToken)
		if execClient != nil {
			r.Header.Set("x-device-id", execClient.GetDeviceID())
		}
	})
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
	if mergePayload.AffinityDeviceID == "" && mergePayload.DeviceOutput == nil && !mergePayload.IsPartial {
		return errors.New("missing affinity_device_id or device_output")
	}
	if len(mergePayload.Chunks) == 0 && !mergePayload.IsPartial {
		return errors.New("no chunks provided")
	}

	storageMode := string(mergePayload.Strategy)
	backend := GetStorageBackend()

	useS3Merge := backend != nil && storageMode != "shared_mount" && storageMode != "affinity"

	var chunks []dataset.ChunkInfo

	if !useS3Merge {
		// Local-file strategies (shared_mount / affinity): validate chunk files exist
		chunks = make([]dataset.ChunkInfo, 0, len(mergePayload.Chunks))
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
		// (fall through to deviceOutput / local merge below)
	}

	if useS3Merge {
		datasetSlug := mergePayload.DatasetSlug
		chunkReaders := make([]io.Reader, 0, len(mergePayload.Chunks))
		for _, chunk := range mergePayload.Chunks {
			chunkKey := storage.GetRemotePathWithSlug(mergePayload.DatasetID, datasetSlug, chunk.ChunkIndex, "result")
			objReader, err := backend.ReadObject(ctx, chunkKey)
			if err != nil {
				return fmt.Errorf("failed to read chunk %s: %w", chunk.ChunkID, err)
			}
			defer objReader.Close()
			chunkReaders = append(chunkReaders, objReader)
		}

		mergedKey := storage.GetRemotePathWithSlug(mergePayload.DatasetID, datasetSlug, 0, "merged")
		mergedReader, err := dataset.MergeReaders(ctx, chunkReaders)
		if err != nil {
			return fmt.Errorf("failed to merge readers: %w", err)
		}

		if err := backend.WriteObject(ctx, mergedKey, mergedReader); err != nil {
			return fmt.Errorf("failed to write merged result: %w", err)
		}

		// Clean up chunks after successful merge
		if mergePayload.DeleteChunksAfter {
			for _, chunk := range mergePayload.Chunks {
				chunkKey := storage.GetRemotePathWithSlug(mergePayload.DatasetID, datasetSlug, chunk.ChunkIndex, "result")
				if err := backend.DeleteObject(ctx, chunkKey); err != nil {
					obs.Warn("failed to delete chunk after merge", obs.Field{
						"chunk_key": chunkKey,
						"error":     err.Error(),
					})
				} else {
					obs.Info("deleted chunk after merge", obs.Field{"chunk_key": chunkKey})
				}
			}
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
		DatasetSlug:       mergePayload.DatasetSlug,
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
	DatasetSlug string          `json:"dataset_slug,omitempty"`
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
	datasetSlug := processPayload.DatasetSlug
	chunkIndex := processPayload.ChunkIndex

	chunkKey := storage.GetRemotePathWithSlug(datasetID, datasetSlug, chunkIndex, "chunk")
	resultKey := storage.GetRemotePathWithSlug(datasetID, datasetSlug, chunkIndex, "result")

	workDirID := job.ID
	if workDirID == "" {
		workDirID = processPayload.ChunkID
	}
	if workDirID == "" {
		workDirID = processPayload.DatasetID
	}
	workDir, cleanup, err := plugin.PrepareJobWorkDir(workDirID)
	if err != nil {
		return fmt.Errorf("failed to prepare work dir: %w", err)
	}
	defer cleanup()

	inputPath := processPayload.InputPath
	outputPath := processPayload.OutputPath

	stepIndex := job.StepIndex

	if storageMode == "shared_mount" {
		mountBasePath := deriveMountBasePath(storageMode)
		if inputPath == "" && datasetID != "" {
			if stepIndex > 0 {
				// Chain from previous step's output
				inputPath = filepath.Join(mountBasePath, "datasets", datasetID, "chunks", fmt.Sprintf("chunk_%d.out", chunkIndex))
			} else {
				// First step: read from chunks
				inputPath = filepath.Join(mountBasePath, "datasets", datasetID, "chunks", fmt.Sprintf("chunk_%d.bin", chunkIndex))
			}
		}
		if outputPath == "" && datasetID != "" {
			outputPath = filepath.Join(mountBasePath, "datasets", datasetID, "chunks", fmt.Sprintf("chunk_%d.out", chunkIndex))
		}
		processPayload.InputPath = inputPath
		processPayload.OutputPath = outputPath
	} else {
		// S3/object storage: download input based on step index
		inputKey := chunkKey
		if stepIndex > 0 {
			inputKey = resultKey
		}
		objReader, err := backend.ReadObject(ctx, inputKey)
		if err != nil {
			return fmt.Errorf("failed to read input object: %w", err)
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

		processPayload.InputPath = inputPath
		processPayload.OutputPath = outputPath
	}

	if inputPath == "" {
		return errors.New("could not derive input_path")
	}
	if outputPath == "" {
		return errors.New("could not derive output_path")
	}

	pluginName := ResolvePluginName(job.PluginID, job.PluginName)
	if pluginName == "" {
		obs.Warn("no plugin name or ID in job payload — agent cannot load plugin", obs.Field{
			"job_id":    job.ID,
			"plugin_id": job.PluginID,
		})
		return fmt.Errorf("no plugin_id in job payload — cannot determine which plugin to execute")
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
		// If Docker is unavailable and this is a script plugin, fall back to v2 runtime
		if strings.Contains(err.Error(), "Docker") || strings.Contains(err.Error(), "docker") {
			obs.Info("docker sandbox unavailable, falling back to v2 runtime", obs.Field{
				"plugin": manifest.Name,
				"error":  err.Error(),
			})
			execResult, err = fallbackToV2Runtime(ctx, pluginPath, manifest, &processPayload, &job)
			if err != nil {
				return fmt.Errorf("process plugin failed (native+v2 fallback): %w", err)
			}
		} else {
			return fmt.Errorf("process plugin failed: %w", err)
		}
	}

	var result map[string]any
	if execResult != nil && execResult.Output != "" {
		if err := json.Unmarshal([]byte(execResult.Output), &result); err != nil {
			return fmt.Errorf("failed to parse process plugin output: %w", err)
		}
	}

	if storageMode != "shared_mount" {
		if _, err := os.Stat(outputPath); os.IsNotExist(err) && execResult != nil && execResult.Output != "" {
			if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err == nil {
				os.WriteFile(outputPath, []byte(execResult.Output), 0644)
			}
		}
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
		resultsPath = filepath.Join(basePath, "datasets", datasetID, "chunks")
		mergedPath = filepath.Join(basePath, "datasets", datasetID)
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
			return filepath.Join(basePath, "chunks", "chunk_"+strconv.Itoa(chunkIndex)+".out")
		case "merged":
			return filepath.Join(basePath, datasetID+".csv")
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

func normalizeStorageModeForJob(mode string) string {
	switch mode {
	case "s3", "aws_s3", "s3_compatible", "object_storage":
		return "s3"
	case "gcs", "google_cloud_storage":
		return "gcs"
	case "azure_blob":
		return "azure_blob"
	case "shared_mount", "local":
		return "shared_mount"
	default:
		return mode
	}
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

	pluginName := ResolvePluginName(job.PluginID, job.PluginName)
	if pluginName == "" {
		return fmt.Errorf("no plugin_id in job payload — cannot determine which plugin to execute for ingestion")
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

func scanDatasetBuiltin(ctx context.Context, inputPath, workDir string) (map[string]any, error) {
	summary := map[string]any{
		"file_count":        0,
		"total_size_bytes":  int64(0),
		"file_types":        map[string]int{},
		"headers":            []string{},
		"columns":            []string{},
		"sample_row_count":   0,
		"detected_formats":  []string{},
		"schema":             map[string]string{},
	}

	inputIsDir := inputPath
	if inputIsDir == "" {
		return summary, nil
	}

	info, err := os.Stat(inputIsDir)
	if err != nil {
		if os.IsNotExist(err) {
			obs.Warn("scan input path does not exist", obs.Field{"path": inputIsDir})
			return summary, nil
		}
		return nil, err
	}

	var files []os.FileInfo
	if info.IsDir() {
		entries, err := os.ReadDir(inputIsDir)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if !e.IsDir() {
				f, err := e.Info()
				if err == nil {
					files = append(files, f)
				}
			}
		}
	} else {
		files = []os.FileInfo{info}
	}

	fileCount := len(files)
	totalSize := int64(0)
	fileTypes := map[string]int{}
	var sampleFile os.FileInfo
	var ext string

	for _, f := range files {
		totalSize += f.Size()
		ext = filepath.Ext(f.Name())
		if ext == "" {
			ext = "(no extension)"
		}
		fileTypes[ext]++
		
		// Use first file as sample for metadata extraction
		if sampleFile == nil && f.Size() > 0 {
			sampleFile = f
		}
	}

	summary["file_count"] = fileCount
	summary["total_size_bytes"] = totalSize
	summary["file_types"] = fileTypes
	summary["detected_formats"] = []string{}

	// Only extract metadata if we have a sample file
	if sampleFile != nil {
		samplePath := filepath.Join(inputIsDir, sampleFile.Name())
		if !info.IsDir() {
			samplePath = inputPath
		}

		metadata, err := extractFileMetadata(ctx, samplePath)
		if err != nil {
			obs.Warn("failed to extract file metadata", obs.Field{
				"path": samplePath,
				"error": err.Error(),
			})
		} else {
			// Merge extracted metadata into summary
			for k, v := range metadata {
				summary[k] = v
			}
			
			// Track detected formats
			if format, ok := metadata["format"].(string); ok && format != "" {
				summary["detected_formats"] = []string{format}
			}
		}
	}

	obs.Info("built-in scan complete", obs.Field{
		"file_count":      fileCount,
		"total_size_mb":   totalSize / (1024 * 1024),
		"unique_types":   len(fileTypes),
		"sample_file":    summary["sample_file"],
		"columns":        len(summary["columns"].([]string)),
	})

	return summary, nil
}

func extractFileMetadata(ctx context.Context, filePath string) (map[string]any, error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	
	switch ext {
	case ".csv", ".tsv", ".txt":
		return extractCSVMetadata(ctx, filePath)
	case ".json", ".jsonl":
		return extractJSONMetadata(ctx, filePath)
	case ".parquet":
		return extractParquetMetadata(ctx, filePath)
	case ".ndjson":
		return extractJSONLMetadata(ctx, filePath)
	default:
		// Try to detect format by reading first bytes
		return detectAndExtractMetadata(ctx, filePath)
	}
}

func extractCSVMetadata(ctx context.Context, filePath string) (map[string]any, error) {
	metadata := map[string]any{
		"format":       "csv",
		"sample_file":  filepath.Base(filePath),
	}

	file, err := os.Open(filePath)
	if err != nil {
		return metadata, err
	}
	defer file.Close()

	// Detect delimiter
	delimiter := detectDelimiter(file)
	metadata["delimiter"] = string(delimiter)

	// Reset to beginning
	file.Seek(0, 0)

	reader := csv.NewReader(file)
	reader.Comma = delimiter
	reader.FieldsPerRecord = 0 // Allow variable fields

	// Read header
	headers, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return metadata, nil
		}
		return metadata, err
	}

	metadata["headers"] = headers
	metadata["columns"] = headers
	metadata["column_count"] = len(headers)

	// Read first few rows to detect types and row count sample
	rowCount := 0
	typeHints := map[string]string{}
	sampleRows := []map[string]any{}

	for i := 0; i < 100; i++ {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			continue
		}
		rowCount++

		// Collect sample rows
		if i < 10 {
			rowMap := map[string]any{}
			for j, val := range row {
				if j < len(headers) {
					rowMap[headers[j]] = val
				}
			}
			sampleRows = append(sampleRows, rowMap)
		}

		// Detect types
		for j, val := range row {
			if j < len(headers) {
				col := headers[j]
				if _, exists := typeHints[col]; !exists && val != "" {
					typeHints[col] = detectValueType(val)
				}
			}
		}
	}

	metadata["sample_row_count"] = rowCount
	metadata["schema"] = typeHints
	metadata["sample_rows"] = sampleRows
	metadata["has_header"] = true
	metadata["estimated_row_count"] = estimateTotalRows(filePath, rowCount)

	return metadata, nil
}

func extractJSONMetadata(ctx context.Context, filePath string) (map[string]any, error) {
	metadata := map[string]any{
		"format":       "json",
		"sample_file": filepath.Base(filePath),
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return metadata, err
	}

	var jsonData any
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return metadata, err
	}

	switch v := jsonData.(type) {
	case []any:
		metadata["array_length"] = len(v)
		if len(v) > 0 {
			if obj, ok := v[0].(map[string]any); ok {
				headers := make([]string, 0, len(obj))
				schema := map[string]string{}
				for k, val := range obj {
					headers = append(headers, k)
					schema[k] = detectValueTypeFromAny(val)
				}
				metadata["headers"] = headers
				metadata["columns"] = headers
				metadata["column_count"] = len(headers)
				metadata["schema"] = schema

				// Sample rows
				sampleRows := []map[string]any{}
				for i := 0; i < min(10, len(v)); i++ {
					if row, ok := v[i].(map[string]any); ok {
						sampleRows = append(sampleRows, row)
					}
				}
				metadata["sample_rows"] = sampleRows
			}
		}
	case map[string]any:
		headers := make([]string, 0, len(v))
		schema := map[string]string{}
		sampleRow := map[string]any{}
		for k, val := range v {
			headers = append(headers, k)
			schema[k] = detectValueTypeFromAny(val)
			sampleRow[k] = val
		}
		metadata["headers"] = headers
		metadata["columns"] = headers
		metadata["column_count"] = len(headers)
		metadata["schema"] = schema
		metadata["sample_rows"] = []map[string]any{sampleRow}
	}

	metadata["estimated_row_count"] = estimateTotalRows(filePath, 1)
	return metadata, nil
}

func extractJSONLMetadata(ctx context.Context, filePath string) (map[string]any, error) {
	metadata := map[string]any{
		"format":        "jsonl",
		"sample_file":   filepath.Base(filePath),
	}

	file, err := os.Open(filePath)
	if err != nil {
		return metadata, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	rowCount := 0
	headers := []string{}
	schema := map[string]string{}
	sampleRows := []map[string]any{}

	for i := 0; i < 100 && scanner.Scan(); i++ {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			continue
		}
		rowCount++

		// Collect headers from first row
		if i == 0 {
			for k, v := range row {
				headers = append(headers, k)
				schema[k] = detectValueTypeFromAny(v)
			}
		}

		if i < 10 {
			sampleRows = append(sampleRows, row)
		}
	}

	metadata["headers"] = headers
	metadata["columns"] = headers
	metadata["column_count"] = len(headers)
	metadata["schema"] = schema
	metadata["sample_rows"] = sampleRows
	metadata["sample_row_count"] = rowCount
	metadata["estimated_row_count"] = estimateTotalRows(filePath, rowCount)

	return metadata, nil
}

func extractParquetMetadata(ctx context.Context, filePath string) (map[string]any, error) {
	metadata := map[string]any{
		"format":       "parquet",
		"sample_file": filepath.Base(filePath),
	}

	// Open parquet file
	f, err := os.Open(filePath)
	if err != nil {
		return metadata, err
	}
	defer f.Close()

	pr := parquet.NewReader(f)
	if pr == nil {
		return metadata, errors.New("failed to create parquet reader")
	}
	defer func() { pr.Close() }()

	// Get schema from parquet
	schema := map[string]string{}
	headers := []string{}

	schemaFields := pr.Schema().Fields()
	for _, field := range schemaFields {
		headers = append(headers, field.Name())
		schema[field.Name()] = parquetTypeToString(field.Type())
	}

	// Read sample rows
	rowCount := min(10, int(pr.NumRows()))
	sampleRows := []map[string]any{}

	for i := 0; i < rowCount; i++ {
		rowMap := make(map[string]any)
		if err := pr.Read(&rowMap); err != nil {
			if err == io.EOF {
				break
			}
			continue
		}
		sampleRows = append(sampleRows, rowMap)
	}

	metadata["headers"] = headers
	metadata["columns"] = headers
	metadata["column_count"] = len(headers)
	metadata["schema"] = schema
	metadata["sample_rows"] = sampleRows
	metadata["total_row_count"] = int(pr.NumRows())
	metadata["estimated_row_count"] = int(pr.NumRows())

	return metadata, nil
}

func detectAndExtractMetadata(ctx context.Context, filePath string) (map[string]any, error) {
	metadata := map[string]any{
		"format":       "unknown",
		"sample_file": filepath.Base(filePath),
	}

	// Read first 4KB to detect format
	file, err := os.Open(filePath)
	if err != nil {
		return metadata, err
	}
	defer file.Close()

	header := make([]byte, 4096)
	n, err := file.Read(header)
	if err != nil {
		return metadata, err
	}

	// Magic bytes detection
	if n >= 4 {
		// Parquet magic bytes
		if string(header[:4]) == "PAR1" {
			return extractParquetMetadata(ctx, filePath)
		}
		// gzip
		if header[0] == 0x1f && header[1] == 0x8b {
			metadata["format"] = "gzip"
		}
	}

	// Content-based detection
	content := string(header[:min(n, 1000)])
	if strings.HasPrefix(content, "{") || strings.HasPrefix(content, "[") {
		// Try JSON first
		if strings.Contains(content, "\n") {
			return extractJSONLMetadata(ctx, filePath)
		}
		return extractJSONMetadata(ctx, filePath)
	}

	// Default to CSV-like detection
	if strings.Contains(content, ",") || strings.Contains(content, "\t") || strings.Contains(content, ";") {
		return extractCSVMetadata(ctx, filePath)
	}

	return metadata, nil
}

func detectDelimiter(file *os.File) rune {
	// Read first line to detect delimiter
	reader := bufio.NewReader(file)
	firstLine, err := reader.ReadString('\n')
	if err != nil {
		return ','
	}

	delimiters := map[rune]int{',': 0, '\t': 0, ';': 0, '|': 0}
	for _, c := range firstLine {
		if _, ok := delimiters[c]; ok {
			delimiters[c]++
		}
	}

	// Find max
	maxCount := 0
	delimiter := ','
	for d, count := range delimiters {
		if count > maxCount {
			maxCount = count
			delimiter = d
		}
	}

	return delimiter
}

func detectValueType(value string) string {
	if value == "" {
		return "null"
	}

	// Check if number
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return "integer"
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return "float"
	}

	// Check if boolean
	lower := strings.ToLower(value)
	if lower == "true" || lower == "false" {
		return "boolean"
	}

	// Check if date/time
	dateFormats := []string{
		"2006-01-02", "2006/01/02", "01/02/2006",
		"2006-01-02T15:04:05", "2006-01-02 15:04:05",
	}
	for _, format := range dateFormats {
		if _, err := time.Parse(format, value); err == nil {
			return "datetime"
		}
	}

	return "string"
}

func detectValueTypeFromAny(value any) string {
	if value == nil {
		return "null"
	}

	switch value.(type) {
	case int, int64:
		return "integer"
	case float64:
		return "float"
	case bool:
		return "boolean"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "unknown"
	}
}

func parquetTypeToString(t any) string {
	typeStr := fmt.Sprintf("%T", t)
	switch {
	case strings.Contains(typeStr, "Int64"):
		return "integer"
	case strings.Contains(typeStr, "Float"):
		return "float"
	case strings.Contains(typeStr, "Boolean"):
		return "boolean"
	case strings.Contains(typeStr, "ByteArray"):
		return "binary"
	case strings.Contains(typeStr, "UTF8"):
		return "string"
	default:
		return typeStr
	}
}

func estimateTotalRows(filePath string, sampleRowCount int) int {
	if sampleRowCount == 0 {
		return 0
	}

	info, err := os.Stat(filePath)
	if err != nil {
		return 0
	}

	// Estimate based on average row size
	avgRowSize := info.Size() / int64(sampleRowCount)
	if avgRowSize == 0 {
		return 0
	}

	return int(info.Size() / avgRowSize)
}

// fallbackToV2Runtime is used when the Docker sandbox is unavailable for script plugins.
// It reads the cached plugin file and executes it via the v2 runtime (venv/npm).
func fallbackToV2Runtime(
	ctx context.Context,
	pluginPath string,
	manifest plugin.Manifest,
	processPayload *ProcessPayload,
	job *Job,
) (*plugin.ExecutionResult, error) {
	// Read plugin code from cached file
	pluginCode, err := os.ReadFile(pluginPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read plugin file for v2 fallback: %w", err)
	}

	// Map manifest language to v2 runtime type
	var rtType runtimev2.RuntimeType
	switch manifest.Language {
	case "python", "python3", "python2":
		rtType = runtimev2.RuntimePython
	case "node", "nodejs", "javascript", "typescript":
		rtType = runtimev2.RuntimeNode
	default:
		return nil, fmt.Errorf("v2 fallback: unsupported language %q", manifest.Language)
	}

	// Build v2 executor job
	payloadMap := make(map[string]interface{})
	if processPayload != nil {
		payloadMap["dataset_id"] = processPayload.DatasetID
		payloadMap["chunk_id"] = processPayload.ChunkID
		payloadMap["chunk_index"] = processPayload.ChunkIndex
		payloadMap["input_path"] = processPayload.InputPath
		payloadMap["output_path"] = processPayload.OutputPath
	}

	v2Job := executorv2.Job{
		ID:               job.ID,
		Type:             job.Type,
		Payload:          payloadMap,
		PluginID:         job.PluginID,
		PluginCode:       string(pluginCode),
		RuntimeType:      string(rtType),
		RuntimeDeps:      mapDepsToV2(manifest.Dependencies),
		ExecutionID:      job.ExecutionID,
		ExecutionStepID:  job.ExecutionStepID,
		OrgID:            job.OrgID,
		TimeoutSeconds:   300,
		Trusted:          manifest.Trusted,
	}

	obs.Info("falling back to v2 runtime", obs.Field{
		"plugin":     manifest.Name,
		"language":   manifest.Language,
		"runtime":    rtType,
		"plugin_id":  job.PluginID,
	})

	execResult, err := executorInstance.ExecuteJob(ctx, v2Job)
	if err != nil {
		return nil, err
	}
	if !execResult.Success {
		errMsg := execResult.Error
		if errMsg == "" {
			errMsg = "v2 fallback execution returned failure"
		}
		return nil, fmt.Errorf("v2 fallback: %s", errMsg)
	}

	// Marshal v2 result data back to JSON string for plugin.ExecutionResult
	outputJSON := ""
	if execResult.Data != nil {
		dataBytes, _ := json.Marshal(execResult.Data)
		outputJSON = string(dataBytes)
	}

	return &plugin.ExecutionResult{
		Output:     outputJSON,
		DurationMs: execResult.DurationMs,
	}, nil
}

func mapDepsToV2(deps []plugin.RuntimeDependency) []runtimev2.Dependency {
	if len(deps) == 0 {
		return nil
	}
	out := make([]runtimev2.Dependency, len(deps))
	for i, d := range deps {
		out[i] = runtimev2.Dependency{
			Name:    d.Name,
			Version: d.Version,
			Source:  d.Source,
		}
	}
	return out
}
