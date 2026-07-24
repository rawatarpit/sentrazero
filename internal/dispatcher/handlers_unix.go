//go:build linux || darwin
// +build linux darwin

package dispatcher

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"debug/elf"
	"debug/macho"
	"debug/pe"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	pdfcpuapi "github.com/pdfcpu/pdfcpu/pkg/api"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu/model"

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
	projectRef := config.ReadProjectRef()
	supabaseBaseURL = firstNonEmpty(
		os.Getenv("BACKEND_URL"),
		os.Getenv("SUPABASE_URL"),
		config.BuildBackendURL(projectRef),
	)
	supabaseToken = firstNonEmpty(
		os.Getenv("SUPABASE_SERVICE_ROLE_KEY"),
	)
	supabaseAnonKey = firstNonEmpty(
		os.Getenv("SUPABASE_ANON_KEY"),
		os.Getenv("BACKEND_ANON_KEY"),
		config.BuildAnonKey(projectRef),
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
	StrategyAffinity       MergeStrategy = "affinity"
	StrategySharedMount    MergeStrategy = "shared_mount"
	StrategyByteRange      MergeStrategy = "byte_range"
	StrategyFilePerChunk   MergeStrategy = "file_per_chunk"
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
	StorageMode       string                `json:"storage_mode,omitempty"`
	StorageConfigID   string                `json:"storage_config_id,omitempty"`
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
	
	// execution_id may be empty for background scan jobs
	if executionID == "" {
		obs.Warn("executeScanDataset: no execution_id (background scan)", obs.Field{
			"job_id":     job.ID,
			"dataset_id": job.DatasetID,
		})
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

	// Extract nested payload fields as fallback
	var nestedJob struct {
		StorageMode string `json:"storage_mode"`
	}
	if job.Payload != nil {
		json.Unmarshal(job.Payload, &nestedJob)
	}

	storageMode := job.StorageMode
	if storageMode == "" {
		storageMode = job.StorageType
	}
	if storageMode == "" {
		storageMode = nestedJob.StorageMode
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
		return fmt.Errorf("source_path is required for scan_dataset (no /source/ prefix fallback)")
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

	// Override summary with accurate data from ALL listed objects, not just the sample
	summary["file_count"] = len(objects)

	fileTypes := map[string]int{}
	for _, obj := range objects {
		ext := filepath.Ext(obj.Key)
		if ext == "" {
			ext = "(no extension)"
		}
		fileTypes[ext]++
	}
	summary["file_types"] = fileTypes
	for ext := range fileTypes {
		summary["file_type"] = strings.TrimPrefix(ext, ".")
		break
	}

	summary["total_size_bytes"] = totalSize

	sourceFiles := make([]string, len(objects))
	for i, obj := range objects {
		sourceFiles[i] = obj.Key
	}
	summary["source_files"] = sourceFiles

	obs.Info("scan completed", obs.Field{
		"job_id":        job.ID,
		"dataset_id":    job.DatasetID,
		"summary_keys": len(summary),
		"file_count":    len(objects),
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

	isConcatMerge := mergePayload.Strategy == StrategyByteRange || mergePayload.Strategy == StrategyFilePerChunk

	if !isConcatMerge {
		if mergePayload.AffinityDeviceID == "" && mergePayload.DeviceOutput == nil && !mergePayload.IsPartial {
			return errors.New("missing affinity_device_id or device_output")
		}
	}
	if len(mergePayload.Chunks) == 0 && !mergePayload.IsPartial {
		return errors.New("no chunks provided")
	}

	var backend storage.StorageBackend

	// Per-job storage config override: if job has a storage_config_id with a remote
	// storage mode (S3/GCS/Azure), fetch the config and create a dedicated backend.
	if mergePayload.StorageConfigID != "" {
		cfg, err := storage.GetConfigByID(mergePayload.StorageConfigID)
		if err != nil {
			return fmt.Errorf("failed to get storage config %s: %w", mergePayload.StorageConfigID, err)
		}
		obs.Info("executeMergeDataset: got storage config", obs.Field{
			"storage_mode":      cfg.StorageMode,
			"provider":          cfg.Provider,
			"bucket_name":       cfg.BucketName,
			"endpoint":          cfg.Endpoint,
			"storage_config_id": mergePayload.StorageConfigID,
		})
		// Determine storage mode from payload or config
		mode := string(mergePayload.Strategy)
		if mode == "" {
			mode = mergePayload.StorageMode
		}
		if mode == "" {
			mode = cfg.StorageMode
		}
		cfgCopy := &storage.StorageConfig{
			StorageMode:   mode,
			Provider:      cfg.Provider,
			BucketName:    cfg.BucketName,
			Region:        cfg.Region,
			Endpoint:      cfg.Endpoint,
			MountBasePath: cfg.MountBasePath,
			Credentials:   cfg.Credentials,
		}
		newBackend, err := storage.NewBackend(cfgCopy)
		if err != nil {
			return fmt.Errorf("failed to initialize storage backend for %s: %w", mergePayload.StorageConfigID, err)
		}
		backend = newBackend
		obs.Info("executeMergeDataset: created backend from storage_config_id", obs.Field{
			"backend_type":      fmt.Sprintf("%T", backend),
			"storage_config_id": mergePayload.StorageConfigID,
		})
	}

	if backend == nil {
		backend = GetStorageBackend()
		if backend == nil {
			return errors.New("storage backend not initialized")
		}
	}

	// ── Byte-range / file-per-chunk concatenation merge (Phase 2) ──
	if isConcatMerge {
		return executeConcatMerge(ctx, &mergePayload, backend)
	}

	// Try strategy first, then fall back to storage_mode (nested payload)
	storageMode := string(mergePayload.Strategy)
	if storageMode == "" {
		storageMode = mergePayload.StorageMode
	}
	if storageMode == "" {
		var nested struct {
			StorageMode string `json:"storage_mode"`
		}
		if job.Payload != nil {
			json.Unmarshal(job.Payload, &nested)
			storageMode = nested.StorageMode
		}
	}

	// Normalize storage mode: if the backend is a SharedMountBackend,
	// force local paths regardless of what payload says (same logic as process handler)
	if storageMode != "shared_mount" {
		if _, isSharedMount := backend.(*storage.SharedMountBackend); isSharedMount {
			storageMode = "shared_mount"
		}
	}

	useS3Merge := backend != nil && storageMode != "shared_mount" && storageMode != "affinity"

	var chunks []dataset.ChunkInfo

	if !useS3Merge {
		// Local-file strategies (shared_mount / affinity): validate chunk files exist
		chunks = make([]dataset.ChunkInfo, 0, len(mergePayload.Chunks))
		for _, chunk := range mergePayload.Chunks {
			resolvedPath := chunk.Path

			// Normalize strategy: if empty, fall back to storage_mode
			mergeStrategy := mergePayload.Strategy
			if mergeStrategy == "" && mergePayload.StorageMode == "shared_mount" {
				mergeStrategy = StrategySharedMount
			}

			switch mergeStrategy {
			case StrategyAffinity:
				if !filepath.IsAbs(resolvedPath) {
					return fmt.Errorf("affinity strategy requires absolute path for chunk %s", chunk.ChunkID)
				}
			case StrategySharedMount:
				if resolvedPath == "" {
					// Derive chunk path from mount path and dataset
					mountPath := mergePayload.MountPath
					if mountPath == "" {
						mountPath = deriveMountBasePath("shared_mount")
					}
					resolvedPath = filepath.Join(mountPath, "datasets", mergePayload.DatasetID, "chunks", fmt.Sprintf("chunk_%d.out", chunk.ChunkIndex))
				} else if !filepath.IsAbs(resolvedPath) && mergePayload.MountPath != "" {
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

		// Clean up chunks and empty prefixes after successful merge
		for _, chunk := range mergePayload.Chunks {
			for _, pathType := range []string{"result", "chunk"} {
				chunkKey := storage.GetRemotePathWithSlug(mergePayload.DatasetID, datasetSlug, chunk.ChunkIndex, pathType)
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
			// Fall back: try Strategy first, then StorageMode
			strategyForMount := string(mergePayload.Strategy)
			if strategyForMount == "" {
				strategyForMount = mergePayload.StorageMode
			}
			mountPath = deriveMountBasePath(strategyForMount)
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

// ── Byte-range / file-per-chunk concatenation merge (Phase 2) ──

func executeConcatMerge(ctx context.Context, mergePayload *MergePayload, backend storage.StorageBackend) error {
	datasetSlug := mergePayload.DatasetSlug
	useS3 := backend != nil

	if useS3 {
		mergedKey := storage.GetRemotePathWithSlug(mergePayload.DatasetID, datasetSlug, 0, "merged")

		resultReaders := make([]io.Reader, 0, len(mergePayload.Chunks))
		for _, chunk := range mergePayload.Chunks {
			chunkKey := storage.GetRemotePathWithSlug(mergePayload.DatasetID, datasetSlug, chunk.ChunkIndex, "result")
			objReader, err := backend.ReadObject(ctx, chunkKey)
			if err != nil {
				return fmt.Errorf("failed to read chunk %s for concat merge: %w", chunk.ChunkID, err)
			}
			defer objReader.Close()
			resultReaders = append(resultReaders, objReader)
		}

		mergedReader := io.MultiReader(resultReaders...)
		if err := backend.WriteObject(ctx, mergedKey, mergedReader); err != nil {
			return fmt.Errorf("failed to write concatenated result: %w", err)
		}

		// Clean up chunks
		for _, chunk := range mergePayload.Chunks {
			for _, pathType := range []string{"result", "chunk"} {
				chunkKey := storage.GetRemotePathWithSlug(mergePayload.DatasetID, datasetSlug, chunk.ChunkIndex, pathType)
				if err := backend.DeleteObject(ctx, chunkKey); err != nil {
					obs.Warn("failed to delete chunk after concat merge", obs.Field{
						"chunk_key": chunkKey,
						"error":     err.Error(),
					})
				}
			}
		}

		return nil
	}

	// Local concat merge
	mountPath := deriveMountBasePath(string(mergePayload.Strategy))
	mergedPath := filepath.Join(mountPath, "datasets", mergePayload.DatasetID, "merged")

	if err := os.MkdirAll(filepath.Dir(mergedPath), 0755); err != nil {
		return fmt.Errorf("failed to create merged output dir: %w", err)
	}

	mergedFile, err := os.Create(mergedPath)
	if err != nil {
		return fmt.Errorf("failed to create merged file: %w", err)
	}
	defer mergedFile.Close()

	for _, chunk := range mergePayload.Chunks {
		chunkPath := filepath.Join(mountPath, "datasets", mergePayload.DatasetID, "chunks", fmt.Sprintf("chunk_%d.out", chunk.ChunkIndex))
		chunkFile, err := os.Open(chunkPath)
		if err != nil {
			if os.IsNotExist(err) {
				obs.Warn("chunk file not found during concat merge, skipping", obs.Field{
					"chunk_path": chunkPath,
					"chunk_id":   chunk.ChunkID,
				})
				continue
			}
			return fmt.Errorf("failed to open chunk %s for concat merge: %w", chunk.ChunkID, err)
		}
		if _, err := io.Copy(mergedFile, chunkFile); err != nil {
			chunkFile.Close()
			return fmt.Errorf("failed to append chunk %s: %w", chunk.ChunkID, err)
		}
		chunkFile.Close()

		// Clean up chunk result
		os.Remove(chunkPath)
	}

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
	PluginID    string          `json:"plugin_id,omitempty"`
	Rows        int             `json:"rows,omitempty"`
	Checksum    string          `json:"checksum,omitempty"`
	Config      json.RawMessage `json:"config,omitempty"`
	MockMode    bool            `json:"mock_mode,omitempty"`

	// Chunking metadata
	TotalChunks int    `json:"total_chunks,omitempty"`
	SourcePath  string `json:"source_path,omitempty"`

	// Compound job fields (in nested payload)
	IsCompound       bool            `json:"is_compound,omitempty"`
	Steps            []CompoundStep  `json:"steps,omitempty"`
	TotalSteps       int             `json:"total_steps,omitempty"`
	CurrentStepIndex int             `json:"current_step_index,omitempty"`

	// File/Byte-Range chunking strategy (Phase 2)
	ChunkStrategy   string   `json:"chunk_strategy,omitempty"`
	ByteRangeStart  int64    `json:"byte_range_start,omitempty"`
	ByteRangeEnd    int64    `json:"byte_range_end,omitempty"`
	FileList        []string `json:"file_list,omitempty"`
	SourceFilePath  string   `json:"source_file_path,omitempty"`
}

type CompoundStep struct {
	StepIndex int             `json:"step_index"`
	PluginID  string          `json:"plugin_id"`
	Config    json.RawMessage `json:"config"`
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

	// Some fields are nested inside the "payload" JSONB column — extract them as fallback
	var nestedPayload ProcessPayload
	if job.Payload != nil {
		json.Unmarshal(job.Payload, &nestedPayload)
	}

	// Fill in top-level fields from nested payload if missing
	if processPayload.ChunkID == "" {
		processPayload.ChunkID = nestedPayload.ChunkID
	}
	if processPayload.DatasetID == "" {
		processPayload.DatasetID = nestedPayload.DatasetID
	}
	if processPayload.DatasetSlug == "" {
		processPayload.DatasetSlug = nestedPayload.DatasetSlug
	}
	if processPayload.ChunkIndex == 0 && nestedPayload.ChunkIndex != 0 {
		processPayload.ChunkIndex = nestedPayload.ChunkIndex
	}
	if processPayload.StorageMode == "" {
		processPayload.StorageMode = nestedPayload.StorageMode
	}
	if processPayload.Rows == 0 && nestedPayload.Rows != 0 {
		processPayload.Rows = nestedPayload.Rows
	}
	if processPayload.Checksum == "" {
		processPayload.Checksum = nestedPayload.Checksum
	}
	if processPayload.PluginID == "" {
		processPayload.PluginID = nestedPayload.PluginID
	}
	if len(processPayload.Config) == 0 && len(nestedPayload.Config) > 0 {
		processPayload.Config = nestedPayload.Config
	}

	if processPayload.ChunkStrategy == "" {
		processPayload.ChunkStrategy = nestedPayload.ChunkStrategy
	}
	if processPayload.ByteRangeStart == 0 && nestedPayload.ByteRangeStart != 0 {
		processPayload.ByteRangeStart = nestedPayload.ByteRangeStart
	}
	if processPayload.ByteRangeEnd == 0 && nestedPayload.ByteRangeEnd != 0 {
		processPayload.ByteRangeEnd = nestedPayload.ByteRangeEnd
	}
	if len(processPayload.FileList) == 0 && len(nestedPayload.FileList) > 0 {
		processPayload.FileList = nestedPayload.FileList
	}
	if processPayload.SourceFilePath == "" {
		processPayload.SourceFilePath = nestedPayload.SourceFilePath
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
	storageMode = normalizeStorageModeForJob(storageMode)

	var backend storage.StorageBackend

	// Per-job storage config override: if job has a storage_config_id with a remote
	// storage mode (S3/GCS/Azure), fetch the config and create a dedicated backend.
	if job.StorageConfigID != "" && (storageMode == "s3" || storageMode == "aws_s3" || storageMode == "object_storage" || storageMode == "gcs" || storageMode == "google_cloud_storage" || storageMode == "azure_blob") {
		cfg, err := storage.GetConfigByID(job.StorageConfigID)
		if err != nil {
			return fmt.Errorf("failed to get storage config %s: %w", job.StorageConfigID, err)
		}
		obs.Info("executeProcessChunk: got storage config", obs.Field{
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
		obs.Info("executeProcessChunk: created backend from storage_config_id", obs.Field{
			"backend_type":      fmt.Sprintf("%T", backend),
			"storage_config_id": job.StorageConfigID,
		})
	}

	if backend == nil {
		backend = GetStorageBackend()
		if backend == nil {
			return errors.New("storage backend not initialized")
		}
	}

	// Reconcile storage mode with actual backend:
	// if we fell back to the global backend and it reads/writes locally,
	// force local paths (only relevant for global backend fallback).
	if storageMode != "shared_mount" {
		if _, isSharedMount := backend.(*storage.SharedMountBackend); isSharedMount {
			storageMode = "shared_mount"
		}
	}

	datasetID := processPayload.DatasetID
	datasetSlug := processPayload.DatasetSlug
	chunkIndex := processPayload.ChunkIndex

	resultKey := storage.GetRemotePathWithSlug(datasetID, datasetSlug, chunkIndex, "result")

	// ── Compound job branch ──
	if processPayload.IsCompound && len(processPayload.Steps) > 0 {
		return executeCompoundProcessChunk(ctx, &job, &processPayload, &processPayload, datasetID, datasetSlug, chunkIndex, resultKey, backend, storageMode)
	}

	// ── File/Byte-Range strategy dispatch (Phase 2) ──
	switch processPayload.ChunkStrategy {
	case "byte_range":
		return executeProcessChunkByteRange(ctx, &job, &processPayload, backend, storageMode)
	case "file_per_chunk":
		return executeProcessChunkFileList(ctx, &job, &processPayload, backend, storageMode)
	}

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
				// First step: read from chunks, fall back to S3 source if local file missing
				sharedPath := filepath.Join(mountBasePath, "datasets", datasetID, "chunks", fmt.Sprintf("chunk_%d.bin", chunkIndex))
				if fileExists(sharedPath) {
					inputPath = sharedPath
				} else {
					sourcePath := processPayload.SourcePath
					if sourcePath == "" {
						sourcePath = job.SourcePath
					}
					if sourcePath != "" && backend != nil {
						obs.Info("shared_mount chunk not found, falling back to S3 source", obs.Field{
							"shared_path": sharedPath,
							"source_path": sourcePath,
						})
						objReader, readErr := backend.ReadObject(ctx, sourcePath)
						if readErr != nil {
							return fmt.Errorf("step 0: read source from S3 fallback %s: %w", sourcePath, readErr)
						}
						ext := filepath.Ext(sourcePath)
						outPath := filepath.Join(workDir, "source"+ext)
						outFile, createErr := os.Create(outPath)
						if createErr != nil {
							objReader.Close()
							return fmt.Errorf("step 0: create source file from S3 fallback: %w", createErr)
						}
						_, copyErr := io.Copy(outFile, objReader)
						objReader.Close()
						outFile.Close()
						if copyErr != nil {
							return fmt.Errorf("step 0: copy source from S3 fallback: %w", copyErr)
						}
						inputPath = outPath
					} else {
						return fmt.Errorf("shared_mount chunk not found at %s and no source_path for S3 fallback", sharedPath)
					}
				}
			}
		}
		if outputPath == "" && datasetID != "" {
			outputPath = filepath.Join(mountBasePath, "datasets", datasetID, "chunks", fmt.Sprintf("chunk_%d.out", chunkIndex))
		}
		processPayload.InputPath = inputPath
		processPayload.OutputPath = outputPath
	} else {
		// S3/object storage: download input based on step index
		if stepIndex > 0 {
			objReader, err := backend.ReadObject(ctx, resultKey)
			if err != nil {
				return fmt.Errorf("failed to read result object: %w", err)
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
		} else {
			// Step 0: read source files
			if processPayload.SourcePath != "" {
				if strings.HasSuffix(processPayload.SourcePath, "/") {
					// Directory listing mode: list objects and download all
					obs.Info("compound: using dataset lookup for source path (production optimization)", obs.Field{
						"source_path": processPayload.SourcePath,
					})
					objects, listErr := backend.ListObjects(ctx, processPayload.SourcePath)
					if listErr != nil {
						return fmt.Errorf("failed to list source directory %s: %w", processPayload.SourcePath, listErr)
					}
					if len(objects) == 0 {
						return fmt.Errorf("no files found at %s", processPayload.SourcePath)
					}
					// Extract file_pattern from config
					filePattern := ""
					var configMap map[string]interface{}
					if len(processPayload.Config) > 0 {
						if err := json.Unmarshal(processPayload.Config, &configMap); err == nil {
							if fp, ok := configMap["file_pattern"].(string); ok {
								filePattern = fp
							}
						}
					}
					filesDir := filepath.Join(workDir, "files")
					if err := os.MkdirAll(filesDir, 0755); err != nil {
						return fmt.Errorf("create files dir: %w", err)
					}
					for _, obj := range objects {
						if strings.HasSuffix(obj.Key, "/") {
							continue
						}
						if filePattern != "" {
							matched, matchErr := filepath.Match(filePattern, filepath.Base(obj.Key))
							if matchErr != nil || !matched {
								continue
							}
						}
						objReader, readErr := backend.ReadObject(ctx, obj.Key)
						if readErr != nil {
							return fmt.Errorf("read source file %s: %w", obj.Key, readErr)
						}
						dst := filepath.Join(filesDir, filepath.Base(obj.Key))
						dstFile, createErr := os.Create(dst)
						if createErr != nil {
							objReader.Close()
							return fmt.Errorf("create file %s: %w", dst, createErr)
						}
						_, copyErr := io.Copy(dstFile, objReader)
						objReader.Close()
						dstFile.Close()
						if copyErr != nil {
							return fmt.Errorf("copy file %s: %w", dst, copyErr)
						}
					}
					inputPath = filesDir
				} else {
					// Single root-level file from source_path
					objReader, err := backend.ReadObject(ctx, processPayload.SourcePath)
					if err != nil {
						return fmt.Errorf("failed to read source %s: %w", processPayload.SourcePath, err)
					}
					ext := filepath.Ext(processPayload.SourcePath)
					outPath := filepath.Join(workDir, "source"+ext)
					outFile, createErr := os.Create(outPath)
					if createErr != nil {
						objReader.Close()
						return fmt.Errorf("failed to create source file: %w", createErr)
					}
					_, copyErr := io.Copy(outFile, objReader)
					objReader.Close()
					outFile.Close()
					if copyErr != nil {
						return fmt.Errorf("failed to write source file: %w", copyErr)
					}
					inputPath = outPath
				}
			} else {
				return fmt.Errorf("source_path is required for S3 process jobs (no /source/ prefix fallback)")
			}
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

	// Resolve plugin name: try top-level fields first, then fall back to nested payload
	pluginID := job.PluginID
	if pluginID == "" {
		pluginID = processPayload.PluginID
	}
	if pluginID == "" {
		pluginID = nestedPayload.PluginID
	}

	pluginName := ResolvePluginName(ctx, pluginID, job.PluginName)
	if pluginName == "" {
		obs.Warn("no plugin name or ID in job payload — agent cannot load plugin", obs.Field{
			"job_id":    job.ID,
			"plugin_id": pluginID,
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
		// Check if the plugin still managed to write a real output file.
		// NOTE: os.Stat on an empty directory returns Size()==4096 (inode
		// size), so we must actually verify content with hasRealOutput().
		if hasRealOutput(outputPath) {
			obs.Info("native sandbox returned error but output file exists, treating as success",
				obs.Field{"plugin": manifest.Name, "output_path": outputPath})
			execResult = &plugin.ExecutionResult{Output: ""}
			err = nil
		} else {
			errStr := err.Error()
			isDockerErr := strings.Contains(errStr, "Docker") || strings.Contains(errStr, "docker")
			if isDockerErr {
				obs.Info("falling back to v2 runtime", obs.Field{
					"plugin":       manifest.Name,
					"language":     manifest.Language,
					"docker_error": isDockerErr,
					"error":        errStr,
				})
				execResult, err = fallbackToV2Runtime(ctx, pluginPath, manifest, &processPayload, &job)
				if err != nil {
					return fmt.Errorf("process plugin failed (native+v2 fallback): %w", err)
				}
			} else {
				return fmt.Errorf("process plugin failed: %w", err)
			}
		}
	}

	var result map[string]any
	if execResult != nil && execResult.Output != "" {
		result, err = parsePluginOutputAsJSON(execResult.Output)
		if err != nil {
			return fmt.Errorf("failed to parse process plugin output: %w", err)
		}
	}

	// Ensure the output file exists on disk
	if _, statErr := os.Stat(outputPath); os.IsNotExist(statErr) {
		var actualPath string
		if data, ok := result["data"].(map[string]any); ok {
			if p, ok := data["output_path"].(string); ok && p != "" {
				actualPath = p
			}
		}
		if actualPath != "" {
			if info, err := os.Stat(actualPath); err == nil && !info.IsDir() {
				os.MkdirAll(filepath.Dir(outputPath), 0755)
				if err := os.Rename(actualPath, outputPath); err != nil {
					obs.Warn("rename fallback: copying instead", obs.Field{"from": actualPath, "to": outputPath, "error": err.Error()})
					if copyErr := copyFile(actualPath, outputPath); copyErr != nil {
						obs.Warn("copy fallback also failed", obs.Field{"error": copyErr.Error()})
					}
				}
			}
		}
		if _, err := os.Stat(outputPath); os.IsNotExist(err) && execResult != nil && execResult.Output != "" {
			os.MkdirAll(filepath.Dir(outputPath), 0755)
			os.WriteFile(outputPath, []byte(execResult.Output), 0644)
			obs.Info("created output file from plugin stdout", obs.Field{"output_path": outputPath})
		}
	}

	if storageMode != "shared_mount" {
		if _, err := os.Stat(outputPath); os.IsNotExist(err) && execResult != nil && execResult.Output != "" {
			if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err == nil {
				os.WriteFile(outputPath, []byte(execResult.Output), 0644)
			}
		}
		// Upload to S3 for all steps (compound jobs chain locally, but separate steps need S3)
		outputFile, err := os.Open(outputPath)
		if err != nil {
			return fmt.Errorf("failed to open output file: %w", err)
		}

		if err := backend.WriteObject(ctx, resultKey, outputFile); err != nil {
			outputFile.Close()
			return fmt.Errorf("failed to write result: %w", err)
		}
		outputFile.Close()
		obs.Info("chunk result uploaded to S3", obs.Field{
			"chunk_id":   processPayload.ChunkID,
			"result_key": resultKey,
		})
	}

	obs.Info("chunk processed", obs.Field{
		"job_id":      pluginCtx.JobID,
		"chunk_id":    pluginCtx.ChunkID,
		"output_path": pluginCtx.OutputPath,
	})

	return nil
}

// ── Byte-range chunk handler (Phase 2) ──

func executeProcessChunkByteRange(ctx context.Context, job *Job, payload *ProcessPayload, backend storage.StorageBackend, storageMode string) error {
	workDir, cleanup, err := plugin.PrepareJobWorkDir(job.ID)
	if err != nil {
		return fmt.Errorf("failed to prepare work dir: %w", err)
	}
	defer cleanup()

	datasetID := payload.DatasetID
	datasetSlug := payload.DatasetSlug
	chunkIndex := payload.ChunkIndex
	sourceFilePath := payload.SourceFilePath
	byteStart := payload.ByteRangeStart
	byteEnd := payload.ByteRangeEnd

	if sourceFilePath == "" {
		return errors.New("byte_range chunk requires source_file_path")
	}

	resultKey := storage.GetRemotePathWithSlug(datasetID, datasetSlug, chunkIndex, "result")

	// Derive input file path
	var inputPath string
	if storageMode == "shared_mount" {
		mountBasePath := deriveMountBasePath(storageMode)
		fullSourcePath := sourceFilePath
		if !filepath.IsAbs(fullSourcePath) {
			fullSourcePath = filepath.Join(mountBasePath, fullSourcePath)
		}
		f, err := os.Open(fullSourcePath)
		if err != nil {
			return fmt.Errorf("failed to open source file for byte range: %w", err)
		}
		defer f.Close()

		inputPath = filepath.Join(workDir, fmt.Sprintf("chunk_%d.bin", chunkIndex))
		out, err := os.Create(inputPath)
		if err != nil {
			return fmt.Errorf("failed to create input file: %w", err)
		}
		defer out.Close()

		sr := io.NewSectionReader(f, byteStart, byteEnd-byteStart)
		if _, err := io.Copy(out, sr); err != nil {
			return fmt.Errorf("failed to copy byte range: %w", err)
		}
	} else {
		// S3: download the source file and extract byte range
		objReader, err := backend.ReadObject(ctx, sourceFilePath)
		if err != nil {
			return fmt.Errorf("failed to read source from backend: %w", err)
		}
		defer objReader.Close()

		inputPath = filepath.Join(workDir, "source.bin")
		f, err := os.Create(inputPath)
		if err != nil {
			return fmt.Errorf("failed to create input file: %w", err)
		}
		defer f.Close()

		if byteStart > 0 {
			if _, err := io.CopyN(io.Discard, objReader, byteStart); err != nil {
				return fmt.Errorf("failed to skip to byte start: %w", err)
			}
		}

		written, err := io.CopyN(f, objReader, byteEnd-byteStart)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read byte range from source: %w", err)
		}
		if written == 0 {
			return fmt.Errorf("read 0 bytes from byte range %d-%d", byteStart, byteEnd)
		}
	}

	outputPath := filepath.Join(workDir, fmt.Sprintf("chunk_%d.out", chunkIndex))

	pluginID := job.PluginID
	if pluginID == "" {
		pluginID = payload.PluginID
	}
	pluginName := ResolvePluginName(ctx, pluginID, job.PluginName)
	if pluginName == "" {
		return fmt.Errorf("no plugin_id in job payload")
	}

	pluginPath, manifest, err := plugin.LoadAndUpdatePlugin(ctx, pluginName)
	if err != nil {
		return err
	}

	effectiveConfig := job.StepConfig
	if len(payload.Config) > 0 {
		effectiveConfig = payload.Config
	}

	pluginCtx := PluginContext{
		JobID:       job.ID,
		OrgID:       job.OrgID,
		DatasetID:   datasetID,
		ExecutionID: job.ExecutionID,
		StepIndex:   job.StepIndex,
		ChunkID:     payload.ChunkID,
		ChunkIndex:  chunkIndex,
		InputPath:   inputPath,
		OutputPath:  outputPath,
		Config:      effectiveConfig,
	}

	inJSON, _ := json.Marshal(pluginCtx)
	env := system.DetectExecutionEnv()
	execResult, err := plugin.Execute(ctx, pluginPath, manifest, string(inJSON), env, NativeRunnerFunc())
	if err != nil {
		// Check if the plugin still managed to write a real output file.
		// NOTE: os.Stat on an empty directory returns Size()==4096 (inode
		// size), so we must actually verify content with hasRealOutput().
		if hasRealOutput(outputPath) {
			obs.Info("native sandbox returned error but output file exists, treating as success",
				obs.Field{"plugin": manifest.Name, "output_path": outputPath})
			execResult = &plugin.ExecutionResult{Output: ""}
			err = nil
		} else {
			errStr := err.Error()
			isDockerErr := strings.Contains(errStr, "Docker") || strings.Contains(errStr, "docker")
			if isDockerErr {
				execResult, err = fallbackToV2Runtime(ctx, pluginPath, manifest, payload, job)
				if err != nil {
					return fmt.Errorf("process plugin failed (native+v2 fallback): %w", err)
				}
			} else {
				return fmt.Errorf("process plugin failed: %w", err)
			}
		}
	}

	var result map[string]any
	if execResult != nil && execResult.Output != "" {
		result, _ = parsePluginOutputAsJSON(execResult.Output)
	}

	if _, statErr := os.Stat(outputPath); os.IsNotExist(statErr) {
		var actualPath string
		if data, ok := result["data"].(map[string]any); ok {
			if p, ok := data["output_path"].(string); ok && p != "" {
				actualPath = p
			}
		}
		if actualPath != "" {
			if info, err := os.Stat(actualPath); err == nil && !info.IsDir() {
				os.MkdirAll(filepath.Dir(outputPath), 0755)
				if err := os.Rename(actualPath, outputPath); err != nil {
					if copyErr := copyFile(actualPath, outputPath); copyErr != nil {
						return fmt.Errorf("failed to copy output to expected path: %w", copyErr)
					}
				}
			}
		}
		if _, err := os.Stat(outputPath); os.IsNotExist(err) && execResult != nil && execResult.Output != "" {
			os.MkdirAll(filepath.Dir(outputPath), 0755)
			os.WriteFile(outputPath, []byte(execResult.Output), 0644)
		}
	}

	if storageMode != "shared_mount" {
		if _, err := os.Stat(outputPath); os.IsNotExist(err) && execResult != nil && execResult.Output != "" {
			os.MkdirAll(filepath.Dir(outputPath), 0755)
			os.WriteFile(outputPath, []byte(execResult.Output), 0644)
		}
		outFile, err := os.Open(outputPath)
		if err != nil {
			return fmt.Errorf("failed to open output file for upload: %w", err)
		}
		defer outFile.Close()
		if err := backend.WriteObject(ctx, resultKey, outFile); err != nil {
			return fmt.Errorf("failed to write result to backend: %w", err)
		}
	}

	return nil
}

// ── File-per-chunk handler (Phase 2) ──

func executeProcessChunkFileList(ctx context.Context, job *Job, payload *ProcessPayload, backend storage.StorageBackend, storageMode string) error {
	workDir, cleanup, err := plugin.PrepareJobWorkDir(job.ID)
	if err != nil {
		return fmt.Errorf("failed to prepare work dir: %w", err)
	}
	defer cleanup()

	datasetID := payload.DatasetID
	datasetSlug := payload.DatasetSlug
	chunkIndex := payload.ChunkIndex
	fileList := payload.FileList

	if len(fileList) == 0 {
		return errors.New("file_list chunk requires at least one file")
	}

	resultKey := storage.GetRemotePathWithSlug(datasetID, datasetSlug, chunkIndex, "result")

	// Copy each file to a subdirectory in the workdir
	chunkDir := filepath.Join(workDir, "files")
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		return fmt.Errorf("failed to create chunk file dir: %w", err)
	}

	if storageMode == "shared_mount" {
		mountBasePath := deriveMountBasePath(storageMode)
		for _, f := range fileList {
			src := f
			if !filepath.IsAbs(src) {
				src = filepath.Join(mountBasePath, src)
			}
			dst := filepath.Join(chunkDir, filepath.Base(src))
			if err := copyFile(src, dst); err != nil {
				return fmt.Errorf("failed to copy file %s: %w", f, err)
			}
		}
	} else {
		for _, f := range fileList {
			objReader, err := backend.ReadObject(ctx, f)
			if err != nil {
				return fmt.Errorf("failed to read file %s from backend: %w", f, err)
			}
			dst := filepath.Join(chunkDir, filepath.Base(f))
			out, createErr := os.Create(dst)
			if createErr != nil {
				objReader.Close()
				return fmt.Errorf("failed to create file %s: %w", dst, createErr)
			}
			_, copyErr := io.Copy(out, objReader)
			objReader.Close()
			out.Close()
			if copyErr != nil {
				return fmt.Errorf("failed to write file %s: %w", dst, copyErr)
			}
		}
	}

	inputPath := chunkDir
	outputPath := filepath.Join(workDir, fmt.Sprintf("chunk_%d.out", chunkIndex))

	pluginID := job.PluginID
	if pluginID == "" {
		pluginID = payload.PluginID
	}
	pluginName := ResolvePluginName(ctx, pluginID, job.PluginName)
	if pluginName == "" {
		return fmt.Errorf("no plugin_id in job payload")
	}

	pluginPath, manifest, err := plugin.LoadAndUpdatePlugin(ctx, pluginName)
	if err != nil {
		return err
	}

	effectiveConfig := job.StepConfig
	if len(payload.Config) > 0 {
		effectiveConfig = payload.Config
	}

	pluginCtx := PluginContext{
		JobID:       job.ID,
		OrgID:       job.OrgID,
		DatasetID:   datasetID,
		ExecutionID: job.ExecutionID,
		StepIndex:   job.StepIndex,
		ChunkID:     payload.ChunkID,
		ChunkIndex:  chunkIndex,
		InputPath:   inputPath,
		OutputPath:  outputPath,
		Config:      effectiveConfig,
	}

	inJSON, _ := json.Marshal(pluginCtx)
	env := system.DetectExecutionEnv()
	execResult, err := plugin.Execute(ctx, pluginPath, manifest, string(inJSON), env, NativeRunnerFunc())
	if err != nil {
		// Check if the plugin still managed to write a real output file.
		// NOTE: os.Stat on an empty directory returns Size()==4096 (inode
		// size), so we must actually verify content with hasRealOutput().
		if hasRealOutput(outputPath) {
			obs.Info("native sandbox returned error but output file exists, treating as success",
				obs.Field{"plugin": manifest.Name, "output_path": outputPath})
			execResult = &plugin.ExecutionResult{Output: ""}
			err = nil
		} else {
			errStr := err.Error()
			isDockerErr := strings.Contains(errStr, "Docker") || strings.Contains(errStr, "docker")
			if isDockerErr {
				execResult, err = fallbackToV2Runtime(ctx, pluginPath, manifest, payload, job)
				if err != nil {
					return fmt.Errorf("process plugin failed (native+v2 fallback): %w", err)
				}
			} else {
				return fmt.Errorf("process plugin failed: %w", err)
			}
		}
	}

	var result map[string]any
	if execResult != nil && execResult.Output != "" {
		result, _ = parsePluginOutputAsJSON(execResult.Output)
	}

	if _, statErr := os.Stat(outputPath); os.IsNotExist(statErr) {
		var actualPath string
		if data, ok := result["data"].(map[string]any); ok {
			if p, ok := data["output_path"].(string); ok && p != "" {
				actualPath = p
			}
		}
		if actualPath != "" {
			if info, err := os.Stat(actualPath); err == nil && !info.IsDir() {
				os.MkdirAll(filepath.Dir(outputPath), 0755)
				if err := os.Rename(actualPath, outputPath); err != nil {
					if copyErr := copyFile(actualPath, outputPath); copyErr != nil {
						return fmt.Errorf("failed to copy output to expected path: %w", copyErr)
					}
				}
			}
		}
		if _, err := os.Stat(outputPath); os.IsNotExist(err) && execResult != nil && execResult.Output != "" {
			os.MkdirAll(filepath.Dir(outputPath), 0755)
			os.WriteFile(outputPath, []byte(execResult.Output), 0644)
		}
	}

	if storageMode != "shared_mount" {
		if _, err := os.Stat(outputPath); os.IsNotExist(err) && execResult != nil && execResult.Output != "" {
			os.MkdirAll(filepath.Dir(outputPath), 0755)
			os.WriteFile(outputPath, []byte(execResult.Output), 0644)
		}
		outFile, err := os.Open(outputPath)
		if err != nil {
			return fmt.Errorf("failed to open output file for upload: %w", err)
		}
		defer outFile.Close()
		if err := backend.WriteObject(ctx, resultKey, outFile); err != nil {
			return fmt.Errorf("failed to write result to backend: %w", err)
		}
	}

	return nil
}

// ── Compound job helpers ──

func getCompoundCacheDir(executionID, chunkID string) string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "sentra-cache", executionID, chunkID)
	}
	return filepath.Join(homeDir, ".sentra", "cache", executionID, chunkID)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// hasRealOutput checks that path contains actual output data:
//   - If a regular file, it must have content (size > 0).
//   - If a directory (compound step output), it must contain at least one file.
//     This avoids the trap where os.Stat on an empty directory returns
//     fi.Size()==4096 (dir inode size), falsely implying success.
func hasRealOutput(outputPath string) bool {
	fi, statErr := os.Stat(outputPath)
	if statErr != nil {
		return false
	}
	if !fi.IsDir() {
		return fi.Size() > 0
	}
	entries, rdErr := os.ReadDir(outputPath)
	if rdErr != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() {
			return true
		}
	}
	return false
}

func reportStepProgress(ctx context.Context, jobID, executionID, chunkID string, stepIndex int, status, errorMsg string) {
	deviceToken, err := auth.GetToken()
	if err != nil {
		obs.Warn("reportStepProgress skipped: no token", obs.Field{"error": err.Error()})
		return
	}
	data := map[string]any{
		"type":         "step_progress",
		"job_id":       jobID,
		"execution_id": executionID,
		"chunk_id":     chunkID,
		"step_index":   stepIndex,
		"step_status":  status,
	}
	if errorMsg != "" {
		data["error"] = errorMsg
	}
	body, _ := json.Marshal(map[string]any{
		"channel": "agent-" + getAgentID(),
		"data":    data,
	})
	httpc := httpclient.NewClient(supabaseBaseURL, supabaseAnonKey, deviceToken)
	resp, err := httpc.DoWithReq(ctx, "POST", "/functions/v1/relay_job_event", body, func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+supabaseAnonKey)
		r.Header.Set("x-agent-token", deviceToken)
	})
	if err != nil {
		obs.Warn("reportStepProgress HTTP failed", obs.Field{"error": err.Error()})
		return
	}
	resp.Body.Close()
}

func executePluginStep(ctx context.Context, job *Job, processPayload *ProcessPayload, pluginID string, config json.RawMessage, stepIndex int, inputPath, outputPath string, previousData map[string]interface{}) (map[string]interface{}, error) {
	// Deprecated: use executePluginStepEx which accepts a separate outputDir for
	// compound mode. This wrapper preserves backward compat for any hypothetical
	// caller that passes a file path as outputPath.
	return executePluginStepEx(ctx, job, processPayload, pluginID, config, stepIndex, inputPath, outputPath, outputPath, previousData)
}

// executePluginStepEx executes a plugin step with separate output path (for
// stdout fallback / final result file) and output dir (for file-based plugins
// that write multiple files to a directory, e.g. image processing in compound
// mode). In per-step non-compound mode both point to the same file path.
func executePluginStepEx(ctx context.Context, job *Job, processPayload *ProcessPayload, pluginID string, config json.RawMessage, stepIndex int, inputPath, outputPath, outputDir string, previousData map[string]interface{}) (map[string]interface{}, error) {
	pluginName := ResolvePluginName(ctx, pluginID, "")
	if pluginName == "" {
		return nil, fmt.Errorf("no plugin name for ID %s", pluginID)
	}

	pluginPath, manifest, err := plugin.LoadAndUpdatePlugin(ctx, pluginName)
	if err != nil {
		return nil, fmt.Errorf("load plugin %s: %w", pluginName, err)
	}

	// Build the plugin input in PluginContext canonical format:
	// all fields at top level matching PluginContext JSON tags exactly,
	// so that existing plugins see "input_path", "output_path", "config"
	// in the same positions as in per-step (non-compound) mode.
	// output_dir and previous_data are extra top-level fields that
	// file-based plugins can optionally consume.
	pluginInput := map[string]interface{}{
		"job_id":        job.ID,
		"org_id":        job.OrgID,
		"dataset_id":    processPayload.DatasetID,
		"execution_id":  job.ExecutionID,
		"step_index":    stepIndex,
		"chunk_id":      processPayload.ChunkID,
		"chunk_index":   processPayload.ChunkIndex,
		"input_path":    inputPath,
		"output_path":   outputPath,
		"output_dir":    outputDir,
		"previous_data": previousData,
	}
	if len(config) > 0 {
		var cfg map[string]interface{}
		if json.Unmarshal(config, &cfg) == nil {
			pluginInput["config"] = cfg
		}
	} else {
		pluginInput["config"] = nil
	}
	inJSON, _ := json.Marshal(pluginInput)
	env := system.DetectExecutionEnv()
	execResult, err := plugin.Execute(ctx, pluginPath, manifest, string(inJSON), env, NativeRunnerFunc())
	if err != nil {
		// Check if the plugin still managed to write a real output file.
		// NOTE: os.Stat on an empty directory returns fi.Size()==4096 (inode
		// size), so we must actually verify content with hasRealOutput().
		if hasRealOutput(outputPath) {
			obs.Info("native sandbox returned error but output file exists, treating as success",
				obs.Field{"plugin": manifest.Name, "output_path": outputPath})
			execResult = &plugin.ExecutionResult{Output: ""}
			err = nil
		} else {
			errStr := err.Error()
			isCgroupErr := strings.Contains(errStr, "cgroup:")
			isDockerErr := strings.Contains(errStr, "Docker") || strings.Contains(errStr, "docker")

			if isCgroupErr {
				obs.Warn("cgroup resource limits unavailable, retrying without limits",
					obs.Field{"plugin": pluginName, "error": errStr})
				execResult, err = plugin.Execute(ctx, pluginPath, manifest, string(inJSON), env, NativeRunnerFunc())
				if err != nil {
					// Check output file again after retry
					if hasRealOutput(outputPath) {
						obs.Info("cgroup retry returned error but output file exists, treating as success",
							obs.Field{"plugin": pluginName, "output_path": outputPath})
						execResult = &plugin.ExecutionResult{Output: ""}
						err = nil
					} else {
						return nil, fmt.Errorf("step %d plugin failed: %w", stepIndex, err)
					}
				}
			} else if isDockerErr {
				processPayload.InputPath = inputPath
				processPayload.OutputPath = outputPath
				execResult, err = fallbackToV2Runtime(ctx, pluginPath, manifest, processPayload, job)
				if err != nil {
					return nil, fmt.Errorf("step %d plugin failed (native+v2): %w", stepIndex, err)
				}
			} else {
				return nil, fmt.Errorf("step %d plugin failed: %w", stepIndex, err)
			}
		}
	}

	var result map[string]any
	if execResult != nil && execResult.Output != "" {
		result, _ = parsePluginOutputAsJSON(execResult.Output)
	}

	if _, err := os.Stat(outputPath); os.IsNotExist(err) && execResult != nil && execResult.Output != "" {
		os.MkdirAll(filepath.Dir(outputPath), 0755)
		os.WriteFile(outputPath, []byte(execResult.Output), 0644)
	}

	return result, nil
}

func executeCompoundProcessChunk(
	ctx context.Context,
	job *Job,
	processPayload *ProcessPayload,
	nestedPayload *ProcessPayload,
	datasetID, datasetSlug string,
	chunkIndex int,
	resultKey string,
	backend storage.StorageBackend,
	storageMode string,
) error {
	chunkID := processPayload.ChunkID
	if chunkID == "" {
		chunkID = nestedPayload.ChunkID
	}
	executionID := job.ExecutionID

	steps := processPayload.Steps
	startStep := processPayload.CurrentStepIndex
	lastStep := len(steps) - 1

	cacheDir := getCompoundCacheDir(executionID, chunkID)
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}

	workDirID := job.ID
	if workDirID == "" {
		workDirID = chunkID
	}
	workDir, cleanup, err := plugin.PrepareJobWorkDir(workDirID)
	if err != nil {
		return fmt.Errorf("prepare work dir: %w", err)
	}
	defer cleanup()

	var pluginErr error
	var lastOutputPath string
	var previousData map[string]interface{}

	mountBasePath := deriveMountBasePath(storageMode)

	// If file_list is present, download source files once and reuse for all steps
	var filesDir string
	if len(processPayload.FileList) > 0 {
		filesDir = filepath.Join(workDir, "files")
		if err := os.MkdirAll(filesDir, 0755); err != nil {
			return fmt.Errorf("create files dir: %w", err)
		}
		for _, f := range processPayload.FileList {
			objReader, readErr := backend.ReadObject(ctx, f)
			if readErr != nil {
				return fmt.Errorf("read file %s: %w", f, readErr)
			}
			dst := filepath.Join(filesDir, filepath.Base(f))
			out, createErr := os.Create(dst)
			if createErr != nil {
				objReader.Close()
				return fmt.Errorf("create file %s: %w", dst, createErr)
			}
			_, copyErr := io.Copy(out, objReader)
			objReader.Close()
			out.Close()
			if copyErr != nil {
				return fmt.Errorf("copy file %s: %w", dst, copyErr)
			}
		}
	}

	for si := startStep; si <= lastStep; si++ {
		step := steps[si]
		var inputPath string

		// ── Clean current step's output dir (preserve prior steps' cache on retry) ──
		stepOutDir := filepath.Join(cacheDir, fmt.Sprintf("step_%d_output", si))
		if fileExists(stepOutDir) {
			if err := os.RemoveAll(stepOutDir); err != nil {
				pluginErr = fmt.Errorf("step %d: clean partial output: %w", si, err)
				break
			}
		}
		if err := os.MkdirAll(stepOutDir, 0755); err != nil {
			pluginErr = fmt.Errorf("step %d: create output dir: %w", si, err)
			break
		}

		// ── Resolve input path ──
		// For steps after step 0, prefer previous_data.output_dir (compound chaining)
		if si == 0 {
			if storageMode == "shared_mount" {
				sharedPath := filepath.Join(mountBasePath, "datasets", datasetID, "chunks", fmt.Sprintf("chunk_%d.bin", chunkIndex))
				if fileExists(sharedPath) {
					inputPath = sharedPath
				} else if processPayload.SourcePath != "" {
					// Fallback: download source from S3 when local chunk file doesn't exist
					obs.Info("shared_mount chunk not found, falling back to S3 source", obs.Field{
						"shared_path": sharedPath,
						"source_path": processPayload.SourcePath,
					})
					objReader, readErr := backend.ReadObject(ctx, processPayload.SourcePath)
					if readErr != nil {
						pluginErr = fmt.Errorf("step %d: read source from S3 fallback %s: %w", si, processPayload.SourcePath, readErr)
						break
					}
					ext := filepath.Ext(processPayload.SourcePath)
					outPath := filepath.Join(workDir, "source"+ext)
					outFile, createErr := os.Create(outPath)
					if createErr != nil {
						objReader.Close()
						pluginErr = fmt.Errorf("step %d: create source file from S3 fallback: %w", si, createErr)
						break
					}
					_, copyErr := io.Copy(outFile, objReader)
					objReader.Close()
					outFile.Close()
					if copyErr != nil {
						pluginErr = fmt.Errorf("step %d: copy source from S3 fallback: %w", si, copyErr)
						break
					}
					inputPath = outPath
				} else {
					pluginErr = fmt.Errorf("step %d: shared_mount chunk not found at %s and no source_path for S3 fallback", si, sharedPath)
					break
				}
			} else if filesDir != "" {
				// If filesDir has exactly one entry, pass the file path directly
				// (supports single-file pipelines like CSV scraping). Multiple
				// files → pass the directory (image pipelines, batch processing).
				if entries, readErr := os.ReadDir(filesDir); readErr == nil && len(entries) == 1 {
					inputPath = filepath.Join(filesDir, entries[0].Name())
					obs.Info("compound: single file in filesDir, using file path", obs.Field{
						"input_path": inputPath,
					})
				} else {
					inputPath = filesDir
				}
			} else if processPayload.SourcePath != "" {
				// Directory listing mode: if source_path ends with '/', list objects
				// and download them (supports file_per_chunk strategy from S3 backends)
				if strings.HasSuffix(processPayload.SourcePath, "/") {
					obs.Info("compound: listing source directory", obs.Field{
						"source_path": processPayload.SourcePath,
					})
					objects, listErr := backend.ListObjects(ctx, processPayload.SourcePath)
					if listErr != nil {
						pluginErr = fmt.Errorf("step %d: list source directory %s: %w", si, processPayload.SourcePath, listErr)
						break
					}
					if len(objects) == 0 {
						pluginErr = fmt.Errorf("step %d: no files found at %s", si, processPayload.SourcePath)
						break
					}
					// Create download directory
					filesDir = filepath.Join(workDir, "files")
					if err := os.MkdirAll(filesDir, 0755); err != nil {
						pluginErr = fmt.Errorf("step %d: create files dir: %w", si, err)
						break
					}
					// Extract file_pattern from config
					filePattern := ""
					var configMap map[string]interface{}
					if len(processPayload.Config) > 0 {
						if err := json.Unmarshal(processPayload.Config, &configMap); err == nil {
							if fp, ok := configMap["file_pattern"].(string); ok {
								filePattern = fp
							}
						}
					}
					// Download each file
					for _, obj := range objects {
						// Skip directory entries
						if strings.HasSuffix(obj.Key, "/") {
							continue
						}
						if filePattern != "" {
							matched, matchErr := filepath.Match(filePattern, filepath.Base(obj.Key))
							if matchErr != nil || !matched {
								continue
							}
						}
						objReader, readErr := backend.ReadObject(ctx, obj.Key)
						if readErr != nil {
							pluginErr = fmt.Errorf("step %d: read source file %s: %w", si, obj.Key, readErr)
							break
						}
						dst := filepath.Join(filesDir, filepath.Base(obj.Key))
						outFile, createErr := os.Create(dst)
						if createErr != nil {
							objReader.Close()
							pluginErr = fmt.Errorf("step %d: create file %s: %w", si, dst, createErr)
							break
						}
						_, copyErr := io.Copy(outFile, objReader)
						objReader.Close()
						outFile.Close()
						if copyErr != nil {
							pluginErr = fmt.Errorf("step %d: copy file %s: %w", si, dst, copyErr)
							break
						}
						obs.Info("compound: downloaded source file", obs.Field{
							"key":  obj.Key,
							"dst":  dst,
							"size": obj.Size,
						})
					}
					if pluginErr != nil {
						break
					}
					inputPath = filesDir
					obs.Info("compound: downloaded all source files to", obs.Field{
						"dir":   filesDir,
						"count": len(objects),
					})
				} else {
					// Single file from source_path
					objReader, readErr := backend.ReadObject(ctx, processPayload.SourcePath)
					if readErr != nil {
						pluginErr = fmt.Errorf("step %d: read source %s: %w", si, processPayload.SourcePath, readErr)
						break
					}
					ext := filepath.Ext(processPayload.SourcePath)
					outPath := filepath.Join(workDir, "source"+ext)
					outFile, createErr := os.Create(outPath)
					if createErr != nil {
						objReader.Close()
						pluginErr = fmt.Errorf("step %d: create source file: %w", si, createErr)
						break
					}
					_, copyErr := io.Copy(outFile, objReader)
					objReader.Close()
					outFile.Close()
					if copyErr != nil {
						pluginErr = fmt.Errorf("step %d: copy source: %w", si, copyErr)
						break
					}
					inputPath = outPath
				}
			} else {
				pluginErr = fmt.Errorf("step %d: source_path or file_list required for S3 process jobs", si)
				break
			}
		} else {
			// For steps after step 0, prefer previous_data.output_dir from compound chaining
			if previousData != nil {
				if od, ok := previousData["output_dir"].(string); ok && od != "" {
					inputPath = od
				}
			}
			// If no output_dir but previousData has output_path (e.g. scrape.py
			// writes a single CSV file), use that file path directly.
			if inputPath == "" && previousData != nil {
				if op, ok := previousData["output_path"].(string); ok && op != "" {
					if fileExists(op) {
						inputPath = op
					}
				}
			}
			// Fallback: reuse filesDir if set (original source files)
			if inputPath == "" && filesDir != "" {
				inputPath = filesDir
			}
			// Fallback: existing cache/S3 logic
			if inputPath == "" {
				prevStepOutDir := filepath.Join(cacheDir, fmt.Sprintf("step_%d_output", si-1))
				if fileExists(prevStepOutDir) {
					inputPath = prevStepOutDir
				} else if storageMode == "shared_mount" {
					inputPath = filepath.Join(mountBasePath, "datasets", datasetID, "chunks", fmt.Sprintf("chunk_%d.out", chunkIndex))
					if !fileExists(inputPath) {
						pluginErr = fmt.Errorf("step %d: prev output not found at %s", si, inputPath)
						break
					}
				} else {
					obs.Warn("compound: cached step output not found, downloading from S3", obs.Field{
						"step": si - 1,
						"path": prevStepOutDir,
					})
					inputKey := storage.GetRemotePathWithSlug(datasetID, datasetSlug, chunkIndex, "result")
					objReader, err := backend.ReadObject(ctx, inputKey)
					if err != nil {
						pluginErr = fmt.Errorf("step %d: download prev output: %w", si, err)
						break
					}
					inputPath = filepath.Join(workDir, fmt.Sprintf("chunk_%d.out", chunkIndex))
					inputFile, createErr := os.Create(inputPath)
					if createErr != nil {
						objReader.Close()
						pluginErr = fmt.Errorf("step %d: create prev file: %w", si, createErr)
						break
					}
					_, copyErr := io.Copy(inputFile, objReader)
					objReader.Close()
					inputFile.Close()
					if copyErr != nil {
						pluginErr = fmt.Errorf("step %d: copy prev: %w", si, copyErr)
						break
					}
				}
			}
		}

		// outputPath = file path for stdout-to-file fallback (per-step mode pattern)
		// outputDir  = directory for file-based plugins that produce multiple files
		outputPath := filepath.Join(stepOutDir, fmt.Sprintf("step_%d_output.json", si))
		outputDir := stepOutDir

		result, execErr := executePluginStepEx(ctx, job, processPayload, step.PluginID, step.Config, si, inputPath, outputPath, outputDir, previousData)
		if execErr != nil {
			pluginErr = fmt.Errorf("step %d: %w", si, execErr)
			reportStepProgress(ctx, job.ID, executionID, chunkID, si, "failed", pluginErr.Error())
			break
		}

		reportStepProgress(ctx, job.ID, executionID, chunkID, si, "completed", "")
		lastOutputPath = outputPath

		// Extract file_list and output_dir from step output for next step's previous_data.
		// Some plugins wrap output in a "data" key; others put fields at top level.
		if result != nil {
			if data, ok := result["data"].(map[string]interface{}); ok {
				previousData = data
			} else {
				// Fields at top level — check for known chaining keys
				if _, hasOP := result["output_path"]; hasOP {
					previousData = result
				} else if _, hasOD := result["output_dir"]; hasOD {
					previousData = result
				} else if _, hasFL := result["file_list"]; hasFL {
					previousData = result
				}
			}
		}
	}

	if pluginErr != nil {
		// Preserve cache for retry — don't clean up prior steps
		obs.Warn("compound job failed, cache preserved for retry", obs.Field{
			"cache_dir": cacheDir,
			"error":     pluginErr.Error(),
		})
		return pluginErr
	}

	// Determine which file to upload as the final result.
	// 1. Prefer the outputPath file (stdout fallback or plugin-created file).
	// 2. If that doesn't exist, scan the step output directory for the newest file.
	// 3. If nothing found, upload a minimal metadata JSON (should not happen).
	var uploadFilePath string
	lastOutputDir := filepath.Join(cacheDir, fmt.Sprintf("step_%d_output", lastStep))

	if _, statErr := os.Stat(lastOutputPath); statErr == nil {
		uploadFilePath = lastOutputPath
	} else {
		// Scan the step output directory for the newest file
		var newestMod time.Time
		entries, readErr := os.ReadDir(lastOutputDir)
		if readErr == nil {
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				info, fiErr := entry.Info()
				if fiErr != nil {
					continue
				}
				if info.ModTime().After(newestMod) {
					newestMod = info.ModTime()
					uploadFilePath = filepath.Join(lastOutputDir, entry.Name())
				}
			}
		}
	}

	if uploadFilePath != "" {
		// Upload the chunk result (actual output file)
		if storageMode != "shared_mount" {
			f, fErr := os.Open(uploadFilePath)
			if fErr != nil {
				return fmt.Errorf("open output file: %w", fErr)
			}
			if wErr := backend.WriteObject(ctx, resultKey, f); wErr != nil {
				f.Close()
				return fmt.Errorf("write result: %w", wErr)
			}
			f.Close()
		} else {
			mountBasePath := deriveMountBasePath(storageMode)
			chunkOutPath := filepath.Join(mountBasePath, "datasets", datasetID, "chunks", fmt.Sprintf("chunk_%d.out", chunkIndex))
			if err := os.MkdirAll(filepath.Dir(chunkOutPath), 0755); err != nil {
				return fmt.Errorf("create output dir: %w", err)
			}
			if err := copyFile(uploadFilePath, chunkOutPath); err != nil {
				return fmt.Errorf("copy final output: %w", err)
			}
		}

		obs.Info("compound job completed, uploaded output file", obs.Field{
			"execution_id":  executionID,
			"chunk_id":      chunkID,
			"steps":         len(steps),
			"output_file":   uploadFilePath,
		})
	} else {
		// Nothing to upload — write minimal metadata JSON as last resort
		meta := map[string]interface{}{
			"output_dir": lastOutputDir,
			"file_list":  nil,
			"steps":      len(steps),
		}
		if previousData != nil {
			if fl, ok := previousData["file_list"]; ok {
				meta["file_list"] = fl
			}
		}
		metaJSON, _ := json.Marshal(meta)

		if storageMode != "shared_mount" {
			if err := backend.WriteObject(ctx, resultKey, bytes.NewReader(metaJSON)); err != nil {
				return fmt.Errorf("write final result meta: %w", err)
			}
		} else {
			mountBasePath := deriveMountBasePath(storageMode)
			metaPath := filepath.Join(mountBasePath, "datasets", datasetID, "chunks", fmt.Sprintf("chunk_%d.json", chunkIndex))
			if err := os.MkdirAll(filepath.Dir(metaPath), 0755); err != nil {
				return fmt.Errorf("create meta dir: %w", err)
			}
			if err := os.WriteFile(metaPath, metaJSON, 0644); err != nil {
				return fmt.Errorf("write final result meta: %w", err)
			}
		}

		obs.Info("compound job completed, no output file found", obs.Field{
			"execution_id":  executionID,
			"chunk_id":      chunkID,
			"steps":         len(steps),
			"output_dir":    lastOutputDir,
		})
	}

	// Clean up cache on success
	if err := os.RemoveAll(cacheDir); err != nil {
		obs.Warn("compound cache cleanup failed", obs.Field{"cache_dir": cacheDir, "error": err.Error()})
	}

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

	return "/data/sentra"
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
	if backend == nil {
		return errors.New("storage backend not initialized")
	}

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

				destKey := ingestPayload.DatasetID + "/" + filepath.Base(obj.Key)
				if err := backend.WriteObject(ctx, destKey, objReader); err != nil {
					return fmt.Errorf("failed to write object %s: %w", destKey, err)
				}
			}
		}

		sourcePath = ingestPayload.DatasetID
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

	pluginName := ResolvePluginName(ctx, job.PluginID, job.PluginName)
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
	outputToParse := extractLastJSONString(execResult.Output)
	if err := json.Unmarshal([]byte(outputToParse), &result); err != nil {
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
	case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".tif", ".svg":
		return extractImageMetadata(ctx, filePath)
	case ".mp4", ".mkv", ".webm", ".avi", ".mov", ".wmv", ".flv", ".m4v":
		return extractVideoMetadata(ctx, filePath)
	case ".pdf":
		return extractPDFMetadata(ctx, filePath)
	case ".mp3", ".wav", ".flac", ".aac", ".ogg", ".wma", ".m4a":
		return extractAudioMetadata(ctx, filePath)
	case ".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".7z", ".rar":
		return extractArchiveMetadata(ctx, filePath)
	case ".so", ".dll", ".dylib", ".exe", ".elf", ".o", ".obj":
		return extractBinaryMetadata(ctx, filePath)
	default:
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
		"format":          "unknown",
		"sample_file":     filepath.Base(filePath),
	}

	if info, err := os.Stat(filePath); err == nil {
		metadata["file_size_bytes"] = info.Size()
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
			return metadata, nil
		}
		// PNG
		if n >= 8 && string(header[:8]) == "\x89PNG\r\n\x1a\n" {
			return extractImageMetadata(ctx, filePath)
		}
		// JPEG
		if n >= 3 && header[0] == 0xff && header[1] == 0xd8 && header[2] == 0xff {
			return extractImageMetadata(ctx, filePath)
		}
		// PDF
		if n >= 5 && string(header[:5]) == "%PDF-" {
			return extractPDFMetadata(ctx, filePath)
		}
		// ELF
		if string(header[:4]) == "\x7fELF" {
			return extractBinaryMetadata(ctx, filePath)
		}
		// ZIP (PK\x03\x04)
		if header[0] == 0x50 && header[1] == 0x4b && (header[2] == 0x03 || header[2] == 0x05 || header[2] == 0x07) {
			return extractArchiveMetadata(ctx, filePath)
		}
		// RIFF (WAV/AVI)
		if string(header[:4]) == "RIFF" {
			if n >= 12 && string(header[8:12]) == "WAVE" {
				return extractAudioMetadata(ctx, filePath)
			}
			return metadata, nil
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

func extractImageMetadata(_ context.Context, filePath string) (map[string]any, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	meta := map[string]any{
		"format": "image",
	}

	buf := make([]byte, 512)
	f.Read(buf)
	f.Seek(0, 0)
	mimeType := http.DetectContentType(buf)
	meta["image_mime"] = mimeType

	cfg, _, err := image.DecodeConfig(f)
	if err == nil {
		meta["image_width"] = cfg.Width
		meta["image_height"] = cfg.Height
		meta["image_color_model"] = fmt.Sprintf("%T", cfg.ColorModel)
	}

	fi, _ := os.Stat(filePath)
	meta["file_size_bytes"] = fi.Size()

	return meta, nil
}

func extractVideoMetadata(_ context.Context, filePath string) (map[string]any, error) {
	meta := map[string]any{
		"format": "video",
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, _ := os.Stat(filePath)
	meta["file_size_bytes"] = fi.Size()

	buf := make([]byte, 4096)
	f.Read(buf)
	f.Seek(0, 0)

	switch {
	case bytes.Contains(buf, []byte("ftyp")):
		meta["video_container"] = "mp4"
		ftypIdx := bytes.Index(buf, []byte("ftyp"))
		if ftypIdx >= 0 && ftypIdx+12 < len(buf) {
			meta["video_codec"] = string(bytes.TrimRight(buf[ftypIdx+8:ftypIdx+12], "\x00"))
		}
	case bytes.HasPrefix(buf, []byte{0x1a, 0x45, 0xdf, 0xa3}):
		meta["video_container"] = "matroska"
	case bytes.HasPrefix(buf, []byte("RIFF")) && bytes.Contains(buf[:12], []byte("AVI ")):
		meta["video_container"] = "avi"
	case bytes.HasPrefix(buf, []byte{0x00, 0x00, 0x00, 0x1c, 0x66, 0x74, 0x79, 0x70}):
		meta["video_container"] = "quicktime"
	default:
		meta["video_container"] = "unknown"
	}

	meta["estimated_duration_seconds"] = float64(fi.Size()) / 500000

	if fi.Size() > 500*1024*1024 {
		meta["quality_estimate"] = "high"
	} else if fi.Size() > 50*1024*1024 {
		meta["quality_estimate"] = "medium"
	} else {
		meta["quality_estimate"] = "low"
	}

	return meta, nil
}

func extractPDFMetadata(_ context.Context, filePath string) (map[string]any, error) {
	meta := map[string]any{
		"format": "pdf",
	}

	fi, _ := os.Stat(filePath)
	meta["file_size_bytes"] = fi.Size()

	conf := model.NewDefaultConfiguration()

	f, err := os.Open(filePath)
	if err == nil {
		defer f.Close()
		pdfInfo, err := pdfcpuapi.PDFInfo(f, filePath, nil, conf)
		if err == nil {
			meta["pdf_page_count"] = pdfInfo.PageCount
			meta["pdf_version"] = pdfInfo.Version
			meta["pdf_encrypted"] = pdfInfo.Encrypted
			meta["pdf_linearized"] = pdfInfo.Linearized
		}
	}

	return meta, nil
}

func extractAudioMetadata(_ context.Context, filePath string) (map[string]any, error) {
	meta := map[string]any{
		"format": "audio",
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, _ := os.Stat(filePath)
	meta["file_size_bytes"] = fi.Size()

	buf := make([]byte, 4096)
	f.Read(buf)
	f.Seek(0, 0)

	switch {
	case bytes.HasPrefix(buf, []byte("ID3")):
		meta["audio_codec"] = "mp3"
		if len(buf) > 10 {
			id3Size := int(buf[6])<<21 | int(buf[7])<<14 | int(buf[8])<<7 | int(buf[9])
			meta["id3_tag_present"] = true
			meta["id3_tag_size"] = id3Size
		}
	case bytes.HasPrefix(buf, []byte("RIFF")) && bytes.Contains(buf[:12], []byte("WAVE")):
		meta["audio_codec"] = "pcm"
		if len(buf) > 28 {
			sampleRate := binary.LittleEndian.Uint32(buf[24:28])
			meta["audio_sample_rate"] = sampleRate
		}
	case bytes.HasPrefix(buf, []byte("fLaC")):
		meta["audio_codec"] = "flac"
	case buf[0] == 0xFF && (buf[1]&0xE0) == 0xE0:
		meta["audio_codec"] = "mp3"
	default:
		meta["audio_codec"] = "unknown"
	}

	return meta, nil
}

func extractArchiveMetadata(_ context.Context, filePath string) (map[string]any, error) {
	meta := map[string]any{
		"format": "archive",
	}

	fi, _ := os.Stat(filePath)
	meta["file_size_bytes"] = fi.Size()

	if zr, err := zip.OpenReader(filePath); err == nil {
		meta["archive_format"] = "zip"
		meta["archive_file_count"] = len(zr.File)
		var totalSize int64
		for _, f := range zr.File {
			totalSize += int64(f.UncompressedSize64)
		}
		meta["archive_uncompressed_bytes"] = totalSize
		if fi.Size() > 0 {
			meta["archive_compression_ratio"] = float64(totalSize) / float64(fi.Size())
		}
		zr.Close()
		return meta, nil
	}

	f, _ := os.Open(filePath)
	defer f.Close()

	var tr io.Reader = f
	gzBuf := make([]byte, 2)
	f.Read(gzBuf)
	f.Seek(0, 0)

	if gzBuf[0] == 0x1F && gzBuf[1] == 0x8B {
		meta["archive_format"] = "tar.gz"
		if gr, err := gzip.NewReader(f); err == nil {
			tr = gr
			defer gr.Close()
		}
	} else {
		meta["archive_format"] = "tar"
	}

	tarReader := tar.NewReader(tr)
	var fileCount int
	var totalSize int64
	for {
		hdr, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		if hdr.Typeflag == tar.TypeReg {
			fileCount++
			totalSize += hdr.Size
		}
	}
	meta["archive_file_count"] = fileCount
	meta["archive_uncompressed_bytes"] = totalSize
	if fi.Size() > 0 {
		meta["archive_compression_ratio"] = float64(totalSize) / float64(fi.Size())
	}

	return meta, nil
}

func extractBinaryMetadata(_ context.Context, filePath string) (map[string]any, error) {
	meta := map[string]any{
		"format": "binary",
	}

	fi, _ := os.Stat(filePath)
	meta["file_size_bytes"] = fi.Size()

	if ef, err := elf.Open(filePath); err == nil {
		meta["binary_format"] = "elf"
		meta["binary_arch"] = ef.Machine.String()
		if ef.FileHeader.Type == elf.ET_EXEC {
			meta["binary_type"] = "executable"
		} else if ef.FileHeader.Type == elf.ET_DYN {
			meta["binary_type"] = "shared_library"
		} else if ef.FileHeader.Type == elf.ET_REL {
			meta["binary_type"] = "relocatable"
		}
		ef.Close()
		return meta, nil
	}

	if pef, err := pe.Open(filePath); err == nil {
		meta["binary_format"] = "pe"
		switch pef.Machine {
		case pe.IMAGE_FILE_MACHINE_I386:
			meta["binary_arch"] = "i386"
		case pe.IMAGE_FILE_MACHINE_AMD64:
			meta["binary_arch"] = "x86_64"
		case pe.IMAGE_FILE_MACHINE_ARM64:
			meta["binary_arch"] = "arm64"
		default:
			meta["binary_arch"] = fmt.Sprintf("0x%x", pef.Machine)
		}
		pef.Close()
		return meta, nil
	}

	if mf, err := macho.Open(filePath); err == nil {
		meta["binary_format"] = "macho"
		meta["binary_arch"] = mf.Cpu.String()
		switch mf.Type {
		case macho.TypeExec:
			meta["binary_type"] = "executable"
		case macho.TypeDylib:
			meta["binary_type"] = "dynamic_library"
		case macho.TypeBundle:
			meta["binary_type"] = "bundle"
		}
		mf.Close()
		return meta, nil
	}

	if ff, err := macho.OpenFat(filePath); err == nil {
		meta["binary_format"] = "macho"
		var archs []string
		for _, arch := range ff.Arches {
			archs = append(archs, arch.Cpu.String())
		}
		meta["binary_arch"] = strings.Join(archs, ",")
		meta["binary_type"] = "executable"
		meta["binary_fat"] = true
		ff.Close()
		return meta, nil
	}

	meta["binary_format"] = "unknown"
	return meta, nil
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
		payloadMap["mock_mode"] = processPayload.MockMode
		if len(processPayload.Config) > 0 {
			var cfg map[string]interface{}
			if json.Unmarshal(processPayload.Config, &cfg) == nil {
				for k, v := range cfg {
					payloadMap[k] = v
				}
			}
		}
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

func tryParseJSONString(s string) interface{} {
	s = strings.TrimSpace(s)
	if len(s) < 2 {
		return nil
	}
	if (s[0] == '{' && s[len(s)-1] == '}') || (s[0] == '[' && s[len(s)-1] == ']') {
		var parsed interface{}
		if json.Unmarshal([]byte(s), &parsed) == nil {
			return parsed
		}
	}
	return nil
}

// parsePluginOutputAsJSON extracts the last valid JSON object from combined
// stdout+stderr output. Plugins may emit log lines to stderr and JSON to stdout;
// the sandbox separates the two, but for defense-in-depth we also handle the
// case where they may be interleaved.
func parsePluginOutputAsJSON(output string) (map[string]any, error) {
	output = strings.TrimSpace(output)
	if output == "" {
		return nil, nil
	}

	// Fast path: try the entire output as-is (clean stdout-only case)
	var result map[string]any
	if err := json.Unmarshal([]byte(output), &result); err == nil {
		return result, nil
	}

	// Fallback: look for the last line that is a valid JSON object
	lines := strings.Split(output, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if strings.HasPrefix(line, "{") && strings.HasSuffix(line, "}") {
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err == nil {
				return m, nil
			}
		}
	}

	// Last resort: return the original error
	return nil, fmt.Errorf("no valid JSON object found in plugin output: %s", truncateForLog(output, 200))
}

func truncateForLog(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// extractLastJSONString finds the last valid JSON object in a string that may
// contain mixed stdout+stderr output. Returns the original string if no valid
// JSON object is found (so callers can try parsing it directly).
func extractLastJSONString(output string) string {
	trimmed := strings.TrimSpace(output)
	if trimmed == "" {
		return trimmed
	}

	// Fast path: if the whole thing is valid JSON, use it
	if json.Unmarshal([]byte(trimmed), &json.RawMessage{}) == nil {
		return trimmed
	}

	// Fallback: find last line that is a valid JSON object or array
	lines := strings.Split(trimmed, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if len(line) < 2 {
			continue
		}
		if (line[0] == '{' && line[len(line)-1] == '}') || (line[0] == '[' && line[len(line)-1] == ']') {
			if json.Unmarshal([]byte(line), &json.RawMessage{}) == nil {
				return line
			}
		}
	}

	// Last resort: return original
	return trimmed
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()
	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()
	_, err = io.Copy(dstFile, srcFile)
	return err
}


