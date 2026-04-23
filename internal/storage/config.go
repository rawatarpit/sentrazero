package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"sentra-agent/internal/httpclient"
)

type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

type StorageBackend interface {
	ReadObject(ctx context.Context, remotePath string) (io.ReadCloser, error)
	WriteObject(ctx context.Context, remotePath string, reader io.Reader) error
	ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error)
	StatObject(ctx context.Context, remotePath string) (ObjectInfo, error)
	DeleteObject(ctx context.Context, remotePath string) error
}

var (
	globalBackend      StorageBackend
	globalBackendOnce  sync.Once
	globalBackendErr   error
	globalBackendMu    sync.RWMutex
	backendInvalidated bool
)

type StorageConfig struct {
	StorageMode     string      `json:"storage_mode"`
	Provider        string      `json:"provider"`
	BucketName      string      `json:"bucket_name"`
	Region          string      `json:"region"`
	Endpoint        string      `json:"endpoint"`
	MountBasePath   string      `json:"mount_base_path"`
	Credentials    interface{} `json:"credentials"`
	VaultSecretName string     `json:"vault_secret_name,omitempty"`
}

type S3Credentials struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
}

var (
	cfg        *StorageConfig
	cfgOnce    sync.Once
	cfgErr     error
	cfgMu      sync.RWMutex
	httpClient = &http.Client{Timeout: 10 * time.Second}

	globalOrgID    string
	globalDeviceID string
	globalToken    string
	globalAnonKey  string
	globalBaseURL  string
)

func SetGlobalAPICredentials(orgID, deviceID, token, anonKey, baseURL string) {
	globalOrgID = orgID
	globalDeviceID = deviceID
	globalToken = token
	globalAnonKey = anonKey
	globalBaseURL = baseURL
}

func GetConfig() (*StorageConfig, error) {
	cfgOnce.Do(func() {
		cfg, cfgErr = fetchConfig()
	})
	return cfg, cfgErr
}

func fetchConfig() (*StorageConfig, error) {
	orgID := os.Getenv("ORG_ID")
	deviceID := os.Getenv("DEVICE_ID")
	token := os.Getenv("SUPABASE_SERVICE_ROLE_KEY")
	anonKey := os.Getenv("SUPABASE_ANON_KEY")
	baseURL := os.Getenv("SUPABASE_URL")

	if baseURL == "" || orgID == "" {
		return nil, fmt.Errorf("SUPABASE_URL or ORG_ID not configured")
	}

	client := NewStorageClient(orgID, deviceID, token, anonKey, baseURL)
	return client.FetchConfig()
}

func GetConfigByID(storageConfigID string) (*StorageConfig, error) {
	if storageConfigID == "" {
		return GetConfig()
	}

	orgID := globalOrgID
	deviceID := globalDeviceID
	token := globalToken
	anonKey := globalAnonKey
	baseURL := globalBaseURL

	if baseURL == "" || orgID == "" {
		orgID = os.Getenv("ORG_ID")
		deviceID = os.Getenv("DEVICE_ID")
		token = os.Getenv("SUPABASE_SERVICE_ROLE_KEY")
		anonKey = os.Getenv("SUPABASE_ANON_KEY")
		baseURL = os.Getenv("SUPABASE_URL")
	}

	if baseURL == "" || orgID == "" {
		return nil, fmt.Errorf("SUPABASE_URL or ORG_ID not configured")
	}

	client := NewStorageClient(orgID, deviceID, token, anonKey, baseURL)
	return client.FetchConfigByID(storageConfigID)
}

type SharedMountBackend struct {
	mountBasePath string
}

func NewSharedMountBackend(mountBasePath string) *SharedMountBackend {
	if mountBasePath == "" {
		homeDir, _ := os.UserHomeDir()
		mountBasePath = filepath.Join(homeDir, "sentra", "data")
	}
	return &SharedMountBackend{mountBasePath: mountBasePath}
}

func (b *SharedMountBackend) ReadObject(ctx context.Context, remotePath string) (io.ReadCloser, error) {
	fullPath := filepath.Join(b.mountBasePath, remotePath)
	return os.Open(fullPath)
}

func (b *SharedMountBackend) WriteObject(ctx context.Context, remotePath string, reader io.Reader) error {
	fullPath := filepath.Join(b.mountBasePath, remotePath)
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}
	f, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", fullPath, err)
	}
	defer f.Close()
	_, err = io.Copy(f, reader)
	if err != nil {
		return fmt.Errorf("failed to write to %s: %w", fullPath, err)
	}
	return f.Sync()
}

func (b *SharedMountBackend) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	fullPath := filepath.Join(b.mountBasePath, prefix)
	var results []ObjectInfo
	err := filepath.Walk(fullPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			relPath, err := filepath.Rel(b.mountBasePath, path)
			if err != nil {
				return err
			}
			results = append(results, ObjectInfo{
				Key:          relPath,
				Size:         info.Size(),
				LastModified: info.ModTime(),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})
	return results, nil
}

func (b *SharedMountBackend) StatObject(ctx context.Context, remotePath string) (ObjectInfo, error) {
	fullPath := filepath.Join(b.mountBasePath, remotePath)
	info, err := os.Stat(fullPath)
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{
		Key:          remotePath,
		Size:         info.Size(),
		LastModified: info.ModTime(),
	}, nil
}

func (b *SharedMountBackend) DeleteObject(ctx context.Context, remotePath string) error {
	fullPath := filepath.Join(b.mountBasePath, remotePath)
	return os.Remove(fullPath)
}

type S3Backend struct {
	client     *minio.Client
	bucketName string
}

func NewS3Backend(endpoint, bucketName, region string, creds *S3Credentials) (*S3Backend, error) {
	useSSL := true

	log.Printf("[storage] NewS3Backend: input endpoint=%s bucket=%s region=%s", endpoint, bucketName, region)
	
	// For Supabase Storage S3 API endpoint: https://project.storage.supabase.co/storage/v1/s3
	// We need to use the BASE URL (without path) AND provide custom source path
	// Let me extract the base URL properly:
	var s3Endpoint string
	if strings.HasSuffix(endpoint, "/storage/v1/s3") {
		// Extract just the host: https://project.storage.supabase.co
		parts := strings.TrimSuffix(endpoint, "/storage/v1/s3")
		// Make sure we only have hostname
		s3Endpoint = parts
		log.Printf("[storage] Using base URL: %s", s3Endpoint)
	} else {
		s3Endpoint = endpoint
	}

	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken),
		Secure: useSSL,
	}
	if region != "" {
		opts.Region = region
	}

	log.Printf("[storage] NewS3Backend: minio.New(%s) with bucket=%s", s3Endpoint, bucketName)

	client, err := minio.New(s3Endpoint, opts)
	if err != nil {
		log.Printf("[storage] NewS3Backend: failed with base URL: %v", err)
		// Try again with full endpoint (won't work but let's see the error)
		client, err = minio.New(endpoint, opts)
		if err != nil {
			log.Printf("[storage] NewS3Backend: also failed with full endpoint: %v", err)
			// Try a third approach - just the hostname without https prefix if already has it
			return nil, fmt.Errorf("failed to create S3 client: %w", err)
		}
	}

	log.Printf("[storage] S3 client ready for bucket: %s", bucketName)

	return &S3Backend{
		client:     client,
		bucketName: bucketName,
	}, nil
}

func (b *S3Backend) ReadObject(ctx context.Context, remotePath string) (io.ReadCloser, error) {
	obj, err := b.client.GetObject(ctx, b.bucketName, remotePath, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", remotePath, err)
	}
	return obj, nil
}

func (b *S3Backend) WriteObject(ctx context.Context, remotePath string, reader io.Reader) error {
	_, err := b.client.PutObject(ctx, b.bucketName, remotePath, reader, -1, minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to put object %s: %w", remotePath, err)
	}
	return nil
}

func (b *S3Backend) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var results []ObjectInfo
	for obj := range b.client.ListObjects(ctx, b.bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", obj.Err)
		}
		results = append(results, ObjectInfo{
			Key:          obj.Key,
			Size:         obj.Size,
			LastModified: obj.LastModified,
		})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})
	return results, nil
}

func (b *S3Backend) StatObject(ctx context.Context, remotePath string) (ObjectInfo, error) {
	objInfo, err := b.client.StatObject(ctx, b.bucketName, remotePath, minio.StatObjectOptions{})
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("failed to stat object %s: %w", remotePath, err)
	}
	return ObjectInfo{
		Key:          objInfo.Key,
		Size:         objInfo.Size,
		LastModified: objInfo.LastModified,
	}, nil
}

func (b *S3Backend) DeleteObject(ctx context.Context, remotePath string) error {
	return b.client.RemoveObject(ctx, b.bucketName, remotePath, minio.RemoveObjectOptions{})
}

func normalizeStorageMode(mode string) string {
	switch strings.ToLower(mode) {
	case "aws_s3", "s3", "s3_compatible":
		return "s3"
	case "gcs", "google_cloud_storage":
		return "gcs"
	case "azure_blob":
		return "azure_blob"
	case "shared_mount", "local":
		return "shared_mount"
	case "object_storage":
		return "s3"
	default:
		return mode
	}
}

func NewBackend(cfg *StorageConfig) (StorageBackend, error) {
	if cfg == nil {
		return nil, fmt.Errorf("storage config is nil")
	}

	storageMode := normalizeStorageMode(cfg.StorageMode)

	switch storageMode {
	case "s3":
		if cfg.BucketName == "" {
			return nil, fmt.Errorf("S3 storage mode requires bucket_name")
		}
		if cfg.Endpoint == "" {
			return nil, fmt.Errorf("S3 storage mode requires endpoint")
		}

		var creds *S3Credentials
		if cfg.Credentials != nil {
			credBytes, err := json.Marshal(cfg.Credentials)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal credentials: %w", err)
			}
			creds = &S3Credentials{}
			if err := json.Unmarshal(credBytes, creds); err != nil {
				return nil, fmt.Errorf("failed to unmarshal credentials: %w", err)
			}
		}

		if creds == nil || creds.AccessKeyID == "" || creds.SecretAccessKey == "" {
			return nil, fmt.Errorf("S3 storage mode requires credentials with access_key_id and secret_access_key")
		}

		log.Printf("[storage] initializing S3 backend: endpoint=%s bucket=%s region=%s",
			cfg.Endpoint, cfg.BucketName, cfg.Region)
		
		// Use AWS SDK v2 with custom endpoint resolver
		return NewS3BackendV2(cfg.Endpoint, cfg.BucketName, cfg.Region, creds)

	case "gcs", "azure_blob":
		return nil, fmt.Errorf("%s storage mode not yet implemented", storageMode)

	case "shared_mount":
		log.Printf("[storage] initializing SharedMountBackend: mount_base_path=%s", cfg.MountBasePath)
		return NewSharedMountBackend(cfg.MountBasePath), nil

	default:
		if storageMode != "" {
			return nil, fmt.Errorf("unknown storage mode: %s (must be one of: s3, shared_mount)", storageMode)
		}
		return nil, fmt.Errorf("storage mode is required")
	}
}

func GetBackend() (StorageBackend, error) {
	globalBackendMu.RLock()
	if globalBackend != nil && !backendInvalidated {
		err := globalBackendErr
		globalBackendMu.RUnlock()
		return globalBackend, err
	}
	globalBackendMu.RUnlock()

	globalBackendMu.Lock()
	defer globalBackendMu.Unlock()

	globalBackendOnce.Do(func() {
		cfg, globalBackendErr = GetConfig()
		if globalBackendErr != nil {
			return
		}
		globalBackend, globalBackendErr = NewBackend(cfg)
	})

	if backendInvalidated && globalBackendErr == nil {
		cfg, err := GetConfig()
		if err != nil {
			globalBackendErr = err
			return nil, err
		}
		newBackend, err := NewBackend(cfg)
		if err != nil {
			globalBackendErr = err
			return nil, err
		}
		globalBackend = newBackend
		backendInvalidated = false
	}

	return globalBackend, globalBackendErr
}

func InvalidateBackend() {
	globalBackendMu.Lock()
	defer globalBackendMu.Unlock()
	backendInvalidated = true
	globalBackend = nil
	globalBackendErr = nil
}

func GetRemotePath(datasetID string, chunkIndex int, pathType string) string {
	base := fmt.Sprintf("datasets/%s", datasetID)
	switch pathType {
	case "source":
		return fmt.Sprintf("%s/source", base)
	case "chunk":
		return fmt.Sprintf("%s/chunks/chunk_%d.bin", base, chunkIndex)
	case "result":
		return fmt.Sprintf("%s/results/chunk_%d.out", base, chunkIndex)
	case "merged":
		return fmt.Sprintf("%s/merged/dataset.parquet", base)
	}
	return ""
}

func (s *StorageConfig) GetDatasetPath(datasetID string) string {
	if s.StorageMode == "shared_mount" {
		return s.MountBasePath + "/datasets/" + datasetID
	}
	return ""
}

func (s *StorageConfig) GetChunkPath(datasetID string, chunkIndex int) string {
	if s.StorageMode == "shared_mount" {
		return s.GetDatasetPath(datasetID) + "/chunks/chunk_" + strconv.Itoa(chunkIndex) + ".bin"
	}
	return ""
}

func (s *StorageConfig) GetResultPath(datasetID string, chunkIndex int) string {
	if s.StorageMode == "shared_mount" {
		return s.GetDatasetPath(datasetID) + "/results/chunk_" + strconv.Itoa(chunkIndex) + ".out"
	}
	return ""
}

func (s *StorageConfig) GetMergedPath(datasetID string) string {
	if s.StorageMode == "shared_mount" {
		return s.GetDatasetPath(datasetID) + "/merged/dataset.parquet"
	}
	return ""
}

func (s *StorageConfig) GetSourcePath(datasetID string) string {
	if s.StorageMode == "shared_mount" {
		return s.GetDatasetPath(datasetID) + "/source"
	}
	return ""
}

type StorageClient struct {
	config   *StorageConfig
	OrgID    string
	DeviceID string
	Token    string
	AnonKey  string
	BaseURL  string
}

func NewStorageClient(orgID, deviceID, token, anonKey, baseURL string) *StorageClient {
	return &StorageClient{
		OrgID:    orgID,
		DeviceID: deviceID,
		Token:    token,
		AnonKey:  anonKey,
		BaseURL:  baseURL,
	}
}

func (c *StorageClient) FetchConfig() (*StorageConfig, error) {
	type Response struct {
		StorageMode      string      `json:"storage_mode"`
		Provider        string      `json:"provider"`
		BucketName      string      `json:"bucket_name"`
		Region          string      `json:"region"`
		Endpoint        string      `json:"endpoint"`
		MountBasePath   string      `json:"mount_base_path"`
		Credentials    interface{} `json:"credentials"`
		VaultSecretName string    `json:"vault_secret_name,omitempty"`
	}

	body := map[string]string{
		"org_id": c.OrgID,
	}

	resp, err := c.doRequest("POST", "/functions/v1/get_storage_config", body)
	if err != nil {
		return nil, err
	}

	type StorageConfigResponse struct {
		Ok      bool   `json:"ok"`
		Error   string `json:"error,omitempty"`
		Details string `json:"details,omitempty"`
	}
	var checkResp StorageConfigResponse
	if err := json.Unmarshal(resp, &checkResp); err == nil && !checkResp.Ok {
		return nil, fmt.Errorf("storage config unavailable: %s (%s)", checkResp.Error, checkResp.Details)
	}

	var result Response
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}

	cfg := &StorageConfig{
		StorageMode:     result.StorageMode,
		Provider:        result.Provider,
		BucketName:      result.BucketName,
		Region:          result.Region,
		Endpoint:        result.Endpoint,
		MountBasePath:   result.MountBasePath,
		Credentials:    result.Credentials,
		VaultSecretName: result.VaultSecretName,
	}

	if cfg.VaultSecretName != "" {
		log.Printf("[storage] FetchConfig: resolving vault secret %s", cfg.VaultSecretName)
		resolvedCreds, err := c.resolveVaultSecret(cfg.VaultSecretName)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve vault secret %s: %w", cfg.VaultSecretName, err)
		}
		cfg.Credentials = resolvedCreds
		log.Printf("[storage] FetchConfig: resolved vault secret %s -> credentials", cfg.VaultSecretName)
	}

	if cfg.Credentials == nil && (result.StorageMode == "s3" || result.StorageMode == "object_storage") {
		return nil, fmt.Errorf("storage config unavailable: no credentials and storage_mode=%s", result.StorageMode)
	}

	cfgMu.Lock()
	cfg = cfg
	cfgMu.Unlock()

	return cfg, nil
}

func (c *StorageClient) FetchConfigByID(storageConfigID string) (*StorageConfig, error) {
	type Response struct {
		StorageMode     string      `json:"storage_mode"`
		Provider        string      `json:"provider"`
		BucketName      string      `json:"bucket_name"`
		Region         string      `json:"region"`
		Endpoint       string      `json:"endpoint"`
		MountBasePath  string      `json:"mount_base_path"`
		Credentials   interface{} `json:"credentials"`
		VaultSecretName string    `json:"vault_secret_name,omitempty"`
	}

	body := map[string]string{
		"org_id":            c.OrgID,
		"storage_config_id": storageConfigID,
	}

	resp, err := c.doRequest("POST", "/functions/v1/get_storage_config", body)
	if err != nil {
		return nil, err
	}

	type StorageConfigResponse struct {
		Ok      bool   `json:"ok"`
		Error   string `json:"error,omitempty"`
		Details string `json:"details,omitempty"`
	}
	var checkResp StorageConfigResponse
	if err := json.Unmarshal(resp, &checkResp); err == nil && !checkResp.Ok {
		return nil, fmt.Errorf("storage config unavailable: %s (%s)", checkResp.Error, checkResp.Details)
	}

	var result Response
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}

	cfg := &StorageConfig{
		StorageMode:     result.StorageMode,
		Provider:        result.Provider,
		BucketName:      result.BucketName,
		Region:          result.Region,
		Endpoint:       result.Endpoint,
		MountBasePath:   result.MountBasePath,
		Credentials:    result.Credentials,
		VaultSecretName: result.VaultSecretName,
	}

	if cfg.VaultSecretName != "" {
		log.Printf("[storage] FetchConfigByID: resolving vault secret %s", cfg.VaultSecretName)
		resolvedCreds, err := c.resolveVaultSecret(cfg.VaultSecretName)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve vault secret %s: %w", cfg.VaultSecretName, err)
		}
		secretName := cfg.VaultSecretName
		cfg.Credentials = resolvedCreds
		cfg.VaultSecretName = ""
		log.Printf("[storage] FetchConfigByID: resolved vault secret %s -> credentials", secretName)
	}

	if cfg.Credentials == nil && (result.StorageMode == "s3" || result.StorageMode == "object_storage") {
		return nil, fmt.Errorf("storage config unavailable: no credentials and storage_mode=%s", result.StorageMode)
	}

	return cfg, nil
}

func (c *StorageClient) resolveVaultSecret(secretName string) (*S3Credentials, error) {
	type VaultData struct {
		AccessKeyID     string `json:"access_key_id"`
		SecretAccessKey string `json:"secret_access_key"`
		SessionToken    string `json:"session_token,omitempty"`
	}
	type VaultResponse struct {
		Ok     bool       `json:"ok"`
		Error  string     `json:"error,omitempty"`
		Details string    `json:"details,omitempty"`
		Data   *VaultData `json:"data,omitempty"`
	}

	body := map[string]string{
		"org_id":     c.OrgID,
		"secret_name": secretName,
	}

	resp, err := c.doRequest("POST", "/functions/v1/decrypt_vault_secret", body)
	if err != nil {
		return nil, err
	}

	var result VaultResponse
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("failed to parse vault secret response: %w", err)
	}

	if !result.Ok {
		return nil, fmt.Errorf("vault secret decryption failed: %s (%s)", result.Error, result.Details)
	}

	if result.Data == nil || result.Data.AccessKeyID == "" || result.Data.SecretAccessKey == "" {
		return nil, fmt.Errorf("vault secret %s missing access_key_id or secret_access_key", secretName)
	}

	log.Printf("[storage] resolved vault credentials (access_key_id present: %t, session_token present: %t)",
		result.Data.AccessKeyID != "", result.Data.SessionToken != "")

	return &S3Credentials{
		AccessKeyID:     result.Data.AccessKeyID,
		SecretAccessKey: result.Data.SecretAccessKey,
		SessionToken:    result.Data.SessionToken,
	}, nil
}

func (c *StorageClient) doRequest(method, path string, payload interface{}) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	httpc := httpclient.NewClient(c.BaseURL, c.AnonKey, c.Token)

	var resp *http.Response
	if method == "POST" {
		resp, err = httpc.PostWithHeaders(context.Background(), path, body, func(r *http.Request) {
			r.Header.Set("x-device-id", c.DeviceID)
		})
	} else {
		resp, err = httpc.Get(context.Background(), path)
	}

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
	}

	return io.ReadAll(resp.Body)
}

func (c *StorageClient) GetDatasetPath(datasetID string) string {
	cfgMu.RLock()
	defer cfgMu.RUnlock()

	if cfg == nil {
		return ""
	}

	if cfg.StorageMode == "shared_mount" && cfg.MountBasePath != "" {
		return cfg.MountBasePath + "/datasets/" + datasetID
	}
	return ""
}

func (c *StorageClient) GetChunkPath(datasetID string, chunkIndex int) string {
	cfgMu.RLock()
	defer cfgMu.RUnlock()

	if cfg == nil {
		return ""
	}

	if cfg.StorageMode == "shared_mount" && cfg.MountBasePath != "" {
		return cfg.MountBasePath + "/datasets/" + datasetID + "/chunks/chunk_" + strconv.Itoa(chunkIndex) + ".bin"
	}
	return ""
}

func (c *StorageClient) GetResultPath(datasetID string, chunkIndex int) string {
	cfgMu.RLock()
	defer cfgMu.RUnlock()

	if cfg == nil {
		return ""
	}

	if cfg.StorageMode == "shared_mount" && cfg.MountBasePath != "" {
		return cfg.MountBasePath + "/datasets/" + datasetID + "/results/chunk_" + strconv.Itoa(chunkIndex) + ".out"
	}
	return ""
}

func (c *StorageClient) GetMergedPath(datasetID string) string {
	cfgMu.RLock()
	defer cfgMu.RUnlock()

	if cfg == nil {
		return ""
	}

	if cfg.StorageMode == "shared_mount" && cfg.MountBasePath != "" {
		return cfg.MountBasePath + "/datasets/" + datasetID + "/merged/dataset.parquet"
	}
	return ""
}

func (c *StorageClient) GetSourcePath(datasetID string) string {
	cfgMu.RLock()
	defer cfgMu.RUnlock()

	if cfg == nil {
		return ""
	}

	if cfg.StorageMode == "shared_mount" && cfg.MountBasePath != "" {
		return cfg.MountBasePath + "/datasets/" + datasetID + "/source"
	}
	return ""
}

func (c *StorageClient) GetStorageMode() string {
	cfgMu.RLock()
	defer cfgMu.RUnlock()

	if cfg == nil {
		return "shared_mount"
	}
	return cfg.StorageMode
}

func RefreshConfigPeriodic(ctx context.Context, client *StorageClient, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := client.FetchConfig(); err != nil {
				log.Printf("[storage] ⚠️ Failed to refresh config: %v", err)
			}
		}
	}
}
