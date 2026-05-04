package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"sentra-agent/internal/obs"
)

type EnvState string

const (
	EnvStatePending  EnvState = "pending"
	EnvStateReady    EnvState = "ready"
	EnvStateActive   EnvState = "active"
	EnvStateWarm     EnvState = "warm"
	EnvStateInvalid  EnvState = "invalid"
	EnvStateCleaning EnvState = "cleaning"
)

type CachedEnvironment struct {
	ID             string
	OrgID          string
	RuntimeType    RuntimeType
	RuntimeVersion string
	DependencyHash string
	Path           string
	State          EnvState
	UseCount       int
	LastUsed       time.Time
	CreatedAt      time.Time
	Platform       PlatformInfo
	mu             sync.RWMutex
}

type PlatformInfo struct {
	OS      string `json:"os"`
	Arch    string `json:"arch"`
	Python  string `json:"python,omitempty"`
	Node    string `json:"node,omitempty"`
	Bitness int    `json:"bitness"`
}

func GetCurrentPlatform() PlatformInfo {
	platform := PlatformInfo{
		OS:      runtime.GOOS,
		Arch:    runtime.GOARCH,
		Bitness: 64,
	}

	switch platform.OS {
	case "windows":
		platform.Python = detectPythonVersion("python")
		platform.Node = detectNodeVersion("node")
	case "darwin":
		platform.Python = detectPythonVersion("python3")
		platform.Node = detectNodeVersion("node")
	default:
		platform.Python = detectPythonVersion("python3")
		platform.Node = detectNodeVersion("node")
	}

	return platform
}

func detectPythonVersion(cmd string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cmds := []string{cmd, "python3", "python"}
	if runtime.GOOS == "windows" {
		cmds = []string{"py", "-3", cmd, "python3", "python"}
	}

	for _, c := range cmds {
		out, err := exec.CommandContext(ctx, c, "--version").Output()
		if err != nil {
			continue
		}
		version := string(out)
		version = strings.TrimSpace(version)
		if strings.HasPrefix(version, "Python ") {
			version = strings.TrimPrefix(version, "Python ")
			dotIdx := strings.Index(version, ".")
			if dotIdx > 0 {
				if _, err := strconv.ParseFloat(version[:dotIdx], 64); err == nil {
					return version
				}
			}
		}
	}
	return ""
}

func detectNodeVersion(cmd string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, cmd, "--version").Output()
	if err != nil {
		return ""
	}
	version := string(out)
	if len(version) > 1 {
		return version[1:]
	}
	return ""
}

type EnvironmentPool struct {
	mu               sync.RWMutex
	environments     map[string]*CachedEnvironment
	maxPoolSize      int
	warmTimeout      time.Duration
	maxEnvironments  int
	sandboxBase      string
	envLocks         sync.Map
	evictTicker      *time.Ticker
	stopEvict        chan struct{}
	wg               sync.WaitGroup
	platform         PlatformInfo
	remoteCache      RemoteCache
	healthCheckFn    func(path string) bool
	maxDiskBytes     int64
	currentDiskBytes int64
}

type RemoteCache interface {
	Upload(ctx context.Context, env *CachedEnvironment) error
	Download(ctx context.Context, key string, destPath string) error
	GetStoragePath(orgID string, rt RuntimeType, version string, hash string) string
}

type PoolConfig struct {
	MaxPoolSize     int
	MaxEnvironments int
	WarmTimeout     time.Duration
	SandboxBase     string
	RemoteCache     RemoteCache
	HealthCheck     func(path string) bool
	MaxDiskBytes    int64
}

func NewEnvironmentPoolWithConfig(cfg PoolConfig) *EnvironmentPool {
	platform := GetCurrentPlatform()

	if cfg.MaxPoolSize <= 0 {
		cfg.MaxPoolSize = getEnvInt("ENVIRONMENT_POOL_MAX_SIZE", 10)
	}
	if cfg.MaxEnvironments <= 0 {
		cfg.MaxEnvironments = getEnvInt("ENVIRONMENT_MAX_COUNT", 50)
	}
	if cfg.WarmTimeout <= 0 {
		cfg.WarmTimeout = getEnvDuration("ENVIRONMENT_WARM_TIMEOUT", 30*time.Minute)
	}
	if cfg.SandboxBase == "" {
		cfg.SandboxBase = "/tmp/sentra-runtime"
	}
	if cfg.MaxDiskBytes <= 0 {
		cfg.MaxDiskBytes = getEnvInt64("ENVIRONMENT_MAX_DISK_BYTES", 10*1024*1024*1024)
	}

	pool := &EnvironmentPool{
		environments:     make(map[string]*CachedEnvironment),
		maxPoolSize:      cfg.MaxPoolSize,
		warmTimeout:      cfg.WarmTimeout,
		maxEnvironments:  cfg.MaxEnvironments,
		sandboxBase:      cfg.SandboxBase,
		stopEvict:        make(chan struct{}),
		platform:         platform,
		remoteCache:      cfg.RemoteCache,
		healthCheckFn:    cfg.HealthCheck,
		maxDiskBytes:     cfg.MaxDiskBytes,
		currentDiskBytes: 0,
	}

	evictionInterval := getEnvDuration("ENVIRONMENT_EVICTION_INTERVAL", 5*time.Minute)
	go pool.evictionLoop(evictionInterval)

	obs.Info("environment_pool_initialized", obs.Field{
		"platform":      fmt.Sprintf("%s/%s", platform.OS, platform.Arch),
		"python":        platform.Python,
		"node":          platform.Node,
		"max_pool_size": cfg.MaxPoolSize,
		"warm_timeout":  cfg.WarmTimeout.String(),
	})

	return pool
}

func NewEnvironmentPool(sandboxBase string, maxPoolSize int, warmTimeout time.Duration) *EnvironmentPool {
	return NewEnvironmentPoolWithConfig(PoolConfig{
		MaxPoolSize: maxPoolSize,
		WarmTimeout: warmTimeout,
		SandboxBase: sandboxBase,
	})
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return defaultVal
}

func getEnvInt64(key string, defaultVal int64) int64 {
	if val := os.Getenv(key); val != "" {
		if intVal, err := strconv.ParseInt(val, 10, 64); err == nil {
			return intVal
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if duration, err := time.ParseDuration(val); err == nil {
			return duration
		}
	}
	return defaultVal
}

func (ep *EnvironmentPool) evictionLoop(interval time.Duration) {
	ep.wg.Add(1)
	defer ep.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ep.runEviction()
		case <-ep.stopEvict:
			return
		}
	}
}

func (ep *EnvironmentPool) runEviction() {
	ep.mu.Lock()
	defer ep.mu.Unlock()

	now := time.Now()
	ttl := ep.warmTimeout
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}

	evicted := 0
	var freedBytes int64

	type evictionCandidate struct {
		key  string
		path string
		size int64
		env  *CachedEnvironment
	}

	var candidates []evictionCandidate

	for key, env := range ep.environments {
		env.mu.Lock()
		if env.State == EnvStateWarm || env.State == EnvStateReady {
			if now.Sub(env.LastUsed) > ttl {
				envSize := calculateDirSize(env.Path)
				env.State = EnvStateCleaning
				candidates = append(candidates, evictionCandidate{key: key, path: env.Path, size: envSize, env: env})
				evicted++
				freedBytes += envSize
			}
		}
		env.mu.Unlock()
	}

	for _, cand := range candidates {
		go func(k string, p string, size int64, env *CachedEnvironment) {
			ep.mu.Lock()
			env.mu.Lock()
			if env.State != EnvStateCleaning {
				env.mu.Unlock()
				ep.mu.Unlock()
				return
			}
			env.mu.Unlock()

			if err := os.RemoveAll(p); err != nil {
				obs.Warn("environment_eviction_failed", obs.Field{
					"env_key": k,
					"error":   err.Error(),
				})
			}
			delete(ep.environments, k)
			ep.currentDiskBytes -= size
			if ep.currentDiskBytes < 0 {
				ep.currentDiskBytes = 0
			}
			ep.mu.Unlock()
		}(cand.key, cand.path, cand.size, cand.env)
	}

	if evicted > 0 {
		obs.Info("environments_evicted", obs.Field{
			"count":            evicted,
			"remaining":        len(ep.environments),
			"freed_bytes":      freedBytes,
			"current_disk_use": ep.currentDiskBytes,
		})
	}
}

func (ep *EnvironmentPool) StopEviction() {
	if ep.stopEvict != nil {
		close(ep.stopEvict)
	}
	ep.wg.Wait()
}

func (ep *EnvironmentPool) GetMetrics() map[string]interface{} {
	ep.mu.RLock()
	defer ep.mu.RUnlock()

	total := len(ep.environments)
	warm := 0
	ready := 0
	active := 0

	for _, env := range ep.environments {
		switch env.State {
		case EnvStateWarm:
			warm++
		case EnvStateReady:
			ready++
		case EnvStateActive:
			active++
		}
	}

	return map[string]interface{}{
		"total_environments":  total,
		"warm_environments":   warm,
		"ready_environments":  ready,
		"active_environments": active,
		"max_pool_size":       ep.maxPoolSize,
		"max_environments":    ep.maxEnvironments,
		"platform":            fmt.Sprintf("%s/%s", ep.platform.OS, ep.platform.Arch),
	}
}

func (ep *EnvironmentPool) CalculateDependencyHash(rt RuntimeType, deps []Dependency) string {
	// Match SQL calculate_dependency_hash function
	// SQL: encode(digest(COALESCE(p_runtime_type, 'native') || '|' || COALESCE(jsonb_pretty(p_runtime_dependencies), ''), 'sha256'), 'hex')
	data, _ := json.Marshal(deps)
	// Use jsonb_pretty equivalent by using json.Marshal then json.Unmarshal then json.Marshal for deterministic output
	var normalizedJSON json.RawMessage
	json.Unmarshal(data, &normalizedJSON)
	prettyData, _ := json.Marshal(normalizedJSON)
	content := fmt.Sprintf("%s|%s", rt, string(prettyData))
	hash := sha256.Sum256([]byte(content))
	return hex.EncodeToString(hash[:])
}

func (ep *EnvironmentPool) CalculateDependencyLockHash(rt RuntimeType, version string, deps []Dependency) string {
	data, _ := json.Marshal(deps)
	content := fmt.Sprintf("%s|%s|%s|%s", rt, version, ep.platform.OS, string(data))
	hash := sha256.Sum256([]byte(content))
	return hex.EncodeToString(hash[:])
}

func (ep *EnvironmentPool) GetEnvironmentKey(orgID string, rt RuntimeType, version string, depHash string) string {
	hashLen := 32
	if len(depHash) < 32 {
		hashLen = len(depHash)
	}
	return fmt.Sprintf("%s:%s:%s:%s:%s", orgID, rt, version, ep.platform.OS, depHash[:hashLen])
}

func (ep *EnvironmentPool) AcquireEnvironment(ctx context.Context, orgID string, rt RuntimeType, version string, depLockHash string, deps []Dependency) (*CachedEnvironment, error) {
	if depLockHash == "" {
		depLockHash = ep.CalculateDependencyLockHash(rt, version, deps)
	}
	key := ep.GetEnvironmentKey(orgID, rt, version, depLockHash)

	lockI, _ := ep.envLocks.LoadOrStore(key, &sync.Mutex{})
	lock := lockI.(*sync.Mutex)
	lock.Lock()
	defer lock.Unlock()

	ep.mu.Lock()
	if env, ok := ep.environments[key]; ok {
		env.mu.Lock()
		if env.State == EnvStateCleaning {
			env.mu.Unlock()
			ep.mu.Unlock()
			goto createNew
		}
		if env.State == EnvStateReady || env.State == EnvStateWarm {
			isValid := ep.isValidForRuntime(env.Path, rt)
			if !isValid {
				delete(ep.environments, key)
				envPath := env.Path
				env.mu.Unlock()
				ep.mu.Unlock()
				os.RemoveAll(envPath)
				goto createNew
			}
			if ep.healthCheckFn != nil && !ep.healthCheckFn(env.Path) {
				delete(ep.environments, key)
				envPath := env.Path
				env.mu.Unlock()
				ep.mu.Unlock()
				os.RemoveAll(envPath)
				goto createNew
			}
			env.State = EnvStateActive
			env.UseCount++
			env.LastUsed = time.Now()
			env.mu.Unlock()
			ep.mu.Unlock()
			obs.Info("environment_cache_hit", obs.Field{
				"env_key":   key,
				"use_count": env.UseCount,
				"platform":  fmt.Sprintf("%s/%s", ep.platform.OS, ep.platform.Arch),
			})
			return env, nil
		}
		env.mu.Unlock()
	}
	ep.mu.Unlock()

createNew:
	envStartTime := time.Now()

	hashPrefix := depLockHash
	if len(hashPrefix) > 16 {
		hashPrefix = depLockHash[:16]
	}
	envPath := filepath.Join(ep.sandboxBase, "env", orgID, string(rt), hashPrefix)

	if err := os.MkdirAll(envPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create environment path: %w", err)
	}

	if ep.remoteCache != nil {
		storagePath := ep.remoteCache.GetStoragePath(orgID, rt, version, depLockHash)
		if storagePath != "" {
			err := ep.remoteCache.Download(ctx, key, envPath)
			if err == nil && ep.isValidForRuntime(envPath, rt) {
				obs.Info("environment_downloaded_from_cache", obs.Field{
					"env_key":     key,
					"storage_key": storagePath,
				})
			}
		}
	}

	env := &CachedEnvironment{
		ID:             hashPrefix,
		OrgID:          orgID,
		RuntimeType:    rt,
		RuntimeVersion: version,
		DependencyHash: depLockHash,
		Path:           envPath,
		State:          EnvStatePending,
		CreatedAt:      time.Now(),
		LastUsed:       time.Now(),
		Platform:       ep.platform,
	}

	ep.mu.Lock()
	if len(ep.environments) >= ep.maxEnvironments {
		ep.mu.Unlock()
		ep.runEviction()
		ep.mu.Lock()
		if len(ep.environments) >= ep.maxEnvironments {
			ep.mu.Unlock()
			return nil, fmt.Errorf("environment pool exhausted")
		}
	}

	if ep.maxDiskBytes > 0 && ep.currentDiskBytes >= ep.maxDiskBytes {
		ep.mu.Unlock()
		ep.runEviction()
		ep.mu.Lock()
		if ep.currentDiskBytes >= ep.maxDiskBytes {
			ep.mu.Unlock()
			return nil, fmt.Errorf("environment pool disk limit exceeded")
		}
	}

	ep.environments[key] = env
	ep.mu.Unlock()

	envCreationTime := time.Since(envStartTime).Milliseconds()
	obs.Info("environment_created_cold_start", obs.Field{
		"env_key":          key,
		"creation_time_ms": envCreationTime,
		"platform":         fmt.Sprintf("%s/%s", ep.platform.OS, ep.platform.Arch),
	})

	return env, nil
}

func (ep *EnvironmentPool) ReleaseEnvironment(env *CachedEnvironment, keepWarm bool) {
	envSize := calculateDirSize(env.Path)

	ep.mu.Lock()
	env.mu.Lock()

	if env.State == EnvStateCleaning {
		env.mu.Unlock()
		ep.mu.Unlock()
		return
	}

	if keepWarm && time.Since(env.LastUsed) < ep.warmTimeout {
		env.State = EnvStateWarm
	} else {
		env.State = EnvStateReady
	}
	env.LastUsed = time.Now()

	ep.currentDiskBytes += envSize
	env.mu.Unlock()
	ep.mu.Unlock()
}

func (ep *EnvironmentPool) IsValidEnvironment(envPath string) bool {
	readyMarker := filepath.Join(envPath, ".ready")
	if _, err := os.Stat(readyMarker); err != nil {
		return false
	}
	return true
}

func (ep *EnvironmentPool) IsValidPythonEnvironment(envPath string) bool {
	if !ep.IsValidEnvironment(envPath) {
		return false
	}

	switch runtime.GOOS {
	case "windows":
		venvPath := filepath.Join(envPath, "venv")
		if _, err := os.Stat(venvPath); err != nil {
			return false
		}
		pythonPath := filepath.Join(venvPath, "Scripts", "python.exe")
		if _, err := os.Stat(pythonPath); err != nil {
			return false
		}
	default:
		venvPath := filepath.Join(envPath, "venv")
		if _, err := os.Stat(venvPath); err != nil {
			return false
		}
		binPath := "bin"
		if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
			binPath = "bin"
		}
		pythonPath := filepath.Join(venvPath, binPath, "python")
		if _, err := os.Stat(pythonPath); err != nil {
			return false
		}
	}
	return true
}

func (ep *EnvironmentPool) IsValidNodeEnvironment(envPath string) bool {
	if !ep.IsValidEnvironment(envPath) {
		return false
	}

	packageFile := filepath.Join(envPath, "package.json")
	if _, err := os.Stat(packageFile); err != nil {
		return false
	}

	nodeModulesPath := filepath.Join(envPath, "node_modules")
	if _, err := os.Stat(nodeModulesPath); err != nil {
		return false
	}

	return true
}

func (ep *EnvironmentPool) isValidForRuntime(envPath string, rt RuntimeType) bool {
	switch rt {
	case RuntimePython:
		return ep.IsValidPythonEnvironment(envPath)
	case RuntimeNode:
		return ep.IsValidNodeEnvironment(envPath)
	case RuntimeNative:
		// Native runtime: just check that the path exists and is valid
		return ep.IsValidEnvironment(envPath)
	default:
		// Unknown runtime type: do thorough validation
		// Check .ready marker file exists
		if !ep.IsValidEnvironment(envPath) {
			return false
		}
		// Also check that the runtime type directory exists
		rtDir := filepath.Join(envPath, string(rt))
		if _, err := os.Stat(rtDir); os.IsNotExist(err) {
			return false
		}
		return true
	}
}

func (ep *EnvironmentPool) InvalidateEnvironment(env *CachedEnvironment, reason string) {
	env.mu.Lock()
	env.State = EnvStateInvalid
	env.mu.Unlock()

	ep.mu.Lock()
	defer ep.mu.Unlock()

	envPath := env.Path
	for key, e := range ep.environments {
		if e.ID == env.ID {
			delete(ep.environments, key)
			obs.Info("environment_invalidated", obs.Field{
				"env_key": key,
				"reason":  reason,
			})
		}
	}

	go func() {
		if envPath != "" {
			if err := os.RemoveAll(envPath); err != nil {
				obs.Warn("environment_cleanup_failed", obs.Field{
					"env_path": envPath,
					"error":    err.Error(),
				})
			}
		}
	}()
}

func (ep *EnvironmentPool) UploadToRemoteCache(ctx context.Context, env *CachedEnvironment) error {
	if ep.remoteCache == nil {
		return nil
	}
	return ep.remoteCache.Upload(ctx, env)
}

func (ep *EnvironmentPool) Shutdown(ctx context.Context) error {
	ep.StopEviction()

	ep.mu.Lock()
	defer ep.mu.Unlock()

	var errs []error
	for key, env := range ep.environments {
		env.mu.Lock()
		if env.State == EnvStateActive || env.State == EnvStateCleaning {
			env.mu.Unlock()
			continue
		}
		if env.State == EnvStateWarm || env.State == EnvStateReady {
			env.mu.Unlock()
			continue
		}
		env.mu.Unlock()
		if err := os.RemoveAll(env.Path); err != nil {
			errs = append(errs, fmt.Errorf("failed to remove %s: %w", key, err))
		}
		delete(ep.environments, key)
	}

	ep.environments = make(map[string]*CachedEnvironment)

	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %v", errs)
	}
	return nil
}

type Runtime interface {
	Type() RuntimeType
	Setup(ctx context.Context, spec RuntimeSpec, envPath string) error
	InstallDeps(ctx context.Context, deps []Dependency, envPath string) error
	Run(ctx context.Context, input ExecutionInput, envPath string) (*ExecutionOutput, *ExecutionMetrics, error)
	Cleanup(ctx context.Context, envPath string) error
	IsInstalled(ctx context.Context, envPath string) bool
	Version(ctx context.Context) (string, error)
}

type RuntimeManager struct {
	runtimes    map[RuntimeType]Runtime
	envPool     *EnvironmentPool
	sandboxPath string
	timeout     time.Duration
	orgID       string
}

type RuntimeManagerOption func(*RuntimeManager)

func WithOrgID(orgID string) RuntimeManagerOption {
	return func(m *RuntimeManager) {
		m.orgID = orgID
	}
}

func WithRemoteCache(cache RemoteCache) RuntimeManagerOption {
	return func(m *RuntimeManager) {
		m.envPool.remoteCache = cache
	}
}

func NewRuntimeManager(sandboxPath string, defaultTimeout time.Duration, opts ...RuntimeManagerOption) *RuntimeManager {
	pool := NewEnvironmentPoolWithConfig(PoolConfig{
		SandboxBase: sandboxPath,
		MaxPoolSize: 5,
		WarmTimeout: 30 * time.Minute,
	})

	mgr := &RuntimeManager{
		runtimes:    make(map[RuntimeType]Runtime),
		sandboxPath: sandboxPath,
		timeout:     defaultTimeout,
		envPool:     pool,
	}

	for _, opt := range opts {
		opt(mgr)
	}

	mgr.runtimes[RuntimePython] = NewPythonRuntime("")
	mgr.runtimes[RuntimeNode] = NewNodeRuntime("")

	return mgr
}

func (rm *RuntimeManager) RegisterRuntime(rt Runtime) {
	rm.runtimes[rt.Type()] = rt
}

func (rm *RuntimeManager) GetRuntime(t RuntimeType) (Runtime, error) {
	rt, ok := rm.runtimes[t]
	if !ok {
		return nil, fmt.Errorf("runtime type %s not registered", t)
	}
	return rt, nil
}

func (rm *RuntimeManager) ExecuteWithMetrics(ctx context.Context, spec RuntimeSpec, input ExecutionInput, orgID string) (*ExecutionResult, error) {
	startTime := time.Now()

	rt, err := rm.GetRuntime(spec.Type)
	if err != nil {
		return &ExecutionResult{
			Output: ExecutionOutput{Success: false, Error: err.Error()},
		}, err
	}

	env, err := rm.envPool.AcquireEnvironment(ctx, orgID, spec.Type, spec.Version, spec.DependencyLockHash, spec.Dependencies)
	if err != nil {
		return &ExecutionResult{
			Output: ExecutionOutput{Success: false, Error: fmt.Sprintf("environment acquisition failed: %v", err)},
		}, err
	}

	platform := rm.envPool.platform
	obs.Info("runtime_execution_start", obs.Field{
		"org_id":    orgID,
		"runtime":   spec.Type,
		"version":   spec.Version,
		"env_path":  env.Path,
		"dep_count": len(spec.Dependencies),
		"platform":  fmt.Sprintf("%s/%s", platform.OS, platform.Arch),
	})

	setupStart := time.Now()
	if err := rt.Setup(ctx, spec, env.Path); err != nil {
		rm.envPool.InvalidateEnvironment(env, fmt.Sprintf("setup failed: %v", err))
		return &ExecutionResult{
			Output: ExecutionOutput{Success: false, Error: fmt.Sprintf("setup failed: %v", err)},
		}, err
	}
	setupTime := time.Since(setupStart).Milliseconds()

	var depInstallTime int64
	if len(spec.Dependencies) > 0 {
		depStart := time.Now()
		if err := rt.InstallDeps(ctx, spec.Dependencies, env.Path); err != nil {
			rm.envPool.InvalidateEnvironment(env, fmt.Sprintf("dependency install failed: %v", err))
			return &ExecutionResult{
				Output: ExecutionOutput{Success: false, Error: fmt.Sprintf("dependency installation failed: %v", err)},
			}, err
		}
		depInstallTime = time.Since(depStart).Milliseconds()
	}

	execStart := time.Now()
	output, metrics, err := rt.Run(ctx, input, env.Path)
	execTime := time.Since(execStart).Milliseconds()

	cacheHit := env.UseCount > 1
	metrics.CacheHit = cacheHit

	if output == nil {
		output = &ExecutionOutput{
			Success: false,
			Error:   "runtime execution returned nil output",
		}
	}

	if err != nil && output.Success == false {
		rm.envPool.InvalidateEnvironment(env, fmt.Sprintf("execution failed: %v", err))
		metrics.FailureType = classifyError(err)
	}

	totalTime := time.Since(startTime).Milliseconds()
	metrics.SetupTimeMs = setupTime
	metrics.DependencyInstallMs = depInstallTime
	metrics.ExecutionTimeMs = execTime
	metrics.TotalTimeMs = totalTime
	metrics.EnvKey = rm.envPool.GetEnvironmentKey(orgID, spec.Type, spec.Version, spec.DependencyLockHash)

	keepWarm := true
	if !output.Success {
		keepWarm = false
	}

	rm.envPool.ReleaseEnvironment(env, keepWarm)

	go func() {
		if env.UseCount >= 3 {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			rm.envPool.UploadToRemoteCache(ctx, env)
		}
	}()

	result := &ExecutionResult{
		Output:    *output,
		Metrics:   *metrics,
		Duration:  time.Since(startTime),
		CleanedUp: !keepWarm,
	}

	obs.Info("runtime_execution_complete", obs.Field{
		"org_id":            orgID,
		"runtime":           spec.Type,
		"success":           output.Success,
		"cache_hit":         cacheHit,
		"setup_time_ms":     setupTime,
		"install_time_ms":   depInstallTime,
		"execution_time_ms": execTime,
		"total_time_ms":     totalTime,
		"max_memory_bytes":  metrics.MaxMemoryBytes,
		"exit_code":         metrics.ExitCode,
		"failure_type":      metrics.FailureType,
		"platform":          fmt.Sprintf("%s/%s", platform.OS, platform.Arch),
	})

	if output.Success && output.Data != nil {
		if items, ok := output.Data["items_processed"].(float64); ok && result.Duration.Seconds() > 0 {
			result.Throughput = items / result.Duration.Seconds()
		}
	}

	return result, nil
}

func classifyError(err error) string {
	if err == nil {
		return ""
	}
	errStr := err.Error()
	switch {
	case contains(errStr, "connection refused"), contains(errStr, "timeout"), contains(errStr, "network"):
		return "infra_error"
	case contains(errStr, "no module"), contains(errStr, "cannot find module"), contains(errStr, "dependency"), contains(errStr, "install"):
		return "dependency_error"
	case contains(errStr, "syntax"), contains(errStr, "indentation"), contains(errStr, "nameerror"), contains(errStr, "typeerror"):
		return "user_code_error"
	case contains(errStr, "memory"), contains(errStr, "oom"):
		return "memory_error"
	default:
		return "unknown_error"
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s[:len(substr)] == substr || (len(s) > len(substr) && containsAny(s, substr)))
}

func containsAny(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func calculateDirSize(path string) int64 {
	var size int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		size += info.Size()
		return nil
	})
	return size
}

func (rm *RuntimeManager) Execute(ctx context.Context, spec RuntimeSpec, input ExecutionInput) (*ExecutionResult, error) {
	return rm.ExecuteWithMetrics(ctx, spec, input, rm.orgID)
}

func (rm *RuntimeManager) SupportedRuntimes() []RuntimeType {
	types := make([]RuntimeType, 0, len(rm.runtimes))
	for t := range rm.runtimes {
		types = append(types, t)
	}
	return types
}

func (rm *RuntimeManager) GetEnvironmentPool() *EnvironmentPool {
	return rm.envPool
}

func (rm *RuntimeManager) Shutdown(ctx context.Context) error {
	return rm.envPool.Shutdown(ctx)
}
