package sandbox

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"sentra-agent/internal/obs"
)

type PluginManifest struct {
	Name      string          `json:"name"`
	Language  string          `json:"language,omitempty"`
	Network   bool            `json:"network"`
	Resources PluginResources `json:"resources"`
}

type PluginResources struct {
	MemoryMB       int64   `json:"memory_mb"`
	CPUSeconds     int64   `json:"cpu_seconds"`
	CPULimit       float64 `json:"cpu_limit,omitempty"`
	TimeoutSeconds int64   `json:"timeout_seconds"`
	RequiresGPU    bool    `json:"requires_gpu,omitempty"`
	GPUMemoryMB    int64   `json:"gpu_memory_mb,omitempty"`
}

type SandboxConfig struct {
	Mode             string              `json:"mode"`
	DefaultMemoryMB  int64               `json:"default_memory_mb"`
	DefaultTimeoutS  int64               `json:"default_timeout_s"`
	MaxMemoryMB      int64               `json:"max_memory_mb"`
	MaxTimeoutS      int64               `json:"max_timeout_s"`
	TempDir          string              `json:"temp_dir"`
	NetworkDefault   bool                `json:"network_default"`
	LinuxNamespaces  bool                `json:"linux_namespaces"`
	MacOSSeatbelt    bool                `json:"macos_seatbelt"`
	WindowsJobObject bool                `json:"windows_job_objects"`
	Cgroupsv2Path    string              `json:"cgroupsv2_path"`
	SeccompProfile   string              `json:"seccomp_profile"`
	SandboxUID       string              `json:"sandbox_uid"`
	SandboxGID       string              `json:"sandbox_gid"`
	Capabilities     PlatformCapabilities `json:"-"`
}

type SandboxEnv struct {
	WorkDir  string
	Config   SandboxConfig
	Manifest PluginManifest
	Network  bool
	Platform string
	Cleanup  func()
}

type Sandboxer interface {
	Prepare(ctx context.Context, jobID string, manifest PluginManifest, network bool) (*SandboxEnv, error)
	Execute(ctx context.Context, env *SandboxEnv, cmd *exec.Cmd) error
	Destroy(ctx context.Context, env *SandboxEnv) error
}

func LoadConfig() SandboxConfig {
	cgroupPath := getEnv("SANDBOX_CGROUPS_PATH", "")
	if cgroupPath == "" {
		cgroupPath = detectCgroupsPath()
	}
	cfg := SandboxConfig{
		Mode:             getEnv("SANDBOX_MODE", detectBestMode()),
		DefaultMemoryMB:  getEnvInt64("SANDBOX_DEFAULT_MEMORY_MB", detectSystemMemoryMB()/4),
		DefaultTimeoutS:  getEnvInt64("SANDBOX_DEFAULT_TIMEOUT_S", 300),
		MaxMemoryMB:      getEnvInt64("SANDBOX_MAX_MEMORY_MB", detectSystemMemoryMB()/2),
		MaxTimeoutS:      getEnvInt64("SANDBOX_MAX_TIMEOUT_S", 3600),
		TempDir:          getEnv("SANDBOX_TEMP_DIR", detectTempDir()),
		NetworkDefault:   getEnvBool("SANDBOX_NETWORK_DEFAULT", false),
		LinuxNamespaces:  getEnvBool("SANDBOX_LINUX_NAMESPACES", true),
		MacOSSeatbelt:    getEnvBool("SANDBOX_MACOS_SEATBELT", true),
		WindowsJobObject: getEnvBool("SANDBOX_WINDOWS_JOB_OBJECT", true),
		Cgroupsv2Path:    cgroupPath,
		SeccompProfile:   getEnv("SANDBOX_SECCOMP_PROFILE", "default"),
		SandboxUID:       getEnv("SANDBOX_UID", "65534"),
		SandboxGID:       getEnv("SANDBOX_GID", "65534"),
	}
	cfg.Capabilities = DetectCapabilities(cfg)
	obs.Info("sandbox config loaded", obs.Field{
		"cgroup_path":      cfg.Cgroupsv2Path,
		"detected_cgroup":  cfg.Capabilities.CgroupPath,
		"has_cgroup_write": cfg.Capabilities.HasCgroupWrite,
		"mode":             cfg.Mode,
	})
	return cfg
}

func New(cfg SandboxConfig) Sandboxer {
	switch cfg.Mode {
	case "off":
		return &noopSandbox{cfg: cfg}
	default:
		return newPlatformSandbox(cfg)
	}
}

type noopSandbox struct {
	cfg SandboxConfig
}

func (s *noopSandbox) Prepare(ctx context.Context, jobID string, manifest PluginManifest, network bool) (*SandboxEnv, error) {
	workDir := filepath.Join(getEnv("SANDBOX_TEMP_DIR", s.cfg.TempDir), jobID)
	os.MkdirAll(workDir, 0700)
	return &SandboxEnv{
		WorkDir:  workDir,
		Config:   s.cfg,
		Manifest: manifest,
		Network:  network,
		Platform: runtime.GOOS,
		Cleanup:  func() { os.RemoveAll(workDir) },
	}, nil
}

func (s *noopSandbox) Execute(ctx context.Context, env *SandboxEnv, cmd *exec.Cmd) error {
	return cmd.Run()
}

func (s *noopSandbox) Destroy(ctx context.Context, env *SandboxEnv) error {
	if env.Cleanup != nil {
		env.Cleanup()
	}
	return nil
}

func detectBestMode() string {
	switch runtime.GOOS {
	case "linux", "darwin":
		return "native"
	case "windows":
		if v := os.Getenv("SANDBOX_WINDOWS_JOB_OBJECT"); v == "true" {
			return "native"
		}
		return "off"
	}
	return "off"
}

func detectSystemMemoryMB() int64 {
	switch runtime.GOOS {
	case "linux":
		if data, err := os.ReadFile("/proc/meminfo"); err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(line, "MemTotal:") {
					fields := strings.Fields(line)
					if len(fields) >= 2 {
						if kb, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
							return kb / 1024
						}
					}
				}
			}
		}
	case "darwin":
		if data, err := exec.Command("sysctl", "-n", "hw.memsize").Output(); err == nil {
			val := strings.TrimSpace(string(data))
			if bytes, err := strconv.ParseInt(val, 10, 64); err == nil {
				return bytes / 1024 / 1024
			}
		}
	}
	return 2048
}

func detectTempDir() string {
	for _, d := range []string{os.Getenv("SANDBOX_TEMP_DIR"), os.Getenv("TMPDIR"), os.Getenv("TEMP"), "/tmp"} {
		if d != "" {
			return strings.TrimRight(d, "/")
		}
	}
	return "/tmp"
}

func detectCgroupsPath() string {
	if _, err := os.Stat("/sys/fs/cgroup"); err == nil {
		return "/sys/fs/cgroup"
	}
	return ""
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		return v == "true" || v == "1" || v == "yes"
	}
	return def
}
