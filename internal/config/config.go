package config

import (
	"context"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/joho/godotenv"
)

// Config holds runtime configuration.
// Identity (DeviceID + Token) is injected by main.go ONLY.
type Config struct {
	// --- Backend Control Plane ---
	BackendURL     string
	BackendAnonKey string

	// --- Org ---
	OrgID   string
	OrgName string

	// --- Agent ---
	EnvironmentType string
	StorageType     string
	LogLevel        string
	DeviceName      string
	Capabilities    []string

	// --- Claim Code (for cloud deployment) ---
	ClaimCode string

	// --- Health Check Server ---
	HealthCheckPort string

	// --- Runtime identity (injected later) ---
	DeviceID string
	Token    string

	// --- Concurrency ---
	MaxConcurrency       int32
	MaxConcurrencyAtomic atomic.Int32

	ctx context.Context
}

func LoadStatic() *Config {
	_ = godotenv.Load(".env")

	backendURL := strings.TrimRight(
		firstNonEmpty(
			os.Getenv("BACKEND_URL"),
			os.Getenv("SENTRA_BACKEND_URL"),
			os.Getenv("APP_SUPABASE_URL"),
			os.Getenv("SUPABASE_URL"),
			DefaultBackendURL,
		),
		"/",
	)

	if backendURL == "" {
		log.Fatal("❌ Backend URL not configured. Set BACKEND_URL or rebuild with -ldflags.")
	}

	anonKey := firstNonEmpty(
		os.Getenv("BACKEND_ANON_KEY"),
		os.Getenv("SENTRA_BACKEND_ANON_KEY"),
		os.Getenv("SUPABASE_ANON_KEY"),
		os.Getenv("APP_SUPABASE_ANON_KEY"),
		DefaultAnonKey,
	)

	// Note: anonKey can be empty - will be provided by claim_device response if using cloud

	cfg := &Config{
		BackendURL:     backendURL,
		BackendAnonKey: anonKey,

		OrgID:   os.Getenv("ORG_ID"),
		OrgName: getenvDefault("ORG_NAME", "Sentra Org"),

		EnvironmentType: getenvDefault("AGENT_ENVIRONMENT_TYPE", "local"),
		StorageType:     getenvDefault("AGENT_STORAGE_TYPE", "local"),
		LogLevel:        getenvDefault("LOG_LEVEL", "info"),
		DeviceName:      getenvDefault("AGENT_NAME", hostnameFallback()),

		Capabilities: []string{"scan", "merge", "embedding"},

		ClaimCode:       os.Getenv("CLAIM_CODE"),
		HealthCheckPort: getenvDefault("HEALTH_CHECK_PORT", "8080"),
	}

	if v := os.Getenv("MAX_CONCURRENCY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.MaxConcurrency = int32(n)
		}
	}
	if cfg.MaxConcurrency == 0 {
		cfg.MaxConcurrency = int32(runtime.NumCPU() / 2)
		if cfg.MaxConcurrency < 1 {
			cfg.MaxConcurrency = 1
		}
	}
	cfg.MaxConcurrencyAtomic.Store(cfg.MaxConcurrency)

	log.Println("⚙️ Configuration loaded")
	log.Printf("   • Backend URL: %s", cfg.BackendURL)
	log.Printf("   • Org: %s (%s)", cfg.OrgName, cfg.OrgID)
	log.Printf("   • Environment: %s", cfg.EnvironmentType)
	log.Printf("   • Storage: %s", cfg.StorageType)
	log.Printf("   • Initial concurrency: %d", cfg.MaxConcurrency)
	log.Printf("   • Health check port: %s", cfg.HealthCheckPort)
	if cfg.ClaimCode != "" {
		log.Printf("   • Claim code: provided via env")
	}

	return cfg
}

func (c *Config) Context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}

// -------- Helpers --------

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func hostnameFallback() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "sentra-agent"
	}
	return h
}
