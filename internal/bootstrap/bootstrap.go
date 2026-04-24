package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"sentra-agent/internal/obs"
)

type Config struct {
	BackendURL      string   `json:"backend_url"`
	BackendAnonKey  string   `json:"backend_anon_key"`
	DeviceID        string   `json:"device_id"`
	OrgID           string   `json:"org_id"`
	DeviceToken     string   `json:"device_token"`
	DeviceName      string   `json:"device_name"`
	EnvironmentType string   `json:"environment_type"`
	StorageType     string   `json:"storage_type"`
	Capabilities    []string `json:"capabilities"`
	MaxWorkers      int      `json:"max_workers"`
	RedisURL        string   `json:"redis_url"`
	PluginRegistry  string   `json:"plugin_registry"`
}

type ClaimResponse struct {
	ID         string `json:"id"`
	OrgID      string `json:"org_id"`
	Token      string `json:"token"`
	BackendURL string `json:"backend_url"`
	AnonKey    string `json:"anon_key"`
}

type HealthPolicyResponse struct {
	OK         bool   `json:"ok"`
	MaxWorkers int    `json:"max_workers"`
	RedisURL   string `json:"redis_url"`
	Error      string `json:"error,omitempty"`
}

const (
	FuncClaimDevice  = "/functions/v1/claim_device"
	FuncHealthPolicy = "/functions/v1/agent_health_policy"
	ConfigFileName   = ".sentra_config.enc"
)

func FetchConfig(ctx context.Context, claimCode, deviceName string) (*Config, error) {
	obs.Info("fetching config from claim code", obs.Field{"claim_code": claimCode[:8] + "..."})

	backendURL, anonKey := detectBackendFromEnv()
	if backendURL == "" {
		backendURL = "https://app.sentra.ai"
		anonKey = os.Getenv("SUPABASE_ANON_KEY")
	}

	client := &http.Client{Timeout: 30 * time.Second}

	claimed, err := claimDevice(ctx, client, backendURL, anonKey, claimCode, deviceName)
	if err != nil {
		return nil, fmt.Errorf("claim device failed: %w", err)
	}

	cfg := &Config{
		BackendURL:      claimed.BackendURL,
		BackendAnonKey:  claimed.AnonKey,
		DeviceID:        claimed.ID,
		OrgID:           claimed.OrgID,
		DeviceToken:     claimed.Token,
		DeviceName:      deviceName,
		EnvironmentType: "linux",
		StorageType:     "s3",
		Capabilities:    []string{"python", "docker"},
	}

	policy, err := fetchHealthPolicy(ctx, client, cfg)
	if err != nil {
		obs.Warn("health policy fetch failed, using defaults", obs.Field{"error": err.Error()})
		cfg.MaxWorkers = 4
	} else {
		cfg.MaxWorkers = policy.MaxWorkers
		cfg.RedisURL = policy.RedisURL
	}

	if err := saveConfig(cfg); err != nil {
		obs.Warn("failed to save config", obs.Field{"error": err.Error()})
	}

	return cfg, nil
}

func claimDevice(ctx context.Context, client *http.Client, backendURL, anonKey, claimCode, deviceName string) (*ClaimResponse, error) {
	body := fmt.Sprintf(`{"claim_code":"%s","device_name":"%s","environment_type":"linux","capabilities":["python","docker"]}`,
		claimCode, deviceName)

	req, err := http.NewRequestWithContext(ctx, "POST", backendURL+FuncClaimDevice, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("apikey", anonKey)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("claim failed: HTTP %d - %s", resp.StatusCode, string(respBody))
	}

	var result ClaimResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response failed: %w", err)
	}

	if result.ID == "" || result.Token == "" {
		return nil, fmt.Errorf("invalid claim response: missing id or token")
	}

	if result.BackendURL == "" {
		result.BackendURL = backendURL
	}
	if result.AnonKey == "" {
		result.AnonKey = anonKey
	}

	return &result, nil
}

func fetchHealthPolicy(ctx context.Context, client *http.Client, cfg *Config) (*HealthPolicyResponse, error) {
	body := fmt.Sprintf(`{"device_id":"%s","total_cpu_cores":%d,"memory_free_gb":%d}`,
		cfg.DeviceID, getCPUCores(), getMemoryGB())

	req, err := http.NewRequestWithContext(ctx, "POST", cfg.BackendURL+FuncHealthPolicy, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.DeviceToken)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("health policy failed: HTTP %d", resp.StatusCode)
	}

	var result HealthPolicyResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

func LoadConfig(ctx context.Context) (*Config, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("cannot find home dir: %w", err)
	}

	configPath := filepath.Join(home, ".sentra", ConfigFileName)
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("no config found, need to claim")
		}
		return nil, err
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config corrupted: %w", err)
	}

	return &cfg, nil
}

func saveConfig(cfg *Config) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	dir := filepath.Join(home, ".sentra")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}

	configPath := filepath.Join(dir, ConfigFileName)
	return os.WriteFile(configPath, data, 0600)
}

func detectBackendFromEnv() (string, string) {
	backendURL := os.Getenv("SENTRA_BACKEND_URL")
	if backendURL == "" {
		backendURL = os.Getenv("SUPABASE_URL")
	}
	if backendURL == "" {
		backendURL = os.Getenv("BACKEND_URL")
	}

	anonKey := os.Getenv("SENTRA_BACKEND_ANON_KEY")
	if anonKey == "" {
		anonKey = os.Getenv("SUPABASE_ANON_KEY")
	}
	if anonKey == "" {
		anonKey = os.Getenv("BACKEND_ANON_KEY")
	}

	return backendURL, anonKey
}

func getCPUCores() int {
	return runtime.NumCPU()
}

func getMemoryGB() int {
	return int(totalMemoryGB())
}

var _ func() int

func totalMemoryGB() float64 {
	return 8.0
}

var (
	_ = runtime.NumCPU
)
