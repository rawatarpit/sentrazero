package plugin

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"sentra-agent/internal/auth"
	"sentra-agent/internal/httpclient"
)

const (
	FunctionListPlugins = "/functions/v1/list_plugins_for_org"
)

type DBPlugin struct {
	ID                string          `json:"id"`
	Name              string          `json:"name"`
	Version           string          `json:"version"`
	Language          string          `json:"language"`
	PluginType        string          `json:"plugin_type"`
	StoragePath       string          `json:"storage_path"`
	Checksum          string          `json:"checksum"`
	Signature         string          `json:"signature"`
	SignatureKeyID    string          `json:"signature_key_id"`
	Resources         json.RawMessage `json:"resources"`
	Trusted           bool            `json:"trusted"`
	RolloutPercentage int             `json:"rollout_percentage"`
	SignedURL         string          `json:"signed_url"`
	OS                string          `json:"os"`
	Arch              string          `json:"arch"`
	PluginGroup       string          `json:"plugin_group"`
	Network           bool            `json:"network"`
}

type PluginListResponse struct {
	Plugins  []DBPlugin `json:"plugins"`
	OrgID    string     `json:"org_id"`
	DeviceID string     `json:"device_id"`
	Source   string     `json:"source"`
}

func FetchPluginsFromAPI(ctx context.Context, deviceID, orgID, backendURL, anonKey, deviceToken string) ([]DBPlugin, error) {
	httpc := httpclient.NewClient(backendURL, anonKey, deviceToken,
		httpclient.WithTimeout(30*time.Second),
	)

	resp, err := httpc.Get(ctx, FunctionListPlugins)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("plugin fetch failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result PluginListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	log.Printf("[plugin] Fetched %d plugins from API for org %s", len(result.Plugins), orgID)
	return result.Plugins, nil
}

func ShouldRunPlugin(deviceID string, rolloutPercentage int) bool {
	if rolloutPercentage >= 100 {
		return true
	}
	if rolloutPercentage <= 0 {
		return false
	}

	hash := sha256.Sum256([]byte(deviceID))
	hashValue := int(hash[0]) | (int(hash[1]) << 8)
	return (hashValue % 100) < rolloutPercentage
}

func SyncPluginsFromAPI(ctx context.Context) error {
	log.Printf("🔄 [plugin] Fetching plugins from database API...")

	device, token, err := auth.LoadIdentity()
	if err != nil {
		return fmt.Errorf("failed to load device identity: %w", err)
	}

	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	plugins, err := FetchPluginsFromAPI(ctx, device.DeviceID, device.OrgID, cfg.BackendURL, cfg.BackendAnonKey, token)
	if err != nil {
		return fmt.Errorf("failed to fetch plugins from API: %w", err)
	}

	if len(plugins) == 0 {
		log.Println("ℹ️  No plugins returned from API.")
		return nil
	}

	dir, err := EnsurePluginDir()
	if err != nil {
		return fmt.Errorf("EnsurePluginDir failed: %w", err)
	}

	downloaded := 0
	skipped := 0

	for _, p := range plugins {
		if !ShouldRunPlugin(device.DeviceID, p.RolloutPercentage) {
			log.Printf("⏭️  Skipping plugin %s (rollout %d%%)", p.Name, p.RolloutPercentage)
			skipped++
			continue
		}

		if p.SignedURL == "" {
			log.Printf("⚠️  Plugin %s has no signed URL, skipping", p.Name)
			continue
		}

		manifest := Manifest{
			Name:           p.Name,
			Version:        p.Version,
			Filename:       filepath.Base(p.StoragePath),
			URL:            p.SignedURL,
			Checksum:       p.Checksum,
			PluginType:     p.PluginType,
			Language:       p.Language,
			Trusted:        p.Trusted,
			Network:        p.Network,
			Resources:      PluginResources{},
			Signature:      p.Signature,
			SignatureKeyID: p.SignatureKeyID,
		}

		if len(p.Resources) > 0 {
			if err := json.Unmarshal(p.Resources, &manifest.Resources); err != nil {
				log.Printf("⚠️  Failed to parse resources for %s: %v", p.Name, err)
			}
		}

		pluginDir := filepath.Join(dir, p.Name, p.OS+"-"+p.Arch)
		if err := os.MkdirAll(pluginDir, 0700); err != nil {
			log.Printf("⚠️ Failed to create plugin directory for %s: %v", p.Name, err)
			continue
		}

		localManifestPath := filepath.Join(pluginDir, p.Name+".json")
		data, _ := json.MarshalIndent(manifest, "", " ")

		if err := os.WriteFile(localManifestPath+".tmp", data, 0600); err != nil {
			log.Printf("⚠️  Failed to save manifest for %s: %v", p.Name, err)
			continue
		}
		os.Rename(localManifestPath+".tmp", localManifestPath)

		outPath := filepath.Join(pluginDir, manifest.Filename)
		if st, err := os.Stat(outPath); err == nil && st.Size() > 0 {
			if p.Checksum != "" {
				if err := verifyFileSHA256(outPath, p.Checksum); err != nil {
					log.Printf("⚠️ Cached plugin %s failed checksum verification: %v - re-downloading", p.Name, err)
					os.Remove(outPath)
				} else {
					log.Printf("⏭️ Plugin %s already cached and verified", p.Name)
					downloaded++
					continue
				}
			} else {
				log.Printf("⏭️ Plugin %s already cached (no checksum)", p.Name)
				downloaded++
				continue
			}
		}

		if err := downloadWithRetries(ctx, p.SignedURL, outPath); err != nil {
			log.Printf("⚠️  Failed to download plugin %s: %v", p.Name, err)
			continue
		}

		if p.Checksum != "" {
			if err := verifyFileSHA256(outPath, p.Checksum); err != nil {
				log.Printf("⚠️  Plugin %s checksum verification failed: %v", p.Name, err)
				os.Remove(outPath)
				continue
			}
		}

		downloaded++
		log.Printf("✅ Synced plugin: %s (version: %s)", p.Name, p.Version)
	}

	log.Printf("📦 Plugin sync complete: %d downloaded, %d skipped, %d total", downloaded, skipped, len(plugins))
	return nil
}

func loadConfig() (*Config, error) {
	backendURL := firstNonEmpty(
		os.Getenv("BACKEND_URL"),
		os.Getenv("SENTRA_BACKEND_URL"),
		os.Getenv("APP_SUPABASE_URL"),
		os.Getenv("SUPABASE_URL"),
	)
	if backendURL == "" {
		return nil, fmt.Errorf("backend URL not configured: set SENTRA_BACKEND_URL")
	}

	anonKey := firstNonEmpty(
		os.Getenv("BACKEND_ANON_KEY"),
		os.Getenv("SENTRA_BACKEND_ANON_KEY"),
		os.Getenv("SUPABASE_ANON_KEY"),
	)

	return &Config{
		BackendURL:     backendURL,
		BackendAnonKey: anonKey,
	}, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

type Config struct {
	BackendURL     string
	BackendAnonKey string
}
