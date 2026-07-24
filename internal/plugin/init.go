package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"sentra-agent/internal/obs"
)

// InitAndWait = FULL BLOCKING PLUGIN INITIALIZATION.
// Workers MUST NOT start before this completes.
func InitAndWait(ctx context.Context) error {
	log.Println("🧩 Initializing plugin subsystem (blocking)...")

	MustInitKeyCache()

	verified, failed, err := VerifyAllCachedPlugins(ctx)
	if err != nil {
		return err
	}

	log.Printf("🔒 Plugin verification — OK=%d | FAILED=%d", verified, failed)

	if failed > 0 {
		return fmt.Errorf("plugin verification failed: %d plugins invalid", failed)
	}

	SyncPluginDependencies(ctx)
	StartPluginDepSync(ctx)

	log.Println("✅ Plugin subsystem READY.")
	return nil
}

// Init = asynchronous version (non-blocking).
// Use ONLY if caller explicitly allows degraded startup.
func Init(ctx context.Context) {
	log.Println("🧩 Initializing plugin subsystem (async)...")

	MustInitKeyCache()

	verified, failed, err := VerifyAllCachedPlugins(ctx)
	if err != nil {
		log.Printf("⚠️ Plugin verification error: %v", err)
		return
	}

	log.Printf("🔒 Plugin verification — OK=%d | FAILED=%d", verified, failed)

	if failed > 0 {
		log.Printf("⚠️ Plugin subsystem started in DEGRADED state (%d failed)", failed)
		return
	}

	SyncPluginDependencies(ctx)
	StartPluginDepSync(ctx)

	log.Println("✅ Plugin subsystem initialized successfully.")
}

// StartPluginDepSync launches a background goroutine that periodically
// checks all cached plugin manifests and installs any missing Python
// dependencies. Replaces the cron-based sentra-plugin-manager.sh.
func StartPluginDepSync(ctx context.Context) {
	interval := 60 * time.Second
	if v := os.Getenv("SENTRA_DEP_SYNC_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			interval = d
		}
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				SyncPluginDependencies(ctx)
			}
		}
	}()

	log.Printf("🔁 Plugin dependency sync started (interval: %v)", interval)
}

// SyncPluginDependencies scans all cached plugin manifests and installs
// any missing Python dependencies. This is called at startup and periodically
// by StartPluginDepSync.
func SyncPluginDependencies(ctx context.Context) {
	baseDir, err := EnsurePluginDir()
	if err != nil {
		obs.Warn("dep-sync: failed to get plugin dir", obs.Field{"error": err.Error()})
		return
	}

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		obs.Warn("dep-sync: failed to read plugin dir", obs.Field{"error": err.Error()})
		return
	}

	synced := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		pluginName := e.Name()
		platformDir := filepath.Join(baseDir, pluginName)
		platformEntries, err := os.ReadDir(platformDir)
		if err != nil {
			continue
		}

		for _, pe := range platformEntries {
			if !pe.IsDir() {
				continue
			}

			pluginDir := filepath.Join(platformDir, pe.Name())
			manifestPath := filepath.Join(pluginDir, pluginName+".json")

			data, err := os.ReadFile(manifestPath)
			if err != nil {
				continue
			}

			var manifest Manifest
			if err := json.Unmarshal(data, &manifest); err != nil {
				continue
			}

			if len(manifest.Dependencies) == 0 {
				continue
			}

			if err := EnsurePluginDependencies(ctx, manifest); err != nil {
				obs.Warn("dep-sync: failed for plugin", obs.Field{
					"plugin": pluginName,
					"error":  err.Error(),
				})
				continue
			}
			synced++
		}
	}

	if synced > 0 {
		log.Printf("✅ Dependency sync: %d plugins checked", synced)
	}
}
