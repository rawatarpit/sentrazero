package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"sentra-agent/internal/auth"
	"sentra-agent/internal/backend"
	"sentra-agent/internal/config"
	"sentra-agent/internal/dispatcher"
	"sentra-agent/internal/healthcheck"
	"sentra-agent/internal/heartbeat"
	"sentra-agent/internal/plugin"
	"sentra-agent/internal/realtime"
	"sentra-agent/internal/startup"
	"sentra-agent/internal/storage"
)

var (
	claimCodeFlag = flag.String("claim-code", "", "Claim code for non-interactive device registration")
	healthServer  *healthcheck.Server
)

func main() {
	flag.Parse()

	log.Println("🚀 Sentra Agent starting")

	cfg := config.LoadStatic()

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	claimCode := cfg.ClaimCode
	if claimCode == "" {
		claimCode = *claimCodeFlag
	}

	// ---------------------------------------------------------------------
	// Identity (load → validate → reclaim if needed)
	// ---------------------------------------------------------------------

	id, token, err := auth.LoadIdentity()

	if err != nil || token == "" {
		log.Println("ℹ️ No valid identity found, starting claim flow")

		device, err := auth.ClaimDevice(
			cfg.BackendURL,
			cfg.BackendAnonKey,
			cfg.OrgID,
			cfg.DeviceName,
			cfg.EnvironmentType,
			cfg.StorageType,
			cfg.Capabilities,
			claimCode,
		)
		if err != nil {
			log.Fatal("❌ Device claim failed:", err)
		}

		id = auth.Identity{
			DeviceID: device.ID,
			OrgID:    device.OrgID,
		}
		token = device.Token
	} else {
		log.Println("🔍 Existing identity found, validating token...")

		validateClient := backend.NewExecutionClient(
			cfg.BackendURL,
			cfg.BackendAnonKey,
			token,
			id.DeviceID,
			id.OrgID,
		)

		_, err := validateClient.SendDeviceHeartbeat(ctx, backend.DeviceHeartbeat{
			DeviceID: id.DeviceID,
		})

		if err != nil {
			log.Println("⚠️ Token invalid → reclaiming device")

			device, err := auth.ClaimDevice(
				cfg.BackendURL,
				cfg.BackendAnonKey,
				cfg.OrgID,
				cfg.DeviceName,
				cfg.EnvironmentType,
				cfg.StorageType,
				cfg.Capabilities,
				claimCode,
			)
			if err != nil {
				log.Fatal("❌ Device reclaim failed:", err)
			}

			id = auth.Identity{
				DeviceID: device.ID,
				OrgID:    device.OrgID,
			}
			token = device.Token
		}
	}

	// ---------------------------------------------------------------------
	// Final assignment
	// ---------------------------------------------------------------------

	cfg.DeviceID = id.DeviceID
	cfg.OrgID = id.OrgID
	cfg.Token = token

	log.Printf("🔐 Device ready: %s", cfg.DeviceID)

	// ---------------------------------------------------------------------
	// Health check server (for cloud platform deployment)
	// ---------------------------------------------------------------------
	healthServer = healthcheck.New(cfg.HealthCheckPort)
	healthServer.SetDeviceID(cfg.DeviceID)
	if err := healthServer.Start(ctx); err != nil {
		log.Printf("⚠️ Failed to start health check server: %v", err)
	}

	// ---------------------------------------------------------------------
	// Plugin signing key fetcher
	// ---------------------------------------------------------------------

	keyFetcher := plugin.NewKeyFetcher(
		cfg.BackendURL,
		cfg.BackendAnonKey,
		cfg.Token,
		1*time.Hour,
	)

	loadSigningKeys := func() {
		keys, err := keyFetcher.FetchAllOrgKeys(ctx, cfg.OrgID)
		if err != nil {
			log.Printf("⚠️ Failed to fetch plugin signing keys: %v", err)
			return
		}
		for _, key := range keys {
			sigKey := keyFetcher.ConvertToSignatureKey(key)
			if sigKey == nil {
				continue
			}
			plugin.RegisterSignatureKey(sigKey)
			os.Setenv(
				"PLUGIN_SIGNING_KEY_"+strings.ToUpper(key.KeyID),
				key.PublicKey,
			)
		}
		log.Printf("🔑 Loaded %d plugin signing key(s)", len(keys))
	}

	loadSigningKeys()

	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				loadSigningKeys()
			case <-ctx.Done():
				return
			}
		}
	}()

	// ---------------------------------------------------------------------
	// Plugin auto-update (background ticker)
	// ---------------------------------------------------------------------

	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				updated, skipped, failed, err := plugin.AutoUpdatePlugins(ctx)
				if err != nil {
					log.Printf("⚠️ AutoUpdatePlugins error: %v", err)
					continue
				}
				log.Printf("📦 AutoUpdatePlugins: updated=%d skipped=%d failed=%d", updated, skipped, failed)
			case <-ctx.Done():
				return
			}
		}
	}()

	// ---------------------------------------------------------------------
	// Execution client
	// ---------------------------------------------------------------------

	execClient := backend.NewExecutionClient(
		cfg.BackendURL,
		cfg.BackendAnonKey,
		cfg.Token,
		cfg.DeviceID,
		cfg.OrgID,
	)

	dispatcher.SetExecutionClient(execClient)

	// ---------------------------------------------------------------------
	// Startup validation
	// ---------------------------------------------------------------------

	if err := startup.Validate(ctx, cfg); err != nil {
		log.Fatal("❌ Startup validation failed:", err)
	}

	if err := startup.Reconcile(ctx, cfg, execClient); err != nil {
		log.Fatal("❌ Startup reconcile failed:", err)
	}

	// ---------------------------------------------------------------------
	// Plugin sync — download plugins from DB before verification
	// Without this, ~/.sentra/plugins/ is empty on a fresh install
	// and InitAndWait has nothing to verify.
	// ---------------------------------------------------------------------

	log.Println("🔄 Syncing plugins from API...")
	if err := plugin.SyncPluginsFromAPI(ctx); err != nil {
		// Non-fatal: agent can still run builtins
		log.Printf("⚠️ Plugin sync failed (non-fatal): %v", err)
	}

	// ---------------------------------------------------------------------
	// Plugin init — verify all cached plugins (signatures + checksums)
	// ---------------------------------------------------------------------

	if err := plugin.InitAndWait(ctx); err != nil {
		log.Fatal("❌ Plugin init failed:", err)
	}

	// ---------------------------------------------------------------------
	// Storage
	// ---------------------------------------------------------------------

	storageClient := storage.NewStorageClient(
		cfg.OrgID,
		cfg.DeviceID,
		cfg.Token,
		cfg.BackendAnonKey,
		cfg.BackendURL,
	)

	storageCfg, err := storageClient.FetchConfig()
	if err != nil {
		log.Printf("⚠️ Failed to fetch storage config: %v", err)
		log.Printf("⚠️ Falling back to shared_mount mode")
		storageCfg = &storage.StorageConfig{StorageMode: "shared_mount"}
	}

	storageBackend, err := storage.NewBackend(storageCfg)
	if err != nil {
		log.Fatalf("❌ Failed to initialize storage backend: %v", err)
	}

	dispatcher.SetStorageBackend(storageBackend)
	log.Printf("📦 Storage backend initialized: %s", storageCfg.StorageMode)

	// ---------------------------------------------------------------------
	// Realtime sync
	// ---------------------------------------------------------------------

	if err := realtime.ReconcileAgent(ctx, cfg); err != nil {
		log.Printf("⚠️ reconcile_agent failed: %v", err)
	}

	// ---------------------------------------------------------------------
	// Worker pool
	// ---------------------------------------------------------------------

	workers := cfg.MaxConcurrencyAtomic.Load()
	if workers < 1 {
		workers = int32(runtime.NumCPU() / 2)
		if workers < 1 {
			workers = 1
		}
	}

	dispatcher.InitWorkerPool(int(workers))

	// ---------------------------------------------------------------------
	// Heartbeat
	// ---------------------------------------------------------------------

	go heartbeat.Run(
		ctx,
		auth.Device{
			ID:    cfg.DeviceID,
			OrgID: cfg.OrgID,
		},
		cfg,
		execClient,
	)

	// ---------------------------------------------------------------------
	// Availability + Realtime (WebSocket preferred, SSE as fallback)
	// ---------------------------------------------------------------------

	go realtime.AnnounceAvailable(ctx, cfg)

	preferredRealtime := os.Getenv("SENTRA_REALTIME_MODE")

	// Default to WebSocket for lower latency; SSE available as fallback
	if preferredRealtime == "sse" {
		log.Println("[realtime] Using SSE for job notifications (SENTRA_REALTIME_MODE=sse)")
		go realtime.RunSSEClient(
			ctx,
			auth.Device{
				ID:    cfg.DeviceID,
				OrgID: cfg.OrgID,
				Token: cfg.Token,
			},
			cfg,
		)
	} else if preferredRealtime == "both" {
		log.Println("[realtime] Using both WebSocket and SSE for job notifications (SENTRA_REALTIME_MODE=both)")
		go realtime.RunRealtimeWS(ctx, auth.Device{
			ID:    cfg.DeviceID,
			OrgID: cfg.OrgID,
			Token: cfg.Token,
		}, cfg)
		go realtime.RunSSEClient(
			ctx,
			auth.Device{
				ID:    cfg.DeviceID,
				OrgID: cfg.OrgID,
				Token: cfg.Token,
			},
			cfg,
		)
	} else {
		log.Println("[realtime] Using WebSocket for job notifications (preferred)")
		// Start polling client alongside WebSocket to handle pending jobs
		// WebSocket only receives jobs already assigned to this device
		// Polling picks up new pending jobs
		go realtime.RunPollingClient(
			ctx,
			auth.Device{
				ID:    cfg.DeviceID,
				OrgID: cfg.OrgID,
				Token: cfg.Token,
			},
			cfg,
			cfg.Token,
		)
		go realtime.RunRealtimeWS(ctx, auth.Device{
			ID:    cfg.DeviceID,
			OrgID: cfg.OrgID,
			Token: cfg.Token,
		}, cfg)
	}

	log.Printf(
		"[system] Ready | Device=%s | Workers=%d",
		cfg.DeviceID,
		workers,
	)

	if healthServer != nil {
		healthServer.SetReady(true)
	}

	// ---------------------------------------------------------------------
	// Shutdown
	// ---------------------------------------------------------------------

	<-ctx.Done()
	log.Println("[system] Shutdown signal received")

	gracePeriod := 30 * time.Second
	if envGP := os.Getenv("SENTRA_SHUTDOWN_GRACE_PERIOD"); envGP != "" {
		if gp, err := time.ParseDuration(envGP); err == nil {
			gracePeriod = gp
		}
	}

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		gracePeriod,
	)
	defer cancel()

	gracefulDrain := func() error {
		log.Println("[system] Starting graceful drain...")

		dispatcher.PauseJobProcessing()
		log.Println("[system] Paused job processing")

		waitStart := time.Now()
		for {
			activeCount := dispatcher.ActiveJobsCount()
			if activeCount == 0 {
				log.Printf("[system] All jobs completed (waited %v)", time.Since(waitStart))
				break
			}
			if time.Since(waitStart) >= gracePeriod {
				log.Printf("[system] Grace period expired, %d jobs still running", activeCount)
				break
			}
			log.Printf("[system] Waiting for %d active jobs...", activeCount)
			select {
			case <-shutdownCtx.Done():
				return shutdownCtx.Err()
			case <-time.After(2 * time.Second):
			}
		}

		log.Println("[system] Reporting in-flight job failures...")
		dispatcher.ReportInFlightFailures(execClient)

		log.Println("[system] Releasing merge locks...")
		dispatcher.ReleaseAllMergeLocks()

		return nil
	}

	if err := gracefulDrain(); err != nil {
		log.Printf("[system] Graceful drain error: %v", err)
	}

	if healthServer != nil {
		healthServer.Stop(shutdownCtx)
	}

	dispatcher.StopWorkerPool(shutdownCtx)
	log.Println("[system] Shutdown complete")
}
