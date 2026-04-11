package heartbeat

import (
	"context"
	"log"
	"runtime"
	"time"

	runtimev2 "sentra-agent/cmd/agent/runtime/v2"
	"sentra-agent/internal/auth"
	"sentra-agent/internal/backend"
	"sentra-agent/internal/config"
	"sentra-agent/internal/dispatcher"
	"sentra-agent/internal/sysinfo"
	"sentra-agent/internal/system"
)

const heartbeatInterval = 10 * time.Second

// -----------------------------------------------------------------------------
// Public entrypoint
// -----------------------------------------------------------------------------

func Run(
	ctx context.Context,
	device auth.Device,
	cfg *config.Config,
	execClient *backend.ExecutionClient,
) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	log.Printf("💓 heartbeat started (interval=%s)", heartbeatInterval)

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 heartbeat stopping")
			return

		case <-ticker.C:
			performHeartbeat(ctx, device, cfg, execClient)
		}
	}
}

// -----------------------------------------------------------------------------
// Single heartbeat cycle
// -----------------------------------------------------------------------------

func performHeartbeat(
	ctx context.Context,
	device auth.Device,
	cfg *config.Config,
	execClient *backend.ExecutionClient,
) {
	sys := sysinfo.Detect()

	// -----------------------------------------------------------------
	// CPU + worker pressure (REAL SIGNAL)
	// -----------------------------------------------------------------

	totalCPU := runtime.NumCPU()
	availableCPU := sys.AvailableCPUCores
	if availableCPU <= 0 {
		availableCPU = totalCPU
	}
	activeWorkers := dispatcher.CurrentWorkerCount()
	maxWorkersVal := dispatcher.MaxWorkersCount()
	queueLen := dispatcher.QueueLength()

	cpuFree := int(maxWorkersVal) - activeWorkers
	if cpuFree < 0 {
		cpuFree = 0
	}

	// -----------------------------------------------------------------
	// Memory (OS-level, not Go heap)
	// -----------------------------------------------------------------

	memFreeGB := sys.AvailableMemoryGB
	if memFreeGB <= 0 {
		memFreeGB = sys.TotalMemoryGB * 0.9
	}
	if memFreeGB < 0 {
		memFreeGB = 0
	}

	// -----------------------------------------------------------------
	// Adaptive concurrency (LOCAL ONLY)
	// -----------------------------------------------------------------

	recommended := cpuFree
	if recommended < 1 {
		recommended = 1
	}

	maxAllowed := availableCPU * 2
	if recommended > maxAllowed {
		recommended = maxAllowed
	}

	current := int(cfg.MaxConcurrencyAtomic.Load())
	if recommended != current {
		dispatcher.SetMaxWorkers(recommended)
		cfg.MaxConcurrencyAtomic.Store(int32(recommended))
	}

	// -----------------------------------------------------------------
	// Send heartbeat to backend (BEST EFFORT)
	// -----------------------------------------------------------------

	platform := runtimev2.GetCurrentPlatform()
	env := system.DetectExecutionEnv()
	hasDocker := env.HasDocker

	if execClient != nil {
		err := execClient.SendDeviceHeartbeat(
			ctx,
			backend.DeviceHeartbeat{
				DeviceID:         device.ID,
				TotalCPUCores:    totalCPU,
				CPUCoresFree:     cpuFree,
				TotalMemoryGB:    sys.TotalMemoryGB,
				MemoryFreeGB:     memFreeGB,
				NetworkLatencyMs: sys.NetworkLatency,
				GPUAvailable:     sys.GPUModel != "",
				CPUUsagePercent:  sys.CPUUsagePercent,
				IncomingWorkload: 0,
				ActiveWorkers:    activeWorkers,
				PythonVersion:    platform.Python,
				NodeVersion:      platform.Node,
				DockerAvailable:  hasDocker,
				RuntimeSupported: platform.Python != "" || platform.Node != "",
			},
		)
		if err != nil {
			log.Printf("⚠️ heartbeat send failed: %v", err)
		}
	}

	// -----------------------------------------------------------------
	// Local visibility
	// -----------------------------------------------------------------

	log.Printf(
		"💓 heartbeat | workers=%d queue=%d cpu_free=%d/%d mem_free=%.1fGB gpu=%t",
		activeWorkers,
		queueLen,
		cpuFree,
		totalCPU,
		memFreeGB,
		sys.GPUModel != "",
	)
}
