package heartbeat

import (
	"context"
	"log"
	"os"
	"runtime"
	"time"

	runtimev2 "sentra-agent/cmd/agent/runtime/v2"
	"sentra-agent/internal/auth"
	"sentra-agent/internal/backend"
	"sentra-agent/internal/config"
	"sentra-agent/internal/dispatcher"
	"sentra-agent/internal/sysinfo"
)

var heartbeatInterval = 600 * time.Second // 10min — metrics are already sent during poll_state calls

func init() {
	if v := os.Getenv("SENTRA_HEARTBEAT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			heartbeatInterval = d
		}
	}
}

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
	queueLen := dispatcher.QueueLength()

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
	// Adaptive concurrency — DEFERRED TO BACKEND POLICY (TICKET-001)
	// -----------------------------------------------------------------
	// The old LOCAL-ONLY governor was removed. It computed
	// cpuFree = maxWorkers - activeWorkers, but dispatcher.CurrentWorkerCount()
	// counts SPAWNED worker goroutines (persistent: they spawn once at
	// InitWorkerPool and wait on the job queue), so cpuFree was 0 even when
	// the pool was fully idle. `recommended` therefore clamped to 1 and the
	// pool was collapsed to a single worker on EVERY heartbeat — regardless
	// of MAX_CONCURRENCY, load, or the backend agent_health_policy response.
	// Concurrency is now owned solely by the backend policy via the override
	// below; the pool keeps the startup MAX_CONCURRENCY until the backend
	// responds on the first heartbeat.
	current := int(cfg.MaxConcurrencyAtomic.Load())

	// -----------------------------------------------------------------
	// Heartbeat send policy — ALWAYS SEND.
	// -----------------------------------------------------------------
	// The old conditional-send skipped the backend call when metrics looked
	// unchanged. That permanently disabled the control plane:
	//   - cpuUsagePercent() is intentionally disabled in sysinfo.go and always
	//     returns 0, so the CPU delta check always saw "unchanged".
	//   - dispatcher.CurrentWorkerCount() counts persistent spawned worker
	//     goroutines, which are constant at steady state.
	//   - memFreeGB / gpuAvailable rarely move enough to trip their deltas.
	// Net effect: after the FIRST heartbeat at startup, agent_health_policy was
	// never called again, so the backend could never raise or lower
	// concurrency mid-run (the entire point of TICKET-001 — backend policy is
	// the sole authority). The 600s ticker makes the extra call cost trivial
	// (3 agents * 6 calls/hr), and the edge function has a 5s cooldown plus
	// only fires SetMaxWorkers when the value actually changes.

	gpuAvailable := sys.GPUModel != ""

	platform := runtimev2.GetCurrentPlatform()
	if execClient != nil {
		policyResult, err := execClient.SendDeviceHeartbeat(
			ctx,
			backend.DeviceHeartbeat{
				DeviceID:         device.ID,
				TotalCPUCores:    totalCPU,
				CPUCoresFree:     availableCPU,
				TotalMemoryGB:    sys.TotalMemoryGB,
				MemoryFreeGB:     memFreeGB,
				NetworkLatencyMs: sys.NetworkLatency,
				GPUAvailable:     gpuAvailable,
				GPUModel:         sys.GPUModel,
				GPUMemoryFreeGB:  sys.GPUMemoryFreeGB,
				GPUMemoryTotalGB: sys.GPUMemoryTotalGB,
				CPUUsagePercent:  sys.CPUUsagePercent,
				IncomingWorkload: 0,
				ActiveWorkers:    activeWorkers,
				PythonVersion:    platform.Python,
				NodeVersion:      platform.Node,
				DockerAvailable:  false,
				RuntimeSupported: platform.Python != "" || platform.Node != "",
			},
		)
		if err != nil {
			log.Printf("⚠️ heartbeat send failed: %v", err)
		} else if policyResult.Concurrency > 0 && policyResult.Concurrency != current {
			backendConcurrency := policyResult.Concurrency
			if backendConcurrency != current {
				dispatcher.SetMaxWorkers(backendConcurrency)
				cfg.MaxConcurrencyAtomic.Store(int32(backendConcurrency))
				log.Printf("🔄 concurrency updated from backend: %d", backendConcurrency)
			}
		}
	}

	// -----------------------------------------------------------------
	// Local visibility
	// -----------------------------------------------------------------

	log.Printf(
		"💓 heartbeat | workers=%d queue=%d cpu_free=%d/%d mem_free=%.1fGB gpu=%t",
		activeWorkers,
		queueLen,
		availableCPU,
		totalCPU,
		memFreeGB,
		gpuAvailable,
	)
}
