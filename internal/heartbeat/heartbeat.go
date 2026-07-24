package heartbeat

import (
	"context"
	"log"
	"math"
	"os"
	"runtime"
	"sync"
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

var (
	lastHeartbeat struct {
		mu          sync.Mutex
		activeWorkers int
		cpuUsage     float64
		memFreeGB    float64
		gpuAvailable bool
		derivedStatus string
		hasValue     bool
	}
)

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
	// Conditional heartbeat — skip if metrics unchanged <10%
	// -----------------------------------------------------------------

	gpuAvailable := sys.GPUModel != ""
	needsSend := true

	lastHeartbeat.mu.Lock()
	if lastHeartbeat.hasValue {
		if activeWorkers == lastHeartbeat.activeWorkers &&
			math.Abs(sys.CPUUsagePercent-lastHeartbeat.cpuUsage) < 10.0 &&
			math.Abs(memFreeGB-lastHeartbeat.memFreeGB) < 0.5 &&
			gpuAvailable == lastHeartbeat.gpuAvailable {
			needsSend = false
		}
	}
	if needsSend {
		lastHeartbeat.activeWorkers = activeWorkers
		lastHeartbeat.cpuUsage = sys.CPUUsagePercent
		lastHeartbeat.memFreeGB = memFreeGB
		lastHeartbeat.gpuAvailable = gpuAvailable
		lastHeartbeat.hasValue = true
	}
	lastHeartbeat.mu.Unlock()

	// -----------------------------------------------------------------
	// Send heartbeat to backend (BEST EFFORT, only if needed)
	// -----------------------------------------------------------------

	platform := runtimev2.GetCurrentPlatform()
	if execClient != nil && needsSend {
		policyResult, err := execClient.SendDeviceHeartbeat(
			ctx,
			backend.DeviceHeartbeat{
				DeviceID:         device.ID,
				TotalCPUCores:    totalCPU,
				CPUCoresFree:     cpuFree,
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
		} else if policyResult.Concurrency > 0 && policyResult.Concurrency != recommended {
			backendConcurrency := policyResult.Concurrency
			if backendConcurrency != recommended && backendConcurrency != current {
				dispatcher.SetMaxWorkers(backendConcurrency)
				cfg.MaxConcurrencyAtomic.Store(int32(backendConcurrency))
				log.Printf("🔄 concurrency updated from backend: %d (recommended: %d)", backendConcurrency, recommended)
			}
		}
	}

	// -----------------------------------------------------------------
	// Local visibility
	// -----------------------------------------------------------------

	if needsSend {
		log.Printf(
			"💓 heartbeat | workers=%d queue=%d cpu_free=%d/%d mem_free=%.1fGB gpu=%t",
			activeWorkers,
			queueLen,
			cpuFree,
			totalCPU,
			memFreeGB,
			gpuAvailable,
		)
	}
}
