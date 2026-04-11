package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"sentra-agent/internal/config"
	"sentra-agent/internal/dispatcher"
	"sentra-agent/internal/httpclient"
	"sentra-agent/internal/sysinfo"
)

func AnnounceAvailable(
	ctx context.Context,
	cfg *config.Config,
) error {

	maxWorkers := dispatcher.MaxWorkerLimit()
	activeWorkers := dispatcher.CurrentWorkerCount()
	cpuFree := maxWorkers - activeWorkers
	if cpuFree < 0 {
		cpuFree = 0
	}

	specs := sysinfo.Detect()

	payload := map[string]any{
		"device_id": cfg.DeviceID,
		"metrics": map[string]any{
			"total_cpu_cores":    specs.CPUCores,
			"cpu_cores_free":     specs.AvailableCPUCores,
			"total_memory_gb":    specs.TotalMemoryGB,
			"memory_free_gb":     specs.AvailableMemoryGB,
			"cpu_usage_percent":  specs.CPUUsagePercent,
			"io_bandwidth_mb_s":  specs.IOThroughputMBps,
			"network_latency_ms": specs.NetworkLatency,
			"gpu_available":      specs.GPUModel != "",
		},
		"active_workers": activeWorkers,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	httpc := httpclient.NewClient(
		cfg.BackendURL,
		cfg.BackendAnonKey,
		cfg.Token,
	)

	resp, err := httpc.PostWithHeaders(ctx, "/functions/v1/notify_available_device", body, func(r *http.Request) {
		r.Header.Set("x-device-id", cfg.DeviceID)
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := httpclient.ReadBody(resp)
		return fmt.Errorf("availability failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}
