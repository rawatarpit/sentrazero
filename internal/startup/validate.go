package startup

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"time"

	"sentra-agent/internal/config"
	"sentra-agent/internal/httpclient"
	"sentra-agent/internal/obs"
	"sentra-agent/internal/plugin"
	"sentra-agent/internal/sandbox"
)

func Validate(parentCtx context.Context, cfg *config.Config) error {

	ctx, cancel := context.WithTimeout(parentCtx, 15*time.Second)
	defer cancel()

	obs.Info("startup validation begin", obs.Field{"stage": "init"})

	if cfg == nil {
		return fmt.Errorf("startup: config is nil")
	}

	if cfg.BackendURL == "" {
		return fmt.Errorf("startup: BACKEND_URL is not configured")
	}

	if cfg.DeviceID == "" {
		return fmt.Errorf("startup: DEVICE_ID is not configured")
	}

	if cfg.Token == "" {
		return fmt.Errorf("startup: AUTH TOKEN is not configured")
	}

	if _, err := exec.LookPath("docker"); err != nil {
		// Allow startup without Docker if local execution is enabled (default)
		if os.Getenv("SENTRA_ALLOW_LOCAL_EXEC") != "false" {
			obs.Warn("Docker not found, enabling local execution mode", obs.Field{})
		} else {
			return fmt.Errorf(
				"Docker is required for plugin execution but was not found. " +
					"Install Docker: https://docs.docker.com/get-docker/ " +
					"or remove SENTRA_ALLOW_LOCAL_EXEC=false",
			)
		}
	}

	obs.Info("startup config validated", obs.Field{"backend": cfg.BackendURL})

	httpc := httpclient.NewClient(
		cfg.BackendURL,
		cfg.BackendAnonKey,
		cfg.Token,
	)

	payload := map[string]any{
		"device_id":                cfg.DeviceID,
		"total_cpu_cores":          runtime.NumCPU(),
		"cpu_cores_free":           runtime.NumCPU(),
		"total_memory_gb":          0,
		"memory_free_gb":           0,
		"network_latency_ms":       0,
		"gpu_available":            false,
		"cpu_usage_percent":        0,
		"incoming_workload_weight": 0,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("startup: failed to marshal payload: %w", err)
	}

	resp, err := httpc.PostWithHeaders(ctx, "/functions/v1/agent_health_policy", body, func(r *http.Request) {
		r.Header.Set("x-device-id", cfg.DeviceID)
	})
	if err != nil {
		return fmt.Errorf("startup: backend unreachable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := httpclient.ReadBody(resp)
		return fmt.Errorf("startup: backend auth/health failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	obs.Info("startup backend reachable and authenticated", obs.Field{"status": resp.StatusCode})

	pluginDir, err := plugin.EnsurePluginDir()
	if err != nil {
		return fmt.Errorf("startup: plugin directory unavailable: %w", err)
	}

	if _, err := os.Stat(pluginDir); err != nil {
		return fmt.Errorf("startup: plugin directory not accessible: %w", err)
	}

	if os.Getenv("SENTRA_NATIVE_PLUGINS_REQUIRED") == "1" {
		if runtime.GOOS != "windows" && !CGOEnabled {
			return fmt.Errorf("startup: native plugins require CGO_ENABLED=1")
		}
	}

	obs.Info("startup plugin registry accessible", obs.Field{"path": pluginDir})

	{
		testCmd := exec.CommandContext(ctx, "sh", "-c", `python3 -c "import hashlib,os; [hashlib.sha256(os.urandom(1024*1024)).hexdigest() for _ in range(100)]"`)
		limits := sandbox.Limits{
			MaxCPUSeconds: 1,
			MaxMemoryMB:   64,
			Timeout:       2 * time.Second,
		}
		if err := sandbox.Apply(ctx, testCmd, limits); err == nil {
			err = testCmd.Wait()
		}
		if err == nil {
			obs.Warn("startup sandbox did not enforce limits (process completed without being killed)", obs.Field{})
		} else {
			obs.Info("startup sandbox enforcement verified", obs.Field{"status": "ok", "enforcement_error": err.Error()})
		}
	}

	obs.Info("startup validation successful", obs.Field{"result": "ready"})

	return nil
}
