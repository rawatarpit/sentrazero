package startup

import (
	"context"
	"encoding/json"
	"net/http"

	"sentra-agent/internal/backend"
	"sentra-agent/internal/config"
	"sentra-agent/internal/httpclient"
	"sentra-agent/internal/obs"
)

type reconcileJob struct {
	JobID   string `json:"job_id"`
	ChunkID string `json:"chunk_id,omitempty"`
	JobType string `json:"job_type"`
	Status  string `json:"status"`
}

type reconcileResponse struct {
	Ok           bool   `json:"ok"`
	RestoredJobs int    `json:"restored_jobs"`
	DeviceID     string `json:"device_id,omitempty"`
	OrgID        string `json:"org_id,omitempty"`
	Error        string `json:"error,omitempty"`
}

func Reconcile(
	ctx context.Context,
	cfg *config.Config,
	execClient *backend.ExecutionClient,
) error {

	obs.Info(
		"startup reconcile begin",
		obs.Field{"stage": "reconcile"},
	)

	httpc := httpclient.NewClient(
		cfg.BackendURL,
		cfg.BackendAnonKey,
		cfg.Token,
	)

	payload := map[string]string{}

	body, err := json.Marshal(payload)
	if err != nil {
		obs.Info(
			"startup reconcile skipped (payload marshal failed)",
			obs.Field{"error": err.Error()},
		)
		return nil
	}

	resp, err := httpc.PostWithHeaders(ctx, "/functions/v1/reconcile_agent", body, func(r *http.Request) {
		r.Header.Set("x-device-id", cfg.DeviceID)
	})
	if err != nil {
		obs.Info(
			"startup reconcile skipped (endpoint not available)",
			obs.Field{"error": err.Error()},
		)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		obs.Info(
			"startup reconcile skipped (endpoint returned error status)",
			obs.Field{"status": resp.StatusCode},
		)
		return nil
	}

	respBody, err := httpclient.ReadBody(resp)
	if err != nil {
		obs.Info(
			"startup reconcile skipped (failed to read response)",
			obs.Field{"error": err.Error()},
		)
		return nil
	}

	var result reconcileResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		obs.Info(
			"startup reconcile skipped (failed to parse response)",
			obs.Field{"error": err.Error()},
		)
		return nil
	}

	if result.Ok {
		obs.Info(
			"startup reconcile completed",
			obs.Field{
				"restored_jobs": result.RestoredJobs,
				"device_id":     result.DeviceID,
			},
		)
	} else if result.Error != "" {
		obs.Info(
			"startup reconcile returned error",
			obs.Field{"error": result.Error},
		)
	}

	return nil
}
