package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"sentra-agent/internal/auth"
	"sentra-agent/internal/config"
)

const FunctionReconcileAgent = "/functions/v1/reconcile_agent"

type ReconcileResponse struct {
	OK           bool `json:"ok"`
	RestoredJobs int  `json:"restored_jobs"`
}

func ReconcileAgent(ctx context.Context, cfg *config.Config) error {
	reconcileURL := fmt.Sprintf("%s%s", cfg.BackendURL, FunctionReconcileAgent)

	deviceID := cfg.DeviceID
	deviceToken, err := auth.GetToken()
	if err != nil {
		log.Printf("[reconcile] cannot get device token: %v", err)
		return err
	}

	payload := map[string]string{
		"device_id": deviceID,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reconcileURL, strings.NewReader(string(body)))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.BackendAnonKey)
	req.Header.Set("x-agent-token", deviceToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[reconcile] endpoint not available, skipping")
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		log.Printf("[reconcile] endpoint not available, skipping")
		return nil
	}

	if resp.StatusCode >= 300 {
		return fmt.Errorf("reconcile_agent failed: HTTP %d", resp.StatusCode)
	}

	var result ReconcileResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}

	if result.OK {
		log.Printf("[reconcile] restored_jobs=%d", result.RestoredJobs)
	}

	return nil
}
