package auth

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"sentra-agent/internal/sysinfo"
	"sentra-agent/internal/system"
)

type Device struct {
	ID        string    `json:"device_id"`
	Token     string    `json:"-"`
	OrgID     string    `json:"org_id"`
	Name      string    `json:"name"`
	ClaimedAt time.Time `json:"claimed_at"`
}

type DeviceClaimResponse struct {
	Ok         bool   `json:"ok"`
	DeviceID   string `json:"device_id"`
	AgentToken string `json:"agent_token"`
	OrgID      string `json:"org_id"`
	Message    string `json:"message"`
	Error      string `json:"error"`
}

func ClaimDevice(
	appSupabaseURL string,
	appAnonKey string,
	orgID string,
	deviceName string,
	envType string,
	storageType string,
	capabilities []string,
	claimCode string,
) (*Device, error) {

	// ---------------------------------------------------
	// 1️⃣ Migrate legacy storage
	// ---------------------------------------------------

	if err := MigrateLegacyDevice(); err != nil {
		log.Printf("⚠️ device.json migration warning: %v", err)
	}

	// ---------------------------------------------------
	// 2️⃣ Check existing identity
	// ---------------------------------------------------

	existing, _, err := LoadIdentity()
	if err == nil && existing.DeviceID != "" {

		store := GetTokenStore()
		token, err := store.Load(existing.DeviceID)
		if err == nil && token != "" {

			log.Printf("🔑 Existing device loaded: %s", existing.DeviceID)

			return &Device{
				ID:        existing.DeviceID,
				Token:     token,
				OrgID:     existing.OrgID,
				ClaimedAt: existing.RegisteredAt,
			}, nil
		}
	}

	// ---------------------------------------------------
	// 3️⃣ Collect system info
	// ---------------------------------------------------

	sys := sysinfo.Detect()

	// ---------------------------------------------------
	// 4️⃣ Get claim code
	// ---------------------------------------------------

	if claimCode == "" {
		fmt.Println("🪪 Enter your claim code:")
		fmt.Print("👉 Claim Code: ")
		reader := bufio.NewReader(os.Stdin)
		var err error
		claimCode, err = reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("failed to read claim code: %w", err)
		}
		claimCode = strings.TrimSpace(claimCode)
	}
	if claimCode == "" {
		return nil, fmt.Errorf("claim code is required")
	}

	// ---------------------------------------------------
	// 5️⃣ Build request payload
	// ---------------------------------------------------

	execEnv := system.DetectExecutionEnv()

	payload := map[string]any{
		"claim_code": claimCode,
		"sysinfo": map[string]any{
			"hostname":      deviceName,
			"type":          "agent",
			"environment":   envType,
			"storage":       storageType,
			"cpu_cores":     sys.CPUCores,
			"memory_gb":     sys.TotalMemoryGB,
			"merge_capable": true,
			"os":            sys.OS,
			"arch":          sys.Arch,
			"has_cgo":       execEnv.HasCGO,
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	url := fmt.Sprintf(
		"%s/functions/v1/claim_device",
		strings.TrimRight(appSupabaseURL, "/"),
	)

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+appAnonKey)

	log.Printf("📤 Registering device via: %s", url)

	client := &http.Client{Timeout: 10 * time.Second}

	httpResp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("claim_device request failed: %v", err)
	}
	defer httpResp.Body.Close()

	respBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// ---------------------------------------------------
	// 6️⃣ HTTP error handling
	// ---------------------------------------------------

	if httpResp.StatusCode >= 400 {
		return nil, fmt.Errorf(
			"claim_device HTTP %d: %s",
			httpResp.StatusCode,
			string(respBytes),
		)
	}

	// ---------------------------------------------------
	// 7️⃣ Parse response (STRICT)
	// ---------------------------------------------------

	var claimResp DeviceClaimResponse

	if err := json.Unmarshal(respBytes, &claimResp); err != nil {
		return nil, fmt.Errorf(
			"invalid JSON from claim_device: %w | body=%s",
			err,
			string(respBytes),
		)
	}

	// ---------------------------------------------------
	// 8️⃣ Validate response (STRICT)
	// ---------------------------------------------------

	if !claimResp.Ok {
		return nil, fmt.Errorf(
			"device claim rejected: %s | full_response=%s",
			claimResp.Error,
			string(respBytes),
		)
	}

	if claimResp.DeviceID == "" {
		return nil, fmt.Errorf(
			"missing device_id in response: %s",
			string(respBytes),
		)
	}

	if claimResp.AgentToken == "" {
		return nil, fmt.Errorf(
			"missing agent_token in response: %s",
			string(respBytes),
		)
	}

	log.Printf("🔐 Agent token received (len=%d)", len(claimResp.AgentToken))

	// ---------------------------------------------------
	// 9️⃣ Build device
	// ---------------------------------------------------

	device := &Device{
		ID:        claimResp.DeviceID,
		Token:     claimResp.AgentToken,
		OrgID:     claimResp.OrgID,
		Name:      deviceName,
		ClaimedAt: time.Now().UTC(),
	}

	// ---------------------------------------------------
	// 🔟 Save identity
	// ---------------------------------------------------

	if err := SaveIdentity(Identity{
		DeviceID:     device.ID,
		OrgID:        device.OrgID,
		RegisteredAt: device.ClaimedAt,
	}); err != nil {
		return nil, fmt.Errorf("failed saving identity: %w", err)
	}

	// ---------------------------------------------------
	// 1️⃣1️⃣ Save token (non-fatal: continue running even if persistence fails)
	// ---------------------------------------------------

	if err := SaveToken(device.ID, device.Token); err != nil {
		log.Printf("[claim] ⚠️ Token persistence failed, continuing with in-memory token: %v", err)
	} else {
		log.Printf("[claim] ✅ Token persisted successfully")
	}

	log.Printf(
		"✅ Device registered successfully: %s (Org: %s)",
		device.ID,
		device.OrgID,
	)

	return device, nil
}
