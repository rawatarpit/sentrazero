package update

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"sentra-agent/internal/obs"
)

// AgentRelease represents the latest available agent version from the backend.
type AgentRelease struct {
	Version     string `json:"version"`
	DownloadURL string `json:"download_url"`
	Checksum    string `json:"checksum"`
	Changelog   string `json:"changelog,omitempty"`
}

// AgentUpdateConfig controls the update check behavior.
type AgentUpdateConfig struct {
	CurrentVersion string
	BackendURL     string
	AnonKey        string
	DeviceID       string
	OrgID          string
	Token          string
	CheckInterval  time.Duration
	UpdateURLPath  string // e.g., "/functions/v1/get_agent_version"
}

// CheckResult describes the outcome of an update check.
type CheckResult struct {
	Available bool   // true if a newer version exists
	Version   string // the available version
	Updated   bool   // true if the binary was actually replaced
	Error     error  // any error encountered
}

// CheckForUpdate queries the backend for the latest agent release and compares versions.
func CheckForUpdate(ctx context.Context, cfg AgentUpdateConfig) (*AgentRelease, error) {
	url := strings.TrimRight(cfg.BackendURL, "/") + "/" + strings.TrimLeft(cfg.UpdateURLPath, "/")

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("apikey", cfg.AnonKey)
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	req.Header.Set("X-Device-ID", cfg.DeviceID)
	req.Header.Set("X-Org-ID", cfg.OrgID)
	req.Header.Set("X-Arch", runtime.GOARCH)
	req.Header.Set("X-OS", runtime.GOOS)
	req.Header.Set("User-Agent", "sentra-agent/"+cfg.CurrentVersion)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	var release AgentRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &release, nil
}

// IsNewerVersion checks if the candidate version is newer than the current version.
// Uses simple semver comparison (major.minor.patch). For production use, replace
// with a proper semver library.
func IsNewerVersion(current, candidate string) bool {
	if current == "" || current == "dev" {
		return true
	}
	if candidate == "" {
		return false
	}
	if current == candidate {
		return false
	}

	// Split into parts and compare numerically
	currParts := strings.Split(current, ".")
	candParts := strings.Split(candidate, ".")

	maxLen := len(currParts)
	if len(candParts) > maxLen {
		maxLen = len(candParts)
	}

	for i := 0; i < maxLen; i++ {
		var cPart, nPart int
		if i < len(currParts) {
			fmt.Sscanf(currParts[i], "%d", &cPart)
		}
		if i < len(candParts) {
			fmt.Sscanf(candParts[i], "%d", &nPart)
		}
		if nPart > cPart {
			return true
		}
		if nPart < cPart {
			return false
		}
	}

	return false
}

// DownloadAndReplace downloads a new agent binary, verifies it, and replaces the current one.
// Returns true if replacement succeeded (caller should restart).
func DownloadAndReplace(ctx context.Context, release *AgentRelease, cfg AgentUpdateConfig) (bool, error) {
	if release == nil {
		return false, fmt.Errorf("no release provided")
	}

	if release.DownloadURL == "" {
		return false, fmt.Errorf("release has no download URL")
	}

	execPath, err := os.Executable()
	if err != nil {
		return false, fmt.Errorf("get executable path: %w", err)
	}

	// Resolve symlinks to get the real binary path
	realPath, err := filepath.EvalSymlinks(execPath)
	if err != nil {
		realPath = execPath
	}

	// Download to a temp file in the same directory for atomic rename
	tmpPath := realPath + ".update." + fmt.Sprintf("%d", time.Now().UnixNano())
	defer os.Remove(tmpPath)

	if err := downloadFile(ctx, release.DownloadURL, tmpPath, cfg.AnonKey, cfg.Token); err != nil {
		return false, fmt.Errorf("download: %w", err)
	}

	// Verify checksum
	if release.Checksum != "" {
		if err := verifyChecksum(tmpPath, release.Checksum); err != nil {
			return false, fmt.Errorf("checksum: %w", err)
		}
		obs.Info("agent binary checksum verified", obs.Field{
			"version": release.Version,
		})
	}

	// Make executable
	if err := os.Chmod(tmpPath, 0755); err != nil {
		return false, fmt.Errorf("chmod: %w", err)
	}

	// Rename — atomic on Unix
	if err := os.Rename(tmpPath, realPath); err != nil {
		return false, fmt.Errorf("rename: %w", err)
	}

	obs.Info("agent binary updated", obs.Field{
		"from_version": cfg.CurrentVersion,
		"to_version":   release.Version,
		"path":         realPath,
	})

	return true, nil
}

func downloadFile(ctx context.Context, url, destPath, anonKey, token string) error {
	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer out.Close()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("create download request: %w", err)
	}
	req.Header.Set("apikey", anonKey)
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("download request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download returned status %d", resp.StatusCode)
	}

	written, err := io.Copy(out, resp.Body)
	if err != nil {
		return fmt.Errorf("download body: %w", err)
	}

	if written == 0 {
		return fmt.Errorf("downloaded file is empty")
	}

	return nil
}

func verifyChecksum(filePath, expectedHex string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	hash := sha256.Sum256(data)
	actualHex := hex.EncodeToString(hash[:])

	if !strings.EqualFold(actualHex, expectedHex) {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expectedHex, actualHex)
	}

	return nil
}
