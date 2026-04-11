package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Identity is the persisted, non-secret device identity.
type Identity struct {
	DeviceID     string    `json:"device_id"`
	OrgID        string    `json:"org_id"`
	RegisteredAt time.Time `json:"registered_at"`
}

func identityFilePath() (string, error) {
	if sentraHome := os.Getenv("SENTRA_HOME"); sentraHome != "" {
		return filepath.Join(sentraHome, "identity.json"), nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(home, ".sentra", "identity.json"), nil
}

func LoadIdentity() (Identity, string, error) {
	path, err := identityFilePath()
	if err != nil {
		return Identity{}, "", err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return Identity{}, "", fmt.Errorf("no local identity found: %w", err)
	}

	var id Identity
	if err := json.Unmarshal(data, &id); err != nil {
		return Identity{}, "", err
	}

	if id.DeviceID == "" || id.OrgID == "" {
		return Identity{}, "", errors.New("invalid identity file")
	}

	if path := os.Getenv("SENTRA_TOKEN_PATH"); path != "" && !strings.Contains(path, "{device_id}") {
		log.Println("⚠️ WARNING: SENTRA_TOKEN_PATH does not contain {device_id} — multiple agents on this host will share a token file!")
	}

	store := GetTokenStore()
	token, err := store.Load(id.DeviceID)
	if err != nil || token == "" {
		return id, "", errors.New("device token not found in store")
	}

	return id, token, nil
}

func SaveIdentity(id Identity) error {
	path, err := identityFilePath()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}

	data, _ := json.MarshalIndent(id, "", "  ")
	return os.WriteFile(path, data, 0600)
}

func SaveToken(deviceID, token string) error {
	if token == "" {
		return errors.New("cannot save empty token")
	}

	store := GetTokenStore()
	return store.Save(deviceID, token)
}

// MigrateLegacyDevice handles legacy token file migration
// Migrates old token files from ~/.sentra/<deviceID>.token to ~/.sentra/tokens/<deviceID>.token
func MigrateLegacyDevice() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get home directory: %w", err)
	}

	legacyPaths := []string{
		filepath.Join(home, ".sentra", "token"),
		filepath.Join(home, ".sentra", "device.token"),
	}

	hasLegacy := false
	for _, p := range legacyPaths {
		if _, err := os.Stat(p); err == nil {
			hasLegacy = true
			break
		}
	}

	if !hasLegacy {
		return nil
	}

	for _, legacyPath := range legacyPaths {
		if _, err := os.Stat(legacyPath); err == nil {
			newDir := filepath.Join(home, ".sentra", "tokens")
			if err := os.MkdirAll(newDir, 0700); err != nil {
				return fmt.Errorf("failed to create tokens directory: %w", err)
			}

			idPath, _ := identityFilePath()
			var deviceID string
			if idPath != "" {
				data, _ := os.ReadFile(idPath)
				var id Identity
				if json.Unmarshal(data, &id) == nil && id.DeviceID != "" {
					deviceID = id.DeviceID
				}
			}

			if deviceID == "" {
				deviceID = "legacy"
			}

			newPath := filepath.Join(newDir, deviceID+".token")
			if _, err := os.Stat(newPath); os.IsNotExist(err) {
				if err := os.Rename(legacyPath, newPath); err != nil {
					return fmt.Errorf("failed to migrate legacy token: %w", err)
				}
				os.Chmod(newPath, 0600)
			}

			os.Remove(legacyPath)
		}
	}

	return nil
}

// GetToken retrieves the device token from token store
func GetToken() (string, error) {
	identity, token, err := LoadIdentity()
	if err != nil {
		return "", fmt.Errorf("failed to load identity: %w", err)
	}
	if identity.DeviceID == "" {
		return "", errors.New("no device ID in identity")
	}
	if token == "" {
		return "", errors.New("no token in store")
	}
	return token, nil
}
