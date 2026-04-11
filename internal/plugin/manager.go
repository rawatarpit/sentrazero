package plugin

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"sentra-agent/internal/obs"
)

func verifyManifestSignatureFromEnv(manifest Manifest) error {
	if manifest.Signature == "" {
		return fmt.Errorf("manifest signature is required but missing")
	}
	if manifest.SignatureKeyID == "" {
		return fmt.Errorf("manifest signature_key_id is required")
	}

	pubKeyBase64 := os.Getenv("PLUGIN_SIGNING_KEY_" + strings.ToUpper(manifest.SignatureKeyID))

	if pubKeyBase64 == "" {
		if sigKey, ok := GetSignatureKey(manifest.SignatureKeyID); ok && IsKeyValid(sigKey) {
			pubKeyBase64 = base64.StdEncoding.EncodeToString(sigKey.PublicKey)
		}
	}

	if pubKeyBase64 == "" {
		return fmt.Errorf(
			"signing key not found for key_id=%s (checked env PLUGIN_SIGNING_KEY_%s and in-memory store)",
			manifest.SignatureKeyID,
			strings.ToUpper(manifest.SignatureKeyID),
		)
	}

	keyBytes, err := base64.StdEncoding.DecodeString(pubKeyBase64)
	if err != nil {
		return fmt.Errorf("invalid public key: %w", err)
	}
	if len(keyBytes) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid public key size: expected %d, got %d", ed25519.PublicKeySize, len(keyBytes))
	}
	pubKey := ed25519.PublicKey(keyBytes)

	sigBytes, err := base64.StdEncoding.DecodeString(manifest.Signature)
	if err != nil {
		return fmt.Errorf("invalid signature encoding: %w", err)
	}

	signingData := fmt.Sprintf("%s|%s|%s|%s",
		manifest.Name,
		manifest.Version,
		manifest.Filename,
		manifest.Checksum,
	)
	hash := sha256.Sum256([]byte(signingData))

	if !ed25519.Verify(pubKey, hash[:], sigBytes) {
		obs.Warn("manifest signature verification failed", obs.Field{
			"plugin_name": manifest.Name,
			"key_id":      manifest.SignatureKeyID,
		})
		return fmt.Errorf("manifest signature verification failed for plugin %s", manifest.Name)
	}

	obs.Info("manifest signature verified", obs.Field{
		"plugin_name": manifest.Name,
		"key_id":      manifest.SignatureKeyID,
	})
	return nil
}

// -----------------------------------------------------------------------------
// Filesystem helpers
// -----------------------------------------------------------------------------

func EnsurePluginDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	sentraDir := filepath.Join(home, ".sentra")
	if err := os.MkdirAll(sentraDir, 0700); err != nil {
		return "", err
	}
	dir := filepath.Join(sentraDir, "plugins")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	if err := enforcePermissions(dir); err != nil {
		return "", err
	}
	return dir, nil
}

func enforcePermissions(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	mode := info.Mode().Perm()
	if mode&0077 != 0 {
		if err := os.Chmod(path, 0700); err != nil {
			return fmt.Errorf("failed to secure permissions on %s: %w", path, err)
		}
	}
	return nil
}

func validatePluginPermissions(pluginDir, binaryPath string) error {
	if err := enforcePermissions(pluginDir); err != nil {
		return err
	}
	if err := enforcePermissions(binaryPath); err != nil {
		return err
	}
	return nil
}

func verifyFileSHA256(path, expected string) error {
	if expected == "" {
		return fmt.Errorf("checksum is required but missing")
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}

	got := hex.EncodeToString(h.Sum(nil))
	if got != expected {
		return fmt.Errorf("checksum mismatch: expected=%s got=%s", expected, got)
	}
	return nil
}

// -----------------------------------------------------------------------------
// Public API
// -----------------------------------------------------------------------------

// LoadAndUpdatePlugin enforces strict plugin trust rules.
// NO network fetches.
// NO auto-repair.
// Fail closed.
func LoadAndUpdatePlugin(
	_ context.Context,
	pluginName string,
) (string, Manifest, error) {

	baseDir, err := EnsurePluginDir()
	if err != nil {
		return "", Manifest{}, err
	}

	platform := getCurrentPlatform()
	pluginDir := filepath.Join(baseDir, pluginName, platform)
	manifestPath := filepath.Join(pluginDir, pluginName+".json")

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return "", Manifest{}, fmt.Errorf("missing manifest for plugin %s", pluginName)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return "", Manifest{}, fmt.Errorf("invalid manifest for %s: %w", pluginName, err)
	}

	if !manifest.Trusted {
		return "", Manifest{}, fmt.Errorf("plugin %s is not trusted", pluginName)
	}

	if err := verifyManifestSignatureFromEnv(manifest); err != nil {
		return "", Manifest{}, fmt.Errorf("plugin %s signature verification failed: %w", pluginName, err)
	}

	if manifest.Filename == "" {
		return "", Manifest{}, fmt.Errorf("plugin %s missing filename", pluginName)
	}

	binaryPath := filepath.Join(pluginDir, manifest.Filename)

	if _, err := os.Stat(binaryPath); err != nil {
		return "", Manifest{}, fmt.Errorf("plugin binary missing: %s", binaryPath)
	}

	if err := validatePluginPermissions(pluginDir, binaryPath); err != nil {
		return "", Manifest{}, fmt.Errorf("plugin %s permission validation failed: %w", pluginName, err)
	}

	if err := verifyFileSHA256(binaryPath, manifest.Checksum); err != nil {
		return "", Manifest{}, fmt.Errorf("plugin %s failed verification: %w", pluginName, err)
	}

	return binaryPath, manifest, nil
}

func getCurrentPlatform() string {
	return runtime.GOOS + "-" + runtime.GOARCH
}

func InstallBundledPlugins() error {
	baseDir, err := EnsurePluginDir()
	if err != nil {
		return fmt.Errorf("failed to ensure plugin dir: %w", err)
	}

	bundledDir := os.Getenv("SENTRA_PLUGIN_BUNDLE_PATH")
	if bundledDir == "" {
		execPath, _ := os.Executable()
		bundledDir = filepath.Join(filepath.Dir(execPath), "bundled", "plugins")
	} else {
		bundledDir = filepath.Join(bundledDir, "plugins")
	}

	if _, err := os.Stat(bundledDir); os.IsNotExist(err) {
		bundledDir = filepath.Join(os.Getenv("HOME"), ".sentra", "bundled", "plugins")
	}

	bundledManifestPath := filepath.Join(bundledDir, "plugin_scan_metadata.json")
	if _, err := os.Stat(bundledManifestPath); err == nil {
		pluginDir := filepath.Join(baseDir, "plugin_scan_metadata", getCurrentPlatform())
		if err := os.MkdirAll(pluginDir, 0700); err != nil {
			obs.Warn("failed to create plugin directory", obs.Field{
				"error": err.Error(),
			})
			return err
		}

		binarySrc := filepath.Join(bundledDir, "scan_metadata.py")
		binaryDst := filepath.Join(pluginDir, "scan_metadata.py")

		if _, err := os.Stat(binarySrc); err == nil {
			data, _ := os.ReadFile(binarySrc)
			if err := os.WriteFile(binaryDst, data, 0700); err != nil {
				obs.Warn("failed to install bundled plugin", obs.Field{
					"error": err.Error(),
				})
			} else {
				manifestData, _ := os.ReadFile(bundledManifestPath)
				manifestDst := filepath.Join(pluginDir, "plugin_scan_metadata.json")
				os.WriteFile(manifestDst, manifestData, 0644)
				obs.Info("bundled plugin installed", obs.Field{
					"plugin": "plugin_scan_metadata",
				})
			}
		}
	}

	return nil
}

func init() {
	InstallBundledPlugins()
}
