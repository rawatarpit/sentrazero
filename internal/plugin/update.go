package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"sentra-agent/internal/auth"
)

func AutoUpdatePlugins(ctx context.Context) (updated, skipped, failed int, newPlugins []DBPlugin, err error) {
	log.Println("🔄 Checking for plugin updates...")

	baseDir, err := EnsurePluginDir()
	if err != nil {
		return
	}

	device, token, err := auth.LoadIdentity()
	if err != nil {
		return 0, 0, 0, nil, fmt.Errorf("failed to load device identity: %w", err)
	}

	cfg, err := loadConfig()
	if err != nil {
		return 0, 0, 0, nil, fmt.Errorf("failed to load config: %w", err)
	}

	remotePlugins, err := FetchPluginsFromAPI(ctx, device.DeviceID, device.OrgID, cfg.BackendURL, cfg.BackendAnonKey, token)
	if err != nil {
		return 0, 0, 0, nil, fmt.Errorf("fetch remote plugins from DB: %w", err)
	}

	remoteMap := map[string]DBPlugin{}
	for _, rp := range remotePlugins {
		key := rp.Name + "|" + rp.OS + "|" + rp.Arch
		remoteMap[key] = rp
	}

	localDirs := map[string]bool{}

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return 0, 0, 0, nil, fmt.Errorf("read base plugin dir: %w", err)
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		pluginName := e.Name()
		localDirs[pluginName] = true
		platformDir := filepath.Join(baseDir, pluginName)

		platformEntries, err := os.ReadDir(platformDir)
		if err != nil {
			continue
		}

		for _, pe := range platformEntries {
			if !pe.IsDir() {
				continue
			}

			platformKey := pe.Name()
			parts := splitPlatformKey(platformKey)
			if parts == nil {
				continue
			}

			pluginDir := filepath.Join(platformDir, platformKey)
			manifestPath := filepath.Join(pluginDir, pluginName+".json")

			data, err := os.ReadFile(manifestPath)
			if err != nil {
				log.Printf("⚠️ Missing manifest for %s/%s", pluginName, platformKey)
				failed++
				continue
			}

			var local Manifest
			_ = json.Unmarshal(data, &local)

			remoteKey := pluginName + "|" + parts.os + "|" + parts.arch
			remote, ok := remoteMap[remoteKey]
			if !ok {
				continue
			}

			binaryPath := filepath.Join(pluginDir, filepath.Base(remote.StoragePath))
			needsDownload := false

			st, err := os.Stat(binaryPath)
			if err != nil || st.Size() == 0 {
				needsDownload = true
			}

			if isNewerVersion(remote.Version, local.Version) {
				needsDownload = true
			}

			if needsDownload {
				if remote.SignedURL == "" {
					log.Printf("⚠️ Plugin %s has no signed URL, skipping update", pluginName)
					failed++
					continue
				}

				log.Printf("⬆️ Updating plugin %s/%s → %s", pluginName, platformKey, remote.Version)

			manifest := Manifest{
				Name:           remote.Name,
				Version:        remote.Version,
				Filename:       filepath.Base(remote.StoragePath),
				URL:            remote.SignedURL,
				Checksum:       remote.Checksum,
				PluginType:     remote.PluginType,
				Language:       remote.Language,
				Trusted:        remote.Trusted,
				Signature:      remote.Signature,
				SignatureKeyID: remote.SignatureKeyID,
				Network:        remote.Network,
				Resources:      PluginResources{},
			}

			if len(remote.Resources) > 0 {
				if err := json.Unmarshal(remote.Resources, &manifest.Resources); err != nil {
					log.Printf("⚠️ Failed to parse resources for %s during auto-update: %v", remote.Name, err)
				}
			}

				if err := downloadWithRetries(ctx, remote.SignedURL, binaryPath); err != nil {
					log.Printf("⚠️ Failed to download plugin %s/%s: %v", pluginName, platformKey, err)
					failed++
					continue
				}

				if remote.Checksum != "" {
					if err := verifyFileSHA256(binaryPath, remote.Checksum); err != nil {
						log.Printf("⚠️ Plugin %s/%s checksum verification failed: %v", pluginName, platformKey, err)
						os.Remove(binaryPath)
						failed++
						continue
					}
				}

				// Bug #11 fix: Verify signature after download
				if remote.Signature != "" && remote.SignatureKeyID != "" {
					if err := VerifyPluginSignature(ctx, binaryPath, remote.Signature, remote.SignatureKeyID); err != nil {
						log.Printf("⚠️ Plugin %s/%s signature verification failed: %v", pluginName, platformKey, err)
						os.Remove(binaryPath)
						manifestPathBroken := filepath.Join(pluginDir, pluginName+"_broken_"+time.Now().Format("20060102_150405")+".json")
						os.Rename(manifestPath, manifestPathBroken)
						log.Printf("❌ Corrupted plugin quarantined: %s", manifestPathBroken)
						failed++
						continue
					}
					log.Printf("✅ Plugin %s/%s signature verified", pluginName, platformKey)
				}

				nData, _ := json.MarshalIndent(manifest, "", "  ")
				_ = os.WriteFile(manifestPath, nData, 0644)

				updated++
			} else {
				skipped++
			}
		}
	}

	// Install any plugins that exist remotely but are missing locally
	for _, rp := range remotePlugins {
		if !ShouldRunPlugin(device.DeviceID, rp.RolloutPercentage) {
			continue
		}

		pluginOS := rp.OS
		pluginArch := rp.Arch
		if pluginOS == "" || pluginArch == "" {
			pluginOS = runtime.GOOS
			pluginArch = runtime.GOARCH
		}
		localName := rp.Name

		if localDirs[localName] {
			continue
		}

		installed, err := InstallPlugin(ctx, rp, baseDir)
		if err != nil {
			log.Printf("⚠️ Failed to install new plugin %s: %v", rp.Name, err)
			failed++
			continue
		}
		if installed {
			newPlugins = append(newPlugins, rp)
			log.Printf("✅ Installed new plugin: %s (version: %s)", rp.Name, rp.Version)
		}
	}

	log.Printf("📦 Plugin update summary: Updated=%d | Skipped=%d | Failed=%d | New=%d", updated, skipped, failed, len(newPlugins))
	return
}

type platformParts struct {
	os   string
	arch string
}

func splitPlatformKey(key string) *platformParts {
	knownOS := map[string]bool{
		"linux": true, "darwin": true, "windows": true, "freebsd": true, "netbsd": true, "openbsd": true,
	}
	knownArch := map[string]bool{
		"amd64": true, "386": true, "arm64": true, "arm": true, "armv7": true, "ppc64": true, "riscv64": true,
	}

	for _, sep := range []string{"-", "_", "/"} {
		parts := strings.SplitN(key, sep, 2)
		if len(parts) != 2 {
			continue
		}
		os, arch := parts[0], parts[1]
		if knownOS[os] && knownArch[arch] {
			return &platformParts{
				os:   os,
				arch: arch,
			}
		}
	}
	return nil
}

// ----------------------------
// VERSION COMPARISON SUPPORT
// ----------------------------

func isNewerVersion(remote, local string) bool {
	if local == "" {
		return true
	}

	remoteParts := parseSemver(remote)
	localParts := parseSemver(local)

	if remoteParts == nil || localParts == nil {
		return remote != local
	}

	if remoteParts.major != localParts.major {
		return remoteParts.major > localParts.major
	}
	if remoteParts.minor != localParts.minor {
		return remoteParts.minor > localParts.minor
	}
	return remoteParts.patch > localParts.patch
}

type semverParts struct {
	major, minor, patch int
}

func parseSemver(version string) *semverParts {
	var major, minor, patch int
	parts := strings.Split(version, ".")
	if len(parts) < 3 {
		return nil
	}
	if _, err := fmt.Sscanf(parts[0], "%d", &major); err != nil {
		return nil
	}
	if _, err := fmt.Sscanf(parts[1], "%d", &minor); err != nil {
		return nil
	}
	if _, err := fmt.Sscanf(parts[2], "%d", &patch); err != nil {
		return nil
	}
	return &semverParts{major: major, minor: minor, patch: patch}
}
