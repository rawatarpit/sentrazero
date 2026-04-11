package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"sentra-agent/internal/obs"
)

type PluginMeta struct {
	Name           string          `json:"name"`
	Version        string          `json:"version"`
	Filename       string          `json:"filename"`
	URL            string          `json:"url"`
	PluginType     string          `json:"plugin_type"`
	Language       string          `json:"language,omitempty"`
	Checksum       string          `json:"checksum,omitempty"`
	Trusted        bool            `json:"trusted"`
	Network        bool            `json:"network"`
	Resources      PluginResources `json:"resources"`
	Signature      string          `json:"signature,omitempty"`
	SignatureKeyID string          `json:"signature_key_id,omitempty"`
}

func fetchManifest(ctx context.Context) ([]PluginMeta, error) {
	url := os.Getenv("PLUGIN_MANIFEST_URL")
	if url == "" {
		obs.Error(
			"plugin manifest url not configured",
			obs.Field{"component": "plugin.fetch"},
		)
		return nil, errors.New("PLUGIN_MANIFEST_URL not configured")
	}

	obs.Info(
		"fetching plugin manifest",
		obs.Field{"component": "plugin.fetch"},
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		obs.Error(
			"failed to create manifest request",
			obs.Field{"error": err.Error()},
		)
		return nil, err
	}

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		obs.Error(
			"manifest request failed",
			obs.Field{"error": err.Error()},
		)
		return nil, fmt.Errorf("manifest request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		obs.Error(
			"manifest fetch failed",
			obs.Field{"status": resp.StatusCode},
		)
		return nil, fmt.Errorf(
			"manifest fetch status %d: %s",
			resp.StatusCode,
			string(bodyBytes),
		)
	}

	var manifest []PluginMeta
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		obs.Error(
			"manifest decode failed",
			obs.Field{"error": err.Error()},
		)
		return nil, fmt.Errorf("manifest decode failed: %w", err)
	}

	obs.Info(
		"plugin manifest fetched",
		obs.Field{"plugin_count": len(manifest)},
	)

	return manifest, nil
}

func atomicWrite(path string, r io.Reader) error {
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmp, path)
}

func downloadWithRetries(ctx context.Context, url, outPath string) error {
	retries := 3
	if v := os.Getenv("PLUGIN_DOWNLOAD_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			retries = n
		}
	}

	var lastErr error
	client := &http.Client{Timeout: 90 * time.Second}

	for i := 0; i < retries; i++ {
		obs.Info(
			"downloading plugin",
			obs.Field{"attempt": i + 1, "url": url},
		)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			obs.Warn("download attempt failed", obs.Field{
				"attempt": i + 1,
				"error":   err.Error(),
			})
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}

		if resp.StatusCode >= 300 {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("download status %d: %s", resp.StatusCode, string(bodyBytes))
			obs.Warn("download attempt failed", obs.Field{
				"attempt": i + 1,
				"status":  resp.StatusCode,
			})
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}

		if err := atomicWrite(outPath, resp.Body); err != nil {
			resp.Body.Close()
			lastErr = fmt.Errorf("write failed: %w", err)
			obs.Warn("download write failed", obs.Field{
				"attempt": i + 1,
				"error":   err.Error(),
			})
			time.Sleep(time.Duration(i+1) * time.Second)
			continue
		}

		resp.Body.Close()
		_ = os.Chmod(outPath, 0700)

		obs.Info(
			"plugin downloaded successfully",
			obs.Field{"path": outPath},
		)
		return nil
	}

	obs.Error(
		"plugin download failed after retries",
		obs.Field{"url": url, "error": lastErr.Error()},
	)
	return fmt.Errorf("download failed after %d retries: %w", retries, lastErr)
}

func DownloadPlugin(ctx context.Context, p PluginMeta, dir string) (string, error) {
	if p.URL == "" {
		return "", fmt.Errorf("plugin %s missing url", p.Name)
	}

	outPath := filepath.Join(dir, p.Filename)

	if st, err := os.Stat(outPath); err == nil && st.Size() > 0 {
		obs.Info(
			"plugin already present",
			obs.Field{"plugin": p.Name},
		)
		return outPath, nil
	}

	dctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	obs.Info(
		"starting plugin download",
		obs.Field{"plugin": p.Name},
	)

	if err := downloadWithRetries(dctx, p.URL, outPath); err != nil {
		return "", fmt.Errorf("download plugin %s: %w", p.Name, err)
	}

	return outPath, nil
}
