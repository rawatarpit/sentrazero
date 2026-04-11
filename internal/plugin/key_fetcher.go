package plugin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"sentra-agent/internal/httpclient"
	"sentra-agent/internal/obs"
)

type PluginSigningKey struct {
	KeyID     string     `json:"key_id"`
	PublicKey string     `json:"public_key"`
	Algorithm string     `json:"algorithm"` // ed25519, ecdsa
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt time.Time  `json:"expires_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

type KeyFetcher struct {
	baseURL      string
	anonKey      string
	deviceToken  string
	httpc        *httpclient.Client
	mu           sync.RWMutex
	cache        map[string]*PluginSigningKey
	cacheTTL     time.Duration
	keyLastFetch map[string]time.Time
}

func NewKeyFetcher(baseURL, anonKey, deviceToken string, cacheTTL time.Duration) *KeyFetcher {
	if cacheTTL <= 0 {
		// Bug #12 fix: Reduced TTL from 1 hour to 5 minutes for faster revocation detection
		cacheTTL = 5 * time.Minute
	}
	// Cap maximum TTL to prevent stale keys from being accepted too long
	if cacheTTL > 10*time.Minute {
		cacheTTL = 10 * time.Minute
	}

	return &KeyFetcher{
		baseURL:      baseURL,
		anonKey:      anonKey,
		deviceToken:  deviceToken,
		httpc:        httpclient.NewClient(baseURL, anonKey, deviceToken),
		cache:        make(map[string]*PluginSigningKey),
		cacheTTL:     cacheTTL,
		keyLastFetch: make(map[string]time.Time),
	}
}

func (kf *KeyFetcher) FetchPublicKey(ctx context.Context, keyID string) (*PluginSigningKey, error) {
	kf.mu.RLock()
	if key, ok := kf.cache[keyID]; ok {
		if lastFetch, hasLast := kf.keyLastFetch[keyID]; hasLast {
			if time.Since(lastFetch) < kf.cacheTTL {
				kf.mu.RUnlock()
				return key, nil
			}
		}
	}
	kf.mu.RUnlock()

	path := fmt.Sprintf("/functions/v1/get_plugin_signing_key?key_id=%s", keyID)

	resp, err := kf.httpc.Get(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch key: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("key %s not found", keyID)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("key fetch failed with status %d", resp.StatusCode)
	}

	var keyResp struct {
		KeyID     string `json:"key_id"`
		PublicKey string `json:"public_key"`
		Algorithm string `json:"algorithm"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&keyResp); err != nil {
		return nil, fmt.Errorf("failed to decode key response: %w", err)
	}

	key := &PluginSigningKey{
		KeyID:     keyResp.KeyID,
		PublicKey: keyResp.PublicKey,
		Algorithm: keyResp.Algorithm,
	}

	kf.mu.Lock()
	kf.cache[keyID] = key
	kf.keyLastFetch[keyID] = time.Now()
	kf.mu.Unlock()

	obs.Info("fetched plugin signing key", obs.Field{
		"key_id":    keyID,
		"algorithm": key.Algorithm,
	})

	return key, nil
}

func (kf *KeyFetcher) FetchAllOrgKeys(ctx context.Context, orgID string) ([]*PluginSigningKey, error) {
	path := fmt.Sprintf("/functions/v1/list_plugin_signing_keys?org_id=%s", orgID)

	resp, err := kf.httpc.Get(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch keys: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("key list fetch failed with status %d", resp.StatusCode)
	}

	var keys []*PluginSigningKey
	if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
		return nil, fmt.Errorf("failed to decode keys response: %w", err)
	}

	kf.mu.Lock()
	now := time.Now()
	for _, key := range keys {
		kf.cache[key.KeyID] = key
		kf.keyLastFetch[key.KeyID] = now
	}
	kf.mu.Unlock()

	obs.Info("fetched org plugin signing keys", obs.Field{
		"org_id":    orgID,
		"key_count": len(keys),
	})

	return keys, nil
}

func (kf *KeyFetcher) InvalidateCache(keyID string) {
	kf.mu.Lock()
	defer kf.mu.Unlock()

	delete(kf.cache, keyID)
}

func (kf *KeyFetcher) ClearCache() {
	kf.mu.Lock()
	defer kf.mu.Unlock()

	kf.cache = make(map[string]*PluginSigningKey)
	kf.keyLastFetch = make(map[string]time.Time)
}

func (kf *KeyFetcher) ConvertToSignatureKey(key *PluginSigningKey) *SignatureKey {
	if key == nil {
		return nil
	}

	pubKeyBytes, err := base64.StdEncoding.DecodeString(key.PublicKey)
	if err != nil {
		obs.Warn("failed to decode public key", obs.Field{
			"key_id": key.KeyID,
			"error":  err.Error(),
		})
		return nil
	}

	return &SignatureKey{
		KeyID:     key.KeyID,
		PublicKey: pubKeyBytes,
		CreatedAt: key.CreatedAt,
		ExpiresAt: key.ExpiresAt,
	}
}
