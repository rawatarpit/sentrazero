package plugin

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"sentra-agent/internal/obs"
)

type SignatureKey struct {
	KeyID     string    `json:"key_id"`
	PublicKey []byte    `json:"public_key"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	RevokedAt time.Time `json:"revoked_at,omitempty"`
}

type SignatureVerificationConfig struct {
	RequireSignature bool
	TrustOnFirstUse  bool
	KeyCacheTTL      time.Duration
}

var (
	signatureKeys   = make(map[string]*SignatureKey)
	signatureKeysMu sync.RWMutex
	keyCache        *KeyCache
	keyCacheOnce    sync.Once
	defaultKeyTTL   = 1 * time.Hour
)

type KeyCache struct {
	mu   sync.RWMutex
	keys map[string]*cachedKey
	ttl  time.Duration
}

type cachedKey struct {
	key       *SignatureKey
	fetchedAt time.Time
}

func NewKeyCache(ttl time.Duration) *KeyCache {
	if ttl <= 0 {
		ttl = defaultKeyTTL
	}
	return &KeyCache{
		keys: make(map[string]*cachedKey),
		ttl:  ttl,
	}
}

func (kc *KeyCache) Get(keyID string) (*SignatureKey, bool) {
	kc.mu.RLock()
	defer kc.mu.RUnlock()

	cached, ok := kc.keys[keyID]
	if !ok {
		return nil, false
	}

	if time.Since(cached.fetchedAt) > kc.ttl {
		return nil, false
	}

	return cached.key, true
}

func (kc *KeyCache) Set(keyID string, key *SignatureKey) {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	kc.keys[keyID] = &cachedKey{
		key:       key,
		fetchedAt: time.Now(),
	}
}

func (kc *KeyCache) Invalidate(keyID string) {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	delete(kc.keys, keyID)
}

func (kc *KeyCache) Clear() {
	kc.mu.Lock()
	defer kc.mu.Unlock()

	kc.keys = make(map[string]*cachedKey)
}

func initKeyCache() {
	keyCacheOnce.Do(func() {
		keyCache = NewKeyCache(defaultKeyTTL)
	})
	if keyCache == nil {
		panic("KeyCache: failed to initialize - nil pointer after sync.Once")
	}
}

func GetKeyCache() *KeyCache {
	initKeyCache()
	if keyCache == nil {
		panic("KeyCache: initialized but still nil - unsafe state")
	}
	return keyCache
}

func MustInitKeyCache() {
	initKeyCache()
	if keyCache == nil {
		panic("KeyCache: MustInitKeyCache failed - nil pointer")
	}
	log.Printf("✅ KeyCache initialized (TTL: %v)", defaultKeyTTL)
}

func RegisterSignatureKey(key *SignatureKey) {
	if key == nil || key.KeyID == "" {
		return
	}

	signatureKeysMu.Lock()
	defer signatureKeysMu.Unlock()

	if _, exists := signatureKeys[key.KeyID]; exists {
		return
	}

	signatureKeys[key.KeyID] = key
	GetKeyCache().Invalidate(key.KeyID)
}

func GetSignatureKey(keyID string) (*SignatureKey, bool) {
	signatureKeysMu.RLock()
	defer signatureKeysMu.RUnlock()

	key, ok := signatureKeys[keyID]
	return key, ok
}

func IsKeyValid(key *SignatureKey) bool {
	if key == nil {
		return false
	}

	if !key.RevokedAt.IsZero() && time.Since(key.RevokedAt) > 0 {
		return false
	}

	if !key.ExpiresAt.IsZero() && time.Since(key.ExpiresAt) > 0 {
		return false
	}

	return true
}

func VerifyPluginSignature(ctx context.Context, pluginPath, signatureB64, keyID string) error {
	if signatureB64 == "" {
		return fmt.Errorf("plugin signature is required but not provided")
	}

	if keyID == "" {
		return fmt.Errorf("signature key ID is required but not provided")
	}

	key, ok := GetKeyCache().Get(keyID)
	if !ok {
		key, ok = GetSignatureKey(keyID)
		if !ok {
			return fmt.Errorf("signature key %s not found in cache or local store", keyID)
		}
	}

	if !IsKeyValid(key) {
		return fmt.Errorf("signature key %s is invalid or expired", keyID)
	}

	signature, err := base64.StdEncoding.DecodeString(signatureB64)
	if err != nil {
		return fmt.Errorf("invalid signature encoding: %w", err)
	}

	if len(signature) != ed25519.SignatureSize {
		return fmt.Errorf("invalid signature size: expected %d, got %d", ed25519.SignatureSize, len(signature))
	}

	pluginData, err := os.ReadFile(pluginPath)
	if err != nil {
		return fmt.Errorf("failed to read plugin for signature verification: %w", err)
	}

	if !ed25519.Verify(key.PublicKey, pluginData, signature) {
		return fmt.Errorf("plugin signature verification failed")
	}

	return nil
}

func ComputeFileChecksum(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

func VerifyPluginIntegrity(ctx context.Context, manifest Manifest, pluginDir string) error {
	binaryPath := filepath.Join(pluginDir, manifest.Filename)

	if manifest.Trusted {
		obs.Info("skipping verification for trusted bundled plugin", obs.Field{
			"plugin": manifest.Name,
		})
		return nil
	}

	if manifest.Checksum == "" {
		return fmt.Errorf("checksum is required but not provided")
	}

	actualChecksum, err := ComputeFileChecksum(binaryPath)
	if err != nil {
		return fmt.Errorf("checksum compute failed: %w", err)
	}
	if actualChecksum != manifest.Checksum {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", manifest.Checksum, actualChecksum)
	}

	if manifest.Signature == "" || manifest.SignatureKeyID == "" {
		return fmt.Errorf("signature and signature_key_id are required but not provided")
	}

	if err := VerifyPluginSignature(ctx, binaryPath, manifest.Signature, manifest.SignatureKeyID); err != nil {
		return fmt.Errorf("signature verification failed: %w", err)
	}
	manifest.SignatureVerified = true

	return nil
}

func VerifyAllCachedPlugins(ctx context.Context) (verified, failed int, err error) {
	baseDir, err := EnsurePluginDir()
	if err != nil {
		return 0, 0, err
	}

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return 0, 0, fmt.Errorf("read plugin dir: %w", err)
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		pluginName := e.Name()
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
			pluginDir := filepath.Join(platformDir, platformKey)
			manifestPath := filepath.Join(pluginDir, pluginName+".json")

			data, err := os.ReadFile(manifestPath)
			if err != nil {
				log.Printf("❌ %s/%s: missing manifest", pluginName, platformKey)
				failed++
				continue
			}

			var m Manifest
			if err := json.Unmarshal(data, &m); err != nil {
				log.Printf("❌ %s/%s: invalid manifest", pluginName, platformKey)
				failed++
				continue
			}

			if err := VerifyPluginIntegrity(ctx, m, pluginDir); err != nil {
				log.Printf("❌ %s/%s: verification failed: %v", pluginName, platformKey, err)
				failed++
				continue
			}

			verified++
		}
	}

	log.Printf("🔒 Plugin verification — OK=%d | FAILED=%d", verified, failed)
	return
}
