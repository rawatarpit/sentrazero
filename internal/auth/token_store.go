package auth

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/zalando/go-keyring"
)

var (
	ErrTokenNotFound = errors.New("token not found")
	ErrEmptyToken    = errors.New("token is empty")

	tokenStoreManager *TokenStoreManager
	tokenStoreOnce    sync.Once
)

type TokenStore interface {
	Save(deviceID, token string) error
	Load(deviceID string) (string, error)
	Name() string
}

type TokenStoreManager struct {
	primary    TokenStore
	fallback   TokenStore
	useKeyring bool
}

func NewTokenStoreManager() *TokenStoreManager {
	envValue := strings.ToLower(os.Getenv("SENTRA_USE_KEYRING"))
	useKeyring := envValue == "true"

	if !useKeyring && envValue == "" {
		if runtime.GOOS == "darwin" {
			isInteractive := os.Getenv("TERM") != "dumb"
			if isInteractive {
				f, _ := os.Stdin.Stat()
				useKeyring = (f.Mode() & os.ModeCharDevice) != 0
			}
		}
	}

	fileStore := &FileTokenStore{}

	if useKeyring {
		log.Println("[token-store] Keyring enabled for token storage")
		keyringStore := &KeyringTokenStore{}
		return &TokenStoreManager{
			primary:    keyringStore,
			fallback:   fileStore,
			useKeyring: true,
		}
	}

	log.Println("[token-store] Using file-based token storage (set SENTRA_USE_KEYRING=true for keyring)")
	return &TokenStoreManager{
		primary:    fileStore,
		fallback:   nil,
		useKeyring: false,
	}
}

func (m *TokenStoreManager) Save(deviceID, token string) error {
	if token == "" {
		return ErrEmptyToken
	}

	if err := m.primary.Save(deviceID, token); err != nil {
		if m.useKeyring && m.fallback != nil {
			log.Printf("[token-store] ⚠️ %s failed: %v, falling back to file storage", m.primary.Name(), err)
			if fbErr := m.fallback.Save(deviceID, token); fbErr != nil {
				return fbErr
			}
			log.Printf("[token-store] ✅ Token saved via fallback (%s)", m.fallback.Name())
			return nil
		}
		return err
	}

	if m.useKeyring {
		log.Printf("[token-store] ✅ Token saved via %s (fallback available)", m.primary.Name())
	} else {
		log.Printf("[token-store] ✅ Token saved via %s", m.primary.Name())
	}
	return nil
}

func (m *TokenStoreManager) Load(deviceID string) (string, error) {
	token, err := m.primary.Load(deviceID)
	if err == nil && token != "" {
		log.Printf("[token-store] ✅ Token loaded from %s", m.primary.Name())
		return token, nil
	}

	if m.useKeyring && m.fallback != nil {
		if errors.Is(err, ErrTokenNotFound) || token == "" {
			log.Printf("[token-store] ⚠️ Token not found in %s, trying fallback (%s)", m.primary.Name(), m.fallback.Name())
			token, err = m.fallback.Load(deviceID)
			if err == nil && token != "" {
				log.Printf("[token-store] ✅ Token loaded from fallback (%s)", m.fallback.Name())
				return token, nil
			}
		}
	}

	if err != nil {
		log.Printf("[token-store] ⚠️ Failed to load token: %v", err)
		return "", err
	}
	return "", ErrTokenNotFound
}

func (m *TokenStoreManager) Name() string {
	return m.primary.Name()
}

type FileTokenStore struct {
	mu sync.Mutex
}

func (f *FileTokenStore) tokenFilePath(deviceID string) (string, error) {
	if tokenPath := os.Getenv("SENTRA_TOKEN_PATH"); tokenPath != "" {
		hasDevicePlaceholder := strings.Contains(tokenPath, "{device_id}")
		if deviceID != "" && hasDevicePlaceholder {
			tokenPath = strings.Replace(tokenPath, "{device_id}", deviceID, 1)
		} else if deviceID != "" && !hasDevicePlaceholder {
			tokenPath = filepath.Join(filepath.Dir(tokenPath), deviceID+".token")
		}
		return tokenPath, nil
	}

	baseDir := filepath.Join(os.Getenv("HOME"), ".sentra", "tokens")
	if err := os.MkdirAll(baseDir, 0700); err != nil {
		return "", err
	}

	return filepath.Join(baseDir, deviceID+".token"), nil
}

func (f *FileTokenStore) Save(deviceID, token string) error {
	if deviceID == "" || token == "" {
		return ErrEmptyToken
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	path, err := f.tokenFilePath(deviceID)
	if err != nil {
		return err
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(token), 0600); err != nil {
		os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}

	perm, _ := os.Stat(path)
	if perm != nil && (perm.Mode().Perm()&0o77) != 0 {
		os.Chmod(path, 0600)
	}

	return nil
}

func (f *FileTokenStore) Load(deviceID string) (string, error) {
	if deviceID == "" {
		return "", errors.New("deviceID is required")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	path, err := f.tokenFilePath(deviceID)
	if err != nil {
		return "", err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrTokenNotFound
		}
		return "", err
	}

	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", ErrTokenNotFound
	}

	return token, nil
}

func (f *FileTokenStore) Name() string {
	return "file"
}

type KeyringTokenStore struct{}

func (k *KeyringTokenStore) Save(deviceID, token string) error {
	if deviceID == "" || token == "" {
		return ErrEmptyToken
	}

	if err := keyring.Set("sentra-agent", deviceID, token); err != nil {
		return err
	}

	verified, err := keyring.Get("sentra-agent", deviceID)
	if err != nil || verified != token {
		return errors.New("keyring round-trip verification failed")
	}

	return nil
}

func (k *KeyringTokenStore) Load(deviceID string) (string, error) {
	if deviceID == "" {
		return "", errors.New("deviceID is required")
	}

	token, err := keyring.Get("sentra-agent", deviceID)
	if err != nil {
		return "", ErrTokenNotFound
	}

	if token == "" {
		return "", ErrTokenNotFound
	}

	return token, nil
}

func (k *KeyringTokenStore) Name() string {
	return "keyring"
}

func GetTokenStore() *TokenStoreManager {
	tokenStoreOnce.Do(func() {
		tokenStoreManager = NewTokenStoreManager()
	})
	if tokenStoreManager == nil {
		panic("TokenStoreManager: failed to initialize - nil pointer after sync.Once")
	}
	return tokenStoreManager
}
