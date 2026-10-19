package plugin

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func writeTestPlugin(t *testing.T, home, name string, body []byte, signed bool) (string, Manifest, ed25519.PublicKey) {
	t.Helper()

	pluginDir := filepath.Join(home, ".sentra", "plugins", name, "any-any")
	if err := os.MkdirAll(pluginDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	pluginPath := filepath.Join(pluginDir, "plugin.py")
	if err := os.WriteFile(pluginPath, body, 0o700); err != nil {
		t.Fatalf("write plugin: %v", err)
	}

	sum := sha256.Sum256(body)
	manifest := Manifest{
		Name:     name,
		Version:  "1.0.0",
		Filename: "plugin.py",
		Checksum: encodeHex(sum[:]),
		Trusted:  true,
	}

	var pub ed25519.PublicKey
	if signed {
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		keyID := "test-key"
		RegisterSignatureKey(&SignatureKey{KeyID: keyID, PublicKey: pub})
		sig := ed25519.Sign(priv, sum[:])
		manifest.Signature = base64.StdEncoding.EncodeToString(sig)
		manifest.SignatureKeyID = keyID
	}

	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(pluginDir, name+".json"), data, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	return pluginPath, manifest, pub
}

func encodeHex(b []byte) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexdigits[v>>4]
		out[i*2+1] = hexdigits[v&0x0f]
	}
	return string(out)
}

// Regression test: plugins installed under any-any must verify successfully.
func TestLoadAndUpdatePlugin_AnyAnySigned(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	_, _, _ = writeTestPlugin(t, home, "any-any-plugin", []byte("print('hi')\n"), true)

	if _, _, err := LoadAndUpdatePlugin(context.Background(), "any-any-plugin"); err != nil {
		t.Fatalf("expected signed any-any plugin to load, got: %v", err)
	}
}

func TestLoadAndUpdatePlugin_AnyAnyTampered(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	pluginPath, _, _ := writeTestPlugin(t, home, "tampered-plugin", []byte("print('hi')\n"), true)
	if err := os.WriteFile(pluginPath, []byte("print('evil')\n"), 0o700); err != nil {
		t.Fatalf("tamper: %v", err)
	}

	if _, _, err := LoadAndUpdatePlugin(context.Background(), "tampered-plugin"); err == nil {
		t.Fatal("expected tampered plugin to be rejected")
	}
}

func TestLoadAndUpdatePlugin_StrictRejectsUnsigned(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("SENTRA_STRICT_PLUGIN_SIGNATURES", "1")

	_, _, _ = writeTestPlugin(t, home, "unsigned-plugin", []byte("print('hi')\n"), false)

	if _, _, err := LoadAndUpdatePlugin(context.Background(), "unsigned-plugin"); err == nil {
		t.Fatal("expected unsigned plugin to be rejected in strict mode")
	}
}

func TestLoadAndUpdatePlugin_PermissiveAllowsUnsigned(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("SENTRA_STRICT_PLUGIN_SIGNATURES", "")

	_, _, _ = writeTestPlugin(t, home, "unsigned-plugin-2", []byte("print('hi')\n"), false)

	if _, _, err := LoadAndUpdatePlugin(context.Background(), "unsigned-plugin-2"); err != nil {
		t.Fatalf("expected unsigned plugin to load in permissive mode, got: %v", err)
	}
}
