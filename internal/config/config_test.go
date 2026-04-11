package config

import (
	"os"
	"testing"
)

func TestFirstNonEmpty(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected string
	}{
		{
			name:     "first non-empty",
			values:   []string{"first", "", "third"},
			expected: "first",
		},
		{
			name:     "second non-empty",
			values:   []string{"", "second", "third"},
			expected: "second",
		},
		{
			name:     "all empty",
			values:   []string{"", "", ""},
			expected: "",
		},
		{
			name:     "single non-empty",
			values:   []string{"only"},
			expected: "only",
		},
		{
			name:     "nil values",
			values:   nil,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := firstNonEmpty(tt.values...)
			if result != tt.expected {
				t.Errorf("firstNonEmpty() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestGetenvDefault(t *testing.T) {
	testKey := "SENTRA_TEST_KEY"
	original := os.Getenv(testKey)
	defer func() {
		if original != "" {
			os.Setenv(testKey, original)
		} else {
			os.Unsetenv(testKey)
		}
	}()

	os.Unsetenv(testKey)
	result := getenvDefault(testKey, "default_value")
	if result != "default_value" {
		t.Errorf("getenvDefault() = %q, want %q", result, "default_value")
	}

	os.Setenv(testKey, "env_value")
	result = getenvDefault(testKey, "default_value")
	if result != "env_value" {
		t.Errorf("getenvDefault() = %q, want %q", result, "env_value")
	}
}

func TestHostnameFallback(t *testing.T) {
	hostname := hostnameFallback()
	if hostname == "" {
		t.Error("hostnameFallback() returned empty string")
	}
	if hostname == "sentra-agent" {
		t.Log("hostnameFallback() returned default (actual hostname unavailable)")
	}
}

func TestConfig_Context(t *testing.T) {
	cfg := &Config{}

	ctx := cfg.Context()
	if ctx == nil {
		t.Error("Config.Context() returned nil")
	}
}

func TestConfig_Fields(t *testing.T) {
	cfg := &Config{
		BackendURL:     "https://example.com",
		BackendAnonKey: "test-key",
		OrgID:          "org-123",
		OrgName:        "Test Org",
		MaxConcurrency: 4,
	}

	if cfg.BackendURL != "https://example.com" {
		t.Errorf("BackendURL = %q, want %q", cfg.BackendURL, "https://example.com")
	}
	if cfg.BackendAnonKey != "test-key" {
		t.Errorf("BackendAnonKey = %q, want %q", cfg.BackendAnonKey, "test-key")
	}
	if cfg.OrgID != "org-123" {
		t.Errorf("OrgID = %q, want %q", cfg.OrgID, "org-123")
	}
	if cfg.OrgName != "Test Org" {
		t.Errorf("OrgName = %q, want %q", cfg.OrgName, "Test Org")
	}
	if cfg.MaxConcurrency != 4 {
		t.Errorf("MaxConcurrency = %d, want %d", cfg.MaxConcurrency, 4)
	}
}

func TestConfig_DefaultCapabilities(t *testing.T) {
	cfg := &Config{}

	cfg.Capabilities = []string{"scan", "merge", "embedding"}

	if len(cfg.Capabilities) != 3 {
		t.Errorf("len(Capabilities) = %d, want 3", len(cfg.Capabilities))
	}

	expected := []string{"scan", "merge", "embedding"}
	for i, cap := range expected {
		if cfg.Capabilities[i] != cap {
			t.Errorf("Capabilities[%d] = %q, want %q", i, cfg.Capabilities[i], cap)
		}
	}
}
