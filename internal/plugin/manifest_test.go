package plugin

import (
	"errors"
	"testing"
)

func TestManifest_RequiredFields(t *testing.T) {
	m := Manifest{
		Name:     "test-plugin",
		Filename: "test.so",
		Trusted:  true,
		Checksum: "sha256:abc123",
		Resources: PluginResources{
			MemoryMB:       512,
			CPUSeconds:     300,
			TimeoutSeconds: 600,
		},
	}

	if m.Name != "test-plugin" {
		t.Errorf("expected Name=test-plugin, got %s", m.Name)
	}
	if m.Filename != "test.so" {
		t.Errorf("expected Filename=test.so, got %s", m.Filename)
	}
	if !m.Trusted {
		t.Error("expected Trusted=true")
	}
}

func TestManifest_DefaultNetworkFalse(t *testing.T) {
	m := Manifest{
		Name:     "test-plugin",
		Trusted:  true,
		Checksum: "sha256:abc123",
	}

	if m.Network {
		t.Error("expected Network=false by default")
	}
}

func TestManifest_SignatureFields(t *testing.T) {
	m := Manifest{
		Name:              "signed-plugin",
		Trusted:           true,
		Checksum:          "sha256:abc123",
		Signature:         "base64:signature",
		SignatureKeyID:    "key-001",
		SignatureVerified: true,
	}

	if m.Signature != "base64:signature" {
		t.Errorf("expected Signature=base64:signature, got %s", m.Signature)
	}
	if m.SignatureKeyID != "key-001" {
		t.Errorf("expected SignatureKeyID=key-001, got %s", m.SignatureKeyID)
	}
	if !m.SignatureVerified {
		t.Error("expected SignatureVerified=true")
	}
}

func TestPluginResources_ResourceLimits(t *testing.T) {
	r := PluginResources{
		MemoryMB:       1024,
		CPUSeconds:     600,
		CPULimit:       2.0,
		TimeoutSeconds: 900,
	}

	if r.MemoryMB != 1024 {
		t.Errorf("expected MemoryMB=1024, got %d", r.MemoryMB)
	}
	if r.CPUSeconds != 600 {
		t.Errorf("expected CPUSeconds=600, got %d", r.CPUSeconds)
	}
	if r.CPULimit != 2.0 {
		t.Errorf("expected CPULimit=2.0, got %f", r.CPULimit)
	}
	if r.TimeoutSeconds != 900 {
		t.Errorf("expected TimeoutSeconds=900, got %d", r.TimeoutSeconds)
	}
}

func TestPluginResources_ZeroValues(t *testing.T) {
	r := PluginResources{}

	if r.MemoryMB != 0 {
		t.Errorf("expected MemoryMB=0, got %d", r.MemoryMB)
	}
	if r.CPUSeconds != 0 {
		t.Errorf("expected CPUSeconds=0, got %d", r.CPUSeconds)
	}
	if r.TimeoutSeconds != 0 {
		t.Errorf("expected TimeoutSeconds=0, got %d", r.TimeoutSeconds)
	}
}

func TestSandboxResult_Fields(t *testing.T) {
	r := SandboxResult{
		Output:     "test output",
		ExitCode:   0,
		DurationMs: 1500,
		Method:     "docker",
	}

	if r.Output != "test output" {
		t.Errorf("expected Output=test output, got %s", r.Output)
	}
	if r.ExitCode != 0 {
		t.Errorf("expected ExitCode=0, got %d", r.ExitCode)
	}
	if r.DurationMs != 1500 {
		t.Errorf("expected DurationMs=1500, got %d", r.DurationMs)
	}
	if r.Method != "docker" {
		t.Errorf("expected Method=docker, got %s", r.Method)
	}
}

func TestManifest_PluginTypes(t *testing.T) {
	testCases := []struct {
		pluginType string
		language   string
	}{
		{"core", "rust"},
		{"client", "python"},
		{"", ""},
	}

	for _, tc := range testCases {
		m := Manifest{
			Name:       "test",
			PluginType: tc.pluginType,
			Language:   tc.language,
			Trusted:    true,
			Checksum:   "sha256:abc",
		}

		if m.PluginType != tc.pluginType {
			t.Errorf("expected PluginType=%s, got %s", tc.pluginType, m.PluginType)
		}
		if m.Language != tc.language {
			t.Errorf("expected Language=%s, got %s", tc.language, m.Language)
		}
	}
}

func TestExitCodeFromError_Nil(t *testing.T) {
	code := exitCodeFromError(nil)
	if code != 0 {
		t.Errorf("expected exit code 0 for nil error, got %d", code)
	}
}

func TestExitCodeFromError_NonExitable(t *testing.T) {
	testErr := errors.New("test error")
	code := exitCodeFromError(testErr)
	if code != -1 {
		t.Errorf("expected exit code -1 for non-exit error, got %d", code)
	}
}

func TestTruncateOutput_ShortString(t *testing.T) {
	input := "short string"
	result := truncateOutput(input)
	if result != input {
		t.Errorf("expected '%s', got '%s'", input, result)
	}
}

func TestTruncateOutput_LongString(t *testing.T) {
	input := string(make([]byte, 5000))
	for i := range input {
		input = input[:i] + "a" + input[i+1:]
	}

	result := truncateOutput(input)

	if len(result) > 4096+len("... [truncated]") {
		t.Error("truncated output too long")
	}

	expectedSuffix := "... [truncated]"
	if len(result) < len(expectedSuffix) || result[len(result)-len(expectedSuffix):] != expectedSuffix {
		t.Errorf("expected suffix '%s', got '%s'", expectedSuffix, result[len(result)-len(expectedSuffix):])
	}
}

func TestTruncateOutput_Exactly4096(t *testing.T) {
	input := string(make([]byte, 4096))
	for i := range input {
		input = input[:i] + "x" + input[i+1:]
	}

	result := truncateOutput(input)
	if result != input {
		t.Errorf("4096-char string should not be truncated, got %d chars", len(result))
	}
}

func TestManifest_HasResourceLimits(t *testing.T) {
	testCases := []struct {
		name      string
		resources PluginResources
		hasLimits bool
	}{
		{
			name:      "no limits",
			resources: PluginResources{},
			hasLimits: false,
		},
		{
			name:      "memory only",
			resources: PluginResources{MemoryMB: 512},
			hasLimits: true,
		},
		{
			name:      "cpu seconds only",
			resources: PluginResources{CPUSeconds: 300},
			hasLimits: true,
		},
		{
			name:      "cpu limit only",
			resources: PluginResources{CPULimit: 2.0},
			hasLimits: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hasResourceLimits := tc.resources.MemoryMB > 0 ||
				tc.resources.CPUSeconds > 0 ||
				tc.resources.CPULimit > 0

			if hasResourceLimits != tc.hasLimits {
				t.Errorf("expected hasLimits=%v, got %v", tc.hasLimits, hasResourceLimits)
			}
		})
	}
}
