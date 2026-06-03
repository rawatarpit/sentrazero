package plugin

// Manifest describes plugin metadata saved locally.
// This MUST match the remote manifest schema exactly.
// Agent treats this as a SECURITY BOUNDARY.
type Manifest struct {
	// Identity
	Name     string `json:"name"`
	Version  string `json:"version,omitempty"`
	Filename string `json:"filename,omitempty"` // REQUIRED
	URL      string `json:"url,omitempty"`

	// Integrity
	Checksum string `json:"checksum,omitempty"` // REQUIRED (SHA256 hex)

	// Classification
	PluginType string `json:"plugin_type,omitempty"` // "core" | "client"
	Language   string `json:"language,omitempty"`    // rust, python, etc.

	// 🔒 TRUST (NON-NEGOTIABLE)
	Trusted bool `json:"trusted"` // MUST be true for execution

	// 🔒 SANDBOX PERMISSIONS
	Network bool `json:"network"` // default = false (explicit allow)

	// 🔒 RESOURCE LIMITS (MANDATORY)
	Resources PluginResources `json:"resources"`

	// 🔒 MANIFEST SIGNATURE (NON-NEGOTIABLE)
	Signature         string `json:"signature,omitempty"`        // Base64-encoded Ed25519/ECDSA signature
	SignatureKeyID    string `json:"signature_key_id,omitempty"` // Key identifier for verification
	SignatureVerified bool   `json:"signature_verified"`         // Set to true after successful verification

	// Dependencies for v2 runtime (pip packages, etc.)
	Dependencies []RuntimeDependency `json:"dependencies,omitempty"`
}

// RuntimeDependency describes a pip/npm dependency for the plugin.
type RuntimeDependency struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
	Source  string `json:"source,omitempty"`
}

// PluginResources defines hard execution limits.
// Jobs MUST NOT run if any required field is missing or zero.
type PluginResources struct {
	// Memory limit in MB (REQUIRED)
	MemoryMB int64 `json:"memory_mb"`

	// Total CPU time allowed in seconds (REQUIRED)
	CPUSeconds int64 `json:"cpu_seconds"`

	// CPU quota (container-based runtimes only)
	CPULimit float64 `json:"cpu_limit,omitempty"`

	// Wall-clock execution timeout (REQUIRED)
	TimeoutSeconds int64 `json:"timeout_seconds"`
}
