//go:build darwin || linux || windows

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sentra-agent/internal/plugin"
	"sentra-agent/internal/system"
)

// pluginE2EScript is a tiny bash plugin used by the sandbox harness. It reads
// the JSON payload from stdin and echoes it back as JSON — the same shape the
// production scan/merge plugins emit. bash is used instead of a heavier
// runtime because it is guaranteed to exist on every CI image the smoke tests
// cover: /bin/bash on macOS and ubuntu, Git Bash on Windows. (Node is shipped
// only inside the GitHub toolcache on some macOS images, so its location is
// not on the PATH of a plain step shell.) bash is a first-class supported
// plugin language (see internal/plugin getRunnerForLanguage).
const pluginE2EScript = `#!/bin/bash
input=$(cat)
echo_val=$(printf '%s' "$input" | sed -n 's/.*"echo"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')
printf '{"ok":true,"echo":"%s"}\n' "$echo_val"
`

// runPluginE2E runs a real plugin through the PRODUCTION plugin execution
// path — plugin.Execute -> executeScript -> RunSandboxedPlugin -> sandbox
// Prepare/Execute/Destroy — with a trusted manifest and a JSON payload. This
// is the end-to-end coverage the unit tests cannot provide: it proves an
// actual plugin executes inside the sandbox (user namespaces + seccomp /
// NO_NEW_PRIVS on Linux, Job Object on Windows, seatbelt on macOS) with its
// output captured and returned. It uses the same LoadConfig() the agent uses
// at runtime, so env-driven overrides apply identically.
func runPluginE2E() {
	dir, err := os.MkdirTemp("", "sentra-plugin-e2e-*")
	if err != nil {
		fmt.Printf("[plugin-e2e] ERROR: tempdir: %v\n", err)
		return
	}
	defer os.RemoveAll(dir)

	scriptPath := filepath.Join(dir, "main.sh")
	if err := os.WriteFile(scriptPath, []byte(pluginE2EScript), 0o700); err != nil {
		fmt.Printf("[plugin-e2e] ERROR: write plugin: %v\n", err)
		return
	}

	manifest := plugin.Manifest{
		Name:     "e2e-plugin",
		Version:  "1.0.0",
		Filename: "main.sh",
		Language: "bash",
		Trusted:  true,
		Network:  false,
		Resources: plugin.PluginResources{
			MemoryMB:       128,
			CPUSeconds:     10,
			TimeoutSeconds: 30,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	start := time.Now()
	res, err := plugin.Execute(
		ctx,
		scriptPath,
		manifest,
		`{"echo":"hello-plugin"}`,
		system.DetectExecutionEnv(),
		nil, // nativeRunner: nil is fine for a bash plugin (script path)
	)
	dur := time.Since(start)

	if err != nil {
		fmt.Printf("[plugin-e2e] ERROR: %v (%s)\n", err, dur)
		return
	}

	out := strings.TrimSpace(res.Output)
	var parsed map[string]interface{}
	if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
		fmt.Printf("[plugin-e2e] ERROR: output is not JSON: %v: %q (%s)\n", jsonErr, out, dur)
		return
	}
	ok, _ := parsed["ok"].(bool)
	echo, _ := parsed["echo"].(string)
	if !ok || echo != "hello-plugin" {
		fmt.Printf("[plugin-e2e] ERROR: unexpected output: %q (%s)\n", out, dur)
		return
	}

	fmt.Printf("[plugin-e2e] OK: plugin ran inside sandbox, method=%s echo=%q (%s)\n",
		res.Method, echo, dur)
}
