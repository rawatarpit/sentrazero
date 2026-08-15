//go:build darwin

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"sentra-agent/internal/sandbox"
)

func runTest(sb sandbox.Sandboxer, name string, args []string, manifest sandbox.PluginManifest, network bool) {
	ctx := context.Background()
	env, err := sb.Prepare(ctx, name, manifest, network)
	if err != nil {
		fmt.Printf("[%s] Prepare: %v\n", name, err)
		return
	}
	defer sb.Destroy(ctx, env)

	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Dir = env.WorkDir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	start := time.Now()
	err = sb.Execute(ctx, env, cmd)
	duration := time.Since(start)

	out := strings.TrimSpace(stdout.String())
	errStr := strings.TrimSpace(stderr.String())

	if err != nil {
		fmt.Printf("[%s] ERROR: %v\n  stderr: %s\n  stdout: %s (%s)\n", name, err, errStr, out, duration)
	} else {
		fmt.Printf("[%s] OK: %s (%s)\n", name, out, duration)
	}

	// Check profile content
	if data, e := os.ReadFile(env.WorkDir + "/.sandbox.sb"); e == nil {
		fmt.Printf("  Profile preview: %s...\n", string(data[:min(200, len(data))]))
	}
}

func main() {
	cfg := sandbox.LoadConfig()
	cfg.MacOSSeatbelt = true
	sb := sandbox.New(cfg)
	fmt.Printf("Type: %T\n\n", sb)

	base := sandbox.PluginManifest{
		Name:    "test",
		Network: false,
		Resources: sandbox.PluginResources{
			MemoryMB: 128, CPUSeconds: 10, TimeoutSeconds: 10,
		},
	}

	// Test: simple echo
	runTest(sb, "simple-echo", []string{"sh", "-c", "echo hello"}, base, false)

	// Test: write to workdir
	runTest(sb, "write-workdir", []string{"touch", "test.txt"}, base, false)

	// Test: write to workdir with abs path
	env, _ := sb.Prepare(context.Background(), "debug-abs", base, false)
	absPath := env.WorkDir + "/abs_test.txt"
	sb.Destroy(context.Background(), env)
	runTest(sb, "write-abs", []string{"touch", absPath}, base, false)

	// Test: write to /etc
	runTest(sb, "write-etc", []string{"touch", "/etc/sentra-test.txt"}, base, false)

	// Test: read root
	runTest(sb, "read-root", []string{"ls", "/"}, base, false)

	// Test: write to /tmp
	runTest(sb, "write-tmp", []string{"touch", "/tmp/sentra-test.txt"}, base, false)

	// Test: write to /Users
	home, _ := os.UserHomeDir()
	runTest(sb, "write-home", []string{"touch", home + "/sentra-test.txt"}, base, false)

	// Test: network blocked
	runTest(sb, "net-blocked", []string{"curl", "-s", "--max-time", "3", "http://example.com"}, base, false)

	// Test: network allowed
	netOk := base
	netOk.Network = true
	runTest(sb, "net-allowed", []string{"curl", "-s", "--max-time", "3", "http://example.com"}, netOk, true)

	// End-to-end: a real plugin through the PRODUCTION execution path
	// (plugin.Execute -> RunSandboxedPlugin -> sandbox Prepare/Execute/Destroy)
	// inside the same seatbelt sandbox the commands above exercise.
	runPluginE2E()
}
