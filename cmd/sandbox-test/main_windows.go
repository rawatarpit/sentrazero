//go:build windows

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

type testResult struct {
	err      error
	stdout   string
	stderr   string
	duration time.Duration
	timedOut bool
}

// executeTest runs one command through the sandbox (Prepare → Execute →
// Destroy) and captures the outcome, keeping the "[name] OK/ERROR" print
// convention used by the darwin and linux harnesses.
func executeTest(sb sandbox.Sandboxer, name string, args []string, manifest sandbox.PluginManifest, network bool, timeout time.Duration) testResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	env, err := sb.Prepare(ctx, name, manifest, network)
	if err != nil {
		return testResult{err: fmt.Errorf("prepare: %w", err)}
	}
	defer sb.Destroy(ctx, env)

	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Dir = env.WorkDir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	start := time.Now()
	err = sb.Execute(ctx, env, cmd)
	return testResult{
		err:      err,
		stdout:   strings.TrimSpace(stdout.String()),
		stderr:   strings.TrimSpace(stderr.String()),
		duration: time.Since(start),
		timedOut: ctx.Err() != nil,
	}
}

func printResult(name string, r testResult) {
	if r.err != nil {
		if r.timedOut {
			fmt.Printf("[%s] ERROR: timed out after %s\n", name, r.duration)
		} else {
			fmt.Printf("[%s] ERROR: %v\n  stderr: %s\n  stdout: %s (%s)\n", name, r.err, r.stderr, r.stdout, r.duration)
		}
		return
	}
	fmt.Printf("[%s] OK: %s (%s)\n", name, r.stdout, r.duration)
}

func runTest(sb sandbox.Sandboxer, name string, args []string, manifest sandbox.PluginManifest, network bool, timeout time.Duration) {
	printResult(name, executeTest(sb, name, args, manifest, network, timeout))
}

func main() {
	cfg := sandbox.LoadConfig()
	// Force the Job Object path. The Windows sandboxer does not re-exec; it
	// uses Job Objects (kill-on-close, per-process memory cap) plus a
	// best-effort netsh firewall block for network isolation.
	cfg.WindowsJobObject = true
	sb := sandbox.New(cfg)
	fmt.Printf("Type: %T\n\n", sb)

	base := sandbox.PluginManifest{
		Name:    "test",
		Network: false,
		Resources: sandbox.PluginResources{
			MemoryMB: 128, CPUSeconds: 10, TimeoutSeconds: 10,
		},
	}

	// REGRESSION TEST for the OpenThread bug: the sandbox used to pass the
	// process PID to OpenThread (which expects a thread ID), so the
	// CREATE_SUSPENDED child was never resumed and `cmd /c echo hello` hung
	// forever. It MUST now complete with exit 0.
	echoRes := executeTest(sb, "simple-echo", []string{"cmd", "/c", "echo hello"}, base, false, 15*time.Second)
	if echoRes.err != nil || echoRes.timedOut || !strings.Contains(echoRes.stdout, "hello") {
		fmt.Printf("[simple-echo] ERROR: REGRESSION — job-object resume path failed to complete the process: err=%v timedOut=%v (%s)\n  stdout: %s\n  stderr: %s\n",
			echoRes.err, echoRes.timedOut, echoRes.duration, echoRes.stdout, echoRes.stderr)
	} else {
		fmt.Printf("[simple-echo] OK: %s (%s)\n", echoRes.stdout, echoRes.duration)
	}

	// write-workdir: create a file inside the sandbox workdir via cmd.exe
	// redirection, then confirm it actually landed there.
	{
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		env, err := sb.Prepare(ctx, "write-workdir", base, false)
		if err != nil {
			fmt.Printf("[write-workdir] Prepare: %v\n", err)
			cancel()
		} else {
			cmd := exec.CommandContext(ctx, "cmd", "/c", "echo hi > test.txt")
			cmd.Dir = env.WorkDir
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			start := time.Now()
			err = sb.Execute(ctx, env, cmd)
			dur := time.Since(start)
			data, readErr := os.ReadFile(env.WorkDir + "\\test.txt")
			sb.Destroy(ctx, env)
			cancel()
			if err != nil {
				fmt.Printf("[write-workdir] ERROR: %v (%s)\n  stderr: %s\n", err, dur, strings.TrimSpace(stderr.String()))
			} else if readErr != nil {
				fmt.Printf("[write-workdir] ERROR: test.txt was not created: %v (%s)\n", readErr, dur)
			} else {
				fmt.Printf("[write-workdir] OK: test.txt = %q (%s)\n", strings.TrimSpace(string(data)), dur)
			}
		}
	}

	// job-memory-cap: a 128 MiB Job Object per-process memory cap vs a 256 MiB
	// allocation. The process must be killed by the Job Object. Best-effort:
	// PowerShell must be installed for this test to run at all. When the host
	// runs the agent inside a non-nesting job object (e.g. a CI runner) the
	// sandboxer cannot enforce Job Object limits and reports the fact via the
	// JobEnforcementReporter interface — in that case this test is skipped
	// (reported, not asserted).
	{
		memManifest := base
		memManifest.Resources.MemoryMB = 128
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		env, err := sb.Prepare(ctx, "job-memory-cap", memManifest, false)
		if err != nil {
			fmt.Printf("[job-memory-cap] Prepare: %v\n", err)
			cancel()
		} else {
			cmd := exec.CommandContext(ctx, "powershell", "-NoProfile", "-Command", "[byte[]]::new(256*1024*1024); Start-Sleep 3")
			cmd.Dir = env.WorkDir
			var stdout, stderr bytes.Buffer
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			start := time.Now()
			err = sb.Execute(ctx, env, cmd)
			dur := time.Since(start)
			timedOut := ctx.Err() != nil
			sb.Destroy(ctx, env)
			cancel()
			// Degraded path: the host already placed the child in a
			// non-nesting job object, so Job Object limits cannot be applied.
			// Report the skip explicitly so the smoke suite does not treat it
			// as a failed assertion.
			if jr, ok := sb.(sandbox.JobEnforcementReporter); ok && !jr.JobEnforcementAvailable() {
				fmt.Printf("[job-memory-cap] WARN: Job Object unavailable on this host (non-nesting host job); memory cap not enforceable - skipped (%s)\n", dur)
			} else if err != nil && !timedOut {
				fmt.Printf("[job-memory-cap] OK: killed by the Job Object memory cap: %v (%s)\n", err, dur)
			} else if timedOut {
				fmt.Printf("[job-memory-cap] ERROR: not killed within %s (memory cap not enforced)\n", dur)
			} else {
				fmt.Printf("[job-memory-cap] ERROR: allocation survived (exit 0), memory cap not enforced (%s)\n", dur)
			}
		}
	}

	// net-blocked: relies on a netsh outbound firewall rule, which requires
	// admin rights. Without admin the rule is skipped (best-effort in the
	// sandboxer) and the request may succeed — the outcome is reported, not
	// asserted.
	fmt.Println("  NOTE: net-blocked requires admin rights to add the netsh firewall rule; without admin the request may succeed.")
	runTest(sb, "net-blocked", []string{"curl.exe", "-s", "--max-time", "3", "http://example.com"}, base, false, 15*time.Second)

	// net-allowed: no firewall rule; expect success.
	netOk := base
	netOk.Network = true
	runTest(sb, "net-allowed", []string{"curl.exe", "-s", "--max-time", "3", "http://example.com"}, netOk, true, 15*time.Second)

	// End-to-end: a real plugin through the PRODUCTION execution path
	// (plugin.Execute -> RunSandboxedPlugin -> sandbox Prepare/Execute/Destroy)
	// inside the same Job Object machinery the commands above exercise.
	runPluginE2E()
}
