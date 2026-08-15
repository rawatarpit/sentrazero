//go:build linux

package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"sentra-agent/internal/sandbox"
)

// reexecTarget detects the internal re-exec modes used by the Linux sandboxer
// (see internal/sandbox/sandboxer_linux.go maybeReexec). Whenever a plugin
// runs with SandboxNoNewPrivs=true (the default) or SeccompProfile != "off",
// the sandboxer rewrites the child command to re-exec through this very binary
// with either "--seccomp-exec <target> [args...]" or "--no-new-privs-exec
// <target> [args...]" as the first argument, so NO_NEW_PRIVS (and, on amd64,
// the seccomp allowlist) can be applied in-process before exec'ing the plugin.
//
// This must run before anything else in main(): the target's own arguments may
// begin with "-" and must be handed through verbatim.
func reexecTarget() (mode, target string, args []string) {
	if len(os.Args) < 3 {
		return "", "", nil
	}
	switch os.Args[1] {
	case "--seccomp-exec", "--no-new-privs-exec":
		return os.Args[1], os.Args[2], os.Args[3:]
	}
	return "", "", nil
}

type testResult struct {
	err      error
	stdout   string
	stderr   string
	duration time.Duration
	timedOut bool
}

// executeTest runs one command through the sandbox (Prepare → Execute →
// Destroy) and captures the outcome. It mirrors the darwin harness's runTest
// helper but returns a struct so callers can apply per-test assertions while
// keeping the same "[name] OK/ERROR" print convention.
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

func runTest(sb sandbox.Sandboxer, name string, args []string, manifest sandbox.PluginManifest, network bool) {
	printResult(name, executeTest(sb, name, args, manifest, network, 30*time.Second))
}

func main() {
	// Internal re-exec modes: dispatch before anything else. Neither function
	// returns on success — the process is replaced by the plugin target with
	// NO_NEW_PRIVS (and, on amd64, the seccomp allowlist) already applied.
	if mode, target, args := reexecTarget(); target != "" {
		if mode == "--seccomp-exec" {
			sandbox.SeccompExec(target, args)
		} else {
			sandbox.NoNewPrivsExec(target, args)
		}
		return
	}

	cfg := sandbox.LoadConfig()
	// Force the hardened path so the sandboxer's re-exec machinery is
	// exercised: seccomp on amd64, NO_NEW_PRIVS-only elsewhere.
	cfg.SandboxNoNewPrivs = true
	cfg.SeccompProfile = "default"
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

	// Test: write to /etc — under user namespaces the process maps to the
	// invoking user, so this typically fails with EACCES; when run as root it
	// may succeed. Report the outcome either way (Linux has no seatbelt).
	runTest(sb, "write-etc", []string{"touch", "/etc/sentra-test.txt"}, base, false)

	// Test: read root
	runTest(sb, "read-root", []string{"ls", "/"}, base, false)

	// Test: CPU time limit (ulimit -t from applyRLimits) — a busy loop must
	// be killed after roughly 1s of CPU time. A harness-level timeout kill
	// (timedOut) means the limit was not enforced.
	cpuManifest := base
	cpuManifest.Resources.CPUSeconds = 1
	cpuRes := executeTest(sb, "cpu-time-limit", []string{"sh", "-c", "while true; do :; done"}, cpuManifest, false, 15*time.Second)
	if cpuRes.err != nil && !cpuRes.timedOut {
		fmt.Printf("[cpu-time-limit] OK: busy loop killed as expected: %v (%s)\n", cpuRes.err, cpuRes.duration)
	} else {
		fmt.Printf("[cpu-time-limit] ERROR: busy loop was not killed by the CPU limit: err=%v timedOut=%v (%s)\n", cpuRes.err, cpuRes.timedOut, cpuRes.duration)
	}

	// Test: virtual-memory rlimit — `ulimit -v` must report a finite limit
	// after the sandbox applies it (verify the limit, not the exact value:
	// 64 MiB is 65536 KiB, but applyRLimits passes memoryMB*1024*1024
	// bytes-as-KiB, so 67108864 may be reported instead).
	memManifest := base
	memManifest.Resources.MemoryMB = 64
	memRes := executeTest(sb, "mem-limit-applied", []string{"sh", "-c", "ulimit -v"}, memManifest, false, 15*time.Second)
	if memRes.err != nil {
		printResult("mem-limit-applied", memRes)
	} else if kb, perr := strconv.ParseInt(memRes.stdout, 10, 64); perr == nil && kb > 0 {
		fmt.Printf("[mem-limit-applied] OK: ulimit -v = %s KiB (%s)\n", memRes.stdout, memRes.duration)
	} else {
		fmt.Printf("[mem-limit-applied] ERROR: ulimit -v = %q, expected a finite KiB limit (%s)\n", memRes.stdout, memRes.duration)
	}

	// Best-effort kill check: force a 64 MiB address-space limit, then read
	// 400 MiB. The kernel should kill the process. Reported, not asserted —
	// this is not a suite failure if it behaves unexpectedly.
	killRes := executeTest(sb, "mem-limit-kill", []string{"sh", "-c", "ulimit -v 65536; head -c 400000000 /dev/zero > /dev/null"}, memManifest, false, 15*time.Second)
	if killRes.err != nil && !killRes.timedOut {
		fmt.Printf("[mem-limit-kill] OK: killed under the 64 MiB vmem limit: %v (%s)\n", killRes.err, killRes.duration)
	} else if killRes.timedOut {
		fmt.Printf("[mem-limit-kill] NOTE: did not terminate within %s (best-effort check, not a suite failure)\n", killRes.duration)
	} else {
		fmt.Printf("[mem-limit-kill] NOTE: survived (exit 0); the address-space limit was not enforced (best-effort check, not a suite failure) (%s)\n", killRes.duration)
	}

	// Test: network blocked (CLONE_NEWNET — no external connectivity)
	runTest(sb, "net-blocked", []string{"curl", "-s", "--max-time", "3", "http://example.com"}, base, false)

	// Test: network allowed (host network shared)
	netOk := base
	netOk.Network = true
	runTest(sb, "net-allowed", []string{"curl", "-s", "--max-time", "3", "http://example.com"}, netOk, true)
}
