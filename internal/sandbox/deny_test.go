package sandbox

import (
	"context"
	"os/exec"
	"strings"
	"testing"
)

// TestNewDenyModeDeniesExecution verifies that the default fail-closed mode on
// unsupported platforms never runs the command: New(SandboxConfig{Mode: "deny"})
// must return a denySandbox whose Execute returns an explicit error (never
// cmd.Run()).
func TestNewDenyModeDeniesExecution(t *testing.T) {
	sb := New(SandboxConfig{Mode: "deny"})
	if _, ok := sb.(*denySandbox); !ok {
		t.Fatalf("expected *denySandbox, got %T", sb)
	}

	env, err := sb.Prepare(context.Background(), "deny-test", PluginManifest{
		Name: "deny-test",
		Resources: PluginResources{
			MemoryMB:   64,
			CPUSeconds: 1,
		},
	}, false)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer sb.Destroy(context.Background(), env)

	cmd := exec.Command("sh", "-c", "echo ok")
	cmd.Dir = env.WorkDir
	err = sb.Execute(context.Background(), env, cmd)
	if err == nil {
		t.Fatal("Execute returned nil; expected an explicit deny error")
	}
	if !strings.Contains(err.Error(), "denied") {
		t.Fatalf("Execute error %q does not contain %q", err.Error(), "denied")
	}
}

// TestNewOffModeRunsCommand verifies that SANDBOX_MODE=off remains the explicit
// opt-in: it returns a noopSandbox and Execute actually runs the command.
func TestNewOffModeRunsCommand(t *testing.T) {
	sb := New(SandboxConfig{Mode: "off"})
	if _, ok := sb.(*noopSandbox); !ok {
		t.Fatalf("expected *noopSandbox, got %T", sb)
	}

	env, err := sb.Prepare(context.Background(), "off-test", PluginManifest{
		Name: "off-test",
		Resources: PluginResources{
			MemoryMB:   64,
			CPUSeconds: 1,
		},
	}, false)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer sb.Destroy(context.Background(), env)

	cmd := exec.Command("sh", "-c", "echo ok")
	cmd.Dir = env.WorkDir
	if err := sb.Execute(context.Background(), env, cmd); err != nil {
		t.Fatalf("Execute: %v", err)
	}
}
