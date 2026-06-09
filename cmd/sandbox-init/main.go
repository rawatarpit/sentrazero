//go:build linux

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: sandbox-init <command> [args...]")
		os.Exit(1)
	}

	workDir := os.Getenv("SANDBOX_WORK_DIR")
	if workDir == "" {
		workDir = "/tmp/sandbox"
	}

	if err := os.MkdirAll(workDir, 0700); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir workdir: %v\n", err)
		os.Exit(1)
	}

	if err := os.Chdir(workDir); err != nil {
		fmt.Fprintf(os.Stderr, "chdir: %v\n", err)
		os.Exit(1)
	}

	newRoot := os.Getenv("SANDBOX_ROOT")
	if newRoot != "" {
		if err := pivotRoot(newRoot); err != nil {
			fmt.Fprintf(os.Stderr, "pivot_root: %v\n", err)
			os.Exit(1)
		}
	}

	if err := dropPrivileges(); err != nil {
		fmt.Fprintf(os.Stderr, "drop privs: %v\n", err)
		os.Exit(1)
	}

	cmd := exec.Command(os.Args[1], os.Args[2:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(exitErr.ExitCode())
		}
		fmt.Fprintf(os.Stderr, "exec: %v\n", err)
		os.Exit(1)
	}
}

func pivotRoot(newRoot string) error {
	if err := syscall.Mount(newRoot, newRoot, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
		return fmt.Errorf("bind mount new root: %w", err)
	}

	putOld := filepath.Join(newRoot, ".old_root")
	if err := os.MkdirAll(putOld, 0700); err != nil {
		return fmt.Errorf("mkdir put_old: %w", err)
	}

	if err := syscall.PivotRoot(newRoot, putOld); err != nil {
		return fmt.Errorf("pivot_root: %w", err)
	}

	if err := os.Chdir("/"); err != nil {
		return fmt.Errorf("chdir to /: %w", err)
	}

	if err := syscall.Unmount("/.old_root", syscall.MNT_DETACH); err != nil {
		return fmt.Errorf("unmount old_root: %w", err)
	}

	os.RemoveAll("/.old_root")
	return nil
}

func dropPrivileges() error {
	uidStr := os.Getenv("SANDBOX_UID")
	gidStr := os.Getenv("SANDBOX_GID")

	if uidStr == "" && gidStr == "" {
		return nil
	}

	uid := 65534
	gid := 65534
	if uidStr != "" {
		fmt.Sscanf(uidStr, "%d", &uid)
	}
	if gidStr != "" {
		fmt.Sscanf(gidStr, "%d", &gid)
	}
	if err := syscall.Setgid(gid); err != nil {
		return fmt.Errorf("setgid: %w", err)
	}
	if err := syscall.Setuid(uid); err != nil {
		return fmt.Errorf("setuid: %w", err)
	}
	return nil
}
