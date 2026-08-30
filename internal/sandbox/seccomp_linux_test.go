//go:build linux

package sandbox

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// TestSeccompSupportedArch verifies the architecture policy: amd64 and arm64
// have audited allowlists (and therefore seccomp enforcement); everything else
// deliberately falls back to NO_NEW_PRIVS-only. Since this test only runs on
// the Go test host's own architecture, the other side of the policy is covered
// by the cross-compile matrix (build/vet for 386/riscv64/ppc64le/s390x).
func TestSeccompSupportedArch(t *testing.T) {
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		t.Skipf("seccomp architecture policy only asserted for amd64/arm64 (running on %s)", runtime.GOARCH)
	}
	if !seccompSupportedArch() {
		t.Fatalf("seccompSupportedArch() = false on %s; expected true", runtime.GOARCH)
	}
}

// TestSyscallTablesAreUnique guards against transcription errors: a duplicated
// syscall number in a table would both waste a BPF slot and silently narrow
// the allowlist's intent (the duplicate command is dead weight, not a bug that
// kills processes — but a dup usually signals a wrong-number mistake nearby).
func TestSyscallTablesAreUnique(t *testing.T) {
	tables := map[string][]uint32{
		"x86_64": allowedSyscallsX86_64,
		"arm64":  allowedSyscallsArm64,
	}
	for name, table := range tables {
		if len(table) == 0 {
			t.Fatalf("%s allowlist is empty", name)
		}
		seen := make(map[uint32]bool, len(table))
		for _, nr := range table {
			if seen[nr] {
				t.Fatalf("%s allowlist duplicates syscall number %d", name, nr)
			}
			seen[nr] = true
		}
	}
}

// syscall queries that every runtime needs after exec. If any of these were
// missing from the allowlist, the exec'd process would be SIGSYS-killed before
// it could do anything useful.
func TestRequiredSyscallsPresent(t *testing.T) {
	required := map[string][]uint32{
		// read, write, mmap, mprotect, munmap, execve, exit, exit_group
		"x86_64": {0, 1, 9, 10, 11, 59, 60, 231},
		// read, write, mmap, mprotect, munmap, execve, exit, exit_group
		"arm64": {63, 64, 222, 226, 215, 221, 93, 94},
	}
	for arch, want := range required {
		table := allowedSyscallsX86_64
		if arch == "arm64" {
			table = allowedSyscallsArm64
		}
		present := make(map[uint32]bool, len(table))
		for _, nr := range table {
			present[nr] = true
		}
		for _, nr := range want {
			if !present[nr] {
				t.Errorf("%s allowlist missing required syscall %d", arch, nr)
			}
		}
	}
}

// TestBuildSeccompFilterLayout verifies the BPF program structure without
// needing privileges: arch check first, then the syscall allowlist, then the
// default-deny kill and the allow label. This catches mistakes in the linear
// scan index math (off-by-ones here SIGSYS everything or allow everything).
func TestBuildSeccompFilterLayout(t *testing.T) {
	n := len(syscallsForArch())
	filter := buildSeccompFilter()

	// [0] LD ABS 4 (arch) ; [1] JEQ arch ; [2] RET KILL ; [3] LD ABS 0 (nr);
	// [4..4+n-1] n × JEQ ; [last-1] RET KILL ; [last] RET ALLOW
	wantLen := 4 + n + 2
	if len(filter) != wantLen {
		t.Fatalf("filter length = %d, want %d (n=%d)", len(filter), wantLen, n)
	}
	if filter[0].Code != uint16(bpfLD|bpfABS|bpfK) || filter[0].K != seccompDataArch {
		t.Fatalf("filter[0] = {%#x,%d,...}, want LD ABS seccomp_data.arch", filter[0].Code, filter[0].K)
	}
	if filter[1].Code != uint16(bpfJMP|bpfJEQ|bpfK) || filter[1].K != detectAuditArch() {
		t.Fatalf("filter[1] = {%#x,%d,...}, want JEQ audit-arch %#x", filter[1].Code, filter[1].K, detectAuditArch())
	}
	if filter[2].Code != uint16(bpfRET|bpfK) || filter[2].K != seccompRetKill {
		t.Fatalf("filter[2] = {%#x,%d,...}, want RET KILL for mismatched arch", filter[2].Code, filter[2].K)
	}
	if filter[3].Code != uint16(bpfLD|bpfABS|bpfK) || filter[3].K != seccompDataSyscallNR {
		t.Fatalf("filter[3] = {%#x,%d,...}, want LD ABS seccomp_data.nr", filter[3].Code, filter[3].K)
	}
	// Every syscall entry must be a JEQ.
	for i := 4; i < 4+n; i++ {
		if filter[i].Code != uint16(bpfJMP|bpfJEQ|bpfK) {
			t.Fatalf("filter[%d] = {%#x,...}, want a JEQ syscall check", i, filter[i].Code)
		}
	}
	last := len(filter) - 1
	if filter[last-1].Code != uint16(bpfRET|bpfK) || filter[last-1].K != seccompRetKill {
		t.Fatalf("filter[last-1] should be the default-deny RET KILL")
	}
	if filter[last].Code != uint16(bpfRET|bpfK) || filter[last].K != seccompRetAllow {
		t.Fatalf("filter[last] should be the RET ALLOW label")
	}
}

// TestSeccompFilterBlockedSyscall proves the filter actually kills a blocked
// syscall (reboot) with SIGSYS while letting an allowed syscall (getpid)
// through. It re-execs the test binary as a child that installs the same
// PR_SET_NO_NEW_PRIVS + filter sequence the agent uses.
//
// The child path (helperSeccomp) deliberately never returns: it is killed by
// SIGSYS. Skipped when the kernel lacks CONFIG_SECCOMP_FILTER, exactly like
// the production degrade path.
func TestSeccompFilterBlockedSyscall(t *testing.T) {
	if os.Getenv("SENTRA_SECCOMP_TEST_REEXEC") == "1" {
		helperSeccomp(t)
		return
	}
	if !probeSeccompAvailable() {
		t.Skip("kernel lacks CONFIG_SECCOMP_FILTER; filter would not be applied anyway")
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestSeccompFilterBlockedSyscall")
	cmd.Env = append(os.Environ(), "SENTRA_SECCOMP_TEST_REEXEC=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("child exited 0 unexpectedly (blocked syscall survived); output: %s", out)
	}
	// Exit 77 is the helper's "filter could not be installed" signal (see
	// helperSeccomp). Production degrades gracefully in exactly this case,
	// so the test skips rather than failing — this is the same behavior the
	// smoke script encodes as "kernel lacks CONFIG_SECCOMP_FILTER" (GitHub
	// runner kernels refuse SECCOMP_SET_MODE_FILTER even when the sysctl
	// file exists, so the SIGSYS assertion cannot be demonstrated there).
	if code := cmd.ProcessState.ExitCode(); code == 77 {
		t.Skipf("SECCOMP_SET_MODE_FILTER refused by this environment (kernel/outer filter): %s", out)
	}

	ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok {
		t.Fatalf("no wait status (err=%v); output: %s", err, out)
	}
	if !ws.Signaled() || ws.Signal() != syscall.SIGSYS {
		t.Fatalf("child terminated with %v (exit=%d), want SIGSYS; output: %s",
			ws, cmd.ProcessState.ExitCode(), out)
	}
}

func helperSeccomp(t *testing.T) {
	// Mirror SeccompExec's thread pinning: the filter is per-thread and the
	// kill must happen on the same thread that applied it. This function is
	// the only goroutine left on this thread.
	runtime.LockOSThread()

	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		fmt.Fprintf(os.Stderr, "PR_SET_NO_NEW_PRIVS: %v\n", err)
		os.Exit(1)
	}
	if err := applySeccompFilter(buildSeccompFilter()); err != nil {
		// Same degrade set as production SeccompExec: kernels or outer
		// filters that refuse SECCOMP_SET_MODE_FILTER cannot be tested
		// here. Exit 77 (EX_CONFIG-ish) tells the parent to SKIP, keeping
		// the test green on hosts that disable filter mode.
		fmt.Fprintf(os.Stderr, "applySeccompFilter: %v\n", err)
		os.Exit(77)
	}

	// Allowed syscall must still work: getpid (allowed on every audited arch).
	pid, _, errno := unix.Syscall(unix.SYS_GETPID, 0, 0, 0)
	if errno != 0 || pid == 0 {
		fmt.Fprintf(os.Stderr, "getpid unexpectedly blocked (errno=%v)\n", errno)
		os.Exit(2)
	}

	// Blocked syscall: reboot is excluded from both allowlists. A working
	// filter SIGSYS-kills this process before the syscall executes.
	_, _, errno = unix.Syscall(unix.SYS_REBOOT, unix.LINUX_REBOOT_CMD_RESTART, 0, 0)
	fmt.Fprintf(os.Stderr, "reboot unexpectedly returned (errno=%v); filter is not enforcing\n", errno)
	os.Exit(0)
}
