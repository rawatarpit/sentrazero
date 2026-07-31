//go:build linux

package sandbox

import (
	"fmt"
	"os"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

// BPF/seccomp constants (not all exposed by x/sys/unix).
const (
	seccompSetModeFilter   = 2
	seccompFilterFlagTsync = 1

	seccompRetKill  = 0x00000000
	seccompRetAllow = 0x7fff0000

	// BPF instruction classes
	bpfLD  = 0x00
	bpfJMP = 0x05
	bpfRET = 0x06

	bpfABS = 0x20

	bpfJEQ = 0x10

	bpfK = 0x00

	// Offsets within seccomp_data (64-bit).
	seccompDataArch      = 4
	seccompDataSyscallNR = 0

	// Audit architectures.
	auditArchX86_64  = 0xC000003E
	auditArchI386    = 0x40000003
	auditArchAArch64 = 0xC00000B7
)

// sockFilter is a single BPF instruction (struct sock_filter in C).
type sockFilter struct {
	Code uint16
	Jt   uint8
	Jf   uint8
	K    uint32
}

// sockFprog holds the full BPF program (struct sock_fprog in C).
type sockFprog struct {
	Len    uint16
	Filter *sockFilter
}

func bpfStmt(code uint16, k uint32) sockFilter {
	return sockFilter{Code: code, K: k}
}

func bpfJump(code uint16, k uint32, jt, jf uint8) sockFilter {
	return sockFilter{Code: code, Jt: jt, Jf: jf, K: k}
}

// x86_64 syscall numbers that are always allowed.
// This is a generous allowlist covering file I/O, networking, signals,
// memory management, threads, and common runtime operations.
var allowedSyscalls = []uint32{
	0,   // read
	1,   // write
	2,   // open
	3,   // close
	4,   // stat
	5,   // fstat
	6,   // lstat
	8,   // lseek
	9,   // mmap
	10,  // mprotect
	11,  // munmap
	12,  // brk
	13,  // rt_sigaction
	14,  // rt_sigprocmask
	15,  // rt_sigreturn
	16,  // ioctl
	17,  // pread64
	18,  // pwrite64
	19,  // readv
	20,  // writev
	21,  // access
	25,  // mremap
	35,  // nanosleep
	39,  // getpid
	56,  // clone
	57,  // fork
	58,  // vfork
	59,  // execve
	60,  // exit
	61,  // wait4
	62,  // kill
	63,  // uname
	78,  // getdents
	79,  // getcwd
	89,  // readlink
	97,  // geteuid
	98,  // getegid
	99,  // getuid
	100, // getgid
	102, // getgroups
	110, // getppid
	127, // getrlimit (needed for ulimit -t / -v)
	137, // statfs
	138, // fstatfs
	158, // arch_prctl
	160, // setrlimit (needed for ulimit -t / -v)
	186, // gettid
	202, // getcpu
	217, // getdents64
	218, // settimeofday
	231, // exit_group
	232, // epoll_create
	233, // epoll_ctl
	234, // epoll_wait
	257, // openat
	262, // newfstatat
	273, // fcntl
	281, // eventfd2
	282, // epoll_create1
	283, // dup3
	284, // pipe2
	285, // inotify_init1
	291, // preadv
	292, // pwritev
	302, // prlimit64
	318, // getrandom
	332, // memfd_create
	334, // rseq
}

// SeccompExec applies a seccomp BPF allowlist filter and exec's the target.
// It is used by the agent's Linux sandbox as a re-exec entry point:
//
//	agent --seccomp-exec <plugin-binary> [args...]
//
// This function does not return on success — it replaces the current process
// with the target via syscall.Exec.
func SeccompExec(target string, args []string) {
	// ---- Step 1: PR_SET_NO_NEW_PRIVS ----------------------------------
	// Prevents the process (and anything it exec's) from gaining new
	// privileges. This also makes the seccomp filter irrevocable.
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		fmt.Fprintf(os.Stderr, "seccomp: PR_SET_NO_NEW_PRIVS: %v\n", err)
		os.Exit(1)
	}

	// ---- Step 2: Build and apply seccomp filter -----------------------
	filter := buildSeccompFilter()
	if err := applySeccompFilter(filter); err != nil {
		fmt.Fprintf(os.Stderr, "seccomp: apply: %v\n", err)
		os.Exit(1)
	}

	// ---- Step 3: Exec the target program ------------------------------
	// The seccomp filter is inherited by the new program.
	if err := syscall.Exec(target, args, os.Environ()); err != nil {
		fmt.Fprintf(os.Stderr, "seccomp: exec %q: %v\n", target, err)
		os.Exit(1)
	}
}

// buildSeccompFilter returns a BPF program that implements a default-deny
// allowlist for seccomp. Only the syscalls listed in allowedSyscalls are
// permitted; everything else kills the process.
//
// BPF program layout:
//
//	[0] LD ABS 4           ; load architecture from seccomp_data
//	[1] JEQ arch, +1, +0   ; if arch matches -> skip kill; else -> kill
//	[2] RET KILL           ; wrong architecture
//	[3] LD ABS 0           ; load syscall number
//	[4] JEQ nr0, jt, 0     ; first allowed syscall
//	[5] JEQ nr1, jt, 0     ; ...
//	...
//	[N-1] JEQ nrN-1, 1, 0  ; last allowed syscall
//	[N]   RET KILL          ; default-deny
//	[N+1] RET ALLOW         ; allow label
func buildSeccompFilter() []sockFilter {
	arch := detectAuditArch()
	n := len(allowedSyscalls)

	// The ALLOW instruction sits after: 3 arch checks + n syscall checks
	// + 1 default-deny kill.
	allowIdx := 3 + n + 1

	filter := make([]sockFilter, 0, allowIdx+1)

	// ---- Architecture check -----------------------------------------
	filter = append(filter, bpfStmt(bpfLD|bpfABS|bpfK, seccompDataArch))
	filter = append(filter, bpfJump(bpfJMP|bpfJEQ|bpfK, arch, 1, 0))
	filter = append(filter, bpfStmt(bpfRET|bpfK, seccompRetKill))

	// ---- Load syscall number -----------------------------------------
	filter = append(filter, bpfStmt(bpfLD|bpfABS|bpfK, seccompDataSyscallNR))

	// ---- Syscall allowlist (linear scan) -----------------------------
	for i, nr := range allowedSyscalls {
		if i == n-1 {
			filter = append(filter, bpfJump(bpfJMP|bpfJEQ|bpfK, nr, 1, 0))
		} else {
			jt := uint8(allowIdx - len(filter) - 1)
			filter = append(filter, bpfJump(bpfJMP|bpfJEQ|bpfK, nr, jt, 0))
		}
	}

	// ---- Default deny + allow label ---------------------------------
	filter = append(filter, bpfStmt(bpfRET|bpfK, seccompRetKill))
	filter = append(filter, bpfStmt(bpfRET|bpfK, seccompRetAllow))

	return filter
}

// detectAuditArch returns the audit-architecture constant for the
// running binary. This must match the architecture of the *kernel*.
func detectAuditArch() uint32 {
	switch runtime.GOARCH {
	case "amd64":
		return auditArchX86_64
	case "386":
		return auditArchI386
	case "arm64":
		return auditArchAArch64
	default:
		return auditArchX86_64
	}
}

// applySeccompFilter installs the BPF filter via the seccomp(2) syscall.
func applySeccompFilter(filter []sockFilter) error {
	if len(filter) == 0 {
		return fmt.Errorf("empty filter")
	}

	prog := sockFprog{
		Len:    uint16(len(filter)),
		Filter: &filter[0],
	}

	_, _, errno := unix.Syscall(
		unix.SYS_SECCOMP,
		seccompSetModeFilter,
		seccompFilterFlagTsync,
		uintptr(unsafe.Pointer(&prog)),
	)
	if errno != 0 {
		return fmt.Errorf("SECCOMP_SET_MODE_FILTER: %v", errno)
	}
	return nil
}
