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
	seccompSetModeFilter = 2

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
// memory management, threads, timers, scheduling, and the syscalls the
// Python / Node / Go / Rust runtimes and common JITs actually use.
//
// NOTE: the numbers below were audited against the x86_64 syscall table
// (see include/uapi/asm-generic/unistd.h — x86_64 uses the 64-bit set).
// The original list had wrong numbers for many entries (e.g. 97 != geteuid,
// 202 = futex not getcpu, 273 = set_robust_list not fcntl, 332 = statx not
// memfd_create), which would have SIGSYS-killed every real runtime.
//
// Deliberately EXCLUDED (privilege / namespace / kernel-control surface):
// mount, umount2, ptrace, reboot, setns, unshare, init_module, finit_module,
// delete_module, create_module, get_kernel_syms, query_module, capget,
// capset, syslog, kexec_load, kexec_file_load, swapon, swapoff, sethostname,
// setdomainname, iopl, ioperm, acct, chroot, pivot_root, quotactl,
// nfsservctl, bpf, perf_event_open, process_vm_readv/writev, userfaultfd,
// keyctl/add_key/request_key, lookup_dcookie, all *xattr syscalls,
// setuid/setgid/setreuid/setregid/setresuid/setresgid/setfsuid/setfsgid/
// setgroups, personality, adjtimex, settimeofday, readahead, mlock family,
// sysfs, uselib, ustat, vhangup, modify_ldt, _sysctl, mq_* (SysV msg),
// ioprio_*, mbind/migrate_pages/move_pages/mempolicy, fanotify, seccomp,
// io_uring_*, open_tree/move_mount/fsopen/fsconfig/fsmount/fspick,
// mount_setattr, landlock_*, memfd_secret, process_madvise/mrelease.
var allowedSyscalls = []uint32{
	// ---- File I/O ----------------------------------------------------
	0,   // read
	1,   // write
	2,   // open
	3,   // close
	4,   // stat
	5,   // fstat
	6,   // lstat
	8,   // lseek
	17,  // pread64
	18,  // pwrite64
	19,  // readv
	20,  // writev
	21,  // access
	40,  // sendfile
	72,  // fcntl
	73,  // flock
	74,  // fsync
	75,  // fdatasync
	76,  // truncate
	77,  // ftruncate
	78,  // getdents
	79,  // getcwd
	80,  // chdir
	81,  // fchdir
	82,  // rename
	83,  // mkdir
	84,  // rmdir
	85,  // creat
	86,  // link
	87,  // unlink
	88,  // symlink
	89,  // readlink
	90,  // chmod
	91,  // fchmod
	92,  // chown
	93,  // fchown
	94,  // lchown
	95,  // umask
	137, // statfs
	138, // fstatfs
	217, // getdents64
	221, // fadvise64
	257, // openat
	258, // mkdirat
	259, // mknodat
	260, // fchownat
	261, // futimesat
	262, // newfstatat
	263, // unlinkat
	264, // renameat
	265, // linkat
	266, // symlinkat
	267, // readlinkat
	268, // fchmodat
	269, // faccessat
	275, // splice
	276, // tee
	277, // sync_file_range
	278, // vmsplice
	280, // utimensat
	285, // fallocate
	295, // preadv
	296, // pwritev
	302, // prlimit64
	316, // renameat2
	326, // copy_file_range
	327, // preadv2
	328, // pwritev2
	332, // statx
	436, // close_range
	437, // openat2
	439, // faccessat2

	// ---- Memory ------------------------------------------------------
	9,   // mmap
	10,  // mprotect
	11,  // munmap
	12,  // brk
	25,  // mremap
	26,  // msync
	27,  // mincore
	28,  // madvise
	319, // memfd_create

	// ---- Processes, threads, signals ---------------------------------
	13,  // rt_sigaction
	14,  // rt_sigprocmask
	15,  // rt_sigreturn
	34,  // pause
	39,  // getpid
	56,  // clone
	57,  // fork
	58,  // vfork
	59,  // execve
	60,  // exit
	61,  // wait4
	62,  // kill
	63,  // uname
	109, // setpgid
	110, // getppid
	111, // getpgrp
	112, // setsid
	124, // getsid
	127, // rt_sigpending
	128, // rt_sigtimedwait
	129, // rt_sigqueueinfo
	130, // rt_sigsuspend
	131, // sigaltstack
	186, // gettid
	200, // tkill
	202, // futex
	218, // set_tid_address
	219, // restart_syscall
	231, // exit_group
	234, // tgkill
	247, // waitid
	273, // set_robust_list
	274, // get_robust_list
	322, // execveat
	334, // rseq
	424, // pidfd_send_signal
	434, // pidfd_open
	435, // clone3
	449, // futex_waitv

	// ---- Identity / resource queries ---------------------------------
	97,  // getrlimit (needed for ulimit -t / -v)
	98,  // getrusage
	99,  // sysinfo
	100, // times
	102, // getuid
	104, // getgid
	107, // geteuid
	108, // getegid
	115, // getgroups
	118, // getresuid
	120, // getresgid
	121, // getpgid
	140, // getpriority
	141, // setpriority
	160, // setrlimit (needed for ulimit -t / -v)
	201, // time

	// ---- Timers / clocks ---------------------------------------------
	35,  // nanosleep
	36,  // getitimer
	37,  // alarm
	38,  // setitimer
	96,  // gettimeofday
	222, // timer_create
	223, // timer_settime
	224, // timer_gettime
	225, // timer_getoverrun
	226, // timer_delete
	228, // clock_gettime
	229, // clock_getres
	230, // clock_nanosleep

	// ---- Scheduling ---------------------------------------------------
	24,  // sched_yield
	142, // sched_setparam
	143, // sched_getparam
	144, // sched_setscheduler
	145, // sched_getscheduler
	146, // sched_get_priority_max
	147, // sched_get_priority_min
	148, // sched_rr_get_interval
	203, // sched_setaffinity
	204, // sched_getaffinity
	309, // getcpu
	314, // sched_setattr
	315, // sched_getattr

	// ---- Networking ---------------------------------------------------
	41,  // socket
	42,  // connect
	43,  // accept
	44,  // sendto
	45,  // recvfrom
	46,  // sendmsg
	47,  // recvmsg
	48,  // shutdown
	49,  // bind
	50,  // listen
	51,  // getsockname
	52,  // getpeername
	53,  // socketpair
	54,  // setsockopt
	55,  // getsockopt
	288, // accept4
	299, // recvmmsg
	307, // sendmmsg

	// ---- Poll / epoll / event fds --------------------------------------
	7,   // poll
	16,  // ioctl
	22,  // pipe
	23,  // select
	32,  // dup
	33,  // dup2
	213, // epoll_create
	232, // epoll_wait
	233, // epoll_ctl
	253, // inotify_init
	254, // inotify_add_watch
	255, // inotify_rm_watch
	270, // pselect6
	271, // ppoll
	281, // epoll_pwait
	282, // signalfd
	283, // timerfd_create
	284, // eventfd
	286, // timerfd_settime
	287, // timerfd_gettime
	289, // signalfd4
	290, // eventfd2
	291, // epoll_create1
	292, // dup3
	293, // pipe2
	294, // inotify_init1
	441, // epoll_pwait2

	// ---- Misc / runtime --------------------------------------------------
	157, // prctl (thread names, dumpability)
	158, // arch_prctl (TLS/FS base — required by Go and glibc)
	162, // sync
	306, // syncfs
	318, // getrandom
	324, // membarrier
}

// SeccompExec applies a seccomp BPF allowlist filter and exec's the target.
// It is used by the agent's Linux sandbox as a re-exec entry point:
//
//	agent --seccomp-exec <plugin-binary> [args...]
//
// This function does not return on success — it replaces the current process
// with the target via syscall.Exec.
func SeccompExec(target string, args []string) {
	// The seccomp filter is per-thread, and the filter inherited by the
	// exec'd target is the filter of the thread that calls execve. Pin this
	// goroutine to its OS thread so the PR_SET_NO_NEW_PRIVS + seccomp +
	// exec sequence all run on the same thread.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Defense in depth: honor the same guards as the sandboxer's maybeReexec.
	// The allowlist contains x86_64 syscall numbers only — on ARM64 (e.g.
	// Raspberry Pi) applying it would SIGSYS-kill every syscall. And when the
	// operator disabled seccomp via SANDBOX_SECCOMP_PROFILE=off, the re-exec
	// path must not silently re-enable it. In both cases fall back to
	// NO_NEW_PRIVS-only hardening.
	if runtime.GOARCH != "amd64" || getEnv("SANDBOX_SECCOMP_PROFILE", "default") == "off" {
		NoNewPrivsExec(target, args)
		return
	}

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
	// The seccomp filter is inherited by the new program. argv[0] is the
	// target itself so interpreters (python, node, bash) parse the script
	// argument correctly.
	argv := append([]string{target}, args...)
	if err := syscall.Exec(target, argv, os.Environ()); err != nil {
		fmt.Fprintf(os.Stderr, "seccomp: exec %q: %v\n", target, err)
		os.Exit(1)
	}
}

// NoNewPrivsExec applies PR_SET_NO_NEW_PRIVS only (no seccomp filter) and
// exec's the target. Used when seccomp is disabled but NO_NEW_PRIVS
// hardening is still wanted:
//
//	agent --no-new-privs-exec <plugin-binary> [args...]
//
// This function does not return on success.
func NoNewPrivsExec(target string, args []string) {
	// Pin to this OS thread: PR_SET_NO_NEW_PRIVS is per-thread and must be
	// set on the same thread that performs the execve.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		fmt.Fprintf(os.Stderr, "no-new-privs: PR_SET_NO_NEW_PRIVS: %v\n", err)
		os.Exit(1)
	}
	argv := append([]string{target}, args...)
	if err := syscall.Exec(target, argv, os.Environ()); err != nil {
		fmt.Fprintf(os.Stderr, "no-new-privs: exec %q: %v\n", target, err)
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

	// The ALLOW instruction sits after: 4 arch/load checks (LD arch, JEQ arch,
	// RET KILL, LD nr) + n syscall checks + 1 default-deny kill.
	allowIdx := 4 + n + 1

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
//
// NOTE: flags intentionally use 0 (apply to the calling thread only), NOT
// SECCOMP_FILTER_FLAG_TSYNC. TSYNC attempts to synchronize the filter across
// every thread in the process and fails with EINVAL when any thread is in a
// state that cannot be synced — which happens routinely in a multi-threaded
// Go runtime. We only need the filter on the thread that will exec the
// target (SeccompExec pins the goroutine to its OS thread), and that filter
// is inherited by the exec'd process. TSYNC is therefore both unnecessary
// and harmful here.
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
		0, // no TSYNC — see note above
		uintptr(unsafe.Pointer(&prog)),
	)
	if errno != 0 {
		return fmt.Errorf("SECCOMP_SET_MODE_FILTER: %v", errno)
	}
	return nil
}
