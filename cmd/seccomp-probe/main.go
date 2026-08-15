//go:build linux

package main

import (
	"fmt"
	"os"
	"syscall"
)

func main() {
	// Syscall 425 is io_uring_setup, which is deliberately NOT in the sandbox
	// allowlist (see internal/sandbox/seccomp_linux.go). When run under the
	// default-deny seccomp filter this process is SIGSYS-killed (exit code
	// 159 = 128+31) before the syscall returns. Without the filter the
	// syscall either succeeds (errno 0) or returns an errno, and we print the
	// result and exit 0. The raw number 425 is identical on amd64 and arm64.
	_, _, errno := syscall.Syscall(425, 0, 0, 0)
	if errno == 0 {
		fmt.Println("io_uring_setup executed")
		os.Exit(0)
	}
	fmt.Printf("io_uring_setup returned errno %d\n", errno)
	os.Exit(0)
}
