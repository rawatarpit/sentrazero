//go:build !linux

package sandbox

import (
	"fmt"
	"os"
)

// SeccompExec is a no-op stub for non-Linux platforms.  Seccomp BPF filtering
// is only supported on Linux.  If this is somehow called on a non-Linux system
// (e.g., during testing), it prints an error and exits.
func SeccompExec(target string, args []string) {
	fmt.Fprintf(os.Stderr, "seccomp: not supported on this platform\n")
	os.Exit(1)
}

// NoNewPrivsExec is a no-op stub for non-Linux platforms.  PR_SET_NO_NEW_PRIVS
// is only supported on Linux.
func NoNewPrivsExec(target string, args []string) {
	fmt.Fprintf(os.Stderr, "no-new-privs: not supported on this platform\n")
	os.Exit(1)
}
