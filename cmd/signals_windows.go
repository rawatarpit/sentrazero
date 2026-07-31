//go:build windows

package main

import (
	"os"
)

// watchRestartSignal is a no-op on Windows — SIGUSR1 does not exist.
func watchRestartSignal(usr1Ch chan<- os.Signal) {
	// Windows has no SIGUSR1; graceful restart not supported via signal.
}
