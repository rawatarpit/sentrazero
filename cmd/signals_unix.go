//go:build unix

package main

import (
	"os"
	"os/signal"
	"syscall"
)

// watchRestartSignal registers SIGUSR1 for graceful restart on Unix systems.
func watchRestartSignal(usr1Ch chan<- os.Signal) {
	signal.Notify(usr1Ch, syscall.SIGUSR1)
}
