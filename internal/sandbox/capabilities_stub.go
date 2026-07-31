//go:build !linux

package sandbox

import (
	"fmt"
	"os"
	"runtime"

	"sentra-agent/internal/obs"
)

type PlatformCapabilities struct {
	HasCgroupWrite bool
	HasUserNS      bool
	HasSeccomp     bool
	IsPrivileged   bool
	CgroupPath     string
	Platform       string
}

func DetectCapabilities(cfg SandboxConfig) PlatformCapabilities {
	caps := PlatformCapabilities{
		Platform:       runtime.GOOS + "/" + runtime.GOARCH,
		IsPrivileged:   os.Geteuid() == 0,
		HasUserNS:      false,
		HasSeccomp:     false,
		HasCgroupWrite: false,
		CgroupPath:     "",
	}

	obs.Info("platform capabilities detected", obs.Field{
		"platform":       caps.Platform,
		"is_privileged":  caps.IsPrivileged,
		"has_cgroup":     caps.HasCgroupWrite,
		"has_userns":     caps.HasUserNS,
		"has_seccomp":    caps.HasSeccomp,
		"note":           "seccomp/namespaces/cgroups not available on " + runtime.GOOS,
	})

	return caps
}

func (c PlatformCapabilities) String() string {
	return fmt.Sprintf(
		"PlatformCapabilities{platform=%s privileged=%v cgroup=%v userns=%v seccomp=%v}",
		c.Platform, c.IsPrivileged, c.HasCgroupWrite, c.HasUserNS, c.HasSeccomp,
	)
}
