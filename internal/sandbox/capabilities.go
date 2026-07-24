package sandbox

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"sentra-agent/internal/obs"
	"golang.org/x/sys/unix"
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
		Platform:     runtime.GOOS + "/" + runtime.GOARCH,
		IsPrivileged: os.Geteuid() == 0,
		HasUserNS:    probeUserNamespace(),
		HasSeccomp:   probeSeccompAvailable(),
	}

	cgroupBase := cfg.Cgroupsv2Path
	if cgroupBase == "" {
		cgroupBase = "/sys/fs/cgroup"
	}

	caps.CgroupPath = cgroupBase
	caps.HasCgroupWrite = probeCgroupWrite(cgroupBase)

	obs.Info("platform capabilities detected", obs.Field{
		"platform":       caps.Platform,
		"is_privileged":  caps.IsPrivileged,
		"has_cgroup":     caps.HasCgroupWrite,
		"cgroup_path":    caps.CgroupPath,
		"has_userns":     caps.HasUserNS,
		"has_seccomp":    caps.HasSeccomp,
	})

	return caps
}

func probeCgroupWrite(cgPath string) bool {
	testDir := cgPath + "/.sentra-probe"
	if err := os.MkdirAll(testDir, 0755); err != nil {
		return false
	}
	defer os.RemoveAll(testDir)

	if err := os.WriteFile(testDir+"/memory.max", []byte("1073741824"), 0644); err != nil {
		return false
	}

	return true
}

func probeUserNamespace() bool {
	return unix.Access("/proc/self/ns/user", unix.F_OK) == nil
}

func probeSeccompAvailable() bool {
	files, err := os.ReadDir("/proc/sys/kernel/seccomp")
	if err != nil || len(files) == 0 {
		data, err := os.ReadFile("/proc/sys/kernel/seccomp/actions_avail")
		if err != nil {
			return false
		}
		return len(strings.TrimSpace(string(data))) > 0
	}
	return false
}

func (c PlatformCapabilities) String() string {
	return fmt.Sprintf(
		"PlatformCapabilities{platform=%s privileged=%v cgroup=%v cgroup_path=%s userns=%v seccomp=%v}",
		c.Platform, c.IsPrivileged, c.HasCgroupWrite, c.CgroupPath, c.HasUserNS, c.HasSeccomp,
	)
}
