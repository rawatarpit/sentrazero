//go:build windows

package sandbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"sentra-agent/internal/obs"
)

type windowsSandbox struct {
	cfg                     SandboxConfig
	jobEnforcementAvailable bool
}

// JobEnforcementReporter is implemented by sandboxers that can report whether
// Job Object enforcement is available on this host. On Windows hosts that
// already run the agent inside a non-nesting job object (e.g. CI runner
// services), a child process cannot be moved into a sandbox Job Object, so
// memory caps / kill-on-close cannot be enforced and the sandboxer degrades
// gracefully while reporting the fact.
type JobEnforcementReporter interface {
	JobEnforcementAvailable() bool
}

// JobEnforcementAvailable reports whether Job Object limits (memory cap,
// kill-on-close) are enforceable for plugin processes on this host. It is
// initialized true and flips to false the first time process assignment fails
// because the host already placed the child in a non-nesting job.
func (s *windowsSandbox) JobEnforcementAvailable() bool { return s.jobEnforcementAvailable }

var (
	modkernel32                  = syscall.NewLazyDLL("kernel32.dll")
	procCreateJobObj             = modkernel32.NewProc("CreateJobObjectW")
	procSetInfoJobObj            = modkernel32.NewProc("SetInformationJobObject")
	procAssignProcJob            = modkernel32.NewProc("AssignProcessToJobObject")
	procResumeThread             = modkernel32.NewProc("ResumeThread")
	procOpenThread               = modkernel32.NewProc("OpenThread")
	procCreateToolhelp32Snapshot = modkernel32.NewProc("CreateToolhelp32Snapshot")
	procThread32First            = modkernel32.NewProc("Thread32First")
	procThread32Next             = modkernel32.NewProc("Thread32Next")
)

const (
	createSuspended        = 0x00000004
	createNewConsole       = 0x00000010
	threadSuspendResume    = 0x0002
	processSetQuota        = 0x0100
	jobObjLimitKillOnClose = 0x2000
	jobObjLimitProcMemory  = 0x100
	jobObjLimitActiveProc  = 0x0008
	jobObjExtLimitInfo     = 9
	th32csSnapThread       = 0x00000004
)

func newPlatformSandbox(cfg SandboxConfig) Sandboxer {
	return &windowsSandbox{cfg: cfg, jobEnforcementAvailable: true}
}

func (s *windowsSandbox) Prepare(ctx context.Context, jobID string, manifest PluginManifest, network bool) (*SandboxEnv, error) {
	workDir := s.cfg.TempDir + "\\sentrazero\\" + jobID
	if err := os.MkdirAll(workDir, 0700); err != nil {
		return nil, fmt.Errorf("create work dir: %w", err)
	}

	return &SandboxEnv{
		WorkDir:  workDir,
		Config:   s.cfg,
		Manifest: manifest,
		Network:  network,
		Platform: "windows",
		Cleanup:  func() { os.RemoveAll(workDir) },
	}, nil
}

func (s *windowsSandbox) Execute(ctx context.Context, env *SandboxEnv, cmd *exec.Cmd) error {
	// Network isolation: when the plugin has NOT requested network access,
	// add an outbound firewall block for the plugin executable (best-effort —
	// requires admin rights to modify the Windows Firewall). The rule is
	// removed when the plugin process exits.
	if !env.Network {
		if cleanup, fwErr := s.blockOutbound(cmd, env); fwErr != nil {
			obs.Warn("windows firewall block failed (best-effort)", obs.Field{
				"plugin": env.Manifest.Name,
				"error":  fwErr.Error(),
			})
		} else if cleanup != nil {
			defer cleanup()
		}
	}

	if !s.cfg.WindowsJobObject {
		return cmd.Run()
	}

	memoryMB := env.Manifest.Resources.MemoryMB
	if memoryMB <= 0 {
		memoryMB = s.cfg.DefaultMemoryMB
	}
	if memoryMB > s.cfg.MaxMemoryMB {
		memoryMB = s.cfg.MaxMemoryMB
	}

	// Use an UNNAMED job object. Naming the object from the workdir path
	// (e.g. "SentraZero_C:\Users\...\sentrazero\job") fails: job object names
	// live in the NT object namespace where backslashes are path separators
	// and colons are invalid characters, so CreateJobObjectW returns NULL.
	// An unnamed object supports AssignProcessToJobObject and kill-on-close
	// identically.
	jobHandle, _, _ := procCreateJobObj.Call(0, 0)
	if jobHandle == 0 {
		return fmt.Errorf("CreateJobObject failed")
	}
	defer syscall.CloseHandle(syscall.Handle(jobHandle))

	limitInfo := jobObjectExtendedLimitInformation{
		BasicLimitInformation: jobObjectBasicLimitInformation{
			LimitFlags: jobObjLimitKillOnClose |
				jobObjLimitProcMemory |
				jobObjLimitActiveProc,
			ActiveProcessLimit: 1,
		},
		ProcessMemoryLimit: uintptr(memoryMB * 1024 * 1024),
	}

	ret, _, _ := procSetInfoJobObj.Call(
		jobHandle,
		uintptr(jobObjExtLimitInfo),
		uintptr(unsafe.Pointer(&limitInfo)),
		uintptr(unsafe.Sizeof(limitInfo)),
	)
	if ret == 0 {
		return fmt.Errorf("SetInformationJobObject failed")
	}

	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: createSuspended | createNewConsole,
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start process: %w", err)
	}

	// AssignProcessToJobObject takes a PROCESS HANDLE, not a PID. Go's
	// os.Process does not export its handle, so open one with the access
	// rights MSDN requires for job assignment (PROCESS_SET_QUOTA |
	// PROCESS_TERMINATE). The child is still suspended (CREATE_SUSPENDED),
	// so it cannot run any code before it is inside the job object.
	procHandle, err := syscall.OpenProcess(processSetQuota|syscall.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
	if err != nil {
		cmd.Process.Kill()
		return fmt.Errorf("OpenProcess(pid=%d) for job assignment: %w", cmd.Process.Pid, err)
	}
	defer syscall.CloseHandle(procHandle)

	ret, _, callErr := procAssignProcJob.Call(jobHandle, uintptr(procHandle))
	if ret == 0 {
		// ERROR_ACCESS_DENIED here means the child is already a member of a
		// non-nesting job object owned by the host (e.g. a CI runner service).
		// We cannot move it into our sandbox Job Object, so Job Object limits
		// (memory cap, kill-on-close) are not enforceable for this process.
		// Rather than failing the plugin run, degrade gracefully: resume the
		// suspended child (it must run), report the degradation loudly, and
		// continue. On hosts where job assignment works (normal deployments)
		// enforcement remains fully intact.
		if errors.Is(callErr, syscall.ERROR_ACCESS_DENIED) {
			s.jobEnforcementAvailable = false
			obs.Warn("AssignProcessToJobObject denied (child already in a non-nesting host job); running WITHOUT Job Object limits",
				obs.Field{
					"plugin":   env.Manifest.Name,
					"pid":      cmd.Process.Pid,
					"error":    callErr.Error(),
					"degraded": true,
					"hint":     "run the agent outside a job-object-enforcing service (e.g. CI runner) to enable Job Object memory caps",
				})
			if err := resumePrimaryThread(uint32(cmd.Process.Pid)); err != nil {
				cmd.Process.Kill()
				return fmt.Errorf("resume primary thread (degraded path): %w", err)
			}
			return cmd.Wait()
		}
		cmd.Process.Kill()
		return fmt.Errorf("AssignProcessToJobObject failed: %w", callErr)
	}

	// The process was created with CREATE_SUSPENDED so it cannot execute any
	// code before it is inside the job object. Resume its primary thread now.
	// OpenThread expects a *thread* ID — the process PID is NOT a valid thread
	// ID — so we enumerate threads to find one owned by this process. A freshly
	// created suspended process has exactly one thread.
	if err := resumePrimaryThread(uint32(cmd.Process.Pid)); err != nil {
		cmd.Process.Kill()
		return fmt.Errorf("resume primary thread: %w", err)
	}

	return cmd.Wait()
}

func (s *windowsSandbox) Destroy(ctx context.Context, env *SandboxEnv) error {
	if env.Cleanup != nil {
		env.Cleanup()
	}
	return nil
}

// blockOutbound creates a Windows Firewall outbound-block rule for the plugin
// executable and returns a cleanup func that removes it. The rule name is
// derived from the plugin name (sanitized) plus a timestamp so concurrent
// plugins never collide. Best-effort: returns an error when netsh fails
// (typically due to missing admin rights).
func (s *windowsSandbox) blockOutbound(cmd *exec.Cmd, env *SandboxEnv) (func(), error) {
	if cmd.Path == "" {
		return nil, fmt.Errorf("cannot block network: empty executable path")
	}
	ruleName := "SentraZeroBlock_" + sanitizeRuleName(env.Manifest.Name) + "_" + fmt.Sprintf("%d", time.Now().UnixNano())

	add := exec.Command("netsh", "advfirewall", "firewall", "add", "rule",
		"name="+ruleName,
		"dir=out",
		"action=block",
		"enable=yes",
		"program="+cmd.Path,
	)
	if out, err := add.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("netsh add rule: %v: %s", err, strings.TrimSpace(string(out)))
	}

	obs.Info("windows firewall outbound block applied", obs.Field{
		"plugin":  env.Manifest.Name,
		"rule":    ruleName,
		"program": cmd.Path,
	})

	return func() {
		del := exec.Command("netsh", "advfirewall", "firewall", "delete", "rule", "name="+ruleName)
		if out, err := del.CombinedOutput(); err != nil {
			obs.Warn("netsh delete rule failed", obs.Field{
				"rule":  ruleName,
				"error": err.Error(),
				"out":   strings.TrimSpace(string(out)),
			})
		}
	}, nil
}

// resumePrimaryThread resumes the primary thread of a process that was created
// with CREATE_SUSPENDED. OpenThread requires a thread ID (not a process ID),
// so we enumerate the process's threads via CreateToolhelp32Snapshot and
// resume the first one owned by pid. A freshly created suspended process has
// exactly one thread, so this unblocks the process.
func resumePrimaryThread(pid uint32) error {
	snapshot, _, _ := procCreateToolhelp32Snapshot.Call(uintptr(th32csSnapThread), 0)
	if snapshot == 0 || snapshot == uintptr(syscall.InvalidHandle) {
		return fmt.Errorf("CreateToolhelp32Snapshot failed")
	}
	defer syscall.CloseHandle(syscall.Handle(snapshot))

	entry := threadEntry32{Size: uint32(unsafe.Sizeof(threadEntry32{}))}
	ret, _, _ := procThread32First.Call(snapshot, uintptr(unsafe.Pointer(&entry)))
	for ret != 0 {
		if entry.OwnerProcessID == pid {
			hThread, _, _ := procOpenThread.Call(threadSuspendResume, 0, uintptr(entry.ThreadID))
			if hThread != 0 {
				procResumeThread.Call(hThread)
				syscall.CloseHandle(syscall.Handle(hThread))
				return nil
			}
		}
		ret, _, _ = procThread32Next.Call(snapshot, uintptr(unsafe.Pointer(&entry)))
	}
	return fmt.Errorf("no thread found for pid %d", pid)
}

type threadEntry32 struct {
	Size           uint32
	Usage          uint32
	ThreadID       uint32
	OwnerProcessID uint32
	BasePriority   int32
	DeltaPriority  int32
	Flags          uint32
}

func sanitizeRuleName(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	if b.Len() == 0 {
		return "plugin"
	}
	return b.String()
}

// ioCounters mirrors Windows IO_COUNTERS (6 × ULONGLONG).
type ioCounters struct {
	ReadOperationCount  uint64
	WriteOperationCount uint64
	OtherOperationCount uint64
	ReadTransferCount   uint64
	WriteTransferCount  uint64
	OtherTransferCount  uint64
}

type jobObjectBasicLimitInformation struct {
	PerProcessUserTimeLimit int64
	PerJobUserTimeLimit     int64
	LimitFlags              uint32
	MinimumWorkingSetSize   uintptr
	MaximumWorkingSetSize   uintptr
	ActiveProcessLimit      uint32
	Affinity                uintptr
	PriorityClass           uint32
	SchedulingClass         uint32
}

// jobObjectExtendedLimitInformation MUST include IoInfo: Windows places an
// IO_COUNTERS (48 bytes) between BasicLimitInformation and ProcessMemoryLimit.
// Omitting it makes the struct undersized, so SetInformationJobObject fails
// with STATUS_INFO_LENGTH_MISMATCH. Verified on a real Windows runner.
type jobObjectExtendedLimitInformation struct {
	BasicLimitInformation jobObjectBasicLimitInformation
	IoInfo                ioCounters
	ProcessMemoryLimit    uintptr
	JobMemoryLimit        uintptr
	PeakProcessMemoryUsed uintptr
	PeakJobMemoryUsed     uintptr
}
