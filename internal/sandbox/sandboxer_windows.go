//go:build windows

package sandbox

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"unsafe"
)

type windowsSandbox struct {
	cfg SandboxConfig
}

var (
	modkernel32        = syscall.NewLazyDLL("kernel32.dll")
	procCreateJobObj   = modkernel32.NewProc("CreateJobObjectW")
	procSetInfoJobObj  = modkernel32.NewProc("SetInformationJobObject")
	procAssignProcJob  = modkernel32.NewProc("AssignProcessToJobObject")
	procResumeThread   = modkernel32.NewProc("ResumeThread")
	procOpenThread     = modkernel32.NewProc("OpenThread")
)

const (
	createSuspended       = 0x00000004
	createNewConsole      = 0x00000010
	threadSuspendResume   = 0x0002
	jobObjLimitKillOnClose = 0x2000
	jobObjLimitProcMemory  = 0x100
	jobObjLimitActiveProc  = 0x0008
	jobObjExtLimitInfo     = 9
)

func newPlatformSandbox(cfg SandboxConfig) Sandboxer {
	return &windowsSandbox{cfg: cfg}
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

	jobName := "SentraZero_" + env.WorkDir
	jobNamePtr, _ := syscall.UTF16PtrFromString(jobName)

	jobHandle, _, _ := procCreateJobObj.Call(0, uintptr(unsafe.Pointer(jobNamePtr)))
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

	ret, _, _ = procAssignProcJob.Call(jobHandle, uintptr(cmd.Process.Pid))
	if ret == 0 {
		cmd.Process.Kill()
		return fmt.Errorf("AssignProcessToJobObject failed")
	}

	hThread, _, _ := procOpenThread.Call(threadSuspendResume, 0, uintptr(cmd.Process.Pid))
	if hThread != 0 {
		procResumeThread.Call(hThread)
		syscall.CloseHandle(syscall.Handle(hThread))
	}

	return cmd.Wait()
}

func (s *windowsSandbox) Destroy(ctx context.Context, env *SandboxEnv) error {
	if env.Cleanup != nil {
		env.Cleanup()
	}
	return nil
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

type jobObjectExtendedLimitInformation struct {
	BasicLimitInformation jobObjectBasicLimitInformation
	ProcessMemoryLimit    uintptr
	JobMemoryLimit        uintptr
	PeakProcessMemoryUsed uintptr
	PeakJobMemoryUsed     uintptr
}
