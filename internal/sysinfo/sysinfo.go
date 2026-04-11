package sysinfo

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Specs holds full system telemetry.
type Specs struct {
	OS                string  `json:"os"`
	Arch              string  `json:"arch"`
	CPUCores          int     `json:"cpu_cores"`
	AvailableCPUCores int     `json:"available_cpu_cores"`
	CPUModel          string  `json:"cpu_model,omitempty"`
	CPUUsagePercent   float64 `json:"cpu_usage_percent,omitempty"`
	TotalMemoryGB     float64 `json:"total_memory_gb,omitempty"`
	AvailableMemoryGB float64 `json:"available_memory_gb,omitempty"`
	DiskFreeGB        float64 `json:"disk_free_gb,omitempty"`
	GPUModel          string  `json:"gpu_model,omitempty"`
	GPUMemoryFreeGB   float64 `json:"gpu_memory_free_gb,omitempty"`
	GPUMemoryTotalGB  float64 `json:"gpu_memory_total_gb,omitempty"`
	IOThroughputMBps  float64 `json:"io_throughput_mb_s,omitempty"`
	NetworkLatency    float64 `json:"network_latency_ms,omitempty"`
}

// Detect gathers system information safely within ~1s.
// Missing probes never fail the rest.
func Detect() Specs {
	s := Specs{
		OS:       runtime.GOOS,
		Arch:     runtime.GOARCH,
		CPUCores: runtime.NumCPU(),
	}

	switch runtime.GOOS {
	case "darwin":
		s.detectMac()
	case "linux":
		s.detectLinux()
	case "windows":
		s.detectWindows()
	}

	s.AvailableCPUCores = s.detectAvailableCPUCores()

	// Best-effort probes (cheap only)
	s.CPUUsagePercent = cpuUsagePercent()
	s.detectGPU()
	s.NetworkLatency = probeLatency()
	s.IOThroughputMBps = ioHeuristic()

	return s
}

// -----------------------------------------------------------------------------
// Latency probe (portable)
// -----------------------------------------------------------------------------

func probeLatency() float64 {
	raw := os.Getenv("BACKEND_URL")
	if raw != "" {
		if u, err := url.Parse(raw); err == nil && u.Hostname() != "" {
			return pingOnce(u.Hostname())
		}
	}

	if host := os.Getenv("CDN_PING_HOST"); host != "" {
		return pingOnce(host)
	}

	return 0
}

func pingOnce(host string) float64 {
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.CommandContext(ctx, "ping", "-c", "1", "-t", "1", host)
	case "linux":
		cmd = exec.CommandContext(ctx, "ping", "-c", "1", "-W", "1", host)
	default:
		return 0
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0
	}

	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "time=") {
			parts := strings.Split(line, "time=")
			if len(parts) > 1 {
				val := strings.Fields(parts[1])[0]
				f, _ := strconv.ParseFloat(val, 64)
				return f
			}
		}
	}
	return 0
}

// -----------------------------------------------------------------------------
// OS-specific detection
// -----------------------------------------------------------------------------

func (s *Specs) detectMac() {
	s.CPUModel = runCmd("sysctl", "-n", "machdep.cpu.brand_string")

	mem := runCmd("sysctl", "-n", "hw.memsize")
	if total, err := strconv.ParseFloat(strings.TrimSpace(mem), 64); err == nil && total > 0 {
		s.TotalMemoryGB = total / (1024 * 1024 * 1024)
	}

	s.AvailableMemoryGB = s.detectMacAvailableMemory()

	df := runCmd("df", "-h", "/")
	if lines := strings.Split(df, "\n"); len(lines) > 1 {
		fields := strings.Fields(lines[1])
		if len(fields) >= 4 {
			s.DiskFreeGB = parseSize(fields[3])
		}
	}
}

func (s *Specs) detectMacAvailableMemory() float64 {
	out := runCmd("vm_stat")
	var free, inactive, purgeable, compressed uint64
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		valStr := strings.TrimRight(fields[2], ".")
		val, err := strconv.ParseUint(valStr, 10, 64)
		if err != nil {
			continue
		}
		pages := val * 4096
		if strings.Contains(line, "Pages free:") {
			free = pages
		} else if strings.Contains(line, "Pages inactive:") {
			inactive = pages
		} else if strings.Contains(line, "Pages purgeable:") {
			purgeable = pages
		} else if strings.Contains(line, "Pages compressed:") {
			compressed = pages
		}
	}
	available := float64(free+inactive+purgeable) - float64(compressed)
	available = available / float64(1024*1024*1024)
	if available < 0 {
		available = 0
	}
	return available
}

func (s *Specs) detectLinux() {
	data := runCmd("cat", "/proc/cpuinfo")
	for _, l := range strings.Split(data, "\n") {
		if strings.Contains(l, "model name") {
			parts := strings.SplitN(l, ":", 2)
			if len(parts) == 2 {
				s.CPUModel = strings.TrimSpace(parts[1])
			}
			break
		}
	}

	memInfo := runCmd("grep", "MemTotal", "/proc/meminfo")
	fields := strings.Fields(memInfo)
	if len(fields) >= 2 {
		if kb, err := strconv.ParseFloat(fields[1], 64); err == nil {
			s.TotalMemoryGB = kb / (1024 * 1024)
		}
	}

	availInfo := runCmd("grep", "MemAvailable", "/proc/meminfo")
	availFields := strings.Fields(availInfo)
	if len(availFields) >= 2 {
		if kb, err := strconv.ParseFloat(availFields[1], 64); err == nil {
			s.AvailableMemoryGB = kb / (1024 * 1024)
		}
	}

	df := runCmd("df", "-BG", "/")
	if lines := strings.Split(df, "\n"); len(lines) > 1 {
		fields := strings.Fields(lines[1])
		if len(fields) >= 4 {
			s.DiskFreeGB = parseSize(fields[3])
		}
	}
}

func (s *Specs) detectWindows() {
	s.CPUModel = runCmd("wmic", "cpu", "get", "name")

	mem := runCmd("wmic", "ComputerSystem", "get", "TotalPhysicalMemory")
	if total, err := strconv.ParseFloat(strings.TrimSpace(mem), 64); err == nil && total > 0 {
		s.TotalMemoryGB = total / (1024 * 1024 * 1024)
	}
}

// -----------------------------------------------------------------------------
// GPU detection
// -----------------------------------------------------------------------------

func (s *Specs) detectGPU() {
	out := runCmd("nvidia-smi", "--query-gpu=name,memory.total,memory.free", "--format=csv,noheader,nounits")
	if out != "" {
		parts := strings.Split(out, ",")
		if len(parts) >= 3 {
			s.GPUModel = strings.TrimSpace(parts[0])
			s.GPUMemoryTotalGB, _ = strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
			s.GPUMemoryFreeGB, _ = strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
		}
		return
	}

	if runtime.GOOS == "darwin" {
		s.GPUModel = parseGPUModel(runCmd("system_profiler", "SPDisplaysDataType"))
	}
}

// -----------------------------------------------------------------------------
// Cheap heuristics only
// -----------------------------------------------------------------------------

func (s *Specs) detectAvailableCPUCores() int {
	switch runtime.GOOS {
	case "linux":
		return s.detectLinuxAvailableCores()
	case "darwin":
		return s.detectMacAvailableCores()
	}
	return s.CPUCores
}

func (s *Specs) detectLinuxAvailableCores() int {
	out := runCmd("grep", "-c", "processor", "/proc/cpuinfo")
	if out == "" {
		return s.CPUCores
	}
	count, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil || count <= 0 {
		return s.CPUCores
	}
	return count
}

func (s *Specs) detectMacAvailableCores() int {
	out := runCmd("sysctl", "-n", "hw.ncpu")
	if out == "" {
		return s.CPUCores
	}
	count, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil || count <= 0 {
		return s.CPUCores
	}
	return count
}

func cpuUsagePercent() float64 {
	return 0 // intentionally disabled (too unreliable)
}

func ioHeuristic() float64 {
	// Non-destructive hint only
	if runtime.GOOS == "linux" {
		return 100 // assume SSD-class
	}
	return 0
}

// -----------------------------------------------------------------------------
// Utilities
// -----------------------------------------------------------------------------

func runCmd(name string, args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return ""
	}
	return strings.TrimSpace(out.String())
}

func parseGPUModel(output string) string {
	for _, line := range strings.Split(output, "\n") {
		l := strings.ToLower(line)
		if strings.Contains(l, "chipset") || strings.Contains(l, "graphics") {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

func parseSize(sizeStr string) float64 {
	sizeStr = strings.TrimSpace(sizeStr)
	if sizeStr == "" {
		return 0
	}
	unit := sizeStr[len(sizeStr)-1]
	valStr := strings.TrimRight(sizeStr, "GMK")
	val, _ := strconv.ParseFloat(valStr, 64)

	switch unit {
	case 'M':
		return val / 1024
	case 'K':
		return val / (1024 * 1024)
	default:
		return val
	}
}
