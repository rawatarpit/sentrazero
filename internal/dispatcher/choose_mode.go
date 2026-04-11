package dispatcher

import (
	"fmt"
	"time"

	"sentra-agent/internal/models"
)

// ExecutionMode determines which plugin (embedding model) to run.
type ExecutionMode string

const (
	ModeSmall   ExecutionMode = "small" // hash-based, lightweight, no ML model
	ModeFast    ExecutionMode = "fast"  // CPU-optimized embedding model
	ModeGGUF    ExecutionMode = "gguf"  // quantized model (medium accuracy)
	ModeONNX    ExecutionMode = "onnx"  // GPU-accelerated ONNX model
	DefaultMode ExecutionMode = ModeFast
)

// ChooseExecutionMode decides which execution mode (plugin) to use
// based on device metrics and job characteristics.
// This must remain deterministic — avoid randomness or non-reproducible logic.
func ChooseExecutionMode(job models.Job, device models.DeviceMetrics) ExecutionMode {
	// Jobs not requiring ML model → always use lightweight hash-based embedding
	if !job.RequiresModel {
		return ModeSmall
	}

	// If GPU is available → always prefer full ONNX model
	if device.GPUAvailable {
		return ModeONNX
	}

	// High compute capacity (CPU+Memory) → quantized model
	if device.CPUFree >= 0.75 && device.MemoryFreeGB >= 12 {
		if job.ComplexityScore >= 0.6 || job.DataSizeMB > 50 {
			return ModeGGUF
		}
		return ModeFast // fast mode for high-resource systems with simpler jobs
	}

	// Moderate resources → fast mode
	if device.CPUFree >= 0.5 && device.MemoryFreeGB >= 6 {
		if job.DataSizeMB < 5 && job.ComplexityScore < 0.35 {
			return ModeSmall // small/fallback for simple jobs on moderate hardware
		}
		return ModeFast
	}

	// Very small jobs or constrained device → small/fallback
	if job.DataSizeMB < 1 {
		return ModeSmall
	}

	// Default fallback — consistent for all undefined scenarios
	return DefaultMode
}

// ChooseExecutionModeWithLogging wraps ChooseExecutionMode to include log-friendly context.
// Returns both the selected mode and a formatted message for structured logs or metrics.
func ChooseExecutionModeWithLogging(job models.Job, device models.DeviceMetrics) (ExecutionMode, string) {
	start := time.Now()
	mode := ChooseExecutionMode(job, device)
	elapsed := time.Since(start)

	msg := fmt.Sprintf(
		"[choose_mode] job_id=%s requires_model=%v gpu=%v cpu_free=%.2f mem_gb=%.2f complexity=%.2f data=%.1fMB -> mode=%s (took=%s)",
		job.ID,
		job.RequiresModel,
		device.GPUAvailable,
		device.CPUFree,
		device.MemoryFreeGB,
		job.ComplexityScore,
		job.DataSizeMB,
		mode,
		elapsed,
	)

	return mode, msg
}
