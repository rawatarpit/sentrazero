# Scan Plugin System

This document explains how the scan plugin works in the Sentra Agent worker system.

## Overview

The scan plugin (`plugin_scan_metadata`) is a bundled Python plugin that scans datasets and extracts metadata. It's automatically injected when a job of type `scan_dataset` is dispatched.

## Plugin Components

### 1. Plugin Code (`internal/plugin/bundled/plugins/scan_metadata.py`)

A Python script that:
- Scans a directory or file to extract metadata
- Detects file types (CSV, JSON, Parquet, etc.)
- Extracts headers from CSV files
- Analyzes JSON structure
- Returns metadata including:
  - `file_count`: Total number of files
  - `total_size_bytes`: Total size of all files
  - `file_types`: Count of each file type
  - `headers`: Column headers from CSV/JSON
  - `columns`: Column names detected
  - `largest_file`/`smallest_file`: File info

### 2. Plugin Manifest (`internal/plugin/bundled/plugins/plugin_scan_metadata.json`)

```json
{
  "name": "plugin_scan_metadata",
  "version": "1.0.0",
  "filename": "scan_metadata.py",
  "checksum": "bundled",
  "trusted": true,
  "category": "scan",
  "runtime_type": "python"
}
```

Key fields:
- `trusted: true` - Required for execution
- `runtime_type: python` - Executes via Python runtime
- `checksum: bundled` - Uses bundled plugin installation

## How Scan Works

### 1. Job Dispatch

When `ExecuteJob()` in `internal/dispatcher/execute.go` receives a job with `job_type = "scan_dataset"`:

1. It extracts job metadata (`JobMeta`) from the payload
2. If no `plugin_code` or `plugin_id` is provided, it auto-injects the bundled plugin:
   ```go
   if meta.JobType == "scan_dataset" && fullJob.PluginCode == "" && fullJob.PluginID == "" {
       fullJob.PluginID = "bundled:plugin_scan_metadata"
       execJob.PluginCode = bundledCode // loads from file
   }
   ```

3. The job is passed to the v2 Executor

### 2. Execution

The Executor (`cmd/agent/executor/v2/executor.go`) runs the plugin:

1. **Sandbox Creation**: Creates an isolated sandbox directory (`~/.sentra/sandbox/job-{jobID}`)
2. **Runtime Selection**: Chooses execution mode (Runtime → Docker → Native)
3. **Plugin Code**: The Python plugin code is written to the sandbox
4. **Input**: The plugin receives a JSON payload via stdin:
   ```json
   {
     "input_path": "/path/to/dataset",
     "job_id": "...",
     "org_id": "..."
   }
   ```
5. **Execution**: Runs via Docker or RuntimeManager (Python)

### 3. Plugin Execution Flow

In `cmd/agent/runtime/v2/python.go` (or similar):

1. Python interpreter is invoked with `scan_metadata.py`
2. Input JSON is read from stdin
3. Plugin calls `scan_directory(input_path)`:
   - Scans all files recursively
   - Collects file statistics
   - Detects file types
4. Returns result as JSON to stdout:
   ```json
   {
     "file_count": 150,
     "total_size_bytes": 5368709120,
     "file_types": {"csv": 100, "json": 50},
     "headers": ["id", "name", "value"],
     "columns": ["id", "name", "value"]
   }
   ```

## Input and Output Paths

### Input Path

The dataset path is passed via the job payload:
- `input_path`: Directory containing the dataset files
- Derived from `dataset_id` + storage configuration

### Output

1. **Execution Result**: The plugin stdout is captured as `result.Output` in `ExecutionResult`:
   ```go
   type ExecutionResult struct {
       Output     string // JSON output from plugin
       Method    string // "docker" or "runtime"
       DurationMs int64 // Execution time
   }
   ```

2. **Memory Storage**: Results are stored in the sandbox's `output/` directory:
   - Path: `~/.sentra/sandbox/job-{jobID}/output/`

3. **Result Reporting**: The scan summary is sent to the backend via `report_dataset_scan()` (calls Edge Function `/functions/v1/report_dataset_scan`)

4. **Database Update**: The `record_dataset_metadata` Edge Function:
   - Updates the `datasets` table with:
     - `total_size_gb`
     - `file_count`
     - `avg_file_size_mb`
     - `file_type`
     - `status = "scanned"`
   - Triggers chunk planning via `plan_dataset_chunks`

## Plugin Storage Locations

| Stage | Location |
|-------|----------|
| Bundled Plugin | `internal/plugin/bundled/plugins/scan_metadata.py` |
| Installed Plugin | `~/.sentra/plugins/plugin_scan_metadata/{OS}-{ARCH}/scan_metadata.py` |
| Sandboxed Working Dir | `~/.sentra/sandbox/job-{jobID}/` |
| Plugin Output | `~/.sentra/sandbox/job-{jobID}/output/` |

## Security

- Plugins must have `"trusted": true` in manifest to execute
- Plugins require signature verification (unless bundled)
- Execution runs in Docker sandbox with:
  - No network access
  - Memory limit: 512MB
  - CPU limit: 1 core
  - Isolated filesystem

## Related Files

- `internal/plugin/executor.go` - Plugin execution logic
- `internal/plugin/manager.go` - Plugin loading and installation
- `internal/dispatcher/execute.go` - Job dispatch with auto-injection
- `cmd/agent/executor/v2/executor.go` - Job execution
- `supabase/functions/record_dataset_metadata/index.ts` - Results storage