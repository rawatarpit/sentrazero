# Sentra Agent

A distributed compute runtime for data processing and ML workloads. The agent runs on developer machines, servers, or edge nodes and executes jobs using dynamically loaded plugins.

## What is Sentra?

Sentra is a **decentralized compute network** that transforms idle developer machines and servers into a distributed processing cluster. Instead of relying on expensive cloud infrastructure, organizations can leverage their existing compute resources to run:

- **Data pipeline jobs** (scanning, processing, merging datasets)
- **ML inference** (embedding generation, model inference)
- **Batch processing** (ETL, transformations, chunk-based operations)

### Core Value Proposition

| Feature | Benefit |
|---------|---------|
| **Zero-Config Setup** | Single binary, claim code activation - no env vars needed |
| **Auto-scaling** | Jobs automatically distributed to available devices |
| **Plugin-based** | Extend functionality via dynamically loaded plugins |
| **Warm pools** | Pre-warmed runtime environments for fast job startup |
| **Graceful shutdown** | Drain in-flight jobs before termination |
| **Redis-powered** | Multi-agent coordination with Redis Streams |

## Quick Start

### Prerequisites

- PostgreSQL database with Supabase (for metadata and jobs)
- Redis (optional, for multi-agent coordination)
- Python 3.8+ or Node.js 18+ (if using runtime plugins)

### Start the Agent

```bash
# Run with claim code (recommended - zero setup required)
./bin/sentra-agent --claim-code YOUR_CLAIM_CODE

# Or agent will prompt for claim code if not provided
./bin/sentra-agent
```

That's it! The agent will:
1. Claim itself to your organization using the claim code
2. Fetch configuration from the backend automatically
3. Connect to Redis (if configured)
4. Start processing jobs

### Docker Deployment

```bash
docker run -d \
  --name sentra-agent \
  -v /var/run/docker.sock:/var/run/docker.sock \
  sentra/agent:latest \
  --claim-code YOUR_CLAIM_CODE
```

## Zero-Config Architecture

The agent is designed to run with **zero environment variables**:

```
User runs: ./sentra-agent --claim-code ABC123

Agent does automatically:
1. Call claim_device API → gets device_id, token, backend_url
2. Call health_policy API → gets max_workers, redis_url
3. Saves config to ~/.sentra/config.json
4. Starts polling for jobs
```

### Optional Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SENTRA_CLAIM_CODE` | No* | - | Claim code (*required on first run) |
| `SENTRA_BACKEND_URL` | No | auto-detect | Supabase project URL |
| `SENTRA_REDIS_URL` | No | - | Redis URL for job queue |

*The claim code can also be provided via CLI flag `--claim-code` or the agent will prompt for it.

### CLI Flags & Environment Variables

| Flag/Env | Required | Default | Description |
|----------|---------|---------|-------------|
| `--claim-code` | No* | - | Claim code (required on first run) |
| `SENTRA_CLAIM_CODE` | No* | - | Claim code via env var |
| `SENTRA_BACKEND_URL` | No | auto | Backend URL |
| `SENTRA_REDIS_URL` | No | - | Redis URL |
| `ENVIRONMENT_POOL_MAX_SIZE` | No | 10 | Max env pools |
| `ENVIRONMENT_MAX_COUNT` | No | 50 | Max environments |

*Either `--claim-code` or prompt on first run.

## Wiring: Backend API Calls

The agent binary communicates with Supabase Edge Functions:

```
Agent Binary                    Supabase Backend
    │                           │
    ├─── claim_device() ────────► claim_device
    │     {claim_code, sysinfo}     {device_id, token}
    │                           │
    ├─── agent_health_policy() ──► agent_health_policy
    │     {cpu, memory, etc}      {concurrency}
    │                           │
    ├─── assign_agent_job() ──────► assign_agent_job RPC
    │     (auth header)         {job_id, payload}
    │                           │
    ├─── complete_job() ────────► complete_job
    │     {status, output}       {success}
    │                           │
    ├─── get_plugin() ──────────► get_plugin
    │     {plugin_id}          {plugin}
    │                           │
    └─── verify_job_lease() ──► verify_job_lease
          {job_id, device_id}    {valid}
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Supabase Backend                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   Devices    │  │  Agent Jobs  │  │   Executions/Pipeline │  │
│  └──────────────┘  └──────────────┘  └──────────────────────���  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   Datasets   │  │   Plugins    │  │   Storage Config      │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Executions  │  │ Plugin Execs  │  │  Device Benchmarks  │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼ (Redis Streams for multi-agent)
┌─────────────────────────────────────────────────────────────────┐
│                        Redis (Optional)                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ Job Queue   │  │ Worker State│  │  Results Cache        │  │
│  │ (Streams)  │  │  (Hash)     │  │  (TTL keys)          │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Sentra Agent (Go)                        │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌─────────┐  │
│  │  Config  │  │   Auth    │  │ Dispatcher│  │ Plugin  │  │
│  │         │  │           │  │          │  │ System  │  │
│  └────────────┘  └────────────┘  └────────────┘  └─────────┘  │
│  ┌────────────────────┐  ┌──────────────────────────────┐   │
│  │  Redis Client       │  │   Bootstrap (Zero-Config)         │   │
│  └────────────────────┘  └──────────────────────────────┘   │
│                                                                 │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌─────────┐  │
│  │ Heartbeat  │  │  Realtime  │  │  Backend  │  │ Startup │  │
│  │            │  │            │  │           │  │         │  │
│  └────────────┘  └────────────┘  └────���───────┘  └─────────┘  │
│                                                                  │
│  ┌─────────────────────┐  ┌──────────────────────────────┐    │
│  │  Runtime Manager    │  │   Environment Pool (v2)      │    │
│  │  (Python/Node)     │  │   (Warm pools for fast start)   │    │
│  └─────────────────────┘  └──────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### Job Dispatcher

The dispatcher manages job execution across worker pools:

- **Worker Pool**: Configurable concurrent workers (default: CPU/2)
- **Job Queue**: FIFO processing with lease-based assignment
- **Redis Queue**: Optional Redis Streams for multi-agent coordination
- **Retry Logic**: Automatic retry with configurable backoff
- **Graceful Shutdown**: Waits for in-flight jobs before termination

### Runtime System (v2)

Python and Node.js runtime environments with warm pooling:

- **Environment Pool**: Pre-created virtual environments for fast startup
- **Dependency Caching**: Hash-based caching of dependency installations
- **Remote Cache**: Optional S3-compatible storage for environment sharing

### Plugin System

Plugins are dynamically loaded executable code:

- **Plugin Manager**: Download, verify, and cache plugins
- **Signature Verification**: Ed25519 signature validation
- **Bundled Plugins**: Built-in scan_metadata and merge_metadata plugins

### Redis Integration

For multi-agent deployments, Redis provides:

- **Job Queue**: Redis Streams with consumer groups
- **Worker State**: Real-time worker status tracking
- **Results Cache**: Fast access to job results
- **Pub/Sub**: Real-time notifications

## Job Types

| Type | Description |
|------|-------------|
| `scan_dataset` | Scan and analyze dataset structure |
| `process` | Process individual data chunks |
| `process_dataset` | Full dataset processing pipeline |
| `merge_dataset` | Merge processed chunks into final output |

## Job Lifecycle

```
┌─────────────┐
│   pending   │  ← Jobs enter here from dispatch
└──────┬──────┘
       │ worker claims job
       ▼
┌─────────────┐
│  assigned   │  ← Lease acquired
└──────┬──────┘
       │ start execution
       ▼
┌─────────────┐
│  running    │
└──────┬──────┘
       │ complete_job()
       ▼
┌─────────────┐    ┌─────────────┐
│ completed   │    │   failed    │
└─────────────┘    └──────┬──────┘
                           │ retry_count < max
                           ▼
                     ┌─────────────┐
                     │   pending   │  ← Re-queued
                     └─────────────┘
```

## Security

- **Device Authentication**: Token-based with secure keyring storage
- **Plugin Signing**: Ed25519 signature verification
- **Sandbox Isolation**: Docker-based execution with network isolation
- **Row-Level Security**: Database-level org isolation

## Project Structure

```
sentra-agent/
├── cmd/
│   ├── main.go              # Agent entry point
│   ├── sentra/main.go      # CLI tool
│   ├── agent/
│   │   ├��─ executor/      # Plugin execution (v1, v2)
│   │   │   ├── executor.go
│   │   │   └── v2/
│   │   ├── runtime/      # Runtime environments
│   │   │   ├── runtime.go
│   │   │   └── v2/
│   │   └── sandbox/      # Sandbox management
├── internal/
│   ├── auth/             # Identity & device claiming
│   ├── backend/          # Supabase client
│   ├── bootstrap/       # Zero-config bootstrap
│   ├── config/          # Configuration loading
│   ├── dataset/        # Dataset operations
│   ├── dispatcher/     # Job execution & worker pool
│   ├── healthcheck/    # Health endpoint
│   ├── heartbeat/      # Device health reporting
│   ├── httpclient/     # HTTP client
│   ├── models/        # Data models
│   ├── obs/           # Observability
│   ├── plugin/        # Plugin lifecycle
│   │   └── bundled/  # Bundled plugins
│   ├── redis/         # Redis client (NEW)
│   ├── realtime/      # SSE/WebSocket
│   ├── reporter/     # Job reporting
│   ├── sandbox/      # Resource limits
│   ├── startup:      # Bootstrap & validation
│   ├── storage:      # Storage backend
│   ├── sysinfo:      # System info
│   └── system:       # Environment detection
├── supabase/
│   └── migrations/   # Database migrations
├── Dockerfile
├── Makefile
└── bin/             # Built binaries
```

## Build

```bash
# Build the agent
make build

# Or manually
go build -o bin/sentra-agent ./cmd/main.go

# Build with version info
go build -ldflags="-X main.Version=1.0.0" -o bin/sentra-agent ./cmd/main.go
```

## Environment Pool Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT_POOL_MAX_SIZE` | 10 | Max pools to keep warm |
| `ENVIRONMENT_MAX_COUNT` | 50 | Max environments in pool |
| `ENVIRONMENT_WARM_TIMEOUT` | 30m | Time before cooling warm envs |
| `ENVIRONMENT_EVICTION_INTERVAL` | 5m | How often to run eviction |
| `ENVIRONMENT_MAX_DISK_BYTES` | 10GB | Max disk usage for envs |

## Runtime Dependencies

The v2 runtime manager supports Python and Node.js with dependency pinning:

```yaml
dependencies:
  - name: pandas
    version: "2.0.0"
    source: pypi
  - name: numpy
    version: "1.24.0"
    source: pypi
```

Dependencies are automatically cached and reused across jobs with the same hash.

## Monitoring

### Health Check

The agent exposes a health endpoint (default port 8080):

```bash
curl http://localhost:8080/health
# Returns: {"status":"ok","device_id":"..."}
```

### Metrics

Environment pool metrics are logged periodically:

```
environment_pool_initialized{platform="darwin/arm64",python="3.11.0",node="20.0.0",max_pool_size=10,warm_timeout=30m}
environments_evicted{count=3,remaining=7,freed_bytes=...}
```

## Troubleshooting

### Device Won't Claim

- Ensure claim code is valid
- Check network connectivity to Supabase
- Verify organization exists

### Jobs Not Being Assigned

- Check device status: `SELECT * FROM devices WHERE id = '<device_id>'`
- Verify heartbeat is being received
- Check cron job is running in Supabase

### Plugin Verification Fails

- Ensure plugin is signed with organization's Ed25519 key
- Check signature key is registered
- Trusted plugins bypass verification

### Environment Pool Exhausted

- Increase `ENVIRONMENT_MAX_COUNT`
- Decrease `ENVIRONMENT_WARM_TIMEOUT`
- Add disk space if hitting disk limit

## Bug Fixes Applied

This release includes fixes for the following issues:

| Bug ID | Description |
|--------|-------------|
| C1 | Plugin signature verification now uses metadata hash |
| C2 | Added user_orgs view for RLS policies |
| C3 | Fixed EnvironmentPool race condition |
| C4 | PythonRuntime.Cleanup() no longer deletes warm pool paths |
| H1 | Added bundled merge_metadata.py plugin |
| H2 | Fixed plugin OS/arch path fallback |
| H3 | Worker count now updates from heartbeat |
| H5 | Fixed ActiveJobsCount() double-counting |
| H6 | Added advisory lock to auto_progress_after_scan |
| M1 | Fixed ReleaseEnvironment() lock order |
| M8 | Added cleanup_stuck_jobs cron function |
| L1 | CGO check now guarded by env var |
| L2 | Added fallback path warning |

## API Reference

### Edge Functions (Backend API)

All Edge Functions run on Supabase and are called by the agent binary.

#### Authentication

All functions (except `claim_device` and `register_device`) require:
- `Authorization: Bearer <access_token>` header
- `x-agent-token: <token>` header

#### claim_device

First-time device registration via claim code.

```bash
POST /functions/v1/claim_device
Body: {
  claim_code: "ORG-CLAIM-123",
  sysinfo?: {
    hostname?: string,
    type?: "agent",
    cpu_cores?: number,
    memory_gb?: number,
    benchmark_score?: number,
    environment?: "local" | "docker",
    storage?: "local" | "s3",
    network_zone?: string,
    merge_capable?: boolean
  }
}
Response: { ok, device_id, agent_token, org_id }
```

#### register_device

Device registration (alternative to claim_device with Bearer auth).

```bash
POST /functions/v1/register_device
Authorization: Bearer <claim_code>
Body: {
  name: "device-name",
  specs?: object,
  environment_type?: "local",
  storage_type?: "disk",
  capabilities?: string[],
  benchmark_score?: number,
  force_reclaim?: boolean
}
Response: { ok, device_id, access_token }
```

#### agent_health_policy

Reports device health and receives concurrency policy.

```bash
POST /functions/v1/agent_health_policy
Headers: Authorization, x-agent-token
Body: {
  total_cpu_cores?: number,
  total_memory_gb?: number,
  cpu_cores_free?: number,
  memory_free_gb?: number,
  cpu_usage_percent?: number,
  network_latency_ms?: number,
  gpu_available?: boolean,
  incoming_workload_weight?: number
}
Response: { ok, concurrency, load_factor }
```

#### assign_agent_job

Requests a job assignment from the backend.

```bash
GET /functions/v1/assign_agent_job
Headers: Authorization, x-agent-token
Response: { ok, result: { job_id, job_type, payload, execution_id } }
```

#### agent_stream

Server-Sent Events stream for real-time job delivery.

```bash
GET /functions/v1/agent_stream
Headers: Authorization, x-agent-token
Response: event-stream with job events
```

Events:
- `hello`: { device_id, org_id, realtime_enabled }
- `job`: { id, job_type, status, payload }
- `sync`: { jobs_sent, timestamp }

#### complete_job

Reports job completion.

```bash
POST /functions/v1/complete_job
Body: {
  execution_id: uuid,
  status: "completed" | "failed" | "cancelled",
  duration_ms?: number,
  output?: object,
  error?: string,
  device_id?: uuid
}
Response: { success, execution }
```

#### verify_job_lease

Verifies job lease is still valid.

```bash
POST /functions/v1/verify_job_lease
Body: {
  job_id: uuid,
  device_id: uuid
}
Response: { valid }
```

#### get_plugin

Retrieves plugin code.

```bash
POST /functions/v1/get_plugin
Body: { plugin_id: uuid }
Response: { plugin }
```

#### relay_job_event

Relays job events to notification queue.

```bash
POST /functions/v1/relay_job_event
Body: {
  job_id: uuid,
  event_type: string,
  payload: object
}
Response: { ok }
```

#### report_job_error

Reports job execution error.

```bash
POST /functions/v1/report_job_error
Body: {
  job_id: uuid,
  error: string,
  failure_classification?: "transient" | "permanent" | "resource" | "configuration"
}
Response: { ok }
```

#### record_benchmark

Records benchmark scores.

```bash
POST /functions/v1/record_benchmark
Body: {
  test_name?: string,
  latency_ms?: number,
  device_id?: uuid
}
Response: { ok }
```

#### get_storage_config

Gets storage backend configuration.

```bash
POST /functions/v1/get_storage_config
Body: { storage_type?: string }
Response: { config }
```

#### test_storage_connection

Tests storage connection.

```bash
POST /functions/v1/test_storage_connection
Body: {
  storage_type: string,
  credentials: object
}
Response: { ok }
```

#### store_storage_credentials

Stores encrypted storage credentials.

```bash
POST /functions/v1/store_storage_credentials
Body: {
  storage_type: string,
  credentials: object,
  encryption_key?: string
}
Response: { ok }
```

### Database Schema

#### agent_jobs

Main job queue table.

| Column | Type | Description |
|--------|------|-------------|
| id | uuid | Job ID (PK) |
| job_type | text | Job type: scan_dataset, process, process_dataset, merge_dataset |
| payload | jsonb | Job configuration and data |
| status | text | pending, assigned, running, completed, failed |
| agent_id | uuid | Assigned device |
| org_id | uuid | Organization |
| execution_id | uuid | Pipeline execution |
| retry_count | integer | Retry count |
| max_retries | integer | Max retries |
| error | text | Error message |
| output_token | text | Job output |
| runtime_type | text | python, node, native |
| runtime_dependencies | jsonb | Dependencies |
| execution_timeout_seconds | integer | Timeout |

#### devices

Registered agent devices.

| Column | Type | Description |
|--------|------|-------------|
| id | uuid | Device ID (PK) |
| org_id | uuid | Organization |
| name | text | Device name |
| status | text | online, busy, available, offline |
| access_token_hash | text | Token hash |
| type | text | Device type |
| max_concurrency | integer | Max workers |
| total_cpu_cores | integer | Total CPU cores |
| total_memory_gb | integer | Total memory (GB) |
| cpu_cores_free | integer | Free CPU cores |
| memory_free_gb | integer | Free memory (GB) |
| cpu_usage_percent | integer | CPU usage |
| network_latency_ms | integer | Network latency |
| gpu_available | boolean | GPU available |
| last_heartbeat | timestamptz | Last heartbeat |
| benchmark_score | numeric | Benchmark score |

#### executions

Pipeline execution tracking.

| Column | Type | Description |
|--------|------|-------------|
| id | uuid | Execution ID (PK) |
| org_id | uuid | Organization |
| dataset_id | uuid | Dataset |
| status | text | pending, running, completed, failed |
| current_step_index | integer | Current step |
| output | jsonb | Execution output |
| error_message | text | Error message |

#### datasets

Registered datasets.

| Column | Type | Description |
|--------|------|-------------|
| id | uuid | Dataset ID (PK) |
| org_id | uuid | Organization |
| name | text | Dataset name |
| status | text | registered, scanning, scanned, processing, processed, merged |
| metadata | jsonb | Dataset metadata |
| scan_assigned_device | uuid | Device doing scan |
| merged_output_verified | boolean | Output verified |

#### batch_chunks

Dataset chunks for processing.

| Column | Type | Description |
|--------|------|-------------|
| id | uuid | Chunk ID (PK) |
| batch_id | uuid | Batch ID |
| chunk_index | integer | Chunk index |
| status | text | pending, assigned, completed |
| chunk_vector | vector(16) | Embedding vector |
| assigned_device_id | uuid | Assigned device |
| job_type | text | Job type |
| merged_in | boolean | Merged |
| similarity_score | float | Similarity score |

#### plugins

Available plugins.

| Column | Type | Description |
|--------|------|-------------|
| id | text | Plugin ID |
| org_id | uuid | Organization |
| name | text | Plugin name |
| version | text | Version |
| code_url | text | Plugin code URL |
| signature | text | Ed25519 signature |
| runtime_type | text | python, node |

## License

Part of the Kickin Compute Network.