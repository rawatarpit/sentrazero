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

### CLI Flags

| Flag | Description |
|------|-------------|
| `--claim-code <code>` | Claim code for non-interactive registration |

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

## License

Part of the Kickin Compute Network.