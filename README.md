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
| **Plug-and-play** | Single binary, claim code activation - no config files |
| **Auto-scaling** | Jobs automatically distributed to available devices |
| **Plugin-based** | Extend functionality via dynamically loaded plugins |
| **Warm pools** | Pre-warmed runtime environments for fast job startup |
| **Graceful shutdown** | Drain in-flight jobs before termination |

## Quick Start

```bash
# Run with claim code
./bin/sentra-agent --claim-code YOUR_CLAIM_CODE

# Or run without - agent will prompt for claim code
./bin/sentra-agent
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BACKEND_URL` | Yes | - | Supabase project URL |
| `BACKEND_ANON_KEY` | No | - | Anonymous key for API access |
| `ORG_ID` | No | - | Organization ID (auto-set on claim) |
| `MAX_CONCURRENCY` | No | CPU/2 | Max concurrent jobs |
| `AGENT_NAME` | No | hostname | Device display name |
| `SENTRA_MOUNT_PATH` | No | ~/sentra/data | Local storage path |
| `SENTRA_TOKEN_PATH` | No | ~/.sentra/tokens/{device_id}.token | Token storage path template |
| `SENTRA_USE_KEYRING` | No | auto | Use keyring for token storage (true/false/auto) |
| `SENTRA_REALTIME_MODE` | No | realtime | realtime, sse, or both |
| `SENTRA_SHUTDOWN_GRACE_PERIOD` | No | 30s | Grace period on shutdown |

### CLI Flags

| Flag | Description |
|------|-------------|
| `--claim-code <code>` | Claim code for non-interactive registration |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Supabase Backend                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   Devices    │  │  Agent Jobs  │  │   Executions/Pipeline│  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   Datasets   │  │   Plugins    │  │   Storage Config      │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Executions  │  │ Plugin Execs  │  │  Device Benchmarks  │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Sentra Agent (Go)                          │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌─────────┐  │
│  │   Config  │  │   Auth    │  │ Dispatcher│  │ Plugin  │  │
│  │          │  │           │  │          │  │ System  │  │
│  └────────────┘  └────────────┘  └────────────┘  └─────────┘  │
│                                                                 │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌─────────┐  │
│  │ Heartbeat  │  │  Realtime  │  │  Backend   │  │ Startup │  │
│  │            │  │            │  │           │  │         │  │
│  └────────────┘  └────────────┘  └────────────┘  └─────────┘  │
│                                                                  │
│  ┌─────────────────────┐  ┌──────────────────────────────┐    │
│  │  Runtime Manager    │  │   Environment Pool (v2)      │    │
│  │  (Python/Node)     │  │   (Warm pools for fast start)   │    │
│  └─────────────────────┘  └──────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Plugin Execution Layer                        │
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────┐    │
│  │  v2 Runtime     │  │   Docker        │  │   Native       │    │
│  │  (Python/Node)  │  │   Sandbox      │  │   (CGO)        │    │
│  └─────────────────┘  └─────────────────┘  └────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### Job Dispatcher

The dispatcher manages job execution across worker pools:

- **Worker Pool**: Configurable concurrent workers (default: CPU/2)
- **Job Queue**: FIFO processing with lease-based assignment
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
- **Bundled Plugins**: Built-in scan_metadata plugin for dataset scanning

### Storage Backend

Supports multiple storage modes:

- **Shared Mount**: Local filesystem (default, `~/sentra/data`)
- **Object Storage**: S3-compatible remote storage

## Job Types

| Type | Description |
|------|-------------|
| `scan_dataset` | Scan and analyze dataset structure |
| `process` | Process individual data chunks |
| `process_dataset` | Full dataset processing pipeline |
| `merge_dataset` | Merge processed chunks into final output |
| `ingest_dataset` | Ingest raw data into storage |

## Job Lifecycle

```
┌─────────────┐
│   pending   │  ← Jobs enter here from dispatch
└──────┬──────┘
       │ claim_jobs_for_device()
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

- **Device Authentication**: Token-based with HMAC storage
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
│   │   ├── executor/       # Plugin execution (v1, v2)
│   │   │   ├── executor.go # v1 executor
│   │   │   └── v2/         # v2 executor with runtime manager
│   │   ├── runtime/        # Runtime environments
│   │   │   ├── runtime.go  # v1 runtime
│   │   │   └── v2/         # v2 runtime with pool management
│   │   └── sandbox/        # Sandbox management
├── internal/
│   ├── auth/               # Identity & device claiming
│   ├── backend/            # Supabase client
│   ├── config/              # Configuration loading
│   ├── dataset/            # Dataset operations (merge, lock)
│   ├── dispatcher/          # Job execution & worker pool
│   ├── healthcheck/        # Health endpoint server
│   ├── heartbeat/          # Device health reporting
│   ├── httpclient/         # HTTP client
│   ├── models/             # Data models
│   ├── obs/                # Observability (logging, tracing)
│   ├── plugin/             # Plugin lifecycle
│   │   └── bundled/        # Bundled plugins
│   ├── realtime/           # SSE/WebSocket listeners
│   ├── reporter/           # Job reporting
│   ├── sandbox/           # Resource limits
│   ├── sanitize/           # Error sanitization
│   ├── startup/           # Bootstrap & validation
│   ├── storage/           # Storage backend
│   ├── sysinfo/            # System information
│   └── system/            # Environment detection
├── Dockerfile              # Container build
├── Makefile                # Build automation
└── bin/                   # Built binaries
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

- Ensure `BACKEND_URL` is correct
- Check network connectivity to Supabase
- Verify organization exists

### Jobs Not Being Assigned

- Check device status in dashboard: `SELECT * FROM devices WHERE id = '<device_id>'`
- Verify heartbeat is being received
- Check `system_health_heartbeat` cron is running

### Plugin Verification Fails

- Ensure plugin is signed with organization's Ed25519 key
- Check signature key is registered: `SELECT * FROM org_plugin_signing_keys`
- Trusted plugins bypass verification

### Environment Pool Exhausted

- Increase `ENVIRONMENT_MAX_COUNT`
- Decrease `ENVIRONMENT_WARM_TIMEOUT`
- Add disk space if hitting disk limit

## License

Part of the Kickin Compute Network.