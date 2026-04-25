# Sentra Agent - Technical & Business Documentation

## Table of Contents
1. [Business Model](#business-model)
2. [Technical Architecture](#technical-architecture)
3. [System Flow](#system-flow)
4. [Database Schema](#database-schema)
5. [Edge Functions](#edge-functions)
6. [Agent Binary](#agent-binary)
7. [Security Model](#security-model)
8. [Deployment Guide](#deployment-guide)

---

## 1. Business Model

### Overview
Sentra is a **distributed compute platform** that leverages idle client computing resources (laptops, servers, GPUs) to execute business workloads at minimal cost.

### Value Proposition
- **Massive Cost Savings**: Use client's idle GPUs instead of paying cloud GPU providers
- **Zero Infrastructure**: No need to manage own GPU servers
- **Scalable**: Add more workers by simply deploying agent to more machines
- **Enterprise Ready**: Control plane + dashboard for full visibility

### How It Works

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     YOUR DASHBOARD                        │
│   (Frontend - Control & Monitor All Agents)           │
│   - Add tasks/workloads                              │
│   - Upload datasets                                │
│   - Monitor agent status                           │
│   - View results                                   │
└─────────────────────────────────────────────────────┘
                          ↑
                          │ HTTPS API
┌─────────────────────────┴────────────────────────────────┐
│              SUPABASE (Control Plane)                     │
│                                                      │
│   📋 Jobs Queue      →  Tasks waiting to be executed │
│   📦 Plugin Store   →  Business logic to run          │
│   📊 Device Registry →  Registered worker machines │
│   💾 Results DB     →  Job outputs                  │
│   🔐 Auth/RLS       →  Security policies           │
│   ⚙️ Edge Functions →  API handlers               │
└─────────────────────────────────────────────────────┘
                          ↑
              Polls for work ←
                          │
    ┌──────────────┬──────────────┬──────────────┐
    │              │              │              │
┌───▼───┐    ┌───▼───┐    ┌───▼───┐    ┌───▼───┐
│Worker1│    │Worker2│    │Worker3│    │Worker N│
│Laptop │    │Server │    │Laptop │    │  ...   │
│ + GPU │    │ + GPU │    │ + GPU │    │ + GPU  │
└───────┘    └───────┘    └───────┘    └────────┘
      ↑ Your Clients' Machines (their infra, their GPU)
```

### Claim Code System
- **Per Organization**: One claim code registers all workers for an organization
- **Single Registration**: Workers can self-register using org's claim code
- **Auto-Identity**: Each machine gets unique device_id and token automatically

---

## 2. Technical Architecture

### Components

| Component | Purpose | Technology |
|-----------|---------|------------|
| Agent Binary | Executes jobs on worker machines | Go (compiled) |
| Control Plane | Job queue, auth, device management | Supabase (PostgreSQL) |
| Edge Functions | API handlers for agent communication | Deno/TypeScript |
| Dashboard | Web UI for control & monitoring | Your frontend |
| Plugin Storage | Business logic code storage | Supabase Storage |

### Data Flow

```
1. DASHBOARD → Adds job to Supabase (agent_jobs table)
2. SUPABASE → Job queued in 'pending' status

3. WORKER (polling/websocket) → Checks for available jobs
4. SUPABASE → Assigns job to worker (status → 'assigned')
5. WORKER → Downloads plugin from storage
6. WORKER → Executes plugin code with input data
7. WORKER → Uploads results back to Supabase
8. SUPABASE → Updates job status to 'completed'
9. DASHBOARD → Displays results
```

---

## 3. System Flow

### Flow 1: Worker Registration
```
┌─────────────────┐      ┌──────────────────────┐      ┌────────────────────┐
│ Worker Machine  │ ──► │  claim_device Edge   │ ──► │   devices table   │
│ (sentra-agent)  │     │  Function           │      │   (new entry)    │
└─────────────────┘      └──────────────────────┘      └────────────────────┘
                                                                        
   Steps:                                                             
   1. Send claim_code + sysinfo (CPU, RAM, GPU specs)                 
   2. Edge function validates claim_code                             
   3. Creates unique device_id + token                                 
   4. Returns: { device_id, token, backend_url }                       
   5. Worker saves token locally → ready to accept jobs               
```

### Flow 2: Job Execution
```
┌───────────────┐      ┌──────────────────────┐      ┌──────────────────┐
│  Dashboard   │ ──► │  add job to queue    │ ──► │ agent_jobs table │
└───────────────┘      └──────────────────────┘      └──────────────────┘

Worker polls:                               Job execution:
┌───────────────┐      ┌──────────────────────┐      ┌──────────────────┐
│ Worker polls  │ ◄──► │ assign_agent_job edge │ ◄───│ pending jobs    │
│ for job     │      │ function            │      │ found!          │
└───────────────┘      └──────────────────────┘      └──────────────────┘

┌───────────────┐      ┌──────────────────────┐      ┌──────────────────┐
│ Download plugin│ ◄──► │ plugin_storage bucket │      │ plugin files    │
│ from storage │      │ (Supabase Storage)    │      │                  │
└───────────────┘      └──────────────────────┘      └──────────────────┘

┌───────────────┐      ┌──────────────────────┐      ┌──────────────────┐
│ Execute      │ ──► │ Python/WASM runtime  │ ──► │ Plugin code runs│
│ plugin       │      │                      │      │ with input data │
└───────────────┘      └──────────────────────┘      └──────────────────┘

┌───────────────┐      ┌──────────────────────┐      ┌──────────────────┐
│ Upload result│ ──► │ complete_job edge   │ ──► │ job status =     │
│ to backend  │      │ function           │      │ 'completed'     │
└───────────────┘      └──────────────────────┘      └──────────────────┘
```

### Flow 3: Job Types (Payload Examples)

| Job Type | Payload (JSON) | Description |
|----------|----------------|-------------|
| `scan_dataset` | `{"source_path": "s3://bucket/file.csv"}` | Scan & analyze a dataset |
| `process_data` | `{"input_path": "...", "plugin_id": "uuid"}` | Run plugin on data |
| `preprocess` | `{"dataset_id": "...", "chunk_size": 1000}` | Prepare data for processing |
| `merge` | `{"dataset_id": "...", "level": 1}` | Merge processed chunks |

---

## 4. Database Schema

### Core Tables

#### 📋 agent_jobs (Job Queue)
| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key (auto-generated) |
| `org_id` | UUID | Organization ID |
| `agent_id` | UUID | Assigned worker device |
| `job_type` | TEXT | Type: scan_dataset, process, preprocess, merge |
| `payload` | JSONB | Job configuration & input data |
| `status` | TEXT | pending → assigned → running → completed/failed |
| `created_at` | TIMESTAMP | Job creation time |
| `started_at` | TIMESTAMP | When job started |
| `finished_at` | TIMESTAMP | When job completed |
| `duration_ms` | FLOAT | Execution time in milliseconds |
| `error` | TEXT | Error message if failed |
| `output_token` | TEXT | Result/output data |
| `retry_count` | INTEGER | Number of retries |
| `lease_expires_at` | TIMESTAMP | Job lease expiration (for stale job recovery) |

**Status Flow:**
```
pending ──(assigned)──► assigned ──(started)──► running ──(done)──► completed
                         │                              │
                         └──────────(failed)──────────► failed
```

#### 📊 devices (Worker Registry)
| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key (device_id) |
| `org_id` | UUID | Organization ID |
| `name` | TEXT | Device name (hostname) |
| `status` | TEXT | online, available, busy, offline |
| `access_token_hash` | TEXT | SHA256 hash of device token |
| `total_cpu_cores` | INTEGER | Available CPU cores |
| `total_memory_gb` | FLOAT | Available RAM |
| `gpu_available` | BOOLEAN | Has GPU? |
| `gpu_model` | TEXT | GPU model name |
| `active_jobs` | INTEGER | Currently running jobs |
| `max_concurrency` | INTEGER | Max parallel jobs |
| `last_heartbeat` | TIMESTAMP | Last health check |

#### 📈 executions (Pipeline Runs)
| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Execution ID |
| `org_id` | UUID | Organization |
| `dataset_id` | UUID | Associated dataset |
| `status` | TEXT | running, completed, failed |
| `current_step_index` | INTEGER | Current pipeline step |
| `total_steps` | INTEGER | Total pipeline steps |
| `created_at` | TIMESTAMP | Start time |
| `completed_at` | TIMESTAMP | End time |

#### 📦 execution_steps (Pipeline Steps)
| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Step ID |
| `execution_id` | UUID | Parent execution |
| `step_index` | INTEGER | Step order (0, 1, 2...) |
| `step_type` | TEXT | Type of step |
| `plugin_id` | UUID | Plugin to run |
| `config` | JSONB | Step configuration |
| `status` | TEXT | pending, running, completed, failed |

#### 💾 step_outputs (Step Results)
| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `execution_step_id` | UUID | Parent step |
| `output_key` | TEXT | Named output (e.g., "result") |
| `output_value` | JSONB | Output data |

#### 📊 agent_metrics (Performance Monitoring)
| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID | Primary key |
| `device_id` | UUID | Device ID |
| `org_id` | UUID | Organization |
| `metrics` | JSONB | CPU, RAM, GPU stats |
| `concurrency_returned` | INTEGER | Recommended workers |
| `load_factor` | NUMERIC | System load (0-1) |
| `created_at` | TIMESTAMP | Record time |

### Supporting Tables

| Table | Purpose |
|-------|---------|
| `datasets` | Registered data sources |
| `batch_chunks` | Data chunks for parallel processing |
| `plugin_execution_history` | Plugin run history |
| `job_checkpoints` | Job state for recovery |
| `job_notification_queue` | Real-time job notifications |
| `dataset_merge_locks` | Distributed merge coordination |
| `leases` | Job lease management |

---

## 5. Edge Functions

### Authentication
All functions use `authenticateDevice()` from `_shared/auth.ts`:
- Checks `x-agent-token` header
- Validates device exists & token matches
- Returns device_id + org_id

### Core Functions (API Endpoints)

| Function | Method | Auth | Purpose |
|---------|--------|------|---------|
| `claim_device` | POST | No | Register new worker |
| `agent_health_policy` | POST | Yes | Worker health & concurrency |
| `assign_agent_job` | POST | Yes | Get next available job |
| `verify_job_lease` | POST | Yes | Validate job ownership |
| `complete_job` | POST | Yes | Mark job complete |
| `relay_job_event` | POST | Yes | Report job events |
| `reconcile_agent` | POST | Yes | Recover stale jobs |
| `get_storage_config` | POST | Yes | Get storage credentials |
| `list_plugins_for_org` | GET | Yes | List available plugins |
| `get_plugin` | GET | Yes | Download plugin code |
| `notify_available_device` | POST | Yes | Worker available for work |

### Function Details

#### claim_device
```typescript
// Input
{
  claim_code: "ORG-CLAIM-CODE",
  sysinfo: {
    hostname: "laptop-1",
    cpu_cores: 8,
    memory_gb: 16,
    gpu_available: true,
    gpu_model: "NVIDIA RTX 3080"
  }
}

// Output
{
  ok: true,
  device_id: "uuid-of-device",
  agent_token: "unique-token-for-device",
  org_id: "uuid-of-org"
}
```

#### assign_agent_job
```typescript
// Input (via header)
x-agent-token: "device-token"

// Output
{
  ok: true,
  result: {
    job_id: "uuid",
    job_type: "process_data",
    payload: { input_path: "...", plugin_id: "..." },
    execution_id: "uuid",
    runtime_type: "python",
    runtime_dependencies: ["pandas", "numpy"]
  }
}
```

#### complete_job
```typescript
// Input
{
  execution_id: "uuid",
  job_id: "uuid",
  status: "completed" | "failed",
  duration_ms: 5000,
  output: { result_key: "value" },
  error: "error message if failed"
}

// Output
{
  ok: true,
  job_id: "uuid",
  status: "valid"
}
```

---

## 6. Agent Binary

### Configuration Priority
```go
// Priority (highest to lowest):
// 1. Command line flags (-claim-code)
// 2. Environment variables (BACKEND_URL, BACKEND_ANON_KEY, CLAIM_CODE)
// 3. .env file
// 4. Built-in defaults (DefaultBackendURL, DefaultAnonKey)
```

### Embedded Defaults
```go
var (
    DefaultBackendURL = "https://pqcwgkqrblugplpcaxcy.supabase.co"
    DefaultAnonKey   = "eyJhbGci... (your anon key)"
)
```

### How Agent Works

```
┌────────────────────────────────────────────────────────────┐
│                    SENTRA AGENT                            │
│                                                            │
│  1. STARTUP                                                │
│     ├─ Load config (env/.env/defaults)                    │
│     ├─ Validate token exists?                              │
│     │   └─ NO: Call claim_device → register                │
│     │   └─ YES: Continue                                   │
│     ├─ Fetch plugins from API                             │
│     ├─ Initialize storage backend                          │
│     └─ Start workers (health check server)               │
│                                                            │
│  2. IDLE (waiting for work)                              │
│     ├─ Poll API every N seconds                          │
│     ├─ Or WebSocket for real-time                        │
│     └─ Send periodic heartbeats                          │
│                                                            │
│  3. JOB RECEIVED                                           │
│     ├─ Verify job lease                                   │
│     ├─ Download plugin from storage                     │
│     ├─ Execute plugin (Python/WASM)                     │
│     ├─ Stream results back                                │
│     └─ Mark complete → release lease                    │
│                                                            │
│  4. GRACEFUL SHUTDOWN                                      │
│     ├─ Stop accepting new jobs                           │
│     ���─ Wait for active jobs to complete                   │
│     └─ Update device status to offline                  │
└────────────────────────────────────────────────────────────┘
```

### In-Built Job Handlers
These are compiled into the binary (no plugin download needed):

| Handler | Job Type | Description |
|---------|---------|-------------|
| scan_dataset | `scan_dataset` | Scan data files, detect schema |
| list_directory | `list_directory` | List files in path |
| detect_columns | `detect_columns` | Auto-detect column types |

### Plugin System
Plugins are fetched from Supabase Storage:
- Python scripts (.py)
- Requirements (.txt)
- Config (.json)

### Runtime Options
| Mode | Description |
|------|-------------|
| `native` | Direct Python execution (default) |
| `docker` | Isolated Docker container |
| `wasm` | WebAssembly sandbox (future) |

---

## 7. Security Model

### Authentication Flow
```
Worker → sends x-agent-token header
        ↓
Edge Function → hash token → lookup in devices table
        ↓
Found & valid? → Allow operation
        ↓
No match → 401 Unauthorized
```

### Device Token
- Generated via `crypto.randomUUID()`
- Stored as SHA256 hash in database
- Never stored in plain text

### RLS Policies
- Devices can only see their org's data
- Jobs filtered by org_id + device_id
- Service role for edge functions only

### What If Device is compromised?
1. Revoke device token in dashboard
2. Device can no longer authenticate
3. Jobs auto-reclaimed after lease expiry

---

## 8. Deployment Guide

### Quick Start (Single Machine)
```bash
# Download binary (from GitHub releases or build)
./sentra-agent -claim-code YOUR_ORG_CLAIM_CODE
```

### Deploy to Multiple Machines

#### Option A: Manual
```bash
# Copy binary to each machine
# Run with claim code
./sentra-agent -claim-code ORG_CODE
```

#### Option B: Scripted (SSH)
```bash
# Create list of IPs
cat > servers.txt <<EOF
192.168.1.10
192.168.1.11
192.168.1.12
EOF

# Deploy
for server in $(cat servers.txt); do
  scp sentra-agent user@$server:/tmp/
  ssh user@$server "cd /tmp && ./sentra-agent -claim-code YOUR_CODE &"
done
```

#### Option C: Docker
```bash
# Build Docker image
docker build -t sentra-agent:latest .

# Run containers
docker run -d sentra-agent:latest -claim-code YOUR_CODE
# Or with docker-compose
```

### Environment Variables
```bash
# Optional - override defaults
export BACKEND_URL=https://your-project.supabase.co
export BACKEND_ANON_KEY=your-anon-key
export CLAIM_CODE=your-org-claim-code
export LOG_LEVEL=debug
```

### Health Checks
```bash
# Health endpoint
curl http://localhost:8080/health

# Ready endpoint  
curl http://localhost:8080/ready

# Liveness check
curl http://localhost:8080/live
```

---

## 9. Monitoring

### View Active Jobs
```sql
SELECT id, job_type, status, agent_id, created_at 
FROM agent_jobs 
WHERE status IN ('pending', 'assigned', 'running')
ORDER BY created_at DESC;
```

### View Device Status
```sql
SELECT name, status, active_jobs, max_concurrency, last_heartbeat
FROM devices 
WHERE org_id = 'your-org-id';
```

### View Metrics
```sql
SELECT * FROM agent_metrics 
ORDER BY created_at DESC 
LIMIT 100;
```

---

## 10. Troubleshooting

| Issue | Solution |
|-------|----------|
| "Backend URL not configured" | Set BACKEND_URL env var or ensure binary has defaults |
| "401 Unauthorized" | Re-run claim_device with valid claim code |
| "No pending jobs" | Add jobs via dashboard or API |
| Worker offline | Check network, restart agent |
| Job stuck | Check lease_expiry, run reconcile_agent |

---

## 11. API Reference

### Endpoints Summary
```
POST /functions/v1/claim_device          → Register worker
POST /functions/v1/agent_health_policy → Health & concurrency
POST /functions/v1/assign_agent_job    → Get job assignment
POST /functions/v1/complete_job        → Mark job complete
POST /functions/v1/verify_job_lease    → Validate job lease
POST /functions/v1/relay_job_event     → Event notifications
POST /functions/v1/get_storage_config  → Storage credentials
GET  /functions/v1/list_plugins_for_org → Available plugins
GET  /functions/v1/get_plugin         → Download plugin
POST /functions/v1/reconcile_agent    → Recover stale jobs
```

---

*Document Version: 1.0*
*Last Updated: April 2026*