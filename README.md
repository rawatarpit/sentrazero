# SentraZero - Distributed Compute Platform

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org)
[![Supabase](https://img.shields.io/badge/Supabase-3ECF8E?style=flat&logo=supabase)](https://supabase.com)
[![License](https://img.shields.io/badge/License-Proprietary-red?style=flat)]()
[![OS](https://img.shields.io/badge/OS-macOS%20|%20Linux%20|%20Windows-blue?style=flat)]()

A decentralized compute platform that transforms idle developer machines, servers, and workstations into a distributed processing cluster. Organizations can run data pipelines, ML inference, ETL jobs, and batch processing workloads across their own infrastructure—without expensive cloud compute costs.

---

## Table of Contents

1. [What is SentraZero?](#what-is-sentrazero)
2. [Core Concepts](#core-concepts)
3. [End-to-End Architecture](#end-to-end-architecture)
4. [Complete Data Flow](#complete-data-flow)
5. [Quick Start](#quick-start)
6. [Database Schema](#database-schema)
7. [Edge Functions API](#edge-functions-api)
8. [Go Agent Architecture](#go-agent-architecture)
9. [Plugin System](#plugin-system)
10. [Security Model](#security-model)
11. [Configuration Reference](#configuration-reference)
12. [Monitoring & Observability](#monitoring--observability)
13. [Deployment Guide](#deployment-guide)
14. [Testing](#testing)
15. [Troubleshooting](#troubleshooting)
16. [API Reference](#api-reference)

---

## What is SentraZero?

SentraZero is a **self-hosted distributed compute platform** that enables organizations to:

- **Run data pipelines** across idle machines (scanning, processing, merging datasets)
- **Execute ML workloads** (embedding generation, model inference, data transformations)
- **Process batch jobs** (ETL, transformations, chunk-based operations)
- **Scale horizontally** by adding more agent devices to your fleet
- **Maintain data sovereignty** by keeping data within your infrastructure

### Key Differentiators

| Feature | Benefit |
|---------|---------|
| **Zero-Config Setup** | Single binary with claim code activation—no environment variables needed |
| **Auto-Scaling** | Jobs automatically distributed to available devices based on capability vectors |
| **Plugin Ecosystem** | Extend functionality via signed, sandboxed plugins (Python, Node.js, native) |
| **Warm Runtime Pools** | Pre-warmed Python/Node environments for <1s job startup |
| **Graceful Shutdown** | Agents drain in-flight jobs before termination |
| **Vector Similarity Matching** | Smart device-job matching using pgvector (16-dim device vectors) |
| **Multi-Tenant** | Complete org isolation via PostgreSQL RLS policies |
| **Storage Agnostic** | Supports S3, GCS, Azure Blob, or local filesystem |

---

## Core Concepts

### 1. Organizations (`orgs`)
The top-level entity. Each org has:
- A unique ID and claim secret for device registration
- Quotas (max devices, concurrent jobs) based on plan
- Isolated data (datasets, jobs, plugins) via RLS

### 2. Devices (`devices`)
Agent machines that process jobs:
- Register using a **claim code** from the org admin
- Report capabilities (CPU, RAM, GPU, runtimes supported)
- Have a **profile vector** (16 dimensions) for smart job matching
- Can be online/offline/available/busy/draining/error

### 3. Datasets (`datasets`)
Data to be processed:
- Stored in configured backend (S3, local, GCS)
- Scanned to extract metadata (file count, size, columns)
- Split into **chunks** (`batch_chunks`) for parallel processing
- Processed through pipelines or direct job execution

### 4. Jobs (`agent_jobs`)
Atomic units of work:
- **Types**: `scan_dataset`, `process` (chunk-level plugin execution), `preprocess` (marker, completes immediately), `merge_dataset` (combines chunk outputs)
- **Two levels**: Step-level coordination jobs (no `chunk_id`) → immediately completed as markers to unblock pipeline step; Chunk-level processing jobs (with `chunk_id`) → routed to native plugin handler
- **Lifecycle**: `pending` → `assigned` → `running` → `completed`/`failed` → `dead_letter` (after max retries exhausted)
- **Lease-based**: Devices acquire leases (default TTL) to prevent duplicate processing; leases verified at both start and completion time
- **Retry logic**: Automatic retry with exponential backoff (max 3 by default); dead-lettered after exhaustion
- **Compound jobs** (not currently active): Multi-step pipeline packed into a single agent_job. Each step has its own `plugin_id` + `config`, steps chain through a local cache dir (`~/.sentra/cache/{execID}/{chunkID}/`), and only the final step's output uploads to S3. The `advance_pipeline` edge function no longer creates compound jobs — it creates per-step agent_jobs instead.

### 5. Pipelines (`pipeline_templates` + `executions`)
Multi-step data processing workflows:
- **Template**: Reusable definition with ordered steps
- **Execution**: Instance of a template running against a dataset
- **Steps**: Can be built-in handlers or plugins
- **Auto-advance**: Completing a step triggers the next automatically

### 6. Plugins (`plugins`)
Extensible, signed code modules:
- **Languages**: Python, Node.js, Go, Rust, or native binaries
- **Signing**: Ed25519 signatures verified before execution
- **Sandboxing**: Resource limits (memory, CPU time, timeout) enforced
- **Rollout control**: Percentage-based gradual deployment per device

---

## End-to-End Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DASHBOARD (Web UI)                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │ Datasets │  │ Devices  │  │ Pipelines│  │ Plugins  │       │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘       │
│       │              │              │              │                  │
└───────┼──────────────┼──────────────┼──────────────┼──────────────────┘
        │              │              │              │
        ▼              ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      SUPABASE BACKEND                                │
│  ┌──────────────────────────────────────────────────────────┐        │
│  │  PostgreSQL Database (30+ tables, 100+ functions)     │        │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐             │        │
│  │  │ Devices  │ │ Jobs     │ │Datasets  │  ...         │        │
│  │  └──────────┘ └──────────┘ └──────────┘             │        │
│  │  RLS Policies: Org-level isolation enforced            │        │
│  │  pgvector: 16-dim device vectors, 384-dim chunks     │        │
│  │  Two-tier chunking: pre-chunk (historical) →         │        │
│  │    resize at pipeline time (live CPU/memory/disk)    │        │
│  └──────────────────────────────────────────────────────────┘        │
│  ┌──────────────────────────────────────────────────────────┐        │
│  │  Edge Functions (51 Deno/TypeScript functions)         │        │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐             │        │
│  │  │device mgmt│ │job mgmt  │ │pipeline  │  ...         │        │
│  │  └──────────┘ └──────────┘ └──────────┘             │        │
│  │  SSE: agent_stream for real-time job delivery         │        │
│  └──────────────────────────────────────────────────────────┘        │
│  ┌──────────────────────────────────────────────────────────┐        │
│  │  Supabase Vault (Encrypted secrets storage)              │        │
│  │  - Storage credentials (S3 keys, etc.)                  │        │
│  │  - Plugin signing keys (Ed25519 private keys)           │        │
│  └──────────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
        ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
        │  Agent 1     │ │  Agent 2     │ │  Agent N     │
        │  (Go Binary) │ │  (Go Binary) │ │  (Go Binary) │
        │              │ │              │ │              │
        │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
        │ │Runtime Mgr│ │ │ │Runtime Mgr│ │ │ │Runtime Mgr│ │
        │ │(Py/Node) │ │ │ │(Py/Node) │ │ │ │(Py/Node) │ │
        │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
        │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
        │ │Dispatcher │ │ │ │Dispatcher │ │ │ │Dispatcher │ │
        │ │(WorkerPool)│ │ │ │(WorkerPool)│ │ │ │(WorkerPool)│ │
        │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
        │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
        │ │  Plugin   │ │ │ │  Plugin   │ │ │ │  Plugin   │ │
        │ │ Executor  │ │ │ │ Executor  │ │ │ │ Executor  │ │
        │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
        └──────────────┘ └──────────────┘ └──────────────┘
                    │               │               │
                    └───────────────┼───────────────┘
                                    ▼
                    ┌───────────────────────────────┐
                    │  Storage Backend               │
                    │  - S3 / GCS / Azure / Local   │
                    │  - Dataset files               │
                    │  - Processed outputs           │
                    │  - Plugin binaries             │
                    └───────────────────────────────┘
```

---

## Complete Data Flow

### Flow 1: Organization Setup

```
Org Admin                          Supabase
    │                                   │
    │ 1. Create account                 │
    ├──────────────────────────────────►│
    │                                   │ CREATE org in `orgs`
    │                                   │ Generate claim_secret
    │                                   │ Create default quotas
    │                                   │ Generate Ed25519 signing key
    │                                   │ Store private key in Vault
    │                                   │ Store public key in `plugin_signing_keys`
    │                                   │
    │ 2. Get claim code (from dashboard)│
    │◄──────────────────────────────────┤
    │                                   │
    │ 3. Configure storage              │
    ├──────────────────────────────────►│
    │   (S3 bucket, credentials)       │ INSERT into `org_storage_configs`
    │                                   │ Encrypt creds → Store in Vault
    │                                   │
    │ 4. Invite team members           │
    ├──────────────────────────────────►│
    │                                   │ `invite_member` function
    │                                   │ Send invitation email
    │                                   │
```

### Flow 2: Device Registration (Zero-Config)

```
Device Operator                     Agent Binary                    Supabase
    │                                   │                              │
    │ Run: sentra --claim-code=ABC123  │                              │
    │──────────────────────────────────►│                              │
    │                                   │ 1. Call /bootstrap           │
    │                                   ├────────────────────────────►│
    │                                   │                             │ Verify claim code
    │                                   │                             │ CREATE device in `devices`
    │                                   │                             │ Generate access_token
    │                                   │                             │ Hash token → store
    │                                   │◄───────────────────────────┤
    │                                   │ Receive: device_id, token   │
    │                                   │         org_id, config      │
    │                                   │                             │
    │                                   │ 2. Save config to ~/.sentra │
    │                                   │                             │
    │                                   │ 3. Connect to SSE stream    │
    │                                   ├────────────────────────────►│
    │                                   │   /agent_stream             │ Verify token
    │                                   │                             │ Register for events
    │                                   │◄───────────────────────────┤
    │                                   │ Receive: hello event         │
    │                                   │                             │
    │                                   │ 4. Report system info      │
    │                                   │   (CPU, RAM, OS, etc.)     │
    │                                   │                             │
    │                                   │ 5. Call /agent_health_     │
    │                                   │   policy                   │
    │                                   ├────────────────────────────►│
    │                                   │                             │ Calculate concurrency
    │                                   │◄───────────────────────────┤
    │                                   │ Receive: {concurrency: 4}  │
    │                                   │                             │
    │                                   │ 6. Start worker pool       │
    │                                   │   (4 workers)              │
    │                                   │                             │
    │                                   │ 7. Poll for jobs:          │
    │                                   │   GET /assign_agent_job    │
    │                                   ├────────────────────────────►│
    │                                   │                             │ Find pending job
    │                                   │                             │ Match with device vector
    │                                   │                             │ Assign + lease
    │                                   │◄───────────────────────────┤
    │                                   │ Receive: job details        │
    │                                   │                             │
    │                                   │ 8. Execute job             │
    │                                   │   (plugin or builtin)      │
    │                                   │                             │
    │                                   │ 9. Report completion:      │
    │                                   │   POST /complete_job       │
    │                                   ├────────────────────────────►│
    │                                   │                             │ Update job status
    │                                   │                             │ Trigger pipeline advance
    │                                   │                             │ Archive if complete
    │                                   │◄───────────────────────────┤
    │                                   │ Receive: {success: true}    │
    │                                   │                             │
```

### Flow 3: Dataset Processing Pipeline (Two-Tier Chunking)

```
Data Engineer                  Dashboard              Supabase            Agent
    │                              │                    │                  │
    │ 1. Upload dataset files     │                    │                  │
    ├────────────────────────────►│                    │                  │
    │   to storage (S3/local)    │                    │                  │
    │                              │                    │                  │
    │ 2. Create dataset record   │                    │                  │
    ├────────────────────────────►│                    │                  │
    │                              │ POST /api/datasets │                  │
    │                              ├───────────────────►│                  │
    │                              │                    │ INSERT `datasets`  │
    │                              │                    │ TRIGGER:           │
    │                              │                    │ trg_create_scan_  │
    │                              │                    │ job_on_insert    │
    │                              │                    │ → creates scan   │
    │                              │                    │   agent_job      │
    │                              │◄───────────────────┤                  │
    │                              │                    │                  │
    │ 3. Agent picks up scan job  │                    │                  │
    │                              │                    │  ◄───────────────┤
    │                              │                    │  SSE: job event  │
    │                              │                    │                  │ Run builtin:scan
    │                              │                    │                  │ Scan files, metadata
    │                              │                    │                  │
    │ 4. Report scan complete     │                    │                  │
    │                              │                    │  ◄───────────────┤
    │                              │                    │ POST /report_     │
    │                              │                    │  dataset_scan    │
    │                              │                    │                  │
    │    === TIER 1: PRE-CHUNK (Historical, Automatic) ===              │
    │                              │                    │                  │
    │                              │                    │ TRIGGER:          │
    │                              │                    │ auto_progress_   │
    │                              │                    │ after_scan       │
    │                              │                    │ → SQL: pre_chunk_│
    │                              │                    │   dataset_smart()│
    │                              │                    │ Creates chunks   │
    │                              │                    │ with historical  │
    │                              │                    │ device hints     │
    │                              │                    │ No agent_jobs    │
    │                              │                    │ created          │
    │                              │                    │                  │
    │ 5. Create pipeline & run    │                    │                  │
    ├────────────────────────────►│ run_pipeline()      │                  │
    │                              ├───────────────────►│                  │
    │                              │                    │ activate_pipeline│
    │                              │                    │ → 1 agent_job   │
    │                              │                    │   per step       │
    │                              │                    │                  │
    │    === TIER 2: LIVE RESIZE (Pipeline-Time) ===                    │
    │                              │                    │                  │
    │                              │                    │ plan_dataset_    │
    │                              │                    │ chunks()         │
    │                              │                    │ → SQL: resize_   │
    │                              │                    │   chunks_for_    │
    │                              │                    │   pipeline()     │
    │                              │                    │ Reassigns chunks │
    │                              │                    │ using LIVE CPU%  │
    │                              │                    │ memory_free_gb   │
    │                              │                    │ available_disk_gb│
    │                              │                    │ active_jobs      │
    │                              │                    │                  │
    │ 6. Agent claims step job    │                    │                  │
    │                              │                    │  ◄───────────────┤
    │                              │                    │ claim_jobs_for_  │
    │                              │                    │ device (no       │
    │                              │                    │ rechunk)         │
    │                              │                    │                  │
    │ 7. Agent processes chunks   │                    │                  │
    │                              │                    │                  │ Reads chunks where
    │                              │                    │                  │ assigned_device_id
    │                              │                    │                  │ = device_id
    │                              │                    │                  │ Process via plugin
    │                              │                    │                  │
    │ 8. Step completes → advance │                    │                  │
    │                              │                    │ advance_pipeline │
    │                              │                    │ → next step      │
    │                              │                    │ → plan_dataset_  │
    │                              │                    │   chunks again   │
    │                              │                    │                  │
    │ 9. All steps done → merge   │                    │                  │
    │                              │                    │  ◄───────────────┤
    │                              │                    │                  │ Execute merge step
    │                              │                    │                  │ Output final data
    │                              │                    │                  │
    │ 10. Pipeline complete       │◄───────────────────┤                  │
    │                              │ dataset → 'ready'  │                  │
    │                              │                    │                  │
    │ 11. Download result         │                    │                  │
    ├────────────────────────────►│                    │                  │
    │                              │ GET /api/download │                  │
    │                              ├───────────────────►│                  │
    │                              │                    │ Generate presigned│
    │                              │                    │ URL from storage  │
    │                              │◄───────────────────┤                  │
    │◄────────────────────────────┤                    │                  │
    │ Receive download URL        │                    │                  │
    │                              │                    │                  │
```

### Flow 4: Plugin Execution

```
Plugin Developer               Dashboard           Edge Function         Agent
    │                              │                    │                  │
    │ 1. Create plugin (Python)   │                    │                  │
    │   - manifest.json            │                    │                  │
    │   - plugin code             │                    │                  │
    │                              │                    │                  │
    │ 2. Upload plugin           │                    │                  │
    ├────────────────────────────►│                    │                  │
    │                              │ POST /register_    │                  │
    │                              │ plugin             │                  │
    │                              ├───────────────────►│                  │
    │                              │                    │ 1. Verify JWT    │
    │                              │                    │    (admin role)  │
    │                              │                    │                  │
    │                              │                    │ 2. Upload binary │
    │                              │                    │    to storage    │
    │                              │                    │                  │
    │                              │                    │ 3. Get org's    │
    │                              │                    │    signing key   │
    │                              │                    │    from Vault    │
    │                              │                    │                  │
    │                              │                    │ 4. Hash binary │
    │                              │                    │    (SHA-256)    │
    │                              │                    │                  │
    │                              │                    │ 5. Sign hash   │
    │                              │                    │    (Ed25519)    │
    │                              │                    │                  │
    │                              │                    │ 6. INSERT       │
    │                              │                    │    `plugins`     │
    │                              │                    │    (trusted=true)│
    │                              │                    │                  │
    │                              │                    │ 7. Enable for   │
    │                              │                    │    org (100%)    │
    │                              │◄───────────────────┤                  │
    │                              │ Receive: plugin_id │                  │
    │                              │                    │                  │
    │                              │                    │                  │
    │ Later, when job runs...       │                    │                  │
    │                              │                    │                  │
    │                              │                    │  ◄───────────────┤
    │                              │                    │  GET /get_plugin │
    │                              │                    │  (job has        │
    │                              │                    │   plugin_id)    │
    │                              │                    │                  │
    │                              │                    │ 1. Fetch plugin │
    │                              │                    │    record        │
    │                              │                    │                  │
    │                              │                    │ 2. Download     │
    │                              │                    │    binary from   │
    │                              │                    │    storage      │
    │                              │                    │                  │
    │                              │                    │ 3. Compute      │
    │                              │                    │    SHA-256 of    │
    │                              │                    │    binary       │
    │                              │                    │                  │
    │                              │                    │ 4. Fetch public │
    │                              │                    │    key from      │
    │                              │                    │    plugin_signing│
    │                              │                    │    _keys       │
    │                              │                    │                  │
    │                              │                    │ 5. Verify       │
    │                              │                    │    signature     │
    │                              │                    │                  │
    │                              │                    │ 6. Send plugin  │
    │                              │                    │    + signature   │
    │                              │◄───────────────────┤                  │
    │                              │                    │                  │
    │                              │                    │                  │ 1. Receive plugin
    │                              │                    │                  │
    │                              │                    │                  │ 2. Verify signature
    │                              │                    │                  │    (again)
    │                              │                    │                  │
    │                              │                    │                  │ 3. Check resource
    │                              │                    │                  │    limits present
    │                              │                    │                  │
    │                              │                    │                  │ 4. Select runtime
    │                              │                    │                  │    (Python/Node/native)
    │                              │                    │                  │
    │                              │                    │                  │ 5. Execute in
    │                              │                    │                  │    sandbox with
    │                              │                    │                  │    resource limits
    │                              │                    │                  │
    │                              │                    │                  │ 6. Capture output
    │                              │                    │                  │
    │                              │                    │                  │ 7. Report job
    │                              │                    │                  │    complete
    │                              │                    │  ◄───────────────┤
    │                              │                    │  POST /complete_ │
    │                              │                    │  job            │
    │                              │◄───────────────────┤                  │
    │                              │ Dashboard updated  │                  │
    │                              │ via SSE            │                  │
    │                              │                    │                  │
```

---

## Quick Start

### Prerequisites

- **Go 1.25+** (to build from source)
- **Python 3.8+** or **Node.js 18+** (if using runtime plugins)
- **Redis** (optional, for multi-agent Pub/Sub coordination)

### 1. Build the Agent

```bash
git clone https://github.com/your-org/sentrazero.git
cd sentrazero
make build
```

Or download a pre-built binary for your platform from the releases page.

### 2. Run with a Claim Code

```bash
# Zero-config — agent auto-registers and discovers the backend
./bin/sentra-agent --claim-code <CLAIM_CODE>

# Or via environment variables:
export SENTRA_CLAIM_CODE=<claim_code>
./bin/sentra-agent
```

The agent will register itself, receive backend configuration, and start processing jobs.

### 3. Deploy with Docker

```bash
docker run -d \
  --name sentra-agent \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e SENTRA_CLAIM_CODE=YOUR_CLAIM_CODE \
  sentra/agent:latest
```

---

## Database Schema

The database has **30+ tables** with **100+ functions** and **20+ triggers**.

### Entity Relationship Diagram

```
orgs (1) ────── (∞) devices
  │                         │
  │                         │ (device_id)
  │                         │
  ├────── (∞) datasets     │
  │       │                 │
  │       │ (dataset_id)    │
  │       ▼                 ▼
  │  batch_chunks ──── (∞) agent_jobs
  │       │                 │
  │       │                 │ (execution_id)
  │       │                 ▼
  │       │            executions ──── (∞) execution_steps
  │       │                 │
  │       │                 │ (pipeline_template_id)
  │       │                 ▼
  │       │            pipeline_templates
  │       │
  ├────── (∞) plugins
  │       │
  │       │ (org_id)
  ▼       ▼
org_plugins ──── (∞) plugin_execution_history
```

### Core Tables

#### `orgs` - Organizations
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid (PK) | Organization ID |
| `name` | text | Organization name |
| `plan` | text | `free`, `pro`, `enterprise` |
| `claim_secret` | text | Secret for device registration |
| `team_size` | integer | Team size |
| `created_at` | timestamptz | Creation time |

#### `devices` - Agent Devices
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid (PK) | Device ID |
| `org_id` | uuid (FK) | Organization |
| `name` | text | Device name |
| `status` | enum | `online`, `offline`, `available`, `busy`, `error`, `draining` |
| `access_token_hash` | text | SHA-256 hash of access token |
| `benchmark_score` | numeric | Performance score (0-10) |
| `max_concurrency` | integer | Max concurrent jobs |
| `runtime_supported` | jsonb | `["python", "node"]` |
| `docker_available` | boolean | Docker support |
| `capabilities` | text[] | Capability tags |
| `embedding` | vector(16) | 16-dim profile vector |
| `total_cpu_cores` | integer | Total CPU cores |
| `total_memory_gb` | numeric | Total RAM |
| `gpu_available` | boolean | GPU availability |
| `last_heartbeat` | timestamptz | Last heartbeat |

#### `datasets` - Dataset Registry
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid (PK) | Dataset ID |
| `org_id` | uuid (FK) | Organization |
| `name` | text | Dataset name |
| `status` | text | `registered`, `scanning`, `scanned`, `chunked`, `processing`, `merge_pending`, `merging`, `merged`, `failed` |
| `storage_type` | text | `local`, `s3`, `gcs` |
| `total_size_gb` | double precision | Total size |
| `file_count` | bigint | Number of files |
| `source_path` | text | Source location |
| `dataset_checksum` | text | Integrity checksum |
| `merge_strategy` | text | `auto`, `sequential`, `tree` |

#### `agent_jobs` - Main Job Queue
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid (PK) | Job ID |
| `org_id` | uuid (FK) | Organization |
| `agent_id` | uuid (FK) | Assigned device |
| `job_type` | text | `scan_dataset`, `process`, `merge_dataset`, etc. |
| `status` | text | `pending`, `assigned`, `running`, `completed`, `failed` |
| `payload` | jsonb | Job configuration |
| `lease_expires_at` | timestamptz | Lease expiration |
| `execution_id` | uuid (FK) | Pipeline execution |
| `execution_mode` | text | `native`, `docker`, `runtime` |
| `runtime_type` | text | `python`, `node`, `native` |
| `retry_count` | integer | Current retry attempt |
| `max_retries` | integer | Max retries (default: 3) |
| `failure_classification` | text | `infra_error`, `dependency_error`, `user_code_error`, `timeout_error`, `memory_error` |

#### `batch_chunks` - Dataset Chunks
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid (PK) | Chunk ID |
| `dataset_id` | uuid (FK) | Parent dataset |
| `chunk_index` | integer | Chunk sequence number |
| `status` | text | `pending`, `processing`, `processed`, `failed`, `skipped` |
| `embedding` | vector(384) | 384-dim embedding |
| `chunk_vector` | vector(16) | 16-dim for device matching |
| `chunk_size_gb` | double precision | Chunk size |
| `assigned_device_id` | uuid (FK) | Processing device |

#### `pipeline_templates` - Reusable Pipelines
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid (PK) | Template ID |
| `org_id` | uuid (FK) | Organization |
| `name` | text | Template name |
| `steps` | jsonb | Array of step definitions |
| `description` | text | Template description |

#### `executions` - Pipeline Instances
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid (PK) | Execution ID |
| `org_id` | uuid (FK) | Organization |
| `dataset_id` | uuid (FK) | Target dataset |
| `pipeline_template_id` | uuid (FK) | Template used |
| `status` | text | `pending`, `running`, `completed`, `failed` |
| `current_step_index` | integer | Current step (0-based) |
| `total_steps` | integer | Total steps |

#### `plugins` - Plugin Registry
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid (PK) | Plugin ID |
| `org_id` | uuid (FK) | Organization |
| `name` | text | Plugin name |
| `version` | text | Version string |
| `language` | text | `python`, `node`, `go`, `rust`, etc. |
| `storage_path` | text | Binary location |
| `checksum` | text | SHA-256 of binary |
| `signature` | bytea | Ed25519 signature |
| `trusted` | boolean | Trusted flag (required for execution) |
| `resources` | jsonb | Resource limits |

### Key Database Functions

#### Job Management
| Function | Signature | Description |
|----------|-----------|-------------|
| `assign_best_job_to_best_device` | `(p_org_id)` → jsonb | Match best job to best device using vectors |
| `claim_jobs_for_device` | `(p_device_id, p_org_id, p_limit, p_lease_ttl)` → table | Batch claim jobs — returns dataset_id, chunk_index, step_index as top-level columns |
| `acquire_lease` | `(p_job_id, p_org_id, p_device_id, p_ttl)` → boolean | Acquire job lease |
| `cleanup_stuck_jobs` | `(p_max_retries, p_org_id)` → jsonb | Recover stuck jobs |
| `complete_job_idempotent` | `(p_job_id, p_status, ...)` → boolean | Idempotent completion |

#### Device Management
| Function | Signature | Description |
|----------|-----------|-------------|
| `select_best_device` | `(p_org_id, p_job_type, p_chunk_vector)` → uuid | Find best device |
| `match_best_device` | `(p_org_id, p_chunk_vector, p_job_type)` → table | Device ranking with scores |
| `recalcualte_device_vector` | `(p_device_id)` → void | Recalculate profile vector |
| `record_benchmark` | records benchmark | Update device score |

#### Pipeline & Chunking
| Function | Signature | Description |
|----------|-----------|-------------|
| `pre_chunk_dataset_smart` | `(p_dataset_id, p_org_id)` → jsonb | Tier 1: Create chunks with historical device hints |
| `resize_chunks_for_pipeline` | `(p_dataset_id, p_org_id, p_job_type)` → jsonb | Tier 2: Reassign chunks using live device metrics |
| `rechunk_for_device` | `(p_dataset_id, p_device_id, p_org_id, p_job_type)` → jsonb | Legacy — no longer called (deprecated) |
| `activate_pipeline` | `(p_template_id, p_dataset_id, p_org_id)` → jsonb | Start pipeline execution |
| `get_execution_detail` | `(p_execution_id)` → table | Execution details |

### Active Triggers

| Trigger | Table | Event | Function | Purpose |
|---------|-------|-------|----------|---------|
| `trg_create_scan_job_on_insert` | `datasets` | AFTER INSERT | `create_scan_job_on_dataset_insert` | Creates scan agent_job automatically |
| `trg_auto_progress_after_scan` | `datasets` | AFTER UPDATE OF status | `auto_progress_after_scan` | Calls pre_chunk_dataset_smart() on →'scanned' |
| `trg_cleanup_leases_on_offline` | `devices` | AFTER UPDATE OF status | `cleanup_leases_on_offline` | Deletes active leases when device goes offline |
| `touch_device_vector_trigger` | `device_benchmarks` | AFTER INSERT | `touch_device_vector` | Recalculates device 16-dim profile vector |
| `trg_calculate_optimal_chunk_size` | `device_benchmarks` | AFTER INSERT | `invoke_optimal_chunk_size_calculation` | Invokes chunk size calculation |
| `set_org_id_on_insert` (×8 tables) | Various | BEFORE INSERT | Various `set_*_org_id_trigger` | Auto-fills org_id from user context |
| `increment_vector_count` | `vector_store` | AFTER INSERT | `update_vector_dataset_count` | Tracks vector count |
| `org_storage_configs_updated_at` | `org_storage_configs` | BEFORE UPDATE | `update_org_storage_configs_updated_at` | Updates updated_at |
| `update_pipeline_timestamp` | `pipeline_templates` | BEFORE UPDATE | `update_pipeline_timestamp` | Updates updated_at |

### Ghost Triggers (Defined Functions but NOT Wired)

The following trigger functions exist in the schema but have **no `CREATE TRIGGER`** attached. The operations they'd perform are handled by edge functions or pipeline flow instead.

| Unwired Function | Purpose | Reason Not Wired |
|------------------|---------|-----------------|
| `assign_chunk_job_on_insert` | Auto-create agent_jobs from batch_chunks with merged payload | **Chunk job creation handled by `plan_dataset_chunks` edge function directly** |
| `auto_create_agent_job` | Auto-create merge job when chunks processed | Merge is a pipeline step, not auto-triggered |
| `advance_pipeline_on_job_complete` | Insert notification on job completion | Pipeline advancement handled by edge function |
| `on_merge_job_finished` | Set dataset ready on merge complete | Merge completion flows through pipeline |
| `enqueue_device_online_event` | Queue event on device online | Event queue not currently used |
| `handle_dataset_scan_trigger` | Handle dataset INSERT with scan | Replaced by wired `create_scan_job_on_dataset_insert` |
| `handle_job_failure` | Handle job failure side effects | Handled by edge function `report_job_error` |
| `notify_agent_on_dataset_register` | Notify on dataset register | Newer version of wired `create_scan_job_on_dataset_insert` |
| `on_agent_job_failed` | Handle agent job failure | Handled by edge function `report_job_error` |
| `queue_job_notification` | Queue notification on INSERT | Handled by `job_notification_queue` directly |
| `manage_agent_job_state` | Enforce state machine transitions | Handled by edge functions + RPCs |
| `prevent_overassign_agent_job` | Prevent over-assignment | Handled by `claim_jobs_for_device` RPC |

---

## Edge Functions API

### Authentication

Edge functions use one of the following auth mechanisms:

| Mechanism | Header / Method | Used By |
|-----------|----------------|---------|
| **Device Token** | `x-agent-token: <token>` → HMAC-hashed, compared to stored hash | Agent-facing endpoints (claim jobs, start/complete, heartbeat, etc.) |
| **Relay Key** | `x-relay-key: <secret>` | Internal pipeline functions (`run_pipeline`, `advance_pipeline`, `plan_dataset_chunks`) |
| **Cron Secret** | `Authorization: Bearer <cron_secret>` | Cron-triggered functions (`cleanup_stuck_jobs`, `dispatch_http_jobs`) |
| **Bootstrap Token** | `x-bootstrap-token: <token>` | Only `/bootstrap` |
| **None (service_role)** | Service role key used directly in code | Many admin/internal functions (`register_plugin`, `get_plugin`, `decrypt_vault_secret`, `get_storage_config`, `delete_org`, `invite_member`, `create-user`, etc.) |

> **Note:** Several functions use the service_role key directly with no request-level auth validation (see table below). This is a security consideration for production deployments.

### Complete Function List

#### Device Management (6 functions)

| Function | Method | Auth | Description |
|----------|--------|------|-------------|
| `claim_device` | POST | None (claim_code in body) | First-time registration with claim code |
| `register_device` | POST | None (service_role) | Register with existing org using claim code |
| `verifyAgentToken` | POST | Agent Token (x-agent-token + Authorization) | Verify device token |
| `bootstrap` | **GET** | Bootstrap Token (x-bootstrap-token) | Get backend config (zero-config) |
| `poll_state` | POST | Agent Token | **Consolidated** poll for jobs + report health (replaces separate claim_jobs_for_device + agent_health_policy calls) |
| `reconcile_agent` | POST | Agent Token | Reconcile state on restart |

#### Job Management (8 functions)

| Function | Method | Auth | Description |
|----------|--------|------|-------------|
| `assign_agent_job` | POST | Agent Token | Request job assignment (POST only, not GET) |
| `claim_jobs_for_device` | POST | Agent Token | Batch claim multiple jobs — returns dataset_id, chunk_index, step_index |
| `batch_assign_jobs` | POST | Agent Token | Atomic batch assignment (max 10 per call) |
| `start_job` | POST | Agent Token | Mark job as running (bypasses Supabase client JWT — calls REST API directly with service_role) |
| `complete_job` | POST | Agent Token | Report job completion. Does NOT directly trigger pipeline advancement. |
| `report_job_error` | POST | Agent Token | Report job error (sanitizes paths/IPs/emails, dead-letters after 3 retries) |
| `verify_job_lease` | POST | Agent Token | Verify lease validity (checks leases table, falls back to agent_jobs.lease_expires_at) |
| `cleanup_stuck_jobs` | POST | Bearer (CRON_SECRET) | Recover stuck jobs |

#### Pipeline & Dataset (9 functions)

| Function | Method | Auth | Description |
|----------|--------|------|-------------|
| `run_pipeline` | POST | Relay Key (x-relay-key) | Activate pipeline |
| `advance_pipeline` | POST | Relay Key (x-relay-key) | Advance to next step (publishes Redis notification for retry/merge/dead-letter) |
| `plan_dataset_chunks` | POST | Relay Key (x-relay-key) | Plan chunking strategy (publishes Redis notification on job creation) |
| `pre_chunk_dataset` | POST | Relay Key (x-relay-key) | Create chunk records via pre_chunk_dataset_smart |
| `calculate_optimal_chunk_size` | POST | Agent Token | Calculate chunk size based on device benchmarks |
| `approve_dataset_and_plan_chunks` | POST | Relay Key (x-relay-key) | Approve dataset scan and plan chunks |
| `report_dataset_scan` | POST | Agent Token | Report scan results |
| `record_dataset_metadata` | POST | Agent Token | Record dataset metadata after scan, then calls plan_dataset_chunks |
| `schedule_merge_job` | POST | Agent Token | Schedule dataset merge (selects merge-capable device, acquires merge lock) |

#### Plugin Management (7 functions)

| Function | Method | Auth | Description |
|----------|--------|------|-------------|
| `register_plugin` | POST | None (service_role) | Register new plugin (no JWT/admin validation — uses first org in DB) |
| `get_plugin` | POST | None (service_role) | Retrieve plugin record by ID |
| `list_plugins_for_org` | GET/POST | None (service_role) | List org plugins with signed URLs |
| `list_all_plugins` | GET/POST | Optional JWT | List all plugins |
| `get_plugin_signing_key` | POST | Agent Token | Get signing key by key_id (returns 410 Gone if expired) |
| `list_plugin_signing_keys` | POST | Agent Token | List active signing keys for org |
| `decrypt_vault_secret` | POST | None (service_role) | Decrypt secrets from Vault |

#### Storage & Config (4 functions)

| Function | Method | Auth | Description |
|----------|--------|------|-------------|
| `get_storage_config` | POST | None (service_role) | Get storage config (decrypts credentials from Vault) |
| `store_storage_credentials` | POST | None (service_role) | Store S3 credentials (calls store_s3_credentials_to_vault RPC) |
| `test_storage_connection` | POST | None (service_role) | Validate input fields only — does NOT actually test connectivity |
| `delete_org` | POST | None (service_role) | **DESTRUCTIVE** — deletes org and all related data (no auth validation) |

#### Real-time & Events (5 functions)

| Function | Method | Auth | Description |
|----------|--------|------|-------------|
| `poll_state` | POST | Agent Token | **Consolidated** poll + heartbeat — returns available jobs + updates device health in one call |
| `agent_stream` | GET | Agent Token | SSE job stream (polls every 30s inside ReadableStream, not WebSocket) |
| `relay_job_event` | POST | Dual: Relay Key OR Device Token | Relay events to agents (supports process_dataset and assign_job dispatch types) — kept for reliability since Redis Pub/Sub is fire-and-forget |
| `notify_available_device` | POST | Agent Token | Notify availability (updates metrics, derives busy/available status) — heartbeat now embedded in poll_state calls |
| `dispatch_http_jobs` | POST | Cron Secret (x-cron-secret) | Process http_queue (5 retries, exponential backoff, max 50 per batch) |
| `cleanup_job_notification_queue` | POST | None (service_role) | Clean old notifications (>24h) |

#### Admin & Utility (7 functions)

| Function | Method | Auth | Description |
|----------|--------|------|-------------|
| `invite_member` | POST | None (service_role) | Invite team member (creates auth invite + org_members) |
| `create-user` | POST | None (rate-limited by IP) | Create user + org + signing key (atomic, deletes user on failure) |
| `record_benchmark` | POST | Agent Token | Record benchmark |
| `check_data` | POST | Internal | Data validation |
| `verify_triggers` | POST | Internal | Verify triggers |
| `test_rpc` | POST | Internal | Test RPC functions |
| `upload_complete` | POST | Agent Token | Notify upload done |

### SSE Events (agent_stream)

```
Event: hello
Data: { "device_id": "uuid", "org_id": "uuid", "realtime_enabled": true }

Event: job
Data: { "id": "uuid", "job_type": "process", "status": "assigned", "payload": {...} }

Event: sync
Data: { "jobs_sent": 3, "timestamp": "2026-05-06T14:30:00Z" }
```

---

## Go Agent Architecture

### Directory Structure

```
sentrazero/
├── cmd/
│   ├── main.go                    # Agent entrypoint (identity, heartbeat, polling, dispatcher)
│   └── agent/
│       ├── runtime/               # Runtime environments
│       │   ├── runtime.go        # Runtime interface
│       │   ├── python.go        # Python runtime
│       │   ├── node.go          # Node.js runtime
│       │   └── v2/             # Runtime v2 (warm pools)
│       │       ├── manager.go   # Pool manager
│       │       ├── python.go    # Python v2 with caching
│       │       └── node.go      # Node.js v2 with caching
│       ├── executor/            # Job executor
│       │   ├── executor.go     # Executor interface
│       │   └── v2/            # Executor v2 with sandbox
│       └── sandbox/             # Sandbox isolation
│           └── sandbox.go      # Resource limits
│
├── internal/                     # Core packages
│   ├── auth/                   # Authentication
│   │   ├── identity.go        # Device identity management
│   │   ├── claim.go          # Device claiming flow
│   │   └── token_store.go    # Token storage (keyring/file)
│   │
│   ├── backend/                # Supabase HTTP client
│   │   ├── client.go         # Backend-facing client (claim, heartbeat, assign)
│   │   └── execution_client.go # Execution-facing client (complete_job, start_job, lease verify)
│   │
│   ├── bootstrap/              # Zero-config bootstrap
│   │   └── bootstrap.go      # Initial setup logic
│   │
│   ├── config/                 # Configuration management
│   │   ├── config.go         # Config loading from env/file
│   │   └── defaults.go       # Default values
│   │
│   ├── dataset/                # Dataset operations (merge, lock, recovery)
│   │   ├── merge.go          # Chunk merge logic (streaming + tree merge)
│   │   ├── lock.go           # Merge lock management
│   │   └── recovery.go      # Recovery logic
│   │
│   ├── dispatcher/             # Job dispatcher — core execution engine
│   │   ├── worker_pool.go    # Worker pool with backpressure, lease verification,
│   │   │                     #   completion reporting, graceful drain
│   │   ├── execute.go        # Five-way routing: scan/process/merge/preprocess/v2
│   │   ├── job.go            # Job struct definitions (Job, PluginContext)
│   │   ├── job_dedup.go      # File-based persistent dedup store (60min TTL)
│   │   ├── plugin_lookup.go  # UUID plugin_id → human plugin_name resolution
│   │   ├── handlers_unix.go  # executeProcessChunk, executeScanDataset,
│   │   │                     #   executeMergeDataset, compound job execution
│   │   ├── choose_mode.go    # Execution mode selection (small/fast/gguf/onnx)
│   │   ├── native_runner.go  # Native binary execution (CGO)
│   │   ├── native_runner_stub.go # No-CGO stub
│   │   └── introspection.go  # Metrics (worker count, queue length, etc.)
│   │
│   ├── heartbeat/             # Device heartbeat loop (every 10min, conditional)
│   │   └── heartbeat.go      # Sends metrics to notify_available_device
│   │
│   ├── plugin/                # Plugin lifecycle management
│   │   ├── manager.go        # Plugin cache, sync from API
│   │   ├── manifest.go       # Manifest parsing & verification
│   │   ├── key_fetcher.go   # Signing key fetching from backend
│   │   └── sandbox.go        # Plugin sandbox execution
│   │
│   ├── redis/                 # Redis client
│   │   └── client.go        # Connection pooling
│   │
│   ├── realtime/              # Real-time job communication
│   │   ├── supabase_realtime.go # Adaptive polling (5-60s) via poll_state, Redis Pub/Sub wake-up
│   │   ├── realtime_ws.go    # Supabase Realtime WebSocket
│   │   ├── sse_client.go     # SSE stream client + circuit breaker
│   │   └── available.go     # Announce availability at startup
│   │
│   ├── sandbox/               # OS-native sandbox isolation per platform
│   │   ├── sandboxer.go      # Interface + config + factory (noop vs native)
│   │   ├── limits.go         # Resource limit detection (memory, CPUs, temp dir)
│   │   ├── limits_darwin.go  # macOS memory detection via sysctl
│   │   ├── limits_linux.go   # Linux memory detection via /proc/meminfo
│   │   ├── limits_windows.go # Windows memory detection
│   │   ├── sandboxer_darwin.go  # macOS sandbox with Seatbelt profiles
│   │   ├── sandboxer_linux.go   # Linux sandbox with namespaces + cgroups v2
│   │   ├── sandboxer_windows.go # Windows sandbox with Job Objects
│   │   └── sandboxer_stub.go    # Fallback for unsupported platforms
│   │
│   ├── storage/               # Storage backend abstraction
│   │   ├── config.go        # Storage configuration resolution
│   │   └── s3http.go        # S3 HTTP transport
│   │
│   ├── obs/                   # Observability
│   │   ├── logger.go        # Structured logging
│   │   └── trace.go         # Distributed tracing
│   │
│   ├── reporter/              # Execution heartbeat
│   │   └── heartbeat.go     # Per-job execution heartbeat via relay_job_event
│   │
│   └── healthcheck/           # Health endpoint
│       └── server.go        # HTTP health server
│
├── supabase/ (hosted on Supabase, not in repo)
│
├── plugins/                    # Pipeline plugin manifests and scripts
│   ├── scrape/               # URL scraping plugin
│   │   ├── scrape.json      # Manifest (python, network=true, resource limits)
│   │   └── scrape.py        # Scrapes product attributes from URLs
│   ├── coverage/             # Platform coverage plugin
│   │   ├── coverage.json    # Manifest
│   │   └── coverage.py      # Searches platforms for product coverage
│   ├── compare/              # Product comparison plugin
│   │   ├── compare.json     # Manifest
│   │   └── compare.py       # Compares two URL/attribute columns
│   ├── dedup/                # Deduplication plugin
│   │   ├── dedup.json       # Manifest
│   │   └── dedup.py         # Finds duplicates by configurable field similarity
│   └── RISEOTB_Pipeline_RunGuide.md  # Pipeline run guide
│
├── Dockerfile                   # Docker build
├── Makefile                     # Build automation
├── go.mod / go.sum             # Go dependencies
└── .gitignore                  # Git ignore rules
```

### Key Go Interfaces

#### Runtime Interface
```go
type Runtime interface {
    Type() string                    // "python", "node", "native"
    Setup(env map[string]string) error
    InstallDeps(deps []Dependency) error
    Run(code string, env map[string]string) (*Result, error)
    Cleanup() error
}
```

#### Executor Interface
```go
type Executor interface {
    Execute(job *agent.Job) (*ExecutionResult, error)
    SupportsMode(mode string) bool
    Sandbox() *Sandbox
}
```

#### Dispatcher
```go
type Dispatcher struct {
    pool      *WorkerPool
    backend   *BackendClient
    pluginMgr *PluginManager
    redis     *RedisClient
}

func (d *Dispatcher) SubmitJob(job *agent.Job) error
func (d *Dispatcher) PollForJobs() ([]*agent.Job, error)
func (d *Dispatcher) ReportCompletion(job *agent.Job, result *ExecutionResult) error
```

### Job Execution Flow (`internal/dispatcher/execute.go`)

The `ExecuteJob` function implements five-way routing based on payload shape:

1. **`plugin_code` present** → Routes to v2 executor (`executor/v2/executor.go`) which runs inline plugin code directly. Used for edge function dispatch and inline script execution. Validates `PluginCode != ""` early.

2. **`chunk_id` present + no `plugin_code`** → Routes to `executeProcessChunk` (`handlers_unix.go:629`):
   - Resolves `plugin_id` UUID → human name via `pluginIDToName` sync.Map
   - Loads cached plugin binary via `plugin.LoadAndUpdatePlugin` with Ed25519 signature verification
   - Constructs `PluginContext` with `input_path`, `output_path`, `config`, step metadata
   - Creates sandbox work dir, passes input to plugin via stdin JSON, reads output from stdout
   - On plugin failure, falls back to v2 runtime manager
   - Determines storage mode (shared_mount vs S3) from payload
   - Uploads output to S3 (if not shared_mount) after plugin completion

3. **`job_type = "scan_dataset"`** → Routes to built-in `executeScanDataset`:
   - Resolves storage backend, lists source objects, downloads a sample file
   - Extracts metadata via `scanDatasetBuiltin` (CSV/JSON/Parquet detection, headers, schema, row counts)
   - Enriches summary with data from ALL listed objects (file count, total size, file types)
   - POSTs metadata to `/functions/v1/report_dataset_scan` which writes it to `datasets` table
   - Does **not** write processed data to storage — purely introspective

4. **`job_type = "merge_dataset"`** → Routes to built-in `executeMergeDataset`:
   - Combines all chunk output files into one merged dataset
   - Supports shared_mount (local file merge) and S3 (streaming merge) modes
   - Handles tree merge for large datasets via `dataset.StreamMergeTree`
   - Cleans up individual chunk outputs after successful merge
   - Updates dataset status via `complete_job`

5. **`job_type = "preprocess"` or step-level `process` with no `chunk_id`** → Marker jobs completed immediately. These are coordination markers created by `activate_pipeline` to trigger pipeline advancement without actual work.

#### Compound Job Execution (inactive)

The agent supports `executeCompoundProcessChunk` (`handlers_unix.go:1051`) for multi-step pipelines packed into a single job. When `IsCompound` is true and `Steps` is non-empty:
- Each step executes a different `plugin_id` with its own `config`
- Outputs chain through `~/.sentra/cache/{executionID}/{chunkID}/step_{N}.out`
- Resumes from `current_step_index` on retry (cache preserved on failure)
- Only the **final step's output** is uploaded to S3 (intermediate outputs are local-cached only)
- Per `STATUS.md`: The edge function `plan_dataset_chunks` does not create compound jobs — agents don't receive `is_compound: true`. This is built but unwired.

#### Storage Decisions Per Step

| Scenario | Step 0 | Intermediate Steps | Final Step | Control Flow |
|----------|--------|-------------------|------------|--------------|
| **shared_mount** (default) | Reads `chunk_N.bin` → writes `chunk_N.out` locally | Reads `chunk_N.out` → writes `chunk_N.out` locally | Reads → writes locally | No S3 at all. `is_local=true` for all steps. Merge combines local `.out` files. |
| **S3 + non-compound** | Downloads source from S3 → uploads result to S3 | Downloads previous result from S3 → uploads result to S3 | Downloads → uploads to S3 | **Every step uploads** individually (line 927 of handlers_unix.go). `is_last_step` determines which step triggers the final merge. |
| **S3 + compound** (inactive) | Downloads source from S3 → writes to local cache | Reads from cache dir → writes to cache dir | Uploads final output to S3 | Only the final step uploads. Intermediate outputs are cached locally and discarded on success. |

#### Agent Job Lifecycle (Full)

```
Redis Pub/Sub notification  ──────┐
(sentra:newjob:{org_id})          │
                                  ▼
poll_state  (adaptive 5-60s — consolidated poll + heartbeat)
        │
        ▼  (job assigned with lease)
VerifyJobLease → StartJob (transition: assigned → running)
        │
        ▼
Worker pool: ExecuteJob (plugin execution in sandbox)
        │
        ├── Success → CompleteJob → advance_pipeline (publishes Redis on retry/merge)
        │
        └── Failure → ReportJobFailure → retry (×3) → dead_letter (publishes Redis notification)
```

#### Plugin Name Resolution (`internal/dispatcher/plugin_lookup.go`)

- `pluginIDToName` is a `sync.Map` keyed by UUID `plugin_id` → human-readable `plugin_name`
- `PopulatePluginIDMap(plugins)` is called at startup from `main.go` after `SyncPluginsFromAPI`
- `ResolvePluginName(pluginID, pluginName)` resolves UUID → name, falling back to provided `pluginName`

### Plugin Name Resolution (`internal/dispatcher/plugin_lookup.go`)

- `pluginIDToName` is a `sync.Map` keyed by UUID `plugin_id` → human-readable `plugin_name`
- `PopulatePluginIDMap(plugins)` is called at startup from `main.go:247` after `SyncPluginsFromAPI` returns the plugin list
- `ResolvePluginName(pluginID, pluginName)` returns the resolved name, falling back to the provided `pluginName` if the ID isn't found

---

## Plugin System

### Plugin Manifest Structure
```json
{
  "name": "my-plugin",
  "version": "1.0.0",
  "filename": "plugin.py",
  "language": "python",
  "plugin_type": "core",
  "checksum": "sha256:abc123...",
  "trusted": true,
  "network": false,
  "resources": {
    "memory_mb": 512,
    "cpu_seconds": 300,
    "timeout_seconds": 600
  },
  "signature": "base64encoded...",
  "signature_key_id": "key-001"
}
```

### Pipeline Plugins (RISEOTB)

The repository ships four Python pipeline plugins under `plugins/` for e-commerce data processing:

| Plugin | Directory | Network | Purpose | Configurable Fields |
|--------|-----------|---------|---------|-------------------|
| `plugin_scrape` | `plugins/scrape/` | Yes | Scrape product attributes from URLs | `url_columns`, `platforms`, `user_agent`, `mock_mode` |
| `plugin_coverage` | `plugins/coverage/` | Yes | Search platforms for product coverage | `platform_configs`, `max_results_per_platform`, `user_agent` |
| `plugin_compare` | `plugins/compare/` | Yes | Compare two URL/attribute columns for match decisions | `compare_method`, `weights`, `thresholds` |
| `plugin_dedup` | `plugins/dedup/` | No | Remove duplicate products by field similarity | `match_fields`, `similarity_threshold`, `variant_fields` |

**Pipeline patterns:**
- Scrape → Coverage (2-step)
- Scrape → Compare (2-step)
- Scrape → Dedup (2-step)
- Scrape → Coverage → Compare → Dedup (4-step)

Each plugin reads job context from **stdin** as JSON (`PluginContext`) and writes result to **stdout** as JSON (`{ success, data, error }`). They are installed to `~/.sentra/plugins/{plugin_name}/{os}-{arch}/` and locked to `0700`.

See [`plugins/RISEOTB_Pipeline_RunGuide.md`](./plugins/RISEOTB_Pipeline_RunGuide.md) for full installation and configuration instructions.

### Registration Flow (Backend Handles Signing)

```
User (Admin)                 Edge Function                 Vault + DB
    │                              │                            │
    │ POST /register_plugin        │                            │
    │ - binary file                │                            │
    │ - metadata                  │                            │
    ├────────────────────────────►│                            │
    │                              │ 1. Validate JWT + admin   │
    │                              │ 2. Upload binary to storage│
    │                              ├───────────────────────────►│
    │                              │                            │
    │                              │ 3. Get signing key ID     │
    │                              │    from plugin_signing_keys│
    │                              ├───────────────────────────►│
    │                              │                            │
    │                              │ 4. Decrypt private key    │
    │                              │    from Vault              │
    │                              ├───────────────────────────►│
    │                              │                            │
    │                              │ 5. Hash binary (SHA-256)  │
    │                              │ 6. Sign hash (Ed25519)    │
    │                              │                            │
    │                              │ 7. Insert into plugins    │
    │                              ├───────────────────────────►│
    │                              │                            │
    │                              │ 8. Enable for org (100%)  │
    │                              ├───────────────────────────►│
    │                              │                            │
    │◄─ 201 { plugin_id } ─────────┤                            │
```

### Plugin Execution Flow

```
Agent                        Edge Function              Plugin Binary
  │                              │                         │
  │ Claim job with plugin_id     │                         │
  ├────────────────────────────►│                         │
  │                              │ Fetch plugin record      │
  │                              ├────────────────────────►│
  │                              │ Download binary          │
  │                              │ from storage             │
  │                              │                         │
  │                              │ Compute SHA-256          │
  │                              │ Verify Ed25519 sig       │
  │                              │                         │
  │◄─ plugin + manifest ─────────┤                         │
  │                                                       │
  │ Verify signature (again)                             │
  │ Check trusted flag                                    │
  │ Check resource limits                                  │
  │                                                       │
  │ Select runtime (Python/Node/native)                    │
  │ Execute in sandbox                                    │
  │   - Memory limit (ulimit -v)                         │
  │   - CPU time (ulimit -t)                             │
  │   - Wall-clock timeout                                │
  │                                                       │
  │ Capture output                                        │
  │ Report completion ────────────────────────────────────►│
```

### Supported Languages

| Language | Runtime | Method | Dependencies |
|----------|---------|--------|-------------|
| Python | v2 Runtime Manager | Pre-warmed venv | requirements.txt / PyPI |
| Node.js | v2 Runtime Manager | Pre-warmed node_modules | package.json / npm |
| Go | Native | Direct binary execution | None (compiled) |
| Rust | Native | Direct binary execution | None (compiled) |
| C/C++ | Native | Compiled binary | System libraries |
| Ruby/Bash | System | Script interpreter | System packages |

---

## Dataset Metadata & Plugin Configuration

### Metadata Flow (scan_dataset → DB)

When a dataset is ingested, an automatic `scan_dataset` job is created. The agent:

1. Lists all source objects in storage
2. Downloads a sample file
3. Extracts metadata using built-in format detectors:

| Format | Extracted Info |
|--------|----------------|
| **CSV/TSV** | Delimiter auto-detection, headers, column count, per-column type hints, sample rows (up to 10), estimated row count |
| **JSON/JSONL** | Array length, key names, nested schema |
| **Parquet** | Schema, column types, row groups |

4. Enriches with aggregate data: file count, total size bytes, file type distribution, source file keys
5. POSTs to `/functions/v1/report_dataset_scan` which writes to the `datasets` table:

```json
// datasets.metadata (JSONB column)
{
  "scanned_at":          "2026-06-13T10:00:00Z",
  "storage_type":        "s3",
  "scan_completed":      true,
  "file_count":          3,
  "total_size_bytes":    15728640,
  "file_types":          { ".csv": 2, ".parquet": 1 },
  "file_type":           "csv",
  "schema":              { "title": "string", "price": "number", "url": "string" },
  "headers":             ["title", "price", "url", "brand", "upc"],
  "columns":             ["title", "price", "url", "brand", "upc"],
  "sample_row_count":    100,
  "sample_file":         "products.csv",
  "format":              "csv",
  "delimiter":           ",",
  "estimated_row_count": 50000,
  "source_files":        ["datasets/abc123/products.csv"]
}
```

### How Plugins Can Use Metadata

When `run_pipeline` creates jobs via `plan_dataset_chunks`, the pipeline step `config` from `pipeline_templates.steps[].config` is embedded in each job's payload. Currently this config is static (defined in the template).

**Gap:** The `datasets.metadata` JSONB column already contains rich per-dataset information (column names, types, row counts, format), but `plan_dataset_chunks` does not inject this metadata into job payloads. This means plugins cannot adapt their behavior based on dataset characteristics.

**Potential use cases for metadata-aware plugins:**
- **scrape**: Use `datasets.metadata.columns` to determine which URL columns exist, rather than hardcoding column names in the pipeline config
- **coverage**: Use `estimated_row_count` to throttle API request rate per-platform
- **compare**: Use `schema` type hints to select comparison method (exact match for numeric columns, similarity for text)
- **dedup**: Use `headers` to auto-detect available match fields and field mapping

Closing this gap would involve passing `datasets.metadata` (or selected fields) into the job `payload` when `plan_dataset_chunks` creates per-chunk agent_jobs.

---

## Security Model

### 1. Authentication & Authorization

```
┌─────────────────────────────────────────────────────────┐
│  User Authentication (Supabase Auth)                     │
│  - JWT tokens for dashboard users                       │
│  - Row Level Security (RLS) enforces org isolation     │
└─────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  Device Authentication (Token-based)                     │
│  - Tokens hashed (SHA-256) before storage             │
│  - Timing-safe comparison for token verification       │
│  - Tokens can be rotated via rotate_agent_token()      │
└─────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  Plugin Signing (Ed25519)                              │
│  - Organization generates Ed25519 key pair             │
│  - Private key stored in Supabase Vault                │
│  - Backend signs plugin binaries (not metadata)        │
│  - Agent verifies signature before execution           │
└─────────────────────────────────────────────────────────┘
```

### 2. Row Level Security (RLS)

All org-scoped tables have RLS enabled:

```sql
-- Example RLS policy for agent_jobs
CREATE POLICY "Users can only see their org's jobs"
  ON agent_jobs
  FOR ALL
  USING (org_id = get_current_org_id());

-- Function to get current user's org
CREATE FUNCTION get_current_org_id() RETURNS uuid AS $$
  SELECT org_id FROM org_members WHERE user_id = auth.uid() LIMIT 1;
$$ LANGUAGE sql STABLE;
```

### 3. Plugin Execution Sandbox

The agent uses a **platform-native sandbox** (`internal/sandbox/`) that provides OS-level isolation per operating system. Configured via `SENTRA_SANDBOX_*` env vars.

#### Sandbox Interface

```go
type Sandboxer interface {
    Prepare(ctx context.Context, jobID string, manifest PluginManifest, network bool) (*SandboxEnv, error)
    Execute(ctx context.Context, env *SandboxEnv, cmd *exec.Cmd) error
    Destroy(ctx context.Context, env *SandboxEnv) error
}
```

Each plugin runs in a sandboxed work directory (`~/.sentra/sandbox/{jobID}/`) with resource limits enforced at the OS level. The `SandboxConfig` controls default/ max resource budgets, seccomp profiles, cgroups, and UID/GID sandboxing.

#### Platform-Specific Isolation

| Platform | Sandbox Mode | Implementation |
|----------|-------------|----------------|
| **Linux** | `native` | Linux namespaces (PID, mount, network, UTS) + cgroups v2 (memory, CPU) + seccomp BPF profiles. UID/GID drops to `65534` (nobody). |
| **macOS** | `native` | Seatbelt sandbox profiles (App Sandbox equivalent). Resource limits via `setrlimit`. Temp directory isolation. |
| **Windows** | `native` or `off` | Windows Job Objects (process group limits). Configured via `SANDBOX_WINDOWS_JOB_OBJECT`. Falls back to `off` when unavailable. |
| **Fallback** | `off` | `noopSandbox` — creates work dir, runs process, cleans up. No OS isolation. |

#### Resource Enforcement

| Protection | Method |
|------------|--------|
| **Memory Limit** | `setrlimit(RLIMIT_AS/RLIMIT_DATA)` on Unix; cgroup memory.max on Linux; Job Object memory limit on Windows |
| **CPU Time** | `setrlimit(RLIMIT_CPU)` on Unix; cgroup cpu.max on Linux |
| **Wall-clock Timeout** | `context.WithTimeout()` in Go — kills process tree |
| **Network** | `manifest.Network` flag (default: false); Linux: isolated netns when disabled |
| **File System** | Per-job temp directory (`~/.sentra/sandbox/{jobID}/`), cleaned up after completion |
| **Process Isolation** | Fork/exec with dropped privileges; Linux: PID namespace isolation |
| **Seccomp** | Default seccomp profile (Linux) blocks ~50+ syscalls not needed by script plugins |

#### Plugin Execution Flow

1. Agent resolves `plugin_id` UUID → human `plugin_name` via `internal/dispatcher/plugin_lookup.go`
2. `plugin.LoadAndUpdatePlugin()` loads cached plugin binary from `~/.sentra/plugins/`, verifies Ed25519 signature
3. Sandbox prepares isolated environment via `Prepare()` — creates work dir, sets up OS isolation
4. Plugin binary receives input as JSON on **stdin** (shape: `PluginContext` with `job_id`, `org_id`, `dataset_id`, `execution_id`, `step_index`, `chunk_id`, `input_path`, `output_path`, `config`)
5. Output is read from **stdout** as JSON `{ "success": bool, "data": {...}, "error": "..." }`
6. Fallback chain: native plugin → Docker sandbox → v2 runtime manager (pre-warmed Python/Node venv)
7. Sandbox destroys environment via `Destroy()` — cleans up temp files

### 4. Message Sanitization

Error messages are sanitized before storage:

```go
func SanitizeErrorMessage(msg string) string {
    // Remove file paths
    // Remove IP addresses
    // Remove email addresses
    // Truncate to max length
}
```

---

## Configuration Reference

### Agent Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SENTRA_CLAIM_CODE` | * | - | Claim code for first-time registration |
| `SENTRA_BACKEND_URL` | No | auto-detect | Supabase project URL |
| `SENTRA_REDIS_URL` | No | - | Redis URL for Pub/Sub coordination |
| `MAX_CONCURRENCY` | No | auto | Max concurrent jobs |
| `LOG_LEVEL` | No | `info` | Log level (debug, info, warn, error) |
| `HEALTH_CHECK_PORT` | No | `8080` | Health endpoint port |
| `AGENT_ENVIRONMENT_TYPE` | No | `production` | Environment type |
| `AGENT_STORAGE_TYPE` | No | `local` | Storage backend |
| `SENTRA_POLL_INTERVAL` | No | `5s` | Minimum polling interval |
| `SENTRA_POLL_MAX_INTERVAL` | No | `60s` | Maximum polling interval |
| `SENTRA_HEARTBEAT_INTERVAL` | No | `10m0s` | Heartbeat interval |
| `SENTRA_JOB_HEARTBEAT_INTERVAL` | No | `60s` | Per-job execution heartbeat (via relay_job_event) |
| `SENTRA_KEY_CACHE_TTL` | No | `60m` | Plugin signing key cache TTL |
| `SENTRA_KEY_RELOAD_INTERVAL` | No | `60m` | Plugin key reload interval |
| `SENTRA_PLUGIN_AUTO_UPDATE_INTERVAL` | No | `60m` | Plugin auto-update check interval |

### Runtime Pool Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT_POOL_MAX_SIZE` | `10` | Max warm environments |
| `ENVIRONMENT_MAX_COUNT` | `50` | Max total environments |
| `ENVIRONMENT_WARM_TIMEOUT` | `30m` | Time before cooling |
| `ENVIRONMENT_EVICTION_INTERVAL` | `5m` | Eviction check interval |
| `ENVIRONMENT_MAX_DISK_BYTES` | `10GB` | Max disk for environments |

### Supabase Edge Function Secrets

| Secret | Description |
|--------|-------------|
| `SUPABASE_URL` | Project URL |
| `SUPABASE_SERVICE_ROLE_KEY` | Service role key |
| `CRON_SECRET` | Secret for cron jobs |
| `PLATFORM_SIGNING_PUBLIC_KEY_B64` | Ed25519 public key |
| `PLATFORM_SIGNING_PRIVATE_KEY_B64` | Ed25519 private key |
| `Redis_url` | Upstash Redis REST URL (https://...) or connection URL (rediss://...) |
| `Redis_token` | Upstash Redis REST API token |

---

## Monitoring & Observability

### Health Check Endpoint

```bash
# Agent health endpoint (default :8080)
GET /health

Response:
{
  "status": "ok",
  "device_id": "uuid",
  "active_jobs": 3,
  "uptime_seconds": 3600
}
```

### Metrics Collected

| Metric | Source | Description |
|--------|--------|-------------|
| `device_benchmarks` | Agent | Performance scores |
| `agent_metrics` | Agent | CPU, memory, network |
| `plugin_execution_history` | Agent | Plugin execution records |
| `device_job_performance` | DB | Per-device job stats |
| `system_logs` | DB | System events |

### Dashboard Statistics

The `get_dashboard_stats()` function returns:

```json
{
  "total_jobs": 1234,
  "running_jobs": 5,
  "completed_jobs": 1200,
  "failed_jobs": 29,
  "pending_jobs": 0,
  "total_datasets": 50,
  "active_datasets": 3,
  "total_devices": 10,
  "online_devices": 8,
  "busy_devices": 5,
  "total_executions": 45,
  "running_executions": 2,
  "completed_executions": 43
}
```

---

## Deployment Guide

### Docker Deployment

```dockerfile
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
RUN apk add --no-cache git
RUN go build -ldflags="-s -w" -o /bin/sentra ./cmd/sentra

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /bin/sentra /bin/sentra
EXPOSE 8080
CMD ["/bin/sentra", "--claim-code", "${SENTRA_CLAIM_CODE}"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sentra-agent
spec:
  replicas: 3
  selector:
    matchLabels:
      app: sentra-agent
  template:
    metadata:
      labels:
        app: sentra-agent
    spec:
      containers:
      - name: agent
        image: sentra/agent:latest
        env:
        - name: SENTRA_CLAIM_CODE
          valueFrom:
            secretKeyRef:
              name: sentra-secrets
              key: claim-code
        ports:
        - containerPort: 8080
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2"
```

---

## Testing

### Running Tests

```bash
# Go unit tests
go test ./internal/... -v

# Specific test
go test ./internal/dispatcher -run TestWorkerPool

# With coverage
go test ./... -cover
```

### Test Coverage Areas

| Area | Coverage | Priority |
|------|----------|----------|
| Worker Pool | ✅ 10 tests | P0 |
| Plugin Manifest | ✅ Tests | P0 |
| Config Loading | ✅ Tests | P1 |
| Database Functions | ⚠️ Partial | P0 |
| Edge Functions | ❌ Missing | P0 |
| End-to-End Flows | ❌ Missing | P0 |

---

## Troubleshooting

### Device Won't Register

```
Error: Failed to claim device: Invalid claim code
```

**Solution:**
1. Verify claim code in `orgs` table
2. Check network connectivity to Supabase
3. Ensure device can reach `bootstrap` function

### Jobs Not Being Assigned

```
Symptom: Jobs stuck in "pending" status
```

**Debug:**
```sql
-- Check device status
SELECT id, name, status, last_heartbeat FROM devices WHERE org_id = '...';

-- Check for pending jobs
SELECT COUNT(*) FROM agent_jobs WHERE status = 'pending' AND org_id = '...';

-- Check device capabilities vs job requirements
SELECT * FROM get_device_rankings('...', 'process', NULL);
```

### Plugin Verification Fails

```
Error: Plugin signature verification failed
```

**Solution:**
1. Ensure plugin was registered via `register_plugin` (backend signs it)
2. Check `trusted` flag is `true` in `plugins` table
3. Verify signing key not revoked in `plugin_signing_keys`

### Environment Pool Exhausted

```
Warning: Environment pool full, creating cold environment
```

**Solution:**
1. Increase `ENVIRONMENT_MAX_COUNT`
2. Decrease `ENVIRONMENT_WARM_TIMEOUT`
3. Add disk space or check `ENVIRONMENT_MAX_DISK_BYTES`

### Stuck Jobs

```sql
-- Manually trigger cleanup
SELECT * FROM cleanup_stuck_jobs(3, '...');

-- Check lease expirations
SELECT id, status, lease_expires_at 
FROM agent_jobs 
WHERE lease_expires_at < NOW() AND status IN ('assigned', 'running');
```

---

## API Reference

### Edge Function Endpoints

Base URL: `https://<project>.supabase.co/functions/v1/`

#### Device Management

```
POST /claim_device              # Register device (no auth)
GET  /bootstrap                 # Get config (x-bootstrap-token auth)
POST /register_device           # Register (no auth, service_role)
POST /poll_state                # Consolidated poll + heartbeat (Agent Token)
POST /reconcile_agent           # Reconcile state on restart (Agent Token)
```

#### Job Lifecycle

```
POST /assign_agent_job         # Request job (Agent Token) — POST only
POST /claim_jobs_for_device     # Batch claim (Agent Token) — legacy, use poll_state
POST /start_job                 # Start job (Agent Token)
POST /complete_job              # Complete job (Agent Token)
POST /report_job_error          # Report error (Agent Token) — dead-letters after 3 retries
POST /verify_job_lease          # Verify lease (Agent Token)
POST /cleanup_stuck_jobs        # Recover stuck jobs (CRON_SECRET)
```

#### Dataset & Pipeline

```
POST /run_pipeline                          # Activate pipeline
POST /advance_pipeline                      # Advance step (publishes Redis notification)
POST /plan_dataset_chunks                   # Plan chunks (publishes Redis notification)
POST /pre_chunk_dataset                     # Create chunks
POST /calculate_optimal_chunk_size          # Calc chunk size
POST /approve_dataset_and_plan_chunks      # Approve & plan
POST /report_dataset_scan                   # Report scan
POST /schedule_merge_job                    # Schedule merge
POST /record_dataset_metadata               # Record scan metadata
```

#### Plugin Management

```
POST /register_plugin            # Register (Bearer, Admin)
POST /get_plugin                # Get binary (Agent Token)
GET  /list_plugins_for_org     # List org plugins (Bearer)
GET  /list_all_plugins         # List all (Bearer)
POST /get_plugin_signing_key   # Get key (Agent Token)
GET  /list_plugin_signing_keys # List keys (Bearer)
POST /decrypt_vault_secret     # Decrypt Vault secrets (service_role)
```

#### Real-time & Events

```
GET  /agent_stream              # SSE stream (Agent Token)
POST /relay_job_event          # Relay event (Relay Key or Device Token) — reliable fallback
POST /notify_available_device  # Notify availability (Agent Token) — heartbeat embedded in poll_state
POST /dispatch_http_jobs       # Process HTTP queue (CRON_SECRET)
```

#### Redis Pub/Sub Channels

```
sentra:newjob:{org_id}         # Published by plan_dataset_chunks and advance_pipeline
```

---

## License

Part of the SentraZero Compute Platform.

---

**Built with:**
- Go 1.25+
- Supabase (PostgreSQL + Edge Functions + Vault + Realtime)
- Deno (Edge Functions runtime)
- pgvector (Vector similarity search)
- Redis (Optional coordination)

**Last Updated:** 2026-06-18
