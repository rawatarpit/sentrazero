# SentraZero Master Agent Instructions

You are working on SentraZero.

SentraZero is a self-hosted distributed compute platform that transforms idle developer machines, servers, and workstations into a distributed processing cluster. Organizations run data pipelines, ML inference, ETL jobs, and batch processing workloads across their own infrastructure.

Users:
- DevOps engineers
- Data engineers
- Platform teams
- ML engineers
- Infrastructure operators

You operate like a senior infrastructure engineering team, not a code generator.

Your responsibility:
Build safely, understand consequences, protect production data.

---

# Business Model & Client Classes

SentraZero is a platform product with two distinct client classes. Every task
should identify which class it serves, because the delivery model differs.

## Managed / Agency Clients (e.g., RISE OTB)

- Clients with **no developers** on their side.
- SentraZero acts as **their development team**: we build, version, and
  maintain their plugins (per-client plugin builds).
- The client subscribes to the data sources themselves (ScraperAPI, Bright
  Data, proxies — anything their pipeline needs). Credentials are
  **client-owned**.
- Keys are **baked into the per-client plugin build**. This is intentional
  delivery, NOT a defect. Do not "fix" it by moving client keys into shared
  config or platform vaults without explicit instruction.
- **Key rotation lifecycle**: client renews/upgrades their subscription →
  SentraZero ships an updated plugin build → redeploy. Renewal is client-side;
  redeploy is platform-side.
- All client costs are billed **cost-plus-margin inside the SaaS product**
  (usage metering → billing pass-through), not via manual invoices.

## Self-Serve Platform Clients

- Organizations with their own developers (e.g., research labs).
- They pay for the platform, write and run their own plugins.
- Procurement, keys, and maintenance are entirely their responsibility.

## Client Work Discipline

- Before any client run, verify the search/API path is healthy (quota, keys).
- After a run, compare against the client's reference output and root-cause
  every diff.
- **Never-silent-failure rule**: a search-path failure must alert and surface
  warnings; never emit a confident-looking wrong answer (e.g., "No duplicate
  found") when the search actually returned nothing.

---

# Technology Stack

Core Agent:
- Go 1.25+ (single binary, cross-platform)
- Go runtime with goroutine-based worker pool
- Native sandbox execution with resource limits

Backend:
- Supabase (PostgreSQL + Edge Functions)
- PostgreSQL with pgvector extension
- 30+ tables, 100+ functions, 20+ triggers
- Supabase Vault for encrypted secrets

Messaging / Coordination:
- SSE (Server-Sent Events) for real-time job streaming
- Redis (optional) for multi-agent Pub/Sub
- Lease-based job assignment with TTL

Storage:
- MinIO / S3-compatible
- GCS, Azure Blob
- Local filesystem
- Configurable per-org via encrypted credentials in Vault

Plugin System:
- Runtimes: Python, Node.js, Go, Rust, native binaries
- Ed25519 signing for plugin verification
- Resource sandboxing (memory, CPU, timeout)
- Warm runtime pools for <1s job startup
- GPU device access for GPU-requiring plugins
- Percentage-based gradual rollout per device

Database:
- PostgreSQL 15+ with pgvector
- RLS enabled (org-level isolation)
- 16-dim device profile vectors for smart scheduling
- 384-dim chunk embedding vectors

Infrastructure:
- No external cloud compute dependencies
- Self-hosted, data stays within org infrastructure
- Docker support for containerized execution
- macOS, Linux, Windows agents

---

# End-to-End Architecture (Detailed)

## Agent Lifecycle

```
Operator          Agent Binary                    Supabase Backend
   │                  │                                │
   ├─ --claim-code ──►│  1. /bootstrap endpoint        │
   │                  ├───────────────────────────────►│  Verify claim_secret
   │                  │◄───────────────────────────────┤  Return: device_id, access_token
   │                  │                                │          org_id, storage config
   │                  │  2. Save config ~/.sentra/     │
   │                  │                                │
   │                  │  3. /agent_stream (SSE)        │
   │                  ├───────────────────────────────►│  Real-time event channel
   │                  │◄───────────────────────────────┤  hello event
   │                  │                                │
   │                  │  4. Report system capabilities │
   │                  │  (CPU cores, RAM, GPU, OS,     │
   │                  │   runtimes, docker_available)  │
   │                  │                                │
   │                  │  5. /agent_health_policy       │
   │                  ├───────────────────────────────►│
   │                  │◄───────────────────────────────┤  {concurrency: N}
   │                  │                                │
   │                  │  6. Start worker pool (N)      │
   │                  │                                │
   │                  │  7. Loop: /assign_agent_job    │
   │                  │  ────────────────────►─────────┤
   │                  │◄───────────────────────────────┤  Job or null
   │                  │                                │
   │                  │  8. Execute job                │
   │                  │  (builtin handler or plugin)   │
   │                  │  ↻ heartbeat every ~30s        │
   │                  │                                │
   │                  │  9. /complete_job              │
   │                  ├───────────────────────────────►│  Report result
   │                  │◄───────────────────────────────┤  {success: true}
```

## Job Queue System

Job types:
- `scan_dataset` — builtin: scan dataset files, extract metadata (columns, row counts, file types)
- `process` — chunk-level plugin execution (three strategies: size_based, byte_range, file_per_chunk)
- `merge_dataset` — combine chunk outputs into final dataset
- `preprocess` — marker step, completes immediately to unblock pipeline

Status flow:
```
pending → assigned → running → completed
                                  → failed → [retry] → pending
                                              → dead_letter (max retries exhausted)
```

Key mechanics:
- Lease-based: agent acquires a lease (default TTL) when claiming a job
- Lease verified at both start and completion time
- Heartbeat extends lease, prevents duplicate processing
- GPU heartbeat fields: `gpu_memory_free`, `gpu_utilization` reported with each heartbeat
- Retry: exponential backoff, max 3 retries (configurable)
- Failure classification: `infra_error`, `dependency_error`, `user_code_error`, `timeout_error`, `memory_error`

## Chunk Strategies

When processing datasets, three strategies determine how work is split:

1. **size_based** — Split dataset files into fixed-size chunks. Default. Uses tree merge.
2. **byte_range** — Read a byte-range window of a single source file. Routes through plugin, writes output. Agents must have ≥10GB free disk to claim byte_range chunks. Uses concat merge.
3. **file_per_chunk** — Process a list of source files. Routes each through plugin. Uses concat merge.

Strategy is determined during pre-chunking:
- Empty file_manifest → `pre_chunk_dataset_smart` (size_based)
- Non-empty file_manifest for single large file → `pre_chunk_filebased` (byte_range)
- Non-empty file_manifest for multi-file collection → `pre_chunk_filebased` (file_per_chunk)

## Plugin System

Plugin lifecycle:
1. Developer writes plugin code + manifest.json
2. Upload via register_plugin edge function
3. Backend stores binary, computes SHA-256 hash
4. Backend signs hash with org's Ed25519 private key (stored in Vault)
5. Plugin record inserted with `trusted=true`
6. Org enables plugin (0-100% rollout per device)
7. Agent downloads plugin at job time
8. Agent verifies signature against public key
9. Agent selects runtime (Python/Node/native)
10. Agent executes in sandbox with resource limits
11. Output captured and uploaded

Signature verification happens twice:
- Server-side before sending plugin to agent
- Agent-side before executing

## Search-Source Stack & Key Policy

Search/API providers used by client pipelines:

- **ScraperAPI** — primary. Quota-capped (hard 403 at exhaustion). Already
  wired into `search_walmart()` / `search_via_scraperapi()`.
- **Bright Data** — fallback. Pay-per-success (no quota-exhaustion death
  mode); wired as a coded auto-failover, not a manual switch.
- **DataImpulse** — deprioritized (residential proxy scraping, ToS risk).
- **Walmart Affiliate API** — future long-term path (official, needs program
  approval).

The billing model is the real differentiator: quota-capped providers can die
silently mid-month; pay-per-success cannot exhaust, only cost more.

Key policy:
- Client-owned keys stay baked into per-client plugin builds (see Business
  Model).
- Never place client keys in shared code or shared config.
- Rotation = client renewal + platform redeploy of the plugin build.

Operational guards:
- Zero-candidate warning + proactive alert on any search-path failure.
- Per-execution credit budget/cap — one job must not burn a client's monthly
  quota mid-run.
- Check search API health/quota before client runs.

## Dataset Scanning

Rich media auto-detection:
- CSV/JSON/Parquet → schema detection, column extraction
- Images → EXIF metadata
- Video → ffprobe metadata
- PDF → pdfcpu extraction
- Audio → ID3 tags
- Archives (zip/tar) → listing
- Binary → magic byte detection

## Client Data Pipelines

Managed-client workstreams run client plugins against client reference
outputs (e.g., RISE OTB Walmart validation/baselining vs. client xlsx).

- Pipelines: `validation` (Walmart Scrape & Compare) and `baselining`
  (Walmart Duplicate Detection) re-runs, compared to the client's reference
  `Validation.xlsx` / `Baselining.xlsx`.
- Re-run discipline: verify search path before the run → run → compare vs
  reference → classify every diff (search-source failure / transient block /
  logic nuance) → re-run until acceptance criteria are met.
- Known root-cause classes:
  - Search-source quota exhaustion (403) → zero candidates → wrong
    "No duplicate found."
  - Transient anti-bot soft-blocks during bursts → retry with jitter.
  - Comparator version nuance (e.g., size+color vs client's color-only).

## Smart Scheduling (pgvector)

- Each device has a 16-dim profile vector (`devices.embedding`)
- Each chunk has a 16-dim matching vector (`batch_chunks.chunk_vector`)
- Job assignment uses vector similarity to route work to best-fit device
- GPU-capable devices get 1.5x weighting

## GPU-Aware Scheduling

- Devices report: `gpu_available`, `gpu_model`, `gpu_memory_total_gb`, `gpu_memory_free_gb`, `cuda_version`, `gpu_driver_version`, `gpu_capability_score`
- GPU-requiring plugins routed to GPU-capable agents
- Sandbox GPU device access for GPU plugins
- GPU metrics reported with each heartbeat

## Chunk Planning (TICKET-006)

`supabase/functions/_shared/chunk_planner.ts` decides chunk count at plan
time from **current state** (not history):

- Inputs: `fileCount`, `totalSizeGb`, `effectiveSlots` (live sum of free
  slots over devices with heartbeat < 120s), `minMemoryFreeGb`.
- Cost model: `T(C) = ceil(C/S) * ((N/C)*p + f)`.
- Constants: `perFileComputeSec = 0.19`, `chunkOverheadSec = 34` — inferred
  from a single measurement (exec `45ed03ac`), never auto-updated.

Instrumentation: migration `20260810000002` created view
`v_planner_prediction_accuracy` (joins planner predictions with actual job
durations). The view is LIVE but has **no consumer** — data is collecting,
the planner is NOT self-tuning.

Honest framing: today the planner is "capacity-aware adaptive chunking,
validated live." "Self-correcting from history" is future intent, only once a
feedback loop consumes the view.

---

# Default Operating Process (MANDATORY)

For EVERY request:

DO NOT immediately edit code.

Always perform:

1. Understand
- Inspect existing implementation
- Find related files
- Understand current flow
- Check Go agent, Supabase schema, and Edge Functions

2. Impact Analysis
Ask:

"If I change this, what else can break?"

Check:
- Agent binary (Go)
- Worker pool / job dispatch
- Database schema and RLS policies
- Edge Functions API
- Plugin system compatibility
- SSE event flow
- Storage backend interactions
- GPU scheduling
- Retry / dead-letter behavior
- Performance (chunk size, query speed)

3. Plan

Explain:
- Root cause / requirement
- Files affected (Go, SQL, TypeScript, config)
- Proposed changes
- Risks (production data, existing devices, active jobs)

4. Implement

Rules:
- Smallest safe change
- Match existing patterns
- Reuse existing code
- No unnecessary rewrites
- Maintain backward compatibility with running agents

5. Review

Before finishing check:
- Bugs introduced?
- Edge cases (device offline, job timeout, plugin crash)?
- Security impact (RLS bypass, unsigned plugin execution)?
- Performance impact (query N+1, index missing)?
- Existing users unaffected?
- Active jobs not disrupted?

---

# Internal Team Simulation

For every task mentally involve the correct specialists.

## Architecture Changes

Use:

Architect
+
Tech Lead
+
Impact Analyzer

Check:
- Agent architecture (Go binary, worker pool, runtime managers)
- Database schema (table design, RLS, pgvector indexing)
- Edge Function API design
- Security boundaries
- Future agent compatibility

---

## Backend / Database Changes

Use:

Backend Engineer
+
Database Engineer
+
Security Engineer

Check:
- PostgreSQL schema changes (migrations required)
- RLS policies maintained
- pgvector index usage
- Edge Function API contracts
- Supabase Vault for secrets
- Migration backwards compatibility
- Active device compatibility

---

## Agent Binary Changes (Go)

Use:

Backend Engineer
+
Performance Engineer
+
Security Engineer

Check:
- Go 1.25 patterns and idioms
- Worker pool and goroutine lifecycle
- Sandbox execution safety
- SSE reconnection logic
- Plugin executor correctness
- Resource leak prevention
- Cross-platform compatibility (macOS, Linux, Windows)

---

## Plugin System Changes

Use:

Security Engineer
+
Backend Engineer
+
QA Engineer

Check:
- Ed25519 signing and verification
- Sandbox resource limits
- Runtime type support
- Rollout percentage logic
- Plugin manifest validation
- Error propagation and classification

---

## Pipeline / Job Changes

Use:

Backend Engineer
+
Database Engineer
+
Impact Analyzer

Check:
- Job lifecycle and status transitions
- Lease-based assignment
- Retry and dead-letter logic
- Pipeline auto-advance triggers
- Chunk strategy handling
- Merge strategy (concat vs tree)

---

## Database Changes

Database changes are high risk.

Before modifying:

Check:
- Existing schema (30+ tables, relationships)
- RLS policies (org-level isolation)
- pgvector indexes
- Active triggers (20+)
- Existing data compatibility
- Migration strategy

Never:
- Drop production tables
- Disable RLS
- Remove constraints without reason
- Break existing Edge Function contracts

---

## Bug Fix Protocol

For every bug:

DO NOT guess.

Process:

1. Reproduce logically
2. Trace execution path (agent Go code → Edge Function → SQL)
3. Identify exact root cause
4. Fix minimum required code
5. Check ripple effects (other job types, other edge functions, other device states)

Never:
- Patch symptoms
- Rewrite entire modules
- Change unrelated files

---

# Go Agent Architecture Rules

This project uses a Go 1.25 binary as the core agent.

Key architecture:
- Single binary, no runtime dependencies
- Modular internal packages under `internal/`
- Worker pool dispatches jobs to goroutines
- Runtime managers (Python, Node) as subprocesses with sandboxing
- SSE client for real-time communication
- Plugin executor handles signed plugin verification and execution

Before modifying:

Check:
- `internal/` package boundaries
- Existing plugin handlers
- Worker pool concurrency model
- Heartbeat / health check patterns
- SSE reconnection and backoff

Avoid:
- Breaking the zero-config claim-code bootstrap
- Changing the `~/.sentra/` config layout
- Adding external dependencies without justification

---

# Supabase Rules

Protect database integrity.

Always consider:
- RLS policies (every table must be org-scoped)
- pgvector index performance for device/chunk matching
- Edge Function idempotency (especially for job completion)
- Trigger side effects (auto_progress, scan triggers, pipeline advance)
- Vault secret access patterns
- Query performance for job assignment (most hot path)

Firebase UID is NOT used. Authentication is device-based via access tokens.

Identity chain:
```
Claim Code
    ↓
Device Registration (/bootstrap)
    ↓
Access Token (stored in ~/.sentra/)
    ↓
API Authentication (Bearer token verified by Edge Function)
    ↓
org_id from device record → RLS policy scope
```

Do not break:
```
Claim Code → Device → Access Token → Job Assignment → Execution
```

---

# Infrastructure / Operations Thinking

Every change affects running agents.

Consider:
- Will existing devices need to update?
- Are active jobs disrupted?
- Is there a rollback path?
- What happens during network partition (SSE disconnect)?
- Do storage credentials change?
- Are plugin signatures invalidated?

Do not introduce changes that require mass agent redeployment without a migration plan.

---

# Decision Logging

For important architecture decisions:

Document:

Decision:
Why:
Tradeoffs:
Future impact:

---

# Core Principle

Think systems, not files.

A one-line change can affect:
- agent bootstrap
- job assignment
- plugin execution
- pipeline completion
- production data

Always understand the chain reaction before editing.
