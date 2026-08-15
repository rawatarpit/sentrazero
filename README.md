# SentraZero — Self-Hosted Pipeline Orchestration

[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org)
[![OS](https://img.shields.io/badge/OS-Linux%20%7C%20macOS%20%7C%20Windows-blue?style=flat)]()
[![Backend](https://img.shields.io/badge/Backend-Supabase%20(Postgres%2017)-3ECF8E?style=flat)]()
[![License](https://img.shields.io/badge/License-Proprietary-red?style=flat)]()

> SentraZero is a self-hosted pipeline orchestration platform. A single static Go binary (`sentra-agent`, ~13–14 MB on Linux/macOS, ~8.8 MB on Windows) runs on your hardware — bare metal, VM, Raspberry Pi, Kubernetes — claims work from a Supabase-based control plane, executes your plugins inside a platform-native sandbox, and never lets your data leave your infrastructure. The control plane only sees metadata: job state, device health, and pipeline progress.

---

## What is SentraZero?

**SentraZero is self-hosted pipeline orchestration for teams who can't send their data to a third party to process it.**

The repo contains **two halves**:

1. **The agent** (`./cmd`, `./internal`) — a single statically-linked Go binary with no CGO, no Docker requirement, and no runtime engines to install. It embeds platform-native sandboxing (namespaces/cgroups on Linux, Seatbelt on macOS, Job Objects on Windows), an Ed25519 plugin-verification path, GPU-aware job scheduling, adaptive polling, and rich-media auto-detection (CSV, JSON, Parquet, PDF, SQLite, images, video, audio, archives, ELF).
2. **The backend** (`./supabase`) — the control plane: a Supabase project (Postgres 17) with **47 migrations** (schema, RLS, functions, triggers) and **64 Edge Functions** that handle orgs, devices, job assignment, pipeline orchestration, chunk planning, storage configuration, plugins, and cron maintenance.

Your **data and your plugins** run on your own hardware. The control plane schedules work and tracks metadata only.

### Why it exists

| Pillar | What's true | How it's verified |
|--------|-------------|-------------------|
| **1. Verified data sovereignty** | Zero rows of client data touch the control plane database | Direct queries against production tables — `vector_store`, `step_outputs`, `plugin_execution_history` are empty after real runs |
| **2. Orchestration efficiency** | Compound-mode execution cuts multi-step pipelines from per-step scheduling to a single job | Measured before/after: a 3-step pipeline went from ~35 min to under 30 s (scheduling overhead, not compute) |
| **3. Rich media by default** | One platform for any data shape — rows, images, video, PDF, audio — auto-detected from content headers | Built-in scan extractors (CSV, JSON, Parquet, PDF, SQLite, image EXIF, video, audio, archives, ELF) |
| **4. Sandboxed, signed, auditable** | Every plugin runs in a resource-limited sandbox with Ed25519 signature verification before execution | Per-platform sandboxing (see the table below) + per-execution signature re-verification |

---

## Architecture

```
                        ┌──────────────────────────────────────┐
                        │    Control Plane (Supabase backend)  │
                        │  orgs · devices · jobs · pipelines   │
                        │  Postgres 17 + RLS + Edge Functions  │
                        └───────────────┬──────────────────────┘
                                        │ HTTPS / SSE
                     ┌──────────────────┼──────────────────┐
                     ▼                  ▼                  ▼
             ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
             │   Agent      │  │   Agent      │  │   Agent      │
             │  (Go binary) │  │  (Go binary) │  │  (Go binary) │
             │  on your HW  │  │  on your HW  │  │  on your HW  │
             └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
                    │                  │                  │
                    └──────────────────┼──────────────────┘
                                       ▼
                         ┌─────────────────────────────┐
                         │   Your storage (S3 / mount) │
                         │   raw data · plugin outputs │
                         └─────────────────────────────┘
```

### How work flows

1. **Register an agent** with a claim code issued by your control plane operator. The agent exchanges the claim code for a device identity + HMAC token via the `claim_device` edge function (device creation happens server-side via `register_device`), cached locally in `~/.sentra/`.
2. **Upload a dataset** (rows or files) to your own storage and register it with the control plane.
3. **Define a pipeline** — ordered steps, each a built-in handler or a signed plugin.
4. **The control plane plans chunks and assigns jobs** to the best-fit device (lease-based, GPU-aware, benchmark-driven).
5. **Plugins execute on your hardware**, inside a sandbox, with your data.
6. **Results stay on your storage**; the control plane only sees job state and metadata. Scanned chunk outputs are merged back on your storage.

---

## Agent Runtime Lifecycle

The agent (`./cmd/main.go`) starts in a deterministic order:

1. **Load config** from `.env` / environment (backend URL, anon key, org, claim code).
2. **Identity** — load `~/.sentra` identity + token; if missing or invalid, run the claim-code flow. Invalid tokens are cleared and the device is re-claimed automatically.
3. **Bootstrap** — call the `bootstrap` edge function for the environment configuration. A local cache (`~/.sentra/redis_cache.json`) may carry Redis credentials when the control plane provides them.
4. **Health check server** — `/health`, `/ready`, `/live` on port `8080` for platform deployments.
5. **Plugin signing keys** — fetch org Ed25519 public keys from the control plane (cached, TTL 60 min, reloaded in the background).
6. **Plugin auto-update** — background ticker syncs plugin versions from the API.
7. **Startup validation + reconcile** — validates the environment (backend reachable and authenticated, plugin directory available), reconciles with the backend, restores in-flight state.
8. **Plugin sync + verify** — downloads plugins from the API, verifies every cached plugin signature and checksum (fail-closed), registers plugin IDs.
9. **Storage backend init** — fetches storage config (`shared_mount` or S3), builds the storage client.
10. **Realtime reconcile** — syncs device state with the control plane.
11. **Worker pool** — starts `MAX_CONCURRENCY` workers (default `runtime.NumCPU()/2`).
12. **Dispatch assigned jobs** — picks up any jobs already assigned to this device.
13. **Heartbeat + polling** — starts the heartbeat loop and the job-acquisition poller.

On shutdown, the agent pauses job processing, waits for in-flight jobs (30 s grace by default), reports failures for jobs that couldn't finish, releases merge locks, and stops the worker pool.

### Job delivery — adaptive polling + event-driven

- **Adaptive polling** — the agent polls the unified `poll_state` edge function with exponential backoff and jitter (min 5 s → max 15 min). Active agents (with running jobs or queue pressure) poll aggressively (~20 s); idle agents back off to the ceiling. Each poll refreshes device heartbeat and metrics.
- **Event-driven wakeups** — when Redis is configured, the agent subscribes to the `sentra:newjob:{org_id}` pub/sub channel (`internal/realtime/supabase_realtime.go`) and wakes up immediately on new jobs, with a self-adaptive safety ticker. Redis is optional: a stock deployment (where the `bootstrap` function returns no Redis credentials) runs in adaptive-polling mode.
- **SSE stream** — `agent_stream` provides realtime push for assigned jobs, with `sentJobIds` dedup.
- **Dedup** — three layers prevent double-dispatch:
  - **Realtime layer** (`internal/realtime/supabase_realtime.go`): when Redis is configured, `MarkJobProcessed` (SETNX `sentra:dedup:<device>:<job>`, **10-min TTL**) rejects jobs already seen; otherwise an in-memory `sentJobs` map is used.
  - **Dispatcher layer** (`internal/dispatcher/worker_pool.go`): in-process maps reject a `job_id` that is already running, and allow only one active job per `execution_step_id`.
  - **Persistent layer** (`internal/dispatcher/job_dedup.go`): a file-backed store (`~/.sentra/job_dedup.json`, **60-min TTL**, max 1000 entries, `SENTRA_DEDUP_MAX_JOBS`) is now wired into the dispatcher and rejects duplicate `job_id`s across agent restarts.

### Heartbeat

- The agent reports device health on an adaptive cadence (**15 min idle / 2 min active**, `SENTRA_HEARTBEAT_INTERVAL` is the legacy alias for the idle interval, `SENTRA_HEARTBEAT_ACTIVE_INTERVAL` for the active one) via `agent_health_policy`.
- Per-job execution heartbeats (`reporter`) keep `progress_percent` and `checkpoint_data` fresh (60 s default, `SENTRA_JOB_HEARTBEAT_INTERVAL`).
- The backend policy can remotely adjust `max_concurrency` based on device metrics.

---

## Job Lifecycle

```
pending → assigned → running → completed → advance_pipeline → next step | merge | done
                          └──→ failed → re-queue (failed_agent_ids chain) | dead_letter
```

- **pending → assigned** — `claim_jobs_for_device` RPC with lease-based assignment (30 min lease by default, ~6 min claim-steal window so a slow device's jobs can be re-assigned; a job whose pre-assigned owner is offline is also eligible).
- **assigned → running** — `start_job` verifies the job's `org_id` and `agent_id` match the authenticated device (IDOR-safe), then starts execution.
- **running → completed/failed** — `complete_job` is lease-aware and idempotent; `report_job_error` sanitizes error messages (paths, buckets, emails redacted) before storing.
- **Failure handling** — failed jobs are re-queued to other devices via the `failed_agent_ids` chain (the failed device is excluded); jobs that exhaust retries go to `dead_letter`. A cron `cleanup_stuck_jobs` sweeps stale leases.
- **Gating** — GPU-required jobs (`requires_gpu=true`) are only assigned to devices that report a GPU; `byte_range` chunks are only assigned to devices with sufficient free disk.
- **Telemetry** — execution events are buffered and flushed in batches (5 events or every 2 s) to `relay_job_event`; non-fatal if dropped.

### Job types

The DB constraint `agent_jobs_job_type_check` allows 13 types; the dispatcher routes on a subset of them:

| Job type | Purpose |
|----------|---------|
| `process` | Chunk-level processing: reads `chunk_<i>.bin`, runs a plugin/native handler, writes `chunk_<i>.out` |
| `process_dataset` | Step-level coordination marker for a pipeline step over a whole dataset |
| `scan_dataset` | Built-in scan: extracts metadata from files (CSV, JSON, Parquet, PDF, images, video, audio, archives, ELF) and reports it |
| `merge_dataset` | Merges processed chunks back into a single output (affinity/shared-mount strategies) |
| `merge` | Backend-side merge job |
| `plan` | Chunk planning job |
| `preprocess` | Marker job, completed immediately |
| `http` / `notification` | Control-plane side jobs (http queue, notifications) |
| `validate`, `export`, `import`, `plan_chunks`, `assign_job`, `scan` | Allowed by the schema; no agent-side handler — reserved/backend-side |

### Execution modes

For jobs that need an embedding model, the agent deterministically picks a mode based on device metrics and job characteristics (`internal/dispatcher/choose_mode.go`):

| Mode | When |
|------|------|
| `small` | Hash-based, no ML — jobs that don't require a model, or tiny jobs on constrained devices |
| `fast` | CPU-optimized embedding model (default) |
| `gguf` | Quantized model for high-CPU/high-memory devices (≥0.75 free CPU, ≥12 GB free memory) with complex or large jobs |
| `onnx` | GPU-accelerated ONNX model when the device reports a GPU |

---

## Pipeline Orchestration (backend)

- **`advance_pipeline`** — the core orchestrator: validates step completion, re-queues failed chunk jobs, calls `plan_dataset_chunks` for the next step, publishes Redis wakeups when available, and advances/merges at the end.
- **Chunk strategies** — `size_based` (default), `file_per_chunk`, `byte_range`, `row_range`, planned by `plan_dataset_chunks`; optimal chunk size is calculated from device benchmark scores.
- **Compound executions** — a multi-step pipeline can be packed into a single compound chunk job with embedded steps (`full_executions`), cutting per-step scheduling overhead dramatically.
- **Pre-assignment** — chunk jobs can be pre-assigned to a preferred device up front, avoiding a claim round-trip.
- **Merge** — `dataset_merge_locks` (unique active lock) prevents double-merges; strategies `auto` / `sequential` / `tree`; affinity device preferred, otherwise the most capable `merge_capable` device.
- **GPU scheduling** — device capability vectors + benchmarks route GPU-required jobs to GPU-capable agents (`match_best_device`, `auto_assign_best_device`).

---

## Plugin SDK

Plugins extend the agent. They can be written in **Python, Node.js, Ruby, or Bash/Shell** (managed runtimes with venv/npm for Python/Node; Bash runs on the system shell) or any language that can run on the host and read/write the chunk files.

- **Manifest** — a JSON manifest declares the executable, checksum, runtime, resource limits, and network need. Only plugins with `trusted: true` execute (fail-closed).
- **Signing** — plugins are signed with **Ed25519** on upload; the agent fetches the org's public keys (via `get_plugin_signing_key` / `list_plugin_signing_keys`) and re-verifies the signature before every execution.
- **Integrity** — a SHA-256 checksum is verified against the manifest; plugins are stored under `~/.sentra/plugins/` with strict permissions, and bundled plugins ship in `bundled/plugins` next to the binary.
- **Trust** — only plugins marked `trusted` are executed (fail-closed).
- **Sandboxing** — resource limits (memory, CPU time, timeout) enforced per platform (see the sandbox table below).
- **GPU routing** — plugins that declare `requires_gpu: true` are routed to GPU-capable agents.
- **Auto-update** — the agent syncs plugin versions from the API on an interval and re-verifies them.

The manifest schema (from `internal/plugin/manifest.go`):

| Field | Required | Notes |
|-------|----------|-------|
| `name` | yes | Plugin name |
| `version` | | e.g. `1.0.0` |
| `filename` | **yes** | Executable/entry file |
| `url` | | Download URL for the binary |
| `checksum` | **yes** | SHA-256 hex of the binary |
| `plugin_type` | | `core` (built-in) or `client` |
| `language` | | e.g. `python`, `rust` |
| `trusted` | yes | Must be `true` to execute (fail-closed) |
| `network` | | Default `false` (explicit opt-in) |
| `resources` | yes | `memory_mb`, `cpu_seconds`, `timeout_seconds`, optional `requires_gpu` / `gpu_memory_mb` |
| `signature` | yes | Base64-encoded Ed25519 signature |
| `signature_key_id` | yes | Key identifier for verification |
| `signature_verified` | | Set to `true` after successful verification |
| `dependencies` | | Declared runtime dependencies (name/version/source) |

```json
{
  "name": "example-plugin",
  "version": "1.0.0",
  "filename": "main.py",
  "plugin_type": "client",
  "language": "python",
  "trusted": true,
  "network": false,
  "resources": {
    "memory_mb": 512,
    "cpu_seconds": 60,
    "timeout_seconds": 300,
    "requires_gpu": false
  },
  "signature": "<base64-ed25519-signature>",
  "signature_key_id": "<org-key-id>",
  "dependencies": [
    { "name": "pandas", "version": ">=2.0" }
  ]
}
```

### Runtimes (v2 executor)

The v2 executor (`cmd/agent/executor/v2`) manages cached runtime environments:

- **Python** — creates a venv (`--system-site-packages --without-pip`), installs declared dependencies (with version-spec normalization and retries), and runs the plugin entry via `runner.py`.
- **Node.js** — `npm install` + `runner.mjs`.
- **Bash/Shell** — executed directly via the system `bash` (no venv/toolchain); this is the runtime used by the CI smoke suite because it works on every Tier-1 platform out of the box.
- Environments are pooled and cached by `org:runtime:version:os:dependency-hash`, with an eviction loop and disk limits.
- Execution policy supports configurable max retries, backoff, default and hard timeouts. Errors are classified (`system` / `infra` / `plugin` / `unknown`) for correct retry decisions.
- Native (compiled) plugin execution is **disabled** in the release build: `"native plugin execution disabled (not built with CGO)"`.

There is also a lightweight developer CLI at `./cmd/sentra` (`run`, `debug`, `replay`, `version`) — **this is not the agent** and is intentionally not built by the release pipeline.

---

## Cross-Platform Sandboxing

The agent is a true cross-platform binary — compile once per platform, run anywhere. Each OS gets platform-native sandboxing, not a least-common-denominator approach:

| Feature | Linux | macOS | Windows |
|---------|-------|-------|---------|
| **Syscall filter** | Seccomp BPF allowlist (213 syscalls, default-deny) — **enforced by default on x86_64** via agent re-exec (`agent --seccomp-exec <binary>`); disable with `SANDBOX_SECCOMP_PROFILE=off`. The list is audited for the Python/Node/Go/Rust runtimes. **ARM64 (e.g. Raspberry Pi) automatically falls back to NO_NEW_PRIVS-only** — the x86_64 syscall numbers do not apply to ARM64 | — | — |
| **Namespace isolation** | User/PID/Mount/UTS/IPC, plus Network when the manifest disables network | — | — |
| **Process isolation** | Namespace clone (`CLONE_NEW*`) with user-namespace UID/GID mapping, plus `PR_SET_NO_NEW_PRIVS` (set in the re-exec entry, `SANDBOX_NO_NEW_PRIVS=0` to disable) | Seatbelt (Apple sandbox, deny-default profile) | Job Objects |
| **Network isolation** | `CLONE_NEWNET` (loopback-only, no external connectivity) when `network=false`; host network shared when `network=true` | Seatbelt `(deny network*)` | Per-job Windows Firewall outbound-block rule (best-effort, requires admin) when `network=false` |
| **Memory limit** | cgroup v2 `memory.max` (falls back to rlimit) | rlimit | Job Object memory limit |
| **CPU limit** | cgroup v2 `cpu.max` bandwidth cap (manifest `cpu_limit` percent, default 80, `SANDBOX_DEFAULT_CPU_PERCENT`) + rlimit CPU time (`CPUSeconds` via `ulimit -t`) | rlimit | Job Object CPU quota |
| **GPU access** | Device node passthrough (`device.allow`) + NVIDIA env vars | — | — |
| **Privilege model** | User namespace — uid 0 inside maps to the launching user; no host root; NO_NEW_PRIVS blocks setuid escalation | — | — |
| **Ed25519 plugin verification** | Yes | Yes | Yes |

Linux has the most comprehensive sandbox (namespaces + seccomp + cgroups + rlimits). macOS uses Seatbelt default-deny profiles. Windows uses Job Objects plus a per-job firewall block; it has **no syscall filter** — treat it as containment, not confinement.

### Platform support tiers

The release pipeline (`scripts/build.sh`) builds **10 targets** and CI verifies every tier on every PR:

| Tier | Targets | Verification |
|------|---------|--------------|
| **Tier 1 — native-verified** | `linux/amd64`, `linux/arm64` (Raspberry Pi), `darwin/amd64`, `darwin/arm64`, `windows/amd64`, `windows/arm64` | Built **and smoke-tested on the real OS** in a 6-OS matrix (Ubuntu 24.04 x64/arm64, macOS Intel + Apple Silicon, Windows x64 + arm64) |
| **Tier 2 — cross-compiled** | `linux/386`, `linux/riscv64`, `linux/ppc64le`, `linux/s390x` | Cross-compiled (`CGO_ENABLED=0`), `go build` + `go vet` per PR; cannot run in CI |
| **Tier 3 — denied by default** | FreeBSD, OpenBSD, NetBSD, Solaris, illumos, AIX, Plan 9, `js/wasm` | Not shipped as releases; plugin execution is **refused** unless explicitly `SANDBOX_MODE=off` (fail-closed) |

Sandbox depth follows the tier:

- **Tier 1** gets the full platform-native table above — seccomp allowlist on x86_64, NO_NEW_PRIVS + cgroup fallback on linux/arm64 (the x86_64 syscall numbers don't apply to other arches), Seatbelt on macOS, Job Objects + firewall on Windows.
- **Tier 2** falls back to NO_NEW_PRIVS + cgroup (no seccomp).
- **Tier 3** refuses to run plugins at all by default — the sandboxer fails closed (`internal/sandbox/sandboxer_stub.go`), with `SANDBOX_MODE=off` as the explicit escape hatch.

Verification history: the 6-OS matrix was made fully green with native smoke tests passing on every Tier-1 platform, including `plugin-e2e`, sandbox resource-limit checks, and the network-block test on each OS.

---

## Storage

The agent supports two storage backends, chosen per org from the control plane (`org_storage_configs`):

| Mode | Description |
|------|-------------|
| `shared_mount` | Local/shared filesystem mount (default `SENTRA_MOUNT_PATH` or `/data/sentra`); datasets under `<mount>/datasets/<dataset_id>/` |
| `s3` | S3-compatible object storage (AWS S3, Supabase Storage, R2, MinIO) via a built-in S3 HTTP client with manual AWS SigV4 signing (`internal/storage/s3http.go`); credentials resolved from config or decrypted from Supabase Vault |

> GCS and Azure Blob modes are recognized in configuration but are **not yet implemented** in the agent (`internal/storage/config.go`).

Object paths are **derived locally, never stored** in job payloads: `chunks/chunk_<i>.bin` (input), `chunks/chunk_<i>.out` (output), `<dataset>_merged.csv` (merged result). Datasets may be slugified, and the agent resolves real source paths from the dataset record.

---

## Backend — Edge Functions

The control plane exposes **64 Edge Functions** under `supabase/functions/`, grouped by responsibility. The agent talks to a subset of these; the rest are internal (relay/cron), dashboard-facing (user JWT), or admin/debug helpers.

### Auth & device onboarding

| Function | Purpose |
|----------|---------|
| `bootstrap` | Returns device config (`backend_url`, `anon_key`, `environment`); deliberately exposes **no Redis credentials**; rate-limited |
| `claim_device` | Exchanges a claim code for a device identity + HMAC token (RPC `validate_claim_secret`) |
| `register_device` | Server-side device creation from a claim code (Bearer claim code, `hashClaimCode`) |
| `create-user` | Generates 6-char claim codes (`SENTRA2026`-style), rate-limited 5/min/IP |
| `setup_claims` | Admin helper — creates claim codes for a fixed org |
| `verifyAgentToken` | Token verification helper for other functions (HMAC device tokens, bounded cache) |
| `compute_hash` | Dev tool — HMAC-SHA256 of a token with `SENTRA_HMAC_KEY` |

### Job claiming, assignment & polling

| Function | Purpose |
|----------|---------|
| `poll_state` | **The adaptive poller** — refreshes device heartbeat + metrics, claims jobs via RPC `claim_jobs_for_device` (limit 10, lease 30 min, ~6 min claim-steal); supports `claim_jobs=false` liveness pings |
| `claim_jobs_for_device` | RPC wrapper normalizing claimed jobs into agent payloads |
| `batch_assign_jobs` | RPC `batch_assign_jobs_atomic` — assign up to N jobs in one transaction |
| `assign_agent_job` | RPC `assign_agent_job` — assign a single job |
| `auto_assign_best_device` | Internal — RPC `match_best_device` (GPU bonus/gating, device vectors) |
| `verify_job_lease` | RPC lease check before starting a job |
| `get_assigned_jobs` | Returns jobs already assigned/running on a device (startup restore) |
| `agent_stream` | SSE endpoint pushing realtime job events with `sentJobIds` dedup |
| `reconcile_agent` | Compare-and-swap on `devices.last_refresh` to prevent double-reconciliation |
| `notify_available_device` | Metrics → derived device status (`cpu>90`/`mem<20%` → busy, else available) |
| `agent_health_policy` | L2 metrics dedup; computes desired `max_concurrency` for the backend policy |

### Pipeline orchestration

| Function | Purpose |
|----------|---------|
| `run_pipeline` | User-facing entrypoint — requires `dataset_id` + `pipeline_template_id`, org membership |
| `advance_pipeline` | **Core orchestrator** — validates step completion, re-queues failed chunk jobs, calls `plan_dataset_chunks` for the next step, publishes Redis wakeups when available, advances/merges at the end |
| `plan_dataset_chunks` | Plans chunks for a step (`size_based` default, `file_per_chunk`, compound multi-step packing), publishes Redis wakeup |
| `approve_dataset_and_plan_chunks` | Approves a scanned dataset and plans default-step chunks |
| `complete_job` | Device-facing job completion — lease-aware, idempotent; triggers `advance_pipeline` |
| `start_job` | Device-facing start — IDOR-safe: verifies job `org_id`/`agent_id` match the authenticated device |
| `report_job_error` | Device-facing failure — sanitizes the error message, optional `force_fail` |
| `relay_job_event` | Batched telemetry relay (progress, plugin execution) with event sanitization |
| `reset_pipeline` | Internal — resets execution/jobs/chunks for a pipeline |
| `force_complete` | User-facing — finds stuck jobs and forces pipeline advancement |
| `get_dataset_output` | User-facing — merged output details for a dataset |

### Chunking & scanning

| Function | Purpose |
|----------|---------|
| `calculate_optimal_chunk_size` | Benchmark avg → efficiency/chunk size |
| `pre_chunk_dataset` | RPC `pre_chunk_dataset_smart` |
| `report_dataset_scan` | Marks a dataset scanned (`scanned_at`, `scan_completed`, `file_count`, `file_type`) |
| `complete_dataset_scan` | Service-role — completes scan + plans chunks |
| `record_dataset_metadata` | Device-facing — updates dataset metrics (org access check) |
| `record_benchmark` | Device-facing — inserts `device_benchmarks` |
| `schedule_merge_job` | Picks affinity device, else best `merge_capable` device; creates the merge job with merge-lock protection |
| `fix_scan_job` | Admin — re-triggers a scan job for a hardcoded dataset |

### Storage

| Function | Purpose |
|----------|---------|
| `storage_manage` | S3/R2/GCS via AWS SDK; presigned URLs + multipart upload |
| `get_storage_config` | Device-facing — returns org storage config by id or default |
| `store_storage_credentials` | Stores `org_storage_configs` (CORS-restricted to the dashboard) |
| `upload_complete` | Device-facing — marks a chunk uploaded; verifies org ownership |
| `setup_dataset_shared` | Admin — sets shared-mount storage for a dataset |

### Plugins & vault

| Function | Purpose |
|----------|---------|
| `register_plugin` | Admin — registers plugin metadata + binary to the `plugins` bucket |
| `upload_plugin_file` | Admin — multipart upload to `plugins/org/{org_id}/{pluginId}/...`; SHA-256 checksum |
| `get_plugin` | Public — single plugin row |
| `list_all_plugins` | Built-ins + org plugins (auth optional) |
| `list_plugins_for_org` | Built-in trusted + org plugins |
| `set_plugin_trust` | User-facing — toggles `plugins.trusted` |
| `get_plugin_signing_key` | Device-facing — public key by `key_id` + org (not revoked) |
| `list_plugin_signing_keys` | Device-facing — all non-revoked org keys |
| `decrypt_vault_secret` | RPC — decrypts a named secret from Supabase Vault |

### Cron, maintenance & admin

| Function | Purpose |
|----------|---------|
| `get_agent_version` | Returns the latest `agent_releases` entry (version, download URL, checksum) for agent self-update |
| `dispatch_http_jobs` | Cron — drains `http_queue` with retries (max 5), idempotency keys, concurrency 10 |
| `cleanup_stuck_jobs` | Cron — RPC `cleanup_stuck_jobs(3, org)` sweeps stale leases |
| `cleanup_job_notification_queue` | Cron — prunes old job notifications (24 h) |
| `verify_triggers` | Internal — runs `get_triggers_report` to verify trigger health |
| `admin_reset` | Admin debug — hard reset of org state |
| `check_data` / `check_execution` | Admin debug — inspect org data / execution state |
| `clear_dedup` | Admin debug — clears Redis dedup keys |
| `reset_dataset` | Admin debug — resets a dataset to registered |
| `invite_member` | User-facing — org member invites (admin role, rate-limited) |
| `delete_org` | User-facing — org deletion with `confirm_delete: true` |
| `copy_templates` | Admin — copies `pipeline_templates` between orgs |

> `_shared/` contains shared helpers (CORS, auth, Redis, error sanitization). `scrape.py`/`compare.py` and their `json` manifests are agent-side script scaffolds, **not** Deno edge functions.

---

## Quick Start

### 1. Download the agent

| Platform | Download |
|----------|----------|
| Linux amd64 | [sentra-agent-linux-amd64](https://github.com/rawatarpit/sentrazero/releases/download/v1.1.1/sentra-agent-linux-amd64) |
| Linux arm64 | [sentra-agent-linux-arm64](https://github.com/rawatarpit/sentrazero/releases/download/v1.1.1/sentra-agent-linux-arm64) |
| macOS amd64 | [sentra-agent-darwin-amd64](https://github.com/rawatarpit/sentrazero/releases/download/v1.1.1/sentra-agent-darwin-amd64) |
| macOS arm64 | [sentra-agent-darwin-arm64](https://github.com/rawatarpit/sentrazero/releases/download/v1.1.1/sentra-agent-darwin-arm64) |
| Windows amd64 | [sentra-agent-windows-amd64.exe](https://github.com/rawatarpit/sentrazero/releases/download/v1.1.1/sentra-agent-windows-amd64.exe) |

Windows **arm64** is built and CI-verified on native runners but not yet published as a release asset.

Verify the checksum before running (SHA-256SUMS are generated into `dist/` by the release pipeline and published with each release):

```bash
curl -LO https://github.com/rawatarpit/sentrazero/releases/download/v1.1.1/sentra-agent-linux-amd64
shasum -a 256 sentra-agent-linux-amd64   # compare against the release SHA256SUMS
```

### 2. Run with a claim code

```bash
chmod +x sentra-agent-linux-amd64
./sentra-agent-linux-amd64 --claim-code <CLAIM_CODE>
```

Or via environment variable:

```bash
export CLAIM_CODE=<claim_code>
./sentra-agent-linux-amd64
```

The agent registers itself, receives configuration, and starts processing jobs. Claim codes are issued by your SentraZero control plane operator.

---

## Building from Source

Requires **Go 1.25+**. The canonical build is `scripts/build.sh`, which cross-compiles all 10 targets into `dist/`, verifies each binary (embedded entrypoint must be `sentra-agent/cmd`, never the CLI), and generates `SHA256SUMS`.

```bash
git clone https://github.com/rawatarpit/sentrazero.git
cd sentrazero
make build          # native build only, into bin/sentra-agent
make release        # all 10 targets → dist/ + download/ + SHA256SUMS
```

| Make target | Purpose |
|-------------|---------|
| `make build` | Build native agent (stripped) to `bin/sentra-agent` |
| `make release` | Run `scripts/build.sh` — all platforms, checksums, download sync |
| `make run` | Build + run the agent locally |
| `make verify` | Verify an existing binary is the real agent |
| `make clean` | Remove `bin/`, `dist/`, `download/` |
| `make info` | Show Go/OS/entrypoint info |

Release builds are static (`CGO_ENABLED=0`) and stripped (`-ldflags="-w -s"`). The build matrix: `linux/amd64`, `linux/arm64`, `linux/386`, `linux/riscv64`, `linux/ppc64le`, `linux/s390x`, `darwin/amd64`, `darwin/arm64`, `windows/amd64`, `windows/arm64` — see [Platform support tiers](#platform-support-tiers).

---

## Configuration

Copy `.env.example` → `.env`. The backend URL and anon key can also be auto-built from `supabase/.temp/project-ref`.

| Variable | Purpose | Default |
|----------|---------|---------|
| `BACKEND_URL` / `SENTRA_BACKEND_URL` / `SUPABASE_URL` | Control plane base URL (required) | auto from project ref |
| `BACKEND_ANON_KEY` / `SENTRA_BACKEND_ANON_KEY` / `SUPABASE_ANON_KEY` | Control plane anon key | auto from project ref |
| `CLAIM_CODE` | Device registration claim code (or `--claim-code` flag) | — |
| `ORG_ID` | Org assignment (auto-set on claim) | — |
| `ORG_NAME` | Org display name | `Sentra Org` |
| `AGENT_ENVIRONMENT_TYPE` | `local` / `production` | `local` |
| `AGENT_STORAGE_TYPE` | `local` / `object_storage` | `local` |
| `AGENT_NAME` | Device display name | hostname |
| `LOG_LEVEL` | `debug` / `info` / `warn` / `error` | `info` |
| `MAX_CONCURRENCY` | Worker pool size | `NumCPU()/2` |
| `HEALTH_CHECK_PORT` | Health server port | `8080` |

Advanced runtime knobs:

| Variable | Purpose | Default |
|----------|---------|---------|
| `SENTRA_KEY_CACHE_TTL` | Plugin signing-key cache TTL | 60 min |
| `SENTRA_KEY_RELOAD_INTERVAL` | Signing-key reload interval | 60 min |
| `SENTRA_PLUGIN_AUTO_UPDATE_INTERVAL` | Plugin auto-update interval | 60 min |
| `SENTRA_JOB_HEARTBEAT_INTERVAL` | Per-job heartbeat interval | 60 s |
| `SENTRA_HEARTBEAT_INTERVAL` | Idle device heartbeat interval (legacy alias) | 15 min |
| `SENTRA_HEARTBEAT_ACTIVE_INTERVAL` | Active device heartbeat interval | 2 min |
| `SENTRA_SHUTDOWN_GRACE_PERIOD` | Graceful drain timeout | 30 s |
| `SENTRA_SANDBOX_BASE` | Sandbox working directory root | — |
| `SENTRA_MOUNT_PATH` | Shared-mount storage root | `/data/sentra` |
| `SENTRA_ALLOW_LOCAL_EXEC` | `false` makes Docker mandatory; otherwise local execution is enabled with a warning when Docker is absent | enabled |
| `SENTRA_NATIVE_PLUGINS_REQUIRED` | `1` fails startup if native plugin execution is unavailable | — |
| `SENTRA_DEDUP_MAX_JOBS` | Max entries in the job-dedup store | 1000 |
| `SENTRA_USE_KEYRING` | Store the device token in the OS keyring instead of a file | false |
| `SENTRA_POLL_INTERVAL` / `SENTRA_MIN_POLL_INTERVAL` / `SENTRA_MAX_POLL_INTERVAL` / `SENTRA_POLL_BACKOFF_MULTIPLIER` / `SENTRA_POLL_JITTER` | Adaptive polling tuning | 5 s – 15 min |
| `SANDBOX_SECCOMP_PROFILE` | Linux seccomp allowlist filter: `default` (enforced) or `off` | `default` |
| `SANDBOX_NO_NEW_PRIVS` | Apply `PR_SET_NO_NEW_PRIVS` on Linux plugin processes | `true` |
| `SANDBOX_DEFAULT_CPU_PERCENT` | cgroup v2 `cpu.max` bandwidth cap when the manifest doesn't set `cpu_limit` | 80 |
| `SANDBOX_MODE` | Sandbox mode: `native` (Linux/macOS/Windows), `deny` (default on unsupported platforms — fail-closed), or `off` | `native` (auto) |

**Plugin environment.** Plugin processes run with the host environment by default. When a job enables strict environment mode (`environment_strict`), `SENTRA_API_KEY`, `SENTRA_SECRET`, and `AWS_`-prefixed variables are filtered from the plugin environment (`cmd/agent/sandbox`).

---

## Security & Privacy

**Agent side**
- **Verified sovereignty** — the control plane stores only metadata; verified by direct database queries.
- **Signed plugins** — Ed25519 signatures verified on the agent before every execution; SHA-256 integrity checks; `trusted` flag required (fail-closed).
- **Sandboxed execution** — resource limits and isolation enforced per platform (see the sandbox table above).
- **No data at rest in the control plane** — raw data and outputs live on your storage.
- **Local state** — device identity/config stored under `~/.sentra` with `0600`/`0700` permissions; optional OS keyring.

**Backend side**
- **Agent auth** — HMAC-SHA256 device tokens (`x-agent-token`), validated against `devices.status` (`online/available/busy`), with a bounded cache.
- **Internal/cron auth** — `x-relay-key` for internal relay functions, `x-cron-secret` (timing-safe compare) for cron jobs, user JWT + `verifyOrgMembership` for dashboards.
- **RLS** — **104** row-level-security policies enforce org isolation via `org_members` on every org-scoped table (verified by counting `CREATE POLICY` across all migrations).
- **IDOR checks** — `start_job`, `complete_job`, `upload_complete`, `report_job_*`, `record_dataset_metadata` all verify the authenticated device owns the job/dataset/org.
- **Sanitization** — `sanitizeErrorMessage` / `sanitizeEventData` redact filesystem paths, S3/GS buckets, IPs, and emails before persisting error/telemetry data.
- **Hardening** — migrations `20260722000002`/`20260722000003` restrict raw column access on `agent_jobs` and run `SECURITY DEFINER` functions with revoked privileges; storage credentials are encrypted and resolved via Supabase Vault (`decrypt_vault_secret`).
- **Schema note** — the authoritative schema is the production pg_dump (`supabase/migrations/20260602125622_remote_schema.sql`); the `20260611*`–`20260614*` migration files are intentionally empty placeholders.

This repo intentionally does **not** contain production control-plane credentials or secrets.

---

## Repository Map

### Agent (`./cmd`, `./internal`)

| Path | Purpose |
|------|---------|
| `cmd/main.go` | Agent entrypoint — startup/shutdown lifecycle |
| `cmd/agent/executor/v2` | Job executor: retries, error classification, runtime dispatch |
| `cmd/agent/runtime/v2` | Runtime manager: Python (venv) + Node (npm) environments, pooling, eviction |
| `cmd/agent/sandbox` | Resource-limited sandbox wrapper (strict environment filtering) |
| `cmd/sandbox-init` | Linux `pivot_root`/namespace init for sandboxed processes |
| `cmd/sandbox-test` | macOS Seatbelt test harness |
| `cmd/sentra` | Developer CLI (`run`/`debug`/`replay`) — not the agent |
| `internal/auth` | Claim flow, device identity, token store (file or keyring) |
| `internal/backend` | Control-plane client: poll_state, start/complete/verify_job_lease, relay batching, storage config |
| `internal/bootstrap` | Bootstrap edge-function fetch + local Redis-config cache |
| `internal/config` | Static config loading (`.env`, env chain, project-ref autodetect) |
| `internal/dataset` | File metadata extraction (CSV/JSON/Parquet/PDF/SQLite/archives/ELF), merge locks, recovery |
| `internal/dispatcher` | Worker pool, job routing, execution modes, native handlers (process/scan/merge), job dedup, merge locks, introspection |
| `internal/healthcheck` | `/health`, `/ready`, `/live` server |
| `internal/heartbeat` | Adaptive device heartbeat (15 min idle / 2 min active) |
| `internal/httpclient` | Shared HTTP client with backend auth headers |
| `internal/models` | Job/device/metrics data models |
| `internal/obs` | Structured logging + trace IDs |
| `internal/plugin` | Plugin lifecycle: sync from API, Ed25519 verify, manifest, signing-key cache, auto-update, bundled plugins |
| `internal/realtime` | Adaptive polling, Redis pub/sub wakeups (`sentra:newjob:{org}`), SSE agent stream, reconcile |
| `internal/redis` | Redis client (job dedup, pub/sub) |
| `internal/reporter` | Per-job execution heartbeats / progress reporting |
| `internal/sandbox` | Platform sandboxers: namespaces + seccomp + cgroups (Linux), Seatbelt (macOS), Job Objects + Firewall block (Windows), fail-closed deny mode on unsupported platforms |
| `internal/sanitize` | Error/event sanitization for telemetry |
| `internal/startup` | Startup validation + backend reconciliation |
| `internal/storage` | Storage backends: shared_mount + S3 (custom SigV4 HTTP client), vault-secret resolution, path derivation |
| `internal/sysinfo` | Platform detection (proc/sysctl/wmic), GPU via nvidia-smi, latency/IO heuristics |
| `internal/system` | System helpers |
| `internal/update` | Agent/plugin self-update support |

### Backend (`./supabase`)

| Path | Purpose |
|------|---------|
| `supabase/config.toml` | Supabase project config (Postgres 17, Edge Runtime, auth, secrets) |
| `supabase/migrations/` | 47 migrations — schema (agent_jobs, datasets, devices, executions, batch_chunks, plugins, org_storage_configs, …), RLS policies, functions/RPCs, triggers, indexes (pgvector), seed data, GPU scheduling, byte-range chunking, compound executions, multi-run datasets, adaptive polling, security hardening. Authoritative schema = `20260602125622_remote_schema.sql` |
| `supabase/functions/` | 64 Edge Functions: auth/onboarding, job claiming/polling (`poll_state`, `claim_jobs_for_device`, `agent_stream`), pipeline orchestration (`advance_pipeline`, `plan_dataset_chunks`, `run_pipeline`), storage (`storage_manage`, `get_storage_config`, `upload_complete`), plugins & vault, cron/maintenance (`dispatch_http_jobs`, `cleanup_stuck_jobs`), admin/debug — see [Backend — Edge Functions](#backend--edge-functions) |

---

## Performance Snapshot

Reported from live production agents (v1.0.0, x86_64 and ARM64). Reproducible; methodology documented in the release notes.

| Metric | Value |
|--------|-------|
| Agent idle RAM | **~15 MB** RSS |
| Binary size | **13–14 MB** (static, stripped; Windows ~8.8 MB) |
| Python plugin warm start | **~0.11 s** |
| Python plugin throughput | **~5.7 jobs/s** |
| Native subprocess start | **~1–2 ms** |
| Sandbox namespace creation | **~5–10 ms** |
| Control-plane RTT (same region) | **<2 ms** |
| Cold startup to operational | **<100 ms** |
| Idle poll backoff | **5 s – 15 min** (adaptive, jittered) |
| Job lease TTL / claim-steal window | **30 min / ~6 min** |
| Compound pipeline overhead (3 steps) | **~35 min → <30 s** (scheduling, not compute) |

**Honest limitations:**
- Sandboxing is strongest on Linux; macOS and Windows rely on OS-native mechanisms with no syscall filtering equivalent. Windows network isolation is a per-job Firewall rule and requires admin rights.
- The Linux seccomp allowlist (213 syscalls) is **enforced by default on x86_64**; it is audited for the supported runtimes but a plugin using an unusual syscall will be killed (`SIGSYS`) — tune via `SANDBOX_SECCOMP_PROFILE=off` if needed. **ARM64 builds do not enforce seccomp** (the x86_64 numbers don't apply); they get NO_NEW_PRIVS + cgroup limits, which is native-CI-verified on linux/arm64.
- On unsupported platforms (BSD, Solaris, AIX, Plan 9, wasm) the agent **denies plugin execution by default** rather than running un-sandboxed; `SANDBOX_MODE=off` is the explicit opt-out.
- Windows Job Object memory caps are best-effort: some hosts (e.g. GitHub-hosted runners) don't enforce them, so the smoke suite reports them as a warning rather than a failure.
- Measured throughput is per single agent; scale linearly by adding agents.
- These are platform numbers, not plugin business-logic results. Plugin accuracy/reliability varies per plugin.

---

## License

Proprietary. The agent binary is distributed via GitHub Releases for evaluation; the control plane and plugin SDK remain closed-source. Contact the maintainers for deployment and licensing.
