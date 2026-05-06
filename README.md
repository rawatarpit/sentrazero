# Sentra Agent - Distributed Compute Runtime

A decentralized compute network that transforms idle developer machines and servers into a distributed processing cluster. Instead of relying on expensive cloud infrastructure, organizations can leverage their existing compute resources to run data pipelines, ML inference, and batch processing workloads.

## Table of Contents
- [What is Sentra?](#what-is-sentra)
- [Core Value Proposition](#core-value-proposition)
- [Quick Start](#quick-start)
- [Zero-Config Architecture](#zero-config-architecture)
- [Database Schema](#database-schema)
- [Functions](#functions)
- [Triggers](#triggers)
- [Edge Functions](#edge-functions)
- [Go Codebase Structure](#go-codebase-structure)
- [Job Types & Lifecycle](#job-types--lifecycle)
- [Architecture](#architecture)
- [Components](#components)
- [Security](#security)
- [API Reference](#api-reference)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

---

## What is Sentra?

Sentra is a **decentralized compute network** that transforms idle developer machines and servers into a distributed processing cluster. Organizations can run:
- **Data pipeline jobs** (scanning, processing, merging datasets)
- **ML inference** (embedding generation, model inference)
- **Batch processing** (ETL, transformations, chunk-based operations)

---

## Core Value Proposition

| Feature | Benefit |
|---------|---------|
| **Zero-Config Setup** | Single binary, claim code activation - no env vars needed |
| **Auto-scaling** | Jobs automatically distributed to available devices |
| **Plugin-based** | Extend functionality via dynamically loaded plugins |
| **Warm pools** | Pre-warmed runtime environments for fast job startup |
| **Graceful shutdown** | Drain in-flight jobs before termination |
| **Redis-powered** | Multi-agent coordination with Redis Streams |
| **Vector Similarity** | Smart device selection using pgvector |

---

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

---

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
| `SENTRA_BACKEND_URL` | No | auto | Backend URL |
| `SENTRA_REDIS_URL` | No | - | Redis URL |
| `ENVIRONMENT_POOL_MAX_SIZE` | No | 10 | Max env pools |
| `ENVIRONMENT_MAX_COUNT` | No | 50 | Max environments |

---

## Database Schema

### Tables

#### `agent_jobs` - Main job queue table
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Job ID (PK), default gen_random_uuid() |
| `agent_id` | uuid | Assigned device ID |
| `job_type` | text | Job type: scan_dataset, process, preprocess, merge, plan_chunks |
| `payload` | jsonb | Job configuration and data |
| `status` | text | pending, assigned, running, completed, failed, dead |
| `completed` | boolean | Completion flag |
| `error` | text | Error message |
| `started_at` | timestamptz | Job start time |
| `finished_at` | timestamptz | Job finish time |
| `org_id` | uuid | Organization ID (NOT NULL) |
| `assigned_at` | timestamptz | Assignment time |
| `duration_ms` | double precision | Job duration in milliseconds |
| `throughput` | double precision | Job throughput |
| `output_token` | text | Job output token |
| `plugin_id` | text | Associated plugin ID |
| `plugin_version` | text | Plugin version |
| `updated_at` | timestamptz | Last update time |
| `retry_count` | integer | Retry count, default 0 |
| `lease_expires_at` | timestamptz | Lease expiration |
| `last_error` | text | Last error message |
| `dead_lettered` | boolean | Dead letter flag |
| `execution_id` | uuid | Pipeline execution ID |
| `execution_step_id` | uuid | Execution step ID |
| `max_retries` | integer | Max retries, default 3 |
| `job_dataset_id` | uuid | Generated from payload->dataset_id |
| `job_chunk_id` | uuid | Generated from payload->chunk_id |
| `last_transition_at` | timestamptz | Last state transition |
| `runtime_type` | text | python, node, native (default: python) |
| `runtime_dependencies` | jsonb | Runtime dependencies |
| `entrypoint` | text | Job entrypoint |
| `execution_mode` | text | native, docker, runtime |
| `environment_strict` | boolean | Strict environment mode |
| `execution_timeout_seconds` | integer | Timeout in seconds (default: 300) |
| `dependency_lock_hash` | text | Hash of dependencies |
| `idempotency_key` | text | Idempotency key |
| `checkpoint_id` | uuid | Checkpoint ID |
| `environment_id` | uuid | Runtime environment ID |
| `run_id` | uuid | Run ID for deduplication |
| `attempt_number` | integer | Attempt number (default: 1) |
| `output_data` | jsonb | Output data |
| `output_size_bytes` | bigint | Output size |
| `log_size_bytes` | bigint | Log size |
| `failure_classification` | text | infra_error, dependency_error, user_code_error, timeout_error, memory_error, unknown |
| `actual_execution_mode` | text | Actual mode used |
| `fallback_reason` | text | Fallback reason |
| `heartbeat_at` | timestamptz | Job heartbeat |

**Constraints:**
- `agent_jobs_assignment_valid`: (assigned_at IS NULL) OR (agent_id IS NOT NULL)
- `agent_jobs_execution_mode_check`: execution_mode IN ('docker', 'runtime', 'native')
- `agent_jobs_failure_classification_check`: failure_classification IN ('infra_error', 'dependency_error', 'user_code_error', 'timeout_error', 'memory_error', 'unknown')

#### `agent_jobs_archive` - Archived completed jobs
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Job ID (PK) |
| `agent_id` | uuid | Assigned device |
| `job_type` | text | Job type |
| `payload` | jsonb | Job payload |
| `created_at` | timestamptz | Creation time |
| `status` | text | Final status |
| `completed` | boolean | Completion flag |
| `error` | text | Error message |
| `started_at` | timestamptz | Start time |
| `finished_at` | timestamptz | Finish time |
| `org_id` | uuid | Organization |
| `assigned_at` | timestamptz | Assignment time |
| `duration_ms` | double precision | Duration |
| `throughput` | double precision | Throughput |
| `output_token` | text | Output |
| `plugin_id` | text | Plugin ID |
| `plugin_version` | text | Plugin version |
| `updated_at` | timestamptz | Update time |
| `retry_count` | integer | Retry count |
| `lease_expires_at` | timestamptz | Lease expiry |
| `last_error` | text | Last error |
| `dead_lettered` | boolean | Dead letter flag |
| `execution_id` | uuid | Execution ID |
| `execution_step_id` | uuid | Step ID |
| `processed_path` | text | Processed path |

#### `agent_jobs_dead_letter` - Failed jobs beyond max retries
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Entry ID (PK) |
| `org_id` | uuid | Organization |
| `dataset_id` | uuid | Dataset |
| `job_type` | text | Job type |
| `payload` | jsonb | Job payload |
| `retry_count` | integer | Final retry count |
| `last_error` | text | Final error |
| `original_job_id` | uuid | Original job ID |
| `failed_at` | timestamptz | Failure time |
| `created_at` | timestamptz | Creation time |

#### `agent_metrics` - Device performance metrics
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Metric ID (PK) |
| `device_id` | uuid | Device |
| `org_id` | uuid | Organization |
| `metrics` | jsonb | Metrics data |
| `concurrency_returned` | integer | Concurrency |
| `load_factor` | numeric(4,3) | Load factor |
| `source` | text | Source (default: agent_health_policy) |
| `created_at` | timestamptz | Creation time |

#### `agent_worker_activity` - Worker activity log
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Activity ID (PK) |
| `job_id` | uuid | Job |
| `device_id` | uuid | Device |
| `worker_id` | integer | Worker ID |
| `job_type` | text | Job type |
| `started_at` | timestamptz | Start time |
| `finished_at` | timestamptz | Finish time |
| `duration_ms` | integer | Duration |
| `status` | text | Status |
| `error` | text | Error |

#### `alert_history` - Alert notifications history
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Alert ID (PK) |
| `alert_rule_id` | uuid | Rule |
| `org_id` | uuid | Organization |
| `triggered_at` | timestamptz | Trigger time |
| `condition_type` | text | Condition type |
| `actual_value` | numeric | Actual value |
| `threshold_value` | numeric | Threshold |
| `notification_sent` | boolean | Notification flag |
| `notification_sent_at` | timestamptz | Notification time |

#### `alert_rules` - Configurable alert rules
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Rule ID (PK) |
| `org_id` | uuid | Organization (NOT NULL) |
| `name` | text | Rule name (NOT NULL) |
| `condition_type` | text | stuck_jobs, device_offline, job_failure_rate, merge_failure, pipeline_failed, device_error |
| `threshold_value` | numeric | Threshold (NOT NULL) |
| `threshold_window_minutes` | integer | Window (default: 5) |
| `channel` | text | email, webhook, slack (NOT NULL) |
| `channel_config` | jsonb | Channel config |
| `enabled` | boolean | Enabled flag (default: true) |
| `created_at` | timestamptz | Creation time |
| `created_by` | uuid | Creator |

#### `batch_chunks` - Dataset chunks for processing
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Chunk ID (PK) |
| `batch_id` | uuid | Batch ID |
| `org_id` | uuid | Organization |
| `chunk_index` | integer | Chunk index |
| `status` | text | pending, processing, processed, failed, skipped |
| `embedding` | vector(384) | Embedding vector |
| `processed_at` | timestamptz | Processing time |
| `metadata` | jsonb | Chunk metadata |
| `created_at` | timestamptz | Creation time |
| `job_type` | text | preprocess, process |
| `merged_in` | boolean | Merge flag |
| `similarity_score` | double precision | Similarity |
| `chunk_size_gb` | double precision | Size in GB |
| `required_io` | double precision | IO requirement |
| `parallel_ratio` | double precision | Parallelization |
| `dynamic_size` | boolean | Dynamic sizing (default: true) |
| `type` | text | preprocess, process |
| `assigned_device_id` | uuid | Assigned device |
| `dataset_id` | uuid | Dataset |
| `chunk_vector` | vector(16) | 16-dim vector for device matching |
| `assigned_at` | timestamptz | Assignment time |

**Comment:** Stores dataset chunks assigned to Kickin agents for processing. When `dynamic_size` is TRUE, chunk can be re-sized dynamically based on device capabilities.

#### `bootstrap_rate_limits` - Rate limiting for bootstrap
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Entry ID (PK) |
| `client_key` | text | Client identifier (NOT NULL) |
| `created_at` | timestamptz | Creation time |

#### `chunk_complexity_cache` - Complexity scores cache
| Column | Type | Description |
|--------|------|-------------|
| `dataset_id` | uuid | Dataset (NOT NULL) |
| `chunk_id` | uuid | Chunk (NOT NULL) |
| `complexity_score` | numeric(5,2) | Complexity |
| `last_used_at` | timestamptz | Last use |

#### `chunk_profiles` - Chunk profiles with vectors
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Profile ID (PK) |
| `chunk_id` | uuid | Chunk |
| `dataset_id` | uuid | Dataset |
| `complexity_vector` | vector(16) | 16-dim vector |
| `metadata` | jsonb | Profile metadata |
| `created_at` | timestamptz | Creation time |

#### `dataset_merge_locks` - Merge operation locks
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Lock ID (PK) |
| `dataset_id` | uuid | Dataset (NOT NULL) |
| `agent_id` | uuid | Agent |
| `device_id` | uuid | Device (NOT NULL) |
| `acquired_at` | timestamptz | Acquisition time |
| `heartbeat_at` | timestamptz | Heartbeat time |
| `expires_at` | timestamptz | Expiration (NOT NULL) |
| `status` | text | active, expired, released, cancelled (NOT NULL) |
| `created_at` | timestamptz | Creation time |
| `updated_at` | timestamptz | Update time |

**Constraint:** `dataset_merge_locks_status_canonical`: status IN ('active', 'expired', 'released', 'cancelled')

#### `datasets` - Dataset registry
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Dataset ID (PK) |
| `org_id` | uuid | Organization |
| `name` | text | Dataset name |
| `file_type` | text | File type |
| `total_size_gb` | double precision | Total size |
| `file_count` | bigint | File count |
| `avg_file_size_mb` | double precision | Average file size |
| `status` | text | registered, scanning, scanned, chunked, processing, merge_pending, merging, merged, failed |
| `created_at` | timestamptz | Creation time |
| `scan_assigned_device` | uuid | Device doing scan |
| `scan_assigned_at` | timestamptz | Scan assignment |
| `scanned_at` | timestamptz | Scan completion |
| `storage_type` | text | local, s3, gcs (default: local) |
| `job_type` | text | preprocess, embedding, merge, scan, index, validate |
| `updated_at` | timestamptz | Update time |
| `metadata` | jsonb | Dataset metadata |
| `merged_output_verified` | boolean | Verification flag |
| `merged_at` | timestamptz | Merge time |
| `merged_size_gb` | double precision | Merged size |
| `merge_time_ms` | double precision | Merge duration |
| `affinity_device_id` | uuid | Preferred device |
| `dataset_checksum` | text | Checksum |
| `disk_space_check_enabled` | boolean | Disk check (default: true) |
| `merge_strategy` | text | auto, sequential, tree (default: auto) |
| `merge_started_at` | timestamptz | Merge start |
| `merge_completed_at` | timestamptz | Merge completion |
| `merge_error` | text | Merge error |
| `storage_config_id` | uuid | Storage config |
| `source_path` | text | Source path |
| `detected_columns` | jsonb | Detected columns |
| `scan_completed` | boolean | Scan completion flag |

**Constraints:**
- `datasets_job_type_check`: job_type IN ('preprocess', 'embedding', 'merge', 'scan', 'index', 'validate')
- `datasets_merge_strategy_check`: merge_strategy IN ('auto', 'sequential', 'tree')
- `datasets_status_check`: status IN ('registered', 'scanning', 'scanned', 'chunked', 'processing', 'merge_pending', 'merging', 'merged', 'failed')

#### `device_benchmarks` - Device performance benchmarks
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Benchmark ID |
| `device_id` | uuid | Device |
| `org_id` | uuid | Organization |
| `test_name` | text | Test name |
| `latency_ms` | double precision | Latency |
| `throughput` | double precision | Throughput |
| `status` | text | Status |
| `duration_ms` | integer | Duration |
| `created_at` | timestamptz | Creation time |
| `updated_at` | timestamptz | Update time |

#### `device_claims` - Device claim records
| Column | Type | Description |
|--------|------|-------------|
| Columns defined in schema (referenced by triggers) |

#### `device_events` - Device event log
| Column | Type | Description |
|--------|------|-------------|
| Device event tracking |

#### `device_job_performance` - Per-device job performance
| Column | Type | Description |
|--------|------|-------------|
| `device_id` | uuid | Device |
| `org_id` | uuid | Organization |
| `job_type` | text | Job type |
| `duration_ms` | numeric | Duration |
| `throughput` | numeric | Throughput |
| `success` | boolean | Success flag |
| `created_at` | timestamptz | Creation time |

#### `device_job_type_stats` - Aggregated stats by job type
| Column | Type | Description |
|--------|------|-------------|
| `device_id` | uuid | Device |
| `org_id` | uuid | Organization |
| `job_type` | text | Job type |
| `avg_duration_ms` | numeric | Average duration |
| `avg_throughput` | numeric | Average throughput |
| `job_count` | bigint | Job count |
| `success_count` | bigint | Success count |
| `last_updated` | timestamptz | Last update |

#### `device_learning_history` - Device vector learning history
| Column | Type | Description |
|--------|------|-------------|
| `device_id` | uuid | Device |
| `profile_vector` | vector(16) | Profile vector |
| `recorded_at` | timestamptz | Recording time |

#### `device_vectors` - Device profile vectors for matching
| Column | Type | Description |
|--------|------|-------------|
| `device_id` | uuid | Device (PK) |
| `org_id` | uuid | Organization |
| `profile_vector` | vector(16) | 16-dim profile vector |
| `last_updated` | timestamptz | Last update |

#### `devices` - Registered agent devices
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Device ID (PK) |
| `org_id` | uuid | Organization |
| `name` | text | Device name |
| `status` | text | online, offline, available, busy, error, draining |
| `type` | text | Device type |
| `access_token_hash` | text | Token hash |
| `environment_type` | text | Local, docker |
| `storage_type` | text | local, s3, gcs |
| `os` | text | Operating system |
| `arch` | text | Architecture |
| `cpu_cores_free` | integer | Free CPU cores |
| `total_cpu_cores` | integer | Total CPU cores |
| `memory_free_gb` | numeric | Free memory |
| `total_memory_gb` | numeric | Total memory |
| `gpu_available` | boolean | GPU availability |
| `gpu_model` | text | GPU model |
| `benchmark_score` | numeric | Benchmark score |
| `max_concurrency` | integer | Max concurrent jobs |
| `active_jobs` | integer | Active job count |
| `last_heartbeat` | timestamptz | Last heartbeat |
| `cpu_usage_percent` | integer | CPU usage |
| `memory_usage_percent` | integer | Memory usage |
| `network_latency_ms` | integer | Network latency |
| `io_bandwidth_mb_s` | numeric | IO bandwidth |
| `network_zone` | text | Network zone |
| `merge_capable` | boolean | Merge capability |
| `preferred_chunk_size_gb` | real | Preferred chunk size |
| `runtime_supported` | jsonb | Supported runtimes |
| `docker_available` | boolean | Docker availability |
| `capabilities` | text[] | Capabilities array |
| `specs` | jsonb | Full specifications |
| `active_job_count` | integer | Active job count |
| `token_rotate_fail_count` | integer | Token rotation failures |
| `last_policy_update` | timestamptz | Last policy update |
| `total_jobs` | integer | Total jobs processed |
| `embedding` | vector(16) | Device embedding |
| `region` | text | Device region |
| `platform` | text | Platform identifier |
| `runtime_cache` | jsonb | Runtime cache |
| `deleted_at` | timestamptz | Deletion time |

**Enums:**
- `device_status_enum`: online, offline, available, busy, error, draining

#### `device_policies` - Device-specific policies
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Policy ID |
| `org_id` | uuid | Organization |
| `device_id` | uuid | Device (NULLable) |
| `max_concurrency` | integer | Max concurrency |
| `enabled` | boolean | Enabled flag |
| `created_at` | timestamptz | Creation time |

#### `device_routing_rules` - Job-to-device routing rules
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Rule ID |
| `org_id` | uuid | Organization |
| `device_id` | uuid | Target device (NULLable) |
| `job_type` | text | Job type |
| `action` | text | prefer, exclude, require |
| `priority` | integer | Rule priority |
| `enabled` | boolean | Enabled flag |

#### `dismissed_alerts` - Dismissed alerts tracking
| Column | Type | Description |
|--------|------|-------------|
| Alert dismissal records |

#### `enterprise_integrations` - Enterprise integration configs
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Integration ID |
| `org_id` | uuid | Organization |
| `provider` | text | Provider name |
| `credentials` | jsonb | Credentials |
| `vault_secret_name` | text | Vault secret name |

#### `environment_cache` - Runtime environment cache
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Cache ID |
| `org_id` | uuid | Organization |
| `device_id` | uuid | Device |
| `runtime_type` | text | Runtime type |
| `runtime_version` | text | Runtime version |
| `dependency_hash` | text | Dependency hash |
| `environment_path` | text | Environment path |
| `last_used_at` | timestamptz | Last use |
| `invalidated_at` | timestamptz | Invalidation time |

#### `execution_policies` - Job execution policies
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Policy ID |
| `org_id` | uuid | Organization |
| `name` | text | Policy name |
| `max_retries` | integer | Max retries |
| `retry_backoff_seconds` | integer | Backoff seconds |
| `default_timeout_seconds` | integer | Default timeout |
| `hard_timeout_seconds` | integer | Hard timeout |
| `retryable_errors` | jsonb | Retryable errors |
| `fatal_errors` | jsonb | Fatal errors |
| `mode_priority` | jsonb | Mode priority |
| `enabled` | boolean | Enabled flag |

#### `execution_steps` - Pipeline execution steps
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Step ID (PK) |
| `execution_id` | uuid | Execution |
| `step_index` | integer | Step index |
| `step_type` | text | Step type |
| `plugin_id` | uuid | Plugin |
| `script_id` | text | Script ID |
| `config` | jsonb | Step config |
| `status` | text | pending, running, completed, failed |
| `error` | text | Error message |
| `started_at` | timestamptz | Start time |
| `completed_at` | timestamptz | Completion time |

#### `executions` - Pipeline execution tracking
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Execution ID (PK) |
| `org_id` | uuid | Organization |
| `dataset_id` | uuid | Dataset |
| `pipeline_template_id` | uuid | Pipeline template |
| `status` | text | pending, running, completed, failed |
| `current_step_index` | integer | Current step |
| `total_steps` | integer | Total steps |
| `output` | jsonb | Execution output |
| `error_message` | text | Error message |
| `created_by` | uuid | Creator |
| `created_at` | timestamptz | Creation time |
| `completed_at` | timestamptz | Completion time |
| `updated_at` | timestamptz | Update time |

#### `http_queue` - HTTP request queue for async operations
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Queue ID (PK) |
| `url` | text | Target URL |
| `body` | jsonb | Request body |
| `headers` | jsonb | Request headers |
| `processed` | boolean | Processed flag |
| `processed_at` | timestamptz | Processing time |
| `status_code` | integer | HTTP status |
| `result` | text | Result |
| `retry_count` | integer | Retry count |
| `retry_at` | timestamptz | Next retry |
| `idempotency_key` | text | Idempotency key |
| `created_at` | timestamptz | Creation time |
| `org_id` | uuid | Organization |

#### `job_checkpoints` - Job checkpoint data
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Checkpoint ID |
| `job_id` | uuid | Job |
| `step_index` | integer | Step index |
| `checkpoint_data` | jsonb | Checkpoint data |
| `progress_percent` | numeric | Progress % |
| `checkpointed_at` | timestamptz | Checkpoint time |
| `is_completed` | boolean | Completion flag |

#### `job_notification_queue` - Job event notifications
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Notification ID (PK) |
| `job_id` | uuid | Job |
| `agent_id` | uuid | Device/Agent |
| `event_type` | text | Event type |
| `payload` | jsonb | Event payload |
| `processed` | boolean | Processed flag |
| `created_at` | timestamptz | Creation time |
| `org_id` | uuid | Organization |

#### `leases` - Job leases for concurrency control
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Lease ID (PK) |
| `job_id` | uuid | Job (unique when active) |
| `device_id` | uuid | Device |
| `lease_expires_at` | timestamptz | Expiration |
| `status` | text | active, cancelled |

#### `org_members` - Organization membership
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Membership ID |
| `org_id` | uuid | Organization |
| `user_id` | uuid | User |
| `role` | text | admin, member |
| `member_name` | text | Member name |
| `created_at` | timestamptz | Creation time |

#### `org_plugins` - Organization plugin access
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Entry ID |
| `org_id` | uuid | Organization |
| `plugin_id` | uuid | Plugin |
| `enabled` | boolean | Enabled flag |
| `rollout_percentage` | integer | Rollout % |

#### `org_quotas` - Organization resource quotas
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Quota ID |
| `org_id` | uuid | Organization |
| `max_devices` | integer | Max devices |
| `max_concurrent_jobs` | integer | Max concurrent jobs |
| `max_environments` | integer | Max environments |

#### `org_storage_configs` - Organization storage configurations
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Config ID (PK) |
| `org_id` | uuid | Organization |
| `name` | text | Config name |
| `storage_mode` | text | Mode |
| `provider` | text | s3, gcs, azure |
| `bucket_name` | text | Bucket name |
| `region` | text | Region |
| `endpoint` | text | Custom endpoint |
| `mount_base_path` | text | Mount path |
| `is_default` | boolean | Default flag |
| `created_at` | timestamptz | Creation time |

#### `org_usage` - Organization usage tracking
| Column | Type | Description |
|--------|------|-------------|
| Usage metrics |

#### `orgs` - Organizations
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Org ID (PK) |
| `name` | text | Org name |
| `team_size` | integer | Team size |
| `plan` | text | free, pro, enterprise |
| `created_at` | timestamptz | Creation time |

#### `pipeline_templates` - Reusable pipeline definitions
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Template ID (PK) |
| `org_id` | uuid | Organization |
| `name` | text | Template name |
| `description` | text | Description |
| `dataset_id` | uuid | Dataset |
| `steps` | jsonb | Pipeline steps (array) |
| `created_at` | timestamptz | Creation time |
| `updated_at` | timestamptz | Update time |

#### `plan_limits` - Plan-based limits
| Column | Type | Description |
|--------|------|-------------|
| `plan_name` | text | Plan name |
| `max_devices` | integer | Max devices |
| `max_concurrent_jobs` | integer | Max jobs |

#### `plugin_execution_history` - Plugin execution tracking
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Execution ID (PK) |
| `org_id` | uuid | Organization |
| `plugin_id` | uuid | Plugin |
| `job_id` | uuid | Job |
| `device_id` | uuid | Device |
| `status` | text | started, completed, failed |
| `started_at` | timestamptz | Start time |
| `finished_at` | timestamptz | Finish time |
| `error` | text | Error message |
| `execution_duration_ms` | integer | Duration |

#### `plugin_signing_keys` - Plugin signing keys
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Key ID (PK) |
| `org_id` | uuid | Organization |
| `public_key` | text | Public key |
| `revoked_at` | timestamptz | Revocation time |
| `created_at` | timestamptz | Creation time |

#### `plugins` - Available plugins
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Plugin ID (PK) |
| `org_id` | uuid | Organization |
| `name` | text | Plugin name |
| `version` | text | Version |
| `language` | text | python, node |
| `plugin_type` | text | Type |
| `description` | text | Description |
| `category` | text | Category |
| `storage_path` | text | Storage path |
| `checksum` | text | Checksum |
| `signature` | bytea | Ed25519 signature |
| `signature_key_id` | text | Key ID |
| `resources` | jsonb | Resources |
| `trusted` | boolean | Trusted flag |
| `os` | text | Operating system |
| `arch` | text | Architecture |
| `plugin_group` | text | Plugin group |
| `network` | boolean | Network access |
| `config_schema` | jsonb | Config schema |
| `input_schema` | jsonb | Input schema |
| `output_schema` | jsonb | Output schema |
| `created_at` | timestamptz | Creation time |

#### `runtime_environments` - Cached runtime environments
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Environment ID (PK) |
| `org_id` | uuid | Organization |
| `device_id` | uuid | Device |
| `runtime_type` | text | python, node |
| `runtime_version` | text | Version |
| `dependency_hash` | text | Dependency hash |
| `platform` | text | Platform |
| `environment_path` | text | Path |
| `last_used_at` | timestamptz | Last use |
| `invalidated_at` | timestamptz | Invalidation |
| `created_at` | timestamptz | Creation time |

#### `step_outputs` - Execution step outputs
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Output ID (PK) |
| `execution_step_id` | uuid | Step |
| `output_key` | text | Output key |
| `output_value` | jsonb | Output value |
| `created_at` | timestamptz | Creation time |

#### `system_config` - System configuration
| Column | Type | Description |
|--------|------|-------------|
| `key` | text | Config key (PK) |
| `value` | text | Config value |

#### `system_logs` - System event logs
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Log ID (PK) |
| `event_type` | text | Event type |
| `message` | text | Log message |
| `org_id` | uuid | Organization |
| `created_at` | timestamptz | Creation time |

#### `vector_batches` - Vector batch operations
| Column | Type | Description |
|--------|------|-------------|
| Vector batch data |

#### `vector_datasets` - Vector dataset tracking
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Dataset ID |
| `org_id` | uuid | Organization |
| `name` | text | Name |
| `dimensions` | integer | Vector dimensions |
| `created_at` | timestamptz | Creation time |

#### `vector_store` - Vector storage
| Column | Type | Description |
|--------|------|-------------|
| `id` | uuid | Vector ID |
| `dataset_id` | uuid | Dataset |
| `embedding` | vector | Embedding |
| `metadata` | jsonb | Metadata |

---

## Functions

### Job Management Functions

#### `acquire_dataset_merge_lock(p_dataset_id, p_agent_id, p_device_id, p_org_id, p_duration_minutes)` → `jsonb`
Acquire merge lock for a dataset. Requires p_org_id to verify device ownership. Returns `{success, lock_id, expires_at}`.

#### `acquire_lease(p_job_id, p_org_id, p_device_id, p_ttl_seconds)` → `boolean`
Acquire lease for a job. Validates org_id ownership and device assignment.

#### `activate_pipeline(p_pipeline_template_id, p_dataset_id, p_org_id, p_created_by)` → `jsonb`
Activate a pipeline template for a dataset. Creates execution record and job steps.

#### `assign_agent_job(p_org_id)` → `jsonb`
Assign best job to best device using vector similarity matching.

#### `assign_agent_job(p_org_id, p_agent_id)` → `table(ok, job_id, job_type, payload, error)`
Assign a specific job to a specific device.

#### `assign_agent_job(p_job_id, p_agent_id, p_org_id)` → `boolean`
Force-assign a specific job to a device. **Requires org_id** to prevent cross-org attacks.

#### `assign_best_job_to_best_device(p_org_id)` → `jsonb`
Atomic function that finds the best pending job and best available device, then assigns them.

#### `assign_chunk_job_on_insert()` → `trigger`
Trigger function that automatically assigns a chunk processing job when a new batch_chunk is inserted.

#### `batch_assign_jobs_atomic(p_device_id, p_org_id, p_limit, p_job_type_filter)` → `jsonb`
Atomically assign multiple jobs to a device in a single transaction.

#### `claim_job_with_compatibility(p_org_id, p_device_id)` → `table(ok, job_id, job_type, payload, runtime_type, runtime_dependencies, entrypoint, execution_mode, ...)`
Claim a job with runtime and execution mode compatibility checking. Supports warm environment detection.

#### `claim_jobs_for_device(p_device_id, p_org_id, p_limit, p_lease_ttl_seconds)` → `table(job_id, job_type, job_payload, exec_id)`
Claim multiple jobs for a specific device with re-chunking support.

#### `claim_next_job_for_device(p_org_id, p_device_id)` → `table(ok, job_id, job_type, payload, runtime_type, runtime_dependencies, entrypoint, execution_mode, error)`
Claim the next available job compatible with the device.

#### `cleanup_stuck_jobs(p_max_retries, p_org_id)` → `jsonb`
Recover stuck jobs (assigned/running beyond timeout). Re-queue or move to dead letter.

#### `cleanup_old_agent_jobs()`
Delete completed/failed jobs older than 30 days.

#### `complete_job_idempotent(p_job_id, p_status, p_duration_ms, p_result, p_device_id, p_org_id)` → `boolean`
Idempotent job completion with lease validation. Prevents duplicate completions.

#### `force_assign_job(p_job_id, p_agent_id, p_org_id)` → `boolean`
Force-assign job to device. **SECURITY: Requires org_id validation**.

#### `lease_agent_job(p_job_id, p_org_id, p_agent_id, p_ttl_secs)` → `boolean`
Lease an agent job with atomic upsert.

#### `reclaim_jobs_from_device(p_device_id)` → `jsonb`
Reclaim all jobs from a device and cancel its leases.

#### `reconcile_device_stale_jobs(p_device_id, p_org_id)` → `integer`
Reconcile stale jobs on device restart.

#### `recover_stuck_jobs(_timeout_minutes, _max_retries)` → `jsonb`
Find assigned/running jobs older than timeout → re-queue or fail (safe).

#### `record_agent_job_result(_job_id, _status, _duration_ms, _throughput, _output_token, _plugin_id, _plugin_version, _metrics)` → `void`
Production-safe result recorder: updates job status and stores metrics into agent_metrics.

#### `start_job(p_job_id, p_agent_id)` → `json`
Start a job (transition from pending to running).

#### `record_job_performance(p_device_id, p_org_id, p_job_type, p_duration_ms, p_throughput, p_success)` → `void`
Record job performance metrics for a device.

### Device Management Functions

#### `check_and_set_policy_cooldown(p_device_id)` → `jsonb`
Atomic cooldown check for agent health policy updates (5-second cooldown).

#### `device_has_warm_environment(p_device_id, p_runtime_type, p_runtime_version, p_dependency_lock_hash)` → `boolean`
Check if device has a warm (cached) runtime environment.

#### `device_matches_requirements(p_device_id, p_runtime_type, p_min_python_version, p_required_arch, p_required_os)` → `boolean`
Check if device matches job requirements.

#### `device_supports_execution_mode(p_device_id, p_mode)` → `boolean`
Check if device supports execution mode (docker, runtime, native).

#### `device_supports_runtime(p_device_id, p_runtime_type)` → `boolean`
Check if device supports a runtime type.

#### `elect_merge_device(_org_id, _affinity_device_id, _preferred_network_zone)` → `uuid`
Select best device for merge operations using strategy: affinity → fastest online.

#### `get_agent_job_stats(p_agent_id)` → `table(total_jobs, completed_jobs, failed_jobs, running_jobs, success_rate, avg_duration_ms, avg_jobs_per_day, last_job_at)`
Get comprehensive job statistics for a device.

#### `get_agent_metrics_aggregate(p_agent_id, p_time_range, p_interval)` → `table(period, avg_cpu, avg_memory_free_gb, max_cpu, min_memory_free_gb, metric_count)`
Get aggregated metrics for a device over time.

#### `get_device_by_id(p_device_id)` → `table(id, name, status, environment_type, storage_type, os, arch, cpu_cores_free, ...)`
Get detailed device information.

#### `get_device_job_history(p_device_id, p_limit)` → `table(job_id, job_type, status, created_at, started_at, finished_at, duration_ms, error)`
Get job history for a device.

#### `get_device_job_stats(p_device_id, p_job_type, p_window_hours)` → `table(avg_duration_ms, avg_throughput, success_rate, job_count)`
Get job statistics by type for a device.

#### `get_device_rankings(org_id, job_type, chunk_vector)` → `table(id, name, capabilities, similarity)`
Get device rankings using vector similarity.

#### `get_fleet_health()` → `table(id, name, status, health_score, cpu_usage, memory_usage_gb, memory_total_gb, active_workers, max_concurrency, last_heartbeat, gpu_available, environment_type)`
Get health status of all devices in the fleet.

#### `get_or_create_runtime_environment(p_org_id, p_runtime_type, p_runtime_version, p_dependency_hash, p_device_id)` → `uuid`
Get or create a runtime environment for caching.

#### `match_best_device(_org_id, _chunk_vector, _job_type)` → `table(id, device_id, score)`
Find best device using vector similarity and load balancing.

#### `match_best_execution_target(p_org_id, p_job_vector, p_job_type, p_runtime_type, p_execution_mode)` → `table(device_id, execution_mode, compatibility_score)`
Match best execution target with compatibility scoring.

#### `recalcualte_device_vector(p_device_id)` → `void`
Recalculate device profile vector from benchmarks.

#### `rotate_agent_token(p_device_id, p_org_id)` → `void`
Rotate agent token with archive of old token.

#### `select_best_device(p_org_id, p_job_type, p_chunk_vector)` → `uuid`
Select best device for job. Includes busy devices if they have capacity.

#### `set_device_vector(p_device_id, p_vec_literal)` → `void`
Set device profile vector.

### Dataset Management Functions

#### `auto_create_agent_job()` → `trigger`
Trigger to create a single merge agent_job when batch_chunks complete for a dataset.

#### `auto_progress_after_scan()` → `trigger`
Trigger to notify on dataset scan completion.

#### `create_chunks_from_plan_job()` → `trigger`
Create chunks when plan_chunks job completes.

#### `create_scan_job_on_dataset_insert()` → `trigger`
Create scan_dataset job when dataset status is 'registered'.

#### `get_dataset_executions(p_dataset_id, p_limit)` → `table(id, status, created_at, completed_at, current_step_index, total_steps)`
Get all executions for a dataset.

#### `handle_dataset_scan_trigger()` → `trigger`
Handle dataset scan trigger - create scan job and update status.

#### `notify_agent_on_dataset_register()` → `trigger`
Create scan_dataset job when dataset is registered.

#### `pre_chunk_dataset_smart(p_dataset_id, p_org_id)` → `jsonb`
Smart chunking based on device capabilities.

#### `rechunk_for_device(p_dataset_id, p_device_id, p_org_id, p_job_type)` → `jsonb`
Re-chunk dataset specifically for a device's capabilities.

#### `update_dataset_merge_metadata(p_dataset_id, p_merge_time_ms, p_merged_size_gb, p_verified)` → `void`
Update dataset metadata after merge.

#### `update_dataset_scan(p_dataset_id, p_scan_metadata)` → `jsonb`
Update dataset after scan completion.

### Pipeline Management Functions

#### `advance_pipeline_on_job_complete()` → `trigger`
Advance pipeline when a job completes - inserts into job_notification_queue.

#### `get_execution_detail(p_execution_id)` → `table(execution_id, execution_status, execution_created_at, execution_completed_at, dataset_id, dataset_name, current_step_index, total_steps, step_id, step_index, step_status, step_type, step_error, step_completed_at)`
Get detailed execution information with step details.

#### `get_pipeline_status(p_org_id)` → `table(pending_count, running_count, completed_count, failed_count, total_count)`
Get pipeline status counts.

#### `get_pipeline_template(p_template_id)` → `table(id, name, description, dataset_id, steps, created_at, updated_at, dataset_name)`
Get pipeline template details.

#### `list_pipeline_templates(p_org_id, p_limit)` → `table(id, name, description, dataset_id, dataset_name, step_count, created_at, updated_at)`
List pipeline templates.

#### `update_dataset_status_on_merge_complete()` → `trigger`
Update dataset status to 'merged' when merge job completes.

### Plugin Management Functions

#### `calculate_dependency_hash(p_runtime_type, p_runtime_dependencies)` → `text`
Calculate SHA-256 hash of runtime dependencies.

#### `compute_agent_job_hashes()` → `trigger`
Compute dependency hash when runtime_type changes.

#### `compute_dependency_lock_hash()` → `trigger`
Compute dependency lock hash from runtime_dependencies.

#### `compute_job_lock_hash()` → `trigger`
Compute job lock hash and set defaults for run_id, attempt_number.

#### `get_org_plugins(p_org_id, p_os, p_arch)` → `table(plugin_id, name, version, language, plugin_type, storage_path, checksum, signature, signature_key_id, resources, trusted, rollout_percentage, os, arch, plugin_group, network)`
Get plugins available to an organization.

#### `get_plugin_by_id(p_plugin_id)` → `table(id, name, version, language, plugin_type, description, category, trusted, created_at, config_schema, input_schema, output_schema)`
Get plugin details by ID.

#### `record_plugin_execution_start(p_org_id, p_plugin_id, p_job_id, p_device_id)` → `uuid`
Record plugin execution start, returns execution_id.

#### `record_plugin_execution_end(p_execution_id, p_status, p_error)` → `jsonb` / `void`
Record plugin execution end.

#### `register_plugin()` (Edge Function)
Register a new plugin with Ed25519 signature.

#### `should_run_plugin(p_device_id, p_rollout_percentage)` → `boolean`
Determine if a device should run a plugin based on rollout percentage.

#### `should_run_plugin_for_device(p_device_id, p_rollout_percentage)` → `boolean`
Determine if device should run plugin using MD5 hash.

### Organization & Auth Functions

#### `create_org_with_owner(org_name, team_size, member_name)` → `uuid`
Create organization and attach user as admin.

#### `get_current_org_id()` → `uuid`
Get current user's organization ID from JWT.

#### `get_user_org_role(p_user_id)` → `table(org_id, role)`
Get user's role in their organization.

#### `is_org_admin(_org_id)` → `boolean`
Check if current user is org admin.

#### `is_org_member(_org_id)` → `boolean`
Check if current user is org member.

#### `check_org_quota(p_org_id, p_quota_type, p_value)` → `boolean`
Check if organization is within quota limits (devices, jobs, environments).

#### `check_plan_limit(p_org_id, p_limit_type, p_increment)` → `boolean`
Check if organization is within plan limits.

### Storage & Vault Functions

#### `decrypt_vault_secret(secret_name)` → `text`
Decrypt a secret from Supabase Vault.

#### `get_org_storage_configs(p_org_id)` → `table(id, name, storage_mode, provider, bucket_name, region, endpoint, mount_base_path, is_default, created_at)`
Get organization's storage configurations.

#### `get_storage_config` (Edge Function)
Get storage backend configuration.

#### `migrate_enterprise_credentials_to_vault()` → `void`
Migrate enterprise credentials from table to Vault.

#### `set_default_storage_config(p_org_id, p_config_id)` → `table(updated_id)`
Set default storage configuration for org.

#### `store_s3_credentials_to_vault(p_org_id, p_access_key_id, p_secret_access_key, p_provider, p_secret_name)` → `jsonb`
Store S3 credentials in Vault.

#### `test_storage_connection` (Edge Function)
Test storage connection.

### Notification & Event Functions

#### `enqueue_device_online_event()` → `trigger`
Enqueue device online event to notification queue.

#### `get_org_audit_log(p_org_id, p_limit, p_event_type_filter)` → `table(id, event_type, message, created_at)`
Get organization audit log.

#### `get_audit_event_types()` → `text[]`
Get list of valid audit event types.

#### `insert_device_agent_metric()` → `trigger`
Insert device metric on update.

#### `notify_job_queue()` → `void`
Notify job queue via PostgreSQL NOTIFY.

#### `notify_merge_complete(p_dataset_id)` → `void`
Notify merge completion via pg_notify.

#### `notify_new_job(p_org_id, p_job_id, p_device_id)` → `void`
Notify new job via pg_notify.

#### `relay_job_event` (Edge Function)
Relay job events to notification queue.

### System & Maintenance Functions

#### `auto_rotate_stale_tokens()` → `void`
Rotate tokens for devices with stale tokens (older than 7 days).

#### `cleanup_agent_worker_activity(p_days_old)` → `integer`
Cleanup old agent worker activity records.

#### `cleanup_duplicate_cron_jobs()` → `void`
Remove duplicate cron jobs.

#### `cleanup_expired_merge_locks(p_org_id)` → `integer`
Cleanup expired merge locks.

#### `cleanup_job_notification_queue(p_days_old)` → `integer`
Cleanup old processed notifications.

#### `cleanup_leases_on_offline()` → `trigger`
Cleanup leases when device goes offline.

#### `cleanup_offline_device_leases(p_org_id)` → `jsonb`
Cleanup all leases for offline devices.

#### `cleanup_old_benchmarks()` → `void`
Cleanup benchmarks older than 30 days.

#### `evaluate_alert_rules()` → `void`
Evaluate alert rules (cron job).

#### `get_dashboard_stats(p_org_id)` → `table(total_jobs, running_jobs, completed_jobs, failed_jobs, pending_jobs, total_datasets, active_datasets, total_devices, online_devices, busy_devices, total_executions, running_executions, completed_executions, failed_executions)`
Get dashboard statistics.

#### `get_dashboard_summary()` → `table(total_jobs, running_jobs, completed_jobs, failed_jobs, queued_jobs, success_rate, total_datasets, total_storage_gb, total_files, total_agents, online_agents, busy_agents, gpu_agents, avg_benchmark_score)`
Get overall dashboard summary.

#### `get_recent_activity(p_limit)` → `table(id, job_type, status, created_at, finished_at, duration_ms, agent_name, dataset_name, error)`
Get recent job activity.

#### `global_search(p_org_id, p_query, p_type, p_limit)` → `table(result_type, id, name, status, created_at)`
Global search across jobs, datasets, and devices.

#### `mark_offline_devices()` → `void`
Mark devices as offline if no heartbeat in 10 minutes.

#### `prune_old_agent_metrics()` → `void`
Prune agent metrics older than 30 days.

#### `prune_old_system_logs()` → `void`
Prune system logs older than 90 days.

#### `system_health_heartbeat(_stale_device_minutes, _recovery_timeout, _max_retries)` → `jsonb`
System health check: mark stale devices offline, recover stuck jobs, try assign per org.

#### `consolidated_dispatch()` → `void`
Consolidated dispatch via HTTP queue.

#### `dispatch_http_jobs_secure()` → `void`
Secure HTTP job dispatch.

#### `process_http_queue(p_limit, p_max_retries)` → `integer`
Process HTTP queue items with retry logic.

#### `safe_http_post(target_url, payload, headers)` → `void`
Safe HTTP POST with error handling (doesn't interrupt parent transaction).

#### `log_agent_error(p_device_id, p_job_id, p_error_message)` → `void`
Log agent error with message sanitization (removes paths, IPs, emails).

### Utility Functions

#### `calculate_retry_backoff(p_retry_count, p_base_delay_seconds, p_max_delay_seconds, p_multiplier)` → `interval`
Calculate retry backoff with exponential growth.

#### `check_http_queue_depth()` → `boolean`
Check HTTP queue depth and trigger alerts if threshold exceeded.

#### `check_platform_signing_configured()` → `boolean`
Check if platform signing key is configured.

#### `encode_plugin_signature(sig)` → `text`
Encode plugin signature to base64.

#### `get_advisory_lock_key(p_uuid)` → `bigint`
Get advisory lock key from UUID using hashtextextended.

#### `get_constraints_report()` → `table(constraint_name, constraint_definition)`
Get constraint definitions for datasets table.

#### `get_edge_url()` → `text`
Get edge function base URL from system_config.

#### `get_functions_report()` → `table(routine_name, routine_type)`
Get list of functions with 'chunk' in name.

#### `get_triggers_report()` → `table(table_name, trigger_name, event_manipulation, action_timing)`
Get triggers report for main tables.

#### `list_public_functions()` → `table(function_name, arguments, return_type, language, function_code)`
List all public functions with their source code.

#### `run_all_validation_tests()` → `table(test_name, passed, details)`
Run all validation test cases.

#### `safe_cast_uuid(input)` → `uuid`
Safely cast text to UUID (returns NULL on failure).

#### `set_org_id_from_record()` → `trigger`
Set org_id from related records (datasets, batch_chunks).

#### `touch_device_vector()` → `trigger`
Update device vector last_updated timestamp.

### Validation Test Functions

#### `test_case_1_device_available()` → `table(test_name, passed, details)`
Test: Device available → job assigned immediately.

#### `test_case_2_no_device()` → `table(test_name, passed, details)`
Test: No device → job queued as pending.

#### `test_case_3_multiple_chunks()` → `table(test_name, passed, details)`
Test: Multiple chunks → each gets independent job.

#### `test_case_4_state_machine()` → `table(test_name, passed, details)`
Test: State machine transitions (NULL→assigned→running→completed, terminal states blocked).

#### `test_case_5_no_duplicates()` → `table(test_name, passed, details)`
Test: No duplicate jobs created for same chunk.

---

## Triggers

| Trigger Name | Table | Event | Function | Description |
|-------------|-------|-------|----------|-------------|
| `assign_chunk_job_on_insert` | `batch_chunks` | INSERT | `assign_chunk_job_on_insert()` | Auto-assign chunk job on insert |
| `auto_assign_merge_job` | `datasets` | UPDATE | `auto_assign_merge_job()` | Auto-assign merge job via HTTP queue |
| `auto_create_agent_job` | `batch_chunks` | UPDATE | `auto_create_agent_job()` | Create merge job when chunks complete |
| `auto_progress_after_scan` | `datasets` | UPDATE | `auto_progress_after_scan()` | Notify after dataset scan |
| `advance_pipeline_on_job_complete` | `agent_jobs` | UPDATE | `advance_pipeline_on_job_complete()` | Advance pipeline on job completion |
| `cleanup_leases_on_offline` | `devices` | UPDATE | `cleanup_leases_on_offline()` | Cleanup leases when device offline |
| `create_scan_job_on_dataset_insert` | `datasets` | INSERT | `create_scan_job_on_dataset_insert()` | Create scan job on dataset insert |
| `compute_agent_job_hashes` | `agent_jobs` | UPDATE | `compute_agent_job_hashes()` | Compute dependency hash |
| `compute_dependency_lock_hash` | `agent_jobs` | INSERT/UPDATE | `compute_dependency_lock_hash()` | Compute lock hash |
| `compute_job_lock_hash` | `agent_jobs` | INSERT | `compute_job_lock_hash()` | Compute job hash and defaults |
| `enqueue_device_online_event` | `devices` | UPDATE | `enqueue_device_online_event()` | Notify device online |
| `handle_dataset_scan_trigger` | `datasets` | INSERT/UPDATE | `handle_dataset_scan_trigger()` | Handle dataset scan |
| `handle_job_failure` | `agent_jobs` | UPDATE | `handle_job_failure()` | Move to dead letter on max retries |
| `insert_device_agent_metric` | `devices` | UPDATE | `insert_device_agent_metric()` | Insert device metric |
| `invoke_optimal_chunk_size_calculation` | `device_job_performance` | INSERT | `invoke_optimal_chunk_size_calculation()` | Update chunk complexity cache |
| `manage_agent_job_state` | `agent_jobs` | INSERT/UPDATE | `manage_agent_job_state()` | Enforce state machine rules |
| `on_agent_job_failed` | `agent_jobs` | UPDATE | `on_agent_job_failed()` | Log agent job failure |
| `on_merge_job_finished` | `agent_jobs` | UPDATE | `on_merge_job_finished()` | Update dataset on merge complete |
| `prevent_overassign_agent_job` | `agent_jobs` | INSERT | `prevent_overassign_agent_job()` | Prevent over-assignment |
| `queue_assign_scan_job` | `agent_jobs` | INSERT | `queue_assign_scan_job()` | Assign scan job to org |
| `queue_job_notification` | `agent_jobs` | INSERT | `queue_job_notification()` | Queue job notification |
| `recalcualte_device_vector` | `device_benchmarks` | INSERT | - | Recalculate vector on benchmark |
| `set_alert_rules_org_id_trigger` | `alert_rules` | INSERT/UPDATE | `set_alert_rules_org_id_trigger()` | Set org_id from context |
| `set_enterprise_integrations_org_id_trigger` | `enterprise_integrations` | INSERT/UPDATE | `set_enterprise_integrations_org_id_trigger()` | Set org_id |
| `set_execution_policies_org_id_trigger` | `execution_policies` | INSERT/UPDATE | `set_execution_policies_org_id_trigger()` | Set org_id |
| `set_http_queue_org_id_trigger` | `http_queue` | INSERT/UPDATE | `set_http_queue_org_id_trigger()` | Set org_id |
| `set_job_notification_queue_org_id` | `job_notification_queue` | - | `set_job_notification_queue_org_id()` | Set org_id from job |
| `set_org_id_from_record` | `agent_jobs/batch_chunks` | INSERT/UPDATE | `set_org_id_from_record()` | Set org_id from related record |
| `set_plugin_execution_history_org_id_trigger` | `plugin_execution_history` | INSERT/UPDATE | `set_plugin_execution_history_org_id_trigger()` | Set org_id |
| `set_plugin_signing_keys_org_id_trigger` | `plugin_signing_keys` | INSERT/UPDATE | `set_plugin_signing_keys_org_id_trigger()` | Set org_id |
| `set_runtime_environments_org_id` | `runtime_environments` | - | `set_runtime_environments_org_id()` | Set org_id from device |
| `set_runtime_environments_org_id_trigger` | `runtime_environments` | INSERT/UPDATE | `set_runtime_environments_org_id_trigger()` | Set org_id |
| `set_vector_datasets_org_id_trigger` | `vector_datasets` | INSERT/UPDATE | `set_vector_datasets_org_id_trigger()` | Set org_id |
| `touch_device_vector` | `device_vectors` | UPDATE | `touch_device_vector()` | Update last_updated |
| `trg_cleanup_leases_on_offline` | `devices` | UPDATE | `trg_cleanup_leases_on_offline()` | Cleanup leases on offline |
| `update_dataset_merge_metadata` | `datasets` | - | `update_dataset_merge_metadata()` | Update metadata after merge |
| `update_dataset_status_on_merge_complete` | `agent_jobs` | UPDATE | `update_dataset_status_on_merge_complete()` | Update dataset on merge |
| `update_dataset_status_on_scan_complete` | `datasets` | UPDATE | `update_dataset_status_on_scan_complete()` | Update dataset on scan |

---

## Edge Functions

Edge Functions run on Supabase Edge Runtime (Deno) and provide the API layer for the agent.

### Authentication
All functions (except `claim_device` and `register_device`) require:
- `Authorization: Bearer <access_token>` header (Supabase JWT)
- `x-agent-token: <token>` header (device token)

### Edge Function List

#### `advance_pipeline` (`/functions/v1/advance_pipeline`)
Advance pipeline execution. Validates org access.

#### `agent_health_policy` (`/functions/v1/agent_health_policy`)
Reports device health and receives concurrency policy.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ total_cpu_cores?, total_memory_gb?, cpu_cores_free?, memory_free_gb?, cpu_usage_percent?, network_latency_ms?, gpu_available?, incoming_workload_weight? }`
- **Response**: `{ ok, concurrency, load_factor }`

#### `agent_stream` (`/functions/v1/agent_stream`)
Server-Sent Events stream for real-time job delivery.
- **Method**: GET
- **Auth**: Device token
- **Events**:
  - `hello`: `{ device_id, org_id, realtime_enabled }`
  - `job`: `{ id, job_type, status, payload }`
  - `sync`: `{ jobs_sent, timestamp }`

#### `approve_dataset_and_plan_chunks` (`/functions/v1/approve_dataset_and_plan_chunks`)
Approve dataset and trigger chunk planning.
- **Method**: POST
- **Auth**: Internal
- **Body**: `{ dataset_id }`
- **Response**: `{ ok, message }`

#### `assign_agent_job` (`/functions/v1/assign_agent_job`)
Request a job assignment from the backend.
- **Method**: GET/POST
- **Auth**: Device token
- **Response**: `{ ok, result: { job_id, job_type, payload, execution_id } }`

#### `auto_assign_best_device` (`/functions/v1/auto_assign_best_device`)
Automatically assign best device for a dataset.
- **Method**: POST
- **Auth**: Internal
- **Body**: `{ dataset_id }`

#### `batch_assign_jobs` (`/functions/v1/batch_assign_jobs`)
Batch assign multiple jobs to a device.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ limit?, lease_ttl_seconds? }`
- **Response**: `{ ok, jobs: [...], assigned }`

#### `bootstrap` (`/functions/v1/bootstrap`)
Bootstrap new device with rate limiting.
- **Method**: POST
- **Auth**: Bearer token (claim code)
- **Body**: `{ claim_code, sysinfo?: { hostname?, type?, cpu_cores?, memory_gb?, benchmark_score?, environment?, storage?, network_zone?, merge_capable? } }`
- **Response**: `{ ok, device_id, agent_token, org_id }`

#### `calculate_optimal_chunk_size` (`/functions/v1/calculate_optimal_chunk_size`)
Calculate optimal chunk size based on device capabilities.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ dataset_id }`
- **Response**: `{ ok, chunk_size_gb, strategy }`

#### `claim_device` (`/functions/v1/claim_device`)
First-time device registration via claim code. **No auth required.**
- **Method**: POST
- **Body**: `{ claim_code, sysinfo?: { hostname?, type?, cpu_cores?, memory_gb?, benchmark_score?, environment?, storage?, network_zone?, merge_capable? } }`
- **Response**: `{ ok, device_id, agent_token, org_id }`

#### `claim_jobs_for_device` (`/functions/v1/claim_jobs_for_device`)
Claim multiple jobs for the authenticated device.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ limit?, lease_ttl_seconds? }`
- **Response**: `{ ok, jobs: [...] }`

#### `cleanup_job_notification_queue` (`/functions/v1/cleanup_job_notification_queue`)
Cleanup old job notifications.
- **Method**: POST
- **Response**: `{ success, error? }`

#### `cleanup_stuck_jobs` (`/functions/v1/cleanup_stuck_jobs`)
Cleanup stuck jobs (cron job).
- **Method**: POST
- **Auth**: Internal (cron secret)
- **Response**: `{ ok, reclaimed?, dead_lettered?, fixed_inconsistent? }`

#### `complete_job` (`/functions/v1/complete_job`)
Report job completion.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ execution_id?, status: "completed"|"failed"|"cancelled", duration_ms?, output?, error?, device_id? }`
- **Response**: `{ success, execution }`

#### `create-user` (`/functions/v1/create-user`)
Create new user and organization.
- **Method**: POST
- **Body**: `{ email, password, org_name, team_size?, member_name? }`
- **Response**: `{ ok, user_id, org_id }`

#### `decrypt_vault_secret` (`/functions/v1/decrypt_vault_secret`)
Decrypt a vault secret.
- **Method**: POST
- **Body**: `{ secret_name }`
- **Response**: `{ ok, value }`

#### `delete_org` (`/functions/v1/delete_org`)
Delete an organization and all associated data.
- **Method**: POST
- **Body**: `{ org_id }`
- **Response**: `{ success }`

#### `dispatch_http_jobs` (`/functions/v1/dispatch_http_jobs`)
Dispatch HTTP jobs from queue (cron job).
- **Auth**: Internal (cron secret)
- **Response**: `{ processed }`

#### `get_plugin` (`/functions/v1/get_plugin`)
Retrieve plugin code.
- **Method**: POST
- **Body**: `{ plugin_id }`
- **Response**: `{ plugin }`

#### `get_plugin_signing_key` (`/functions/v1/get_plugin_signing_key`)
Get plugin signing key for organization.
- **Method**: POST
- **Auth**: Device token
- **Response**: `{ ok, key }`

#### `get_storage_config` (`/functions/v1/get_storage_config`)
Get storage backend configuration.
- **Method**: POST
- **Body**: `{ storage_type? }`
- **Response**: `{ config }`

#### `invite_member` (`/functions/v1/invite_member`)
Invite a member to the organization.
- **Method**: POST
- **Body**: `{ email, role, org_id }`
- **Response**: `{ success }`

#### `list_all_plugins` (`/functions/v1/list_all_plugins`)
List all plugins available to user's organization.
- **Method**: GET
- **Response**: `{ ok, plugins: [...] }`

#### `list_plugin_signing_keys` (`/functions/v1/list_plugin_signing_keys`)
List plugin signing keys.
- **Method**: GET
- **Auth**: Device token
- **Response**: `{ ok, keys }`

#### `list_plugins_for_org` (`/functions/v1/list_plugins_for_org`)
List plugins for organization.
- **Method**: GET/POST
- **Response**: `{ ok, plugins }`

#### `notify_available_device` (`/functions/v1/notify_available_device`)
Notify that a device is available for work.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ dataset_id?, job_type? }`
- **Response**: `{ ok, assigned? }`

#### `plan_dataset_chunks` (`/functions/v1/plan_dataset_chunks`)
Plan dataset chunks using vector embeddings.
- **Method**: POST
- **Auth**: Internal
- **Body**: `{ dataset_id }`
- **Response**: `{ ok, chunks_created, chunk_size_gb }`

#### `pre_chunk_dataset` (`/functions/v1/pre_chunk_dataset`)
Pre-chunk dataset for processing.
- **Method**: POST
- **Auth**: Internal
- **Body**: `{ dataset_id, chunk_size? }`
- **Response**: `{ ok, chunks_created }`

#### `reconcile_agent` (`/functions/v1/reconcile_agent`)
Reconcile agent state on restart.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ stale_job_timeout_minutes? }`
- **Response**: `{ ok, reclaimed, fixed }`

#### `record_benchmark` (`/functions/v1/record_benchmark`)
Record device benchmark scores.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ test_name?, latency_ms?, device_id? }`
- **Response**: `{ ok }`

#### `record_dataset_metadata` (`/functions/v1/record_dataset_metadata`)
Record dataset metadata after scan.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ dataset_id, metadata }`
- **Response**: `{ ok }`

#### `register_device` (`/functions/v1/register_device`)
Device registration (alternative to claim_device with Bearer auth).
- **Method**: POST
- **Auth**: Bearer token
- **Body**: `{ name?, specs?, environment_type?, storage_type?, capabilities?, benchmark_score?, force_reclaim? }`
- **Response**: `{ ok, device_id, access_token }`

#### `register_plugin` (`/functions/v1/register_plugin`)
Register a new plugin with Ed25519 signature.
- **Method**: POST
- **Body**: `{ org_id, name, version, language, plugin_type, storage_path, signature, signature_key_id?, resources?, trusted?, os?, arch?, network? }`
- **Response**: `{ ok, plugin_id }`

#### `relay_job_event` (`/functions/v1/relay_job_event`)
Relay job events to notification queue.
- **Method**: POST
- **Auth**: Device token or Internal
- **Body**: `{ job_id, event_type, payload }`
- **Response**: `{ ok }`

#### `report_dataset_scan` (`/functions/v1/report_dataset_scan`)
Report dataset scan completion.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ dataset_id, metadata }`
- **Response**: `{ ok }`

#### `report_job_error` (`/functions/v1/report_job_error`)
Report job execution error.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ job_id, error, failure_classification?: "transient"|"permanent"|"resource"|"configuration" }`
- **Response**: `{ ok }`

#### `run_pipeline` (`/functions/v1/run_pipeline`)
Run a pipeline template.
- **Method**: POST
- **Auth**: Internal
- **Body**: `{ pipeline_template_id, dataset_id, org_id }`
- **Response**: `{ ok, execution_id }`

#### `schedule_merge_job` (`/functions/v1/schedule_merge_job`)
Schedule a merge job for a dataset.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ dataset_id }`
- **Response**: `{ ok, job_id? }`

#### `start_job` (`/functions/v1/start_job`)
Start a job (transition to running state).
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ job_id }`
- **Response**: `{ ok, job_id, status }`

#### `store_storage_credentials` (`/functions/v1/store_storage_credentials`)
Store encrypted storage credentials.
- **Method**: POST
- **Body**: `{ storage_type, credentials, encryption_key? }`
- **Response**: `{ ok }`

#### `test_rpc` (`/functions/v1/test_rpc`)
Test RPC function calls.
- **Response**: `{ results: { start_job?, get_pipeline_status?, ... } }`

#### `test_storage_connection` (`/functions/v1/test_storage_connection`)
Test storage connection.
- **Method**: POST
- **Body**: `{ org_id, provider, endpoint?, bucket_name?, region?, access_key_id?, secret_access_key? }`
- **Response**: `{ success }`

#### `upload_complete` (`/functions/v1/upload_complete`)
Notify upload completion.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ dataset_id, upload_path }`
- **Response**: `{ ok }`

#### `verifyAgentToken` (`/functions/v1/verifyAgentToken`)
Verify agent token (shared module).
- **Exported function**: `verifyAgentToken(req)`
- **Returns**: `{ device?, error? }`

#### `verify_job_lease` (`/functions/v1/verify_job_lease`)
Verify job lease is still valid.
- **Method**: POST
- **Auth**: Device token
- **Body**: `{ job_id, device_id }`
- **Response**: `{ valid }`

#### `verify_triggers` (`/functions/v1/verify_triggers`)
Verify all triggers are properly installed.
- **Method**: POST
- **Auth**: Internal
- **Response**: `{ ok, triggers: [...] }`

---

## Go Codebase Structure

```
sentra-agent/
├── cmd/
│   ├── main.go                    # Agent entry point
│   ├── sentra/main.go            # CLI tool entry point
│   └── agent/
│       ├── runtime/                # Runtime environments
│       │   ├── runtime.go         # Runtime interface
│       │   ├── python.go         # Python runtime
│       │   ├── node.go           # Node.js runtime
│       │   └── v2/                # Runtime v2 (warm pools)
│       │       ├── manager.go      # Runtime manager
│       │       ├── python.go      # Python v2 runtime
│       │       ├── node.go        # Node.js v2 runtime
│       │       └── types.go       # Type definitions
│       ├── executor/               # Plugin execution
│       │   ├── executor.go      # Executor interface
│       │   └── v2/                # Executor v2
│       │       └── executor.go   # V2 executor with sandbox
│       └── sandbox/               # Sandbox management
│           └── sandbox.go       # Sandbox isolation
│
├── internal/
│   ├── auth/                     # Authentication
│   │   ├── identity.go          # Device identity
│   │   ├── claim.go            # Device claiming
│   │   └── token_store.go      # Token storage
│   │
│   ├── backend/                  # Backend client (Supabase)
│   ├── bootstrap/                # Zero-config bootstrap
│   │   └── bootstrap.go        # Bootstrap logic
│   │
│   ├── config/                   # Configuration
│   │   ├── config.go           # Config loading
│   │   ├── defaults.go         # Default values
│   │   └── config_test.go      # Config tests
│   │
│   ├── dataset/                 # Dataset operations
│   │   ├── artifact.go         # Artifact handling
│   │   ├── recovery.go        # Recovery logic
│   │   ├── merge.go           # Merge operations
│   │   └── lock.go            # Dataset locking
│   │
│   ├── dispatcher/              # Job dispatching
│   │   ├── dispatcher_unix.go       # Unix dispatcher
│   │   ├── dispatcher_unix_cgo.go  # CGo dispatcher
│   │   ├── dispatcher_unix_nocgo.go # No CGo
│   │   ├── worker_pool.go          # Worker pool
│   │   ├── execute.go              # Job execution
│   │   ├── job.go                 # Job handling
│   │   ├── job_dedup.go           # Job deduplication
│   │   ├── choose_mode.go          # Mode selection
│   │   ├── native_runner.go       # Native runner
│   │   ├── native_runner_stub.go  # Stub for non-cgo
│   │   ├── handlers_unix.go       # Unix handlers
│   │   └── introspection.go      # Introspection
│   │
│   ├── healthcheck/              # Health endpoint
│   │   └── server.go            # Health server
│   │
│   ├── heartbeat/                # Device heartbeat
│   │   └── heartbeat.go        # Heartbeat logic
│   │
│   ├── httpclient/               # HTTP client
│   │   └── client.go           # HTTP client wrapper
│   │
│   ├── models/                   # Data models
│   │   ├── models.go          # Model definitions
│   │   └── failure.go         # Failure models
│   │
│   ├── obs/                      # Observability
│   │   ├── logger.go          # Logger
│   │   └── trace.go           # Tracing
│   │
│   ├── plugin/                   # Plugin system
│   │   ├── manager.go         # Plugin manager
│   │   ├── executor.go        # Plugin executor
│   │   ├── manifest.go        # Manifest handling
│   │   ├── key_fetcher.go    # Signing key fetcher
│   │   ├── db_sync.go         # DB synchronization
│   │   ├── update.go          # Plugin updates
│   │   ├── fetch.go           # Plugin fetching
│   │   ├── sandbox.go         # Plugin sandbox
│   │   └── bundled/           # Bundled plugins
│   │
│   ├── redis/                    # Redis client
│   │   └── client.go           # Redis connection
│   │
│   ├── realtime/                 # Real-time communication
│   │   ├── realtime_ws.go     # WebSocket client
│   │   ├── supabase_realtime.go # Supabase Realtime
│   │   ├── sse_client.go      # SSE client
│   │   └── available.go       # Availability check
│   │
│   ├── reporter/                 # Job reporting
│   │   └── reporter.go        # Report sending
│   │
│   ├── sandbox/                  # Resource limits
│   │   ├── limits.go          # Generic limits
│   │   ├── limits_unix.go     # Unix limits
│   │   ├── limits_linux.go    # Linux limits
│   │   └── limits_darwin.go   # macOS limits
│   │   └── limits_windows.go  # Windows limits
│   │
│   ├── startup/                  # Startup routines
│   │   ├── validate.go        # Validation
│   │   ├── reconcile.go       # Reconciliation
│   │   ├── cgo_enabled.go    # CGo enabled
│   │   └── cgo_disabled.go   # CGo disabled
│   │
│   ├── storage/                  # Storage backends
│   │   ├── config.go           # Storage config
│   │   └── s3http.go          # S3 HTTP transport
│   │
│   ├── sysinfo/                  # System information
│   │   └── sysinfo.go          # System specs
│   │
│   └── system/                   # Environment detection
│       ├── env.go              # Environment vars
│       ├── env_cgo.go         # CGo environment
│       └── env_nocgo.go       # No CGo environment
│
├── supabase/
│   ├── functions/                # Edge Functions (Deno)
│   │   ├── _shared/              # Shared utilities
│   │   │   ├── auth.ts          # Authentication helpers
│   │   │   ├── cors.ts          # CORS headers
│   │   │   └── security.ts     # Security utilities
│   │   ├── advance_pipeline/
│   │   ├── agent_health_policy/
│   │   ├── agent_stream/
│   │   ├── approve_dataset_and_plan_chunks/
│   │   ├── assign_agent_job/
│   │   ├── auto_assign_best_device/
│   │   ├── batch_assign_jobs/
│   │   ├── bootstrap/
│   │   ├── calculate_optimal_chunk_size/
│   │   ├── claim_device/
│   │   ├── claim_jobs_for_device/
│   │   ├── cleanup_job_notification_queue/
│   │   ├── cleanup_stuck_jobs/
│   │   ├── complete_job/
│   │   ├── create-user/
│   │   ├── decrypt_vault_secret/
│   │   ├── delete_org/
│   │   ├── dispatch_http_jobs/
│   │   ├── get_plugin/
│   │   ├── get_plugin_signing_key/
│   │   ├── get_storage_config/
│   │   ├── invite_member/
│   │   ├── list_all_plugins/
│   │   ├── list_plugin_signing_keys/
│   │   ├── list_plugins_for_org/
│   │   ├── notify_available_device/
│   │   ├── plan_dataset_chunks/
│   │   ├── pre_chunk_dataset/
│   │   ├── reconcile_agent/
│   │   ├── record_benchmark/
│   │   ├── record_dataset_metadata/
│   │   ├── register_device/
│   │   ├── register_plugin/
│   │   ├── relay_job_event/
│   │   ├── report_dataset_scan/
│   │   ├── report_job_error/
│   │   ├── run_pipeline/
│   │   ├── schedule_merge_job/
│   │   ├── start_job/
│   │   ├── store_storage_credentials/
│   │   ├── test_rpc/
│   │   ├── test_storage_connection/
│   │   ├── upload_complete/
│   │   ├── verifyAgentToken/
│   │   ├── verify_job_lease/
│   │   └── verify_triggers/
│   └── migrations/               # Database migrations
│
├── Dockerfile
├── Makefile
└── bin/                        # Built binaries
```

---

## Job Types & Lifecycle

### Job Types

| Type | Description |
|------|-------------|
| `scan_dataset` | Scan and analyze dataset structure |
| `process` | Process individual data chunks |
| `preprocess` | Pre-process chunks before main processing |
| `process_dataset` | Full dataset processing pipeline |
| `merge_dataset` | Merge processed chunks into final output |
| `plan_chunks` | Plan optimal chunking strategy |
| `embedding` | Generate embeddings |
| `index` | Index data |
| `validate` | Validate data |

### Job Lifecycle

```
┌─────────────┐
│   pending   │  ← Jobs enter here from dispatch
└──────┬──────┘
       │ worker claims job (with lease)
       ▼
┌─────────────┐
│  assigned   │  ← Lease acquired (lease_expires_at set)
└──────┬──────┘
       │ start execution
       ▼
┌─────────────┐
│  running    │  ← Job actively executing
└──────┬──────┘
       │ complete_job()
       ▼
┌─────────────┐    ┌─────────────┐
│ completed   │    │   failed    │
└─────────────┘    └──────┬──────┘
                            │ retry_count < max_retries
                            ▼
                      ┌─────────────┐
                      │   pending   │  ← Re-queued
                      └─────────────┘
                            │ retry_count >= max_retries
                            ▼
                      ┌─────────────┐
                      │dead_letter │  ← Max retries exceeded
                      └─────────────┘
```

### State Machine Rules
- **Initial state**: Must be `pending`
- **pending →**: `assigned`, `cancelled`, `failed`
- **assigned →**: `running`, `pending` (requeue), `cancelled`, `failed`
- **running →**: `completed`, `failed`, `pending` (requeue)
- **completed/failed/dead**: **Terminal states** - cannot be changed!

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Supabase Backend                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │   Devices    │  │  Agent Jobs  │  │   Executions/Pipeline │  │
│  └──────────────┘  └──────────────┘  └──────────────────────┘  │
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
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Job Queue   │  │ Worker State│  │  Results Cache        │  │
│  │ (Streams)  │  │  (Hash)     │  │  (TTL keys)          │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
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
│  └────────────┘  └────────────┘  └────────────┘  └─────────┘  │
│                                                                  │
│  ┌─────────────────────┐  ┌──────────────────────────────┐    │
│  │  Runtime Manager    │  │   Environment Pool (v2)      │    │
│  │  (Python/Node)     │  │   (Warm pools for fast start)   │    │
│  └─────────────────────┘  └──────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

---

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

Plugins are dynamically loaded executable code that extend agent capabilities. The system supports **Python**, **Node.js**, **Go**, **Rust**, and native binaries.

### Plugin Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Plugin System                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  Manifest   │  │   Storage   │  │   Runtime   │  │
│  │  (JSON)    │  │  (Supabase)  │  │ (Python/Node) │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
│         │                 │                 │                 │
│         ▼                 ▼                 ▼                 │
│  ┌──────────────────────────────────────────────────┐  │
│  │          Plugin Manager (Go)                     │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐ │  │
│  │  │  Fetch    │  │ Validate  │  │  Execute  │ │  │
│  │  │  (HTTP)   │  │ (Sig/Hex) │  │ (Sandbox) │ │  │
│  │  └────────────┘  └────────────┘  └────────────┘ │  │
│  └──────────────────────────────────────────────────┘  │
│         │                 │                 │                 │
│         ▼                 ▼                 ▼                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Key Store  │  │  Bundled   │  │  Reports   │  │
│  │(Supabase)  │  │  Plugins    │  │(Supabase)  │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### Plugin Manifest Structure

```go
type Manifest struct {
    // Identity
    Name     string `json:"name"`          // Plugin name (required)
    Version  string `json:"version,omitempty"` // Version string
    Filename string `json:"filename,omitempty"` // Binary filename (required)
    URL      string `json:"url,omitempty"`      // Download URL

    // Integrity
    Checksum string `json:"checksum,omitempty"` // SHA-256 hex (required)

    // Classification
    PluginType string `json:"plugin_type,omitempty"` // "core" | "client"
    Language   string `json:"language,omitempty"`    // rust, python, go, node, etc.

    // 🔒 TRUST (NON-NEGOTIABLE)
    Trusted bool `json:"trusted"` // MUST be true for execution

    // 🔒 SANDBOX PERMISSIONS
    Network bool `json:"network"` // default = false (explicit allow)

    // 🔒 RESOURCE LIMITS (MANDATORY)
    Resources PluginResources `json:"resources"`

    // 🔒 MANIFEST SIGNATURE (NON-NEGOTIABLE)
    Signature         string `json:"signature,omitempty"`        // Base64-encoded Ed25519 signature
    SignatureKeyID    string `json:"signature_key_id,omitempty"` // Key identifier
    SignatureVerified bool   `json:"signature_verified"`     // Set true after verification
}

type PluginResources struct {
    // Memory limit in MB (REQUIRED)
    MemoryMB int64 `json:"memory_mb"`

    // Total CPU time allowed in seconds (REQUIRED)
    CPUSeconds int64 `json:"cpu_seconds"`

    // CPU quota (container-based runtimes only)
    CPULimit float64 `json:"cpu_limit,omitempty"`

    // Wall-clock execution timeout (REQUIRED)
    TimeoutSeconds int64 `json:"timeout_seconds"`
}
```

### Plugin Registration Flow

```
Agent Dashboard              Supabase Edge Function              Plugin Storage
      │                           │                                   │
      │  POST /register_plugin   │                                   │
      │ ──────Binary+Metadata──▶│                                   │
      │                           │  1. Validate JWT (admin only)      │
      │                           │  2. Get org signing key           │
      │                           │  3. Sign BINARY HASH (not metadata)│
      │                           │     using Ed25519               │
      │                           │  4. Upload binary to storage    │
      │                           │ ───────────────▶│
      │                           │  5. Insert plugin record        │
      │                           │  6. Enable for org (100%)      │
      │ ◀────plugin_id─────────│                                   │
```

### Plugin Execution Flow

```
┌─────────────────┐
│  Agent Job    │
│  (payload)    │
└──────┬────────┘
       │ plugin_id
       ▼
┌─────────────────────────────────┐
│  Plugin Executor (Go)          │
│                              │
│  1. Fetch plugin from DB      │
│  2. Verify Ed25519 sig       │
│  3. Check resource limits     │
│  4. Select runtime           │
│     ├─ Python (v2 manager)    │
│     ├─ Node.js (v2 manager)  │
│     └─ Native (CGO/win)       │
│  5. Execute in sandbox      │
│  6. Return result            │
└─────────────────────────────────┘
       │ ExecutionResult
       ▼
┌─────────────────┐
│  Report Job   │
│  (complete)   │
└─────────────────┘
```

### Execution Modes

| Mode | Description | Runtime | Sandbox |
|------|-------------|---------|---------|
| `native` | Direct binary execution | Native (CGO) | Process limits |
| `runtime` | Language runtime (Python/Node) | v2 Runtime Manager | Docker/Process |
| `docker` | Docker container execution | Container | Full isolation |

### Language Support

| Language | Runtime | Execution Method | Dependencies |
|----------|---------|-------------------|-------------|
| `python`, `python3` | Python interpreter | v2 Python runtime | requirements.txt / pypi |
| `node`, `nodejs`, `javascript`, `typescript` | Node.js | v2 Node runtime | package.json / npm |
| `go`, `rust`, `c`, `cpp`, `native` | Native binary | Direct execution | None (compiled) |
| `ruby`, `bash`, `shell` | System interpreter | Script execution | System packages |

### Resource Limits Enforcement

```go
// MANDATORY: All fields must be non-zero for execution
type PluginResources struct {
    MemoryMB       int64   // Memory limit in MB (required)
    CPUSeconds     int64   // CPU time in seconds (required)
    CPULimit      float64 // CPU quota for containers (optional)
    TimeoutSeconds int64   // Wall-clock timeout (required)
}

// Validation: Jobs MUST NOT run if limits missing
func (r PluginResources) HasLimits() bool {
    return r.MemoryMB > 0 &&
           r.CPUSeconds > 0 &&
           r.TimeoutSeconds > 0
}
```

### Plugin Signing (Ed25519)

**CRITICAL: The backend automatically signs the plugin - users just upload the binary!**

#### Key Generation (One-time per org)
When a new org is created via `create-user` edge function:
1. Backend reads `PLATFORM_SIGNING_PUBLIC_KEY_B64` and `PLATFORM_SIGNING_PRIVATE_KEY_B64` from environment
2. **Public key** stored in `plugin_signing_keys` table (`public_key` column)
3. **Private key** stored securely in **Supabase Vault** via `store_plugin_signing_key_to_vault()` RPC
4. Vault secret name stored in `plugin_signing_keys.vault_secret_name` column

#### Registration Flow (User uploads, backend signs):

```
User (Admin)                    Edge Function                    Vault + DB
    │                                │                                │
    │  POST /register_plugin        │                                │
    │  - binary file                │                                │
    │  - metadata                  │                                │
    │──────────────────────────────>│                                │
    │                                │                                │
    │                                │ 1. Validate JWT + admin role  │
    │                                │ 2. Upload binary to storage    │
    │                                │──────────────────────────────>│
    │                                │                                │
    │                                │ 3. Get signing key ID from    │
    │                                │    plugin_signing_keys table   │
    │                                │──────────────────────────────>│
    │                                │                                │
    │                                │ 4. Fetch vault_secret_name   │
    │                                │    from plugin_signing_keys    │
    │                                │──────────────────────────────>│
    │                                │                                │
    │                                │ 5. Decrypt private key from  │
    │                                │    Vault via decrypt_vault_   │
    │                                │    secret() RPC               │
    │                                │<──────────────────────────────│
    │                                │                                │
    │                                │ 6. Compute SHA-256 of binary │
    │                                │    bytes                       │
    │                                │ 7. Sign hash with private    │
    │                                │    key (Ed25519)              │
    │                                │ 8. Store signature in        │
    │                                │    plugins table (base64)      │
    │                                │──────────────────────────────>│
    │                                │                                │
    │                                │ 9. Enable plugin for org      │
    │                                │──────────────────────────────>│
    │                                │                                │
    │  201 { plugin_id, signature }  │                                │
    │<───────────────────────────────│                                │
```

**User does NOT need to:**
- Generate Ed25519 keys
- Sign the plugin
- Provide a signature

**The backend handles ALL cryptography** using keys stored in Supabase Vault.
┌─────────────┐
│  Dashboard/CLI                           │
│  (Admin user with JWT)                │
│                                          │
│  1. Upload plugin binary                │
│  2. Provide metadata (name, version,     │
│     language, plugin_type, checksum)       │
│  3. OPTIONALLY: signature_key_id         │
└─────────────┬──────────────────────────────┘
              │ multipart/form-data or JSON
              ▼
┌─────────────┐
│  POST /register_plugin (Edge Function)       │
│                                          │
│  ┌──────────────────────────────────────┐  │
│  │ 1. Validate JWT + Admin role        │  │
│  │ 2. Parse form data                  │  │
│  │ 3. Upload binary to Supabase storage │  │
│  │ 4. Get org's Ed25519 private key │  │
│  │    from plugin_signing_keys table      │  │
│  │ 5. Compute SHA-256 of binary bytes │  │
│  │ 6. Sign hash with private key      │  │
│  │ 7. Insert into plugins table         │  │
│  │    (storage_path, signature,         │  │
│  │     signature_key_id, trusted=true)   │  │
│  │ 8. Enable for org (org_plugins)   │  │
│  └──────────────────────────────────────┘  │
└─────────────┘
```

#### What the User Provides:
| Field | Required | Description |
|-------|----------|-------------|
| `name` | YES | Plugin name |
| `version` | YES | Version string |
| `language` | YES | python, node, go, etc. |
| `plugin_type` | YES | core, client |
| `binary` | YES | Plugin binary file |
| `checksum` | YES | SHA-256 hex checksum of binary |
| `signature_key_id` | NO | Override signing key (default: org's active key) |
| `resources` | NO | JSON string: `{"memory_mb":512,"cpu_seconds":300,"timeout_seconds":600}` |
| `network` | NO | true/false (default: false) |
| `runtime_dependencies` | NO | JSON array for v2 runtime |

#### What the Backend Does Automatically:
1. **Validates** user is admin of the org
2. **Uploads** binary to Supabase storage (`plugins/org/{org_id}/{plugin_id}/{filename}`)
3. **Fetches** org's Ed25519 private key from `plugin_signing_keys` table (or uses user-provided `signature_key_id`)
4. **Computes** SHA-256 hash of the binary bytes (NOT the metadata!)
5. **Signs** the hash with the Ed25519 private key
6. **Stores** signature in `plugins` table as base64
7. **Sets** `trusted=true`, `signature_verified=true`
8. **Enables** plugin for org in `org_plugins` table with 100% rollout

#### Signing Code (Edge Function):
```typescript
// FIXED: Sign the FILE BYTES, not the metadata
async function signManifest(pluginBinaryBytes, privKeyB64) {
    if (!privKeyB64) return null;

    // Hash the plugin FILE BYTES (SHA-256) - matches agent verification
    const hashBuffer = await crypto.subtle.digest("SHA-256", pluginBinaryBytes);

    // Import org's Ed25519 private key
    const privKeyBytes = Uint8Array.from(atob(privKeyB64), (c)=>c.charCodeAt(0));
    const privKey = await crypto.subtle.importKey("pkcs8", privKeyBytes, {
        name: "Ed25519"
    }, false, ["sign"]);

    // Sign the hash
    const sigBuffer = await crypto.subtle.sign({
        name: "Ed25519"
    }, privKey, hashBuffer);

    // Return base64 signature
    return btoa(String.fromCharCode(...new Uint8Array(sigBuffer)));
}

// Get org's active signing key from database
async function getOrgSigningKeyId(supabase, orgId) {
    const { data } = await supabase
        .from("plugin_signing_keys")
        .select("id")
        .eq("org_id", orgId)
        .is("revoked_at", null)
        .order("created_at", { ascending: false })
        .limit(1)
        .maybeSingle();
    return data?.id ?? null;
}
```

#### Registration Request Examples:

**Option 1: Multipart Form (file upload)**
```bash
curl -X POST https://your-project.supabase.co/functions/v1/register_plugin \
  -H "Authorization: Bearer <dashboard_jwt>" \
  -F "name=my-plugin" \
  -F "version=1.0.0" \
  -F "language=python" \
  -F "plugin_type=core" \
  -F "checksum=sha256:abc123..." \
  -F "binary=@./my-plugin.py" \
  -F "resources={\"memory_mb\":512,\"cpu_seconds\":300,\"timeout_seconds\":600}" \
  -F "network=true"
```

**Option 2: JSON (if binary already uploaded)**
```bash
POST /functions/v1/register_plugin
Headers:
  Authorization: Bearer <dashboard_jwt>
  Content-Type: application/json

Body:
{
  "name": "my-plugin",
  "version": "1.0.0",
  "language": "python",
  "plugin_type": "core",
  "storage_path": "plugins/org/uuid/filename",
  "checksum": "sha256:abc123...",
  "signature_key_id": "key-001",  // Optional: override default key
  "resources": {"memory_mb\":512,"cpu_seconds\":300},
  "runtime_dependencies": [{"name\":\"pandas\",\"version\":\"2.0.0\"}]
}
```

#### Response:
```json
{
  "ok": true,
  "plugin_id": "uuid",
  "storage_path": "plugins/org/uuid/my-plugin-1.0.0",
  "signature_verified": true,
  "trusted": true
}
```

#### Verification Process (Agent-side):
1. Fetch plugin record from `plugins` table (includes `signature`, `signature_key_id`)
2. Download plugin binary from `storage_path`
3. Compute SHA-256 hash of binary bytes
4. Fetch Ed25519 **public** key from `plugin_signing_keys` table using `signature_key_id`
5. Verify: `crypto.subtle.verify({name:"Ed25519"}, publicKey, signature, hashBuffer)`
6. **CRITICAL**: Only execute if:
   - `manifest.Trusted == true` AND
   - `manifest.SignatureVerified == true` AND
   - Signature verification passes
   - All resource limits are non-zero

#### Plugin Signing Keys Management:

| Function | Description |
|----------|-------------|
| `get_plugin_signing_key` | Get org's active signing key (Edge Function) |
| `list_plugin_signing_keys` | List all keys for org (Edge Function) |
| `plugin_signing_keys` table | Stores Ed25519 keys per org |

**Key Generation (outside this system):**
```bash
# Generate Ed25519 key pair (example using OpenSSL)
openssl genpkey -algorithm Ed25519 -out private_key.pem
openssl pkey -in private_key.pem -pubout -out public_key.pem

# Store PUBLIC key in plugin_signing_keys table
# Store PRIVATE key securely (backend reads from database)
```

**Important:** The private key must be stored in the `plugin_signing_keys` table for the org. The backend uses it to automatically sign plugins on registration.

### Bundled Plugins

Built-in plugins included with the agent:

| Plugin | Type | Language | Description |
|--------|------|----------|-------------|
| `scan_metadata` | core | python | Scan dataset structure and metadata |
| `merge_metadata` | core | python | Merge processed chunk metadata |

Bundled plugins are:
- **Pre-installed**: No download required
- **Trusted by default**: `Trusted: true`
- **Version-synced**: Match agent version

### Plugin Storage (Supabase)

| Table | Purpose |
|-------|---------|
| `plugins` | Plugin metadata, storage_path, signature |
| `org_plugins` | Org-plugin access, rollout %, enabled flag |
| `plugin_signing_keys` | Ed25519 public keys per org |
| `plugin_execution_history` | Execution tracking (start/end) |
| `plugin_executions` | Legacy execution log |

### Plugin API (Edge Functions)

#### `register_plugin` - Register new plugin
```bash
POST /functions/v1/register_plugin
Headers:
  Authorization: Bearer <dashboard_jwt>  # Admin only
Content-Type: multipart/form-data OR application/json

Body (multipart):
  name: "my-plugin"
  version: "1.0.0"
  language: "python"
  plugin_type: "core"
  binary: <plugin_file>
  checksum: "sha256:abc123..."
  network: "true"
  resources: '{"memory_mb":512,"cpu_seconds":300,"timeout_seconds":600}'
  runtime_dependencies: '[{"name":"pandas","version":"2.0.0"}]'

Response:
  { "ok": true, "plugin_id": "uuid", "storage_path": "...", "signature_verified": true }
```

#### `get_plugin` - Retrieve plugin
```bash
POST /functions/v1/get_plugin
Headers:
  Authorization: Bearer <token>
  x-agent-token: <device_token>
Body: { "plugin_id": "uuid" }

Response:
  {
    "id": "uuid",
    "name": "my-plugin",
    "version": "1.0.0",
    "language": "python",
    "plugin_type": "core",
    "storage_path": "plugins/org/uuid/my-plugin-1.0.0",
    "checksum": "sha256:abc123...",
    "signature": [base64_bytes],
    "signature_key_id": "key-001",
    "trusted": true,
    "resources": {"memory_mb": 512, "cpu_seconds": 300, "timeout_seconds": 600},
    "runtime_dependencies": [...]
  }
```

#### `list_plugins_for_org` - List available plugins
```bash
GET /functions/v1/list_plugins_for_org
Headers:
  Authorization: Bearer <token>

Response:
  {
    "ok": true,
    "plugins": [
      {
        "plugin_id": "uuid",
        "name": "my-plugin",
        "version": "1.0.0",
        "language": "python",
        "trusted": true,
        "rollout_percentage": 100,
        "os": "linux",
        "arch": "amd64"
      }
    ]
  }
```

#### `get_plugin_signing_key` - Get signing key
```bash
POST /functions/v1/get_plugin_signing_key
Headers:
  Authorization: Bearer <token>
  x-agent-token: <device_token>

Response:
  { "ok": true, "key": "<base64_public_key>" }
```

### Plugin Rollout Control

```go
// ShouldRunPlugin checks rollout % using MD5 hash of device ID
func ShouldRunPlugin(deviceID uuid.UUID, rolloutPercentage int) bool {
    if rolloutPercentage >= 100 {
        return true
    }
    if rolloutPercentage <= 0 {
        return false
    }

    // Hash-based rollout (consistent per device)
    hash := md5.Sum(deviceID[:])
    deviceValue := binary.BigEndian.Uint32(hash[:4]) % 100

    return int(deviceValue) < rolloutPercentage
}
```

Database function: `should_run_plugin(p_device_id, p_rollout_percentage)` → `boolean`

### Plugin Sandbox Isolation

```go
type SandboxResult struct {
    Output     string        `json:"output"`
    ExitCode   int           `json:"exit_code"`
    DurationMs int64         `json:"duration_ms"`
    Method     string        `json:"method"` // "docker", "native", "runtime"
}

// Execution with resource limits
func executeWithLimits(manifest Manifest, payload string) (*SandboxResult, error) {
    // Apply resource limits
    if !manifest.Resources.HasLimits() {
        return nil, errors.New("plugin missing resource limits")
    }

    // Set memory limit (ulimit -v)
    // Set CPU time limit (ulimit -t)
    // Set wall-clock timeout (context.WithTimeout)

    // Execute in sandbox
    return sandbox.Run(manifest, payload)
}
```

### Plugin Database Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `get_org_plugins` | `(p_org_id, p_os, p_arch)` → table | Get plugins available to org |
| `get_plugin_by_id` | `(p_plugin_id)` → table | Get plugin details by ID |
| `record_plugin_execution_start` | `(p_org_id, p_plugin_id, p_job_id, p_device_id)` → uuid | Record execution start |
| `record_plugin_execution_end` | `(p_execution_id, p_status, p_error)` → jsonb/void | Record execution end |
| `should_run_plugin` | `(p_device_id, p_rollout_percentage)` → boolean | Check rollout % |
| `compute_dependency_hash` | `(p_runtime_type, p_runtime_dependencies)` → text | Calculate dependency hash |
| `encode_plugin_signature` | `(sig)` → text | Encode signature to base64 |

### Plugin Security Model

1. **Ed25519 Signatures**: All plugins MUST be signed with org's private key
2. **Manifest Verification**: Signature verified against binary SHA-256 hash
3. **Trusted Flag**: `manifest.Trusted` MUST be true for execution
4. **Resource Limits**: All resource fields MANDATORY (memory, cpu, timeout)
5. **Sandbox Isolation**: Execution in Docker or process sandbox
6. **Network Control**: `manifest.Network` defaults to false
7. **Rollout Control**: Gradual deployment with percentage-based rollout
8. **Org Isolation**: Plugins scoped to organization via `org_plugins` table

### Redis Integration

For multi-agent deployments, Redis provides:

- **Job Queue**: Redis Streams with consumer groups
- **Worker State**: Real-time worker status tracking
- **Results Cache**: Fast access to job results
- **Pub/Sub**: Real-time notifications

---

## Security

- **Device Authentication**: Token-based with secure keyring storage
- **Plugin Signing**: Ed25519 signature verification
- **Sandbox Isolation**: Docker-based execution with network isolation
- **Row-Level Security**: Database-level org isolation
- **Org Validation**: All functions validate org_id to prevent cross-org access
- **Message Sanitization**: Error messages are sanitized to remove paths, IPs, emails
- **Rate Limiting**: Bootstrap and API endpoints have rate limiting
- **Cron Secret**: Internal functions protected by cron secret header

---

## Build

```bash
# Build the agent
make build

# Or manually
go build -o bin/sentra-agent ./cmd/main.go

# Build with version info
go build -ldflags="-X main.Version=1.0.0" -o bin/sentra-agent ./cmd/main.go
```

---

## Environment Pool Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT_POOL_MAX_SIZE` | 10 | Max pools to keep warm |
| `ENVIRONMENT_MAX_COUNT` | 50 | Max environments in pool |
| `ENVIRONMENT_WARM_TIMEOUT` | 30m | Time before cooling warm envs |
| `ENVIRONMENT_EVICTION_INTERVAL` | 5m | How often to run eviction |
| `ENVIRONMENT_MAX_DISK_BYTES` | 10GB | Max disk usage for envs |

---

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

---

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

---

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

### Stuck Jobs

- Run `SELECT * FROM public.cleanup_stuck_jobs()` to recover
- Check lease expiration times
- Verify device is still online

---

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

---

## License

Part of the Sentra Zero Compute Network.
