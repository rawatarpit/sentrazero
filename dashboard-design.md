# SentraZero Dashboard — Design & Implementation Guide

> **Self-hosted compute platform — zero configuration, infinite scale.**
> *Full-stack monitoring dashboard for pipeline executions, agent fleet, datasets, jobs, and plugins.*
> *Connects directly to the Supabase backend with real-time updates.*

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Authentication & Authorization](#2-authentication--authorization)
3. [Dashboard Pages & Data Sources](#3-dashboard-pages--data-sources)
4. [Real-Time Subscriptions](#4-real-time-subscriptions)
5. [Key Queries & Edge Functions](#5-key-queries--edge-functions)
6. [RLS Policies for Dashboard](#6-rls-policies-for-dashboard)
7. [Quick Start: Single HTML File](#7-quick-start-single-html-file)
8. [E2E Test Checklist](#8-e2e-test-checklist)

---

## 1. Architecture Overview

```
                     Browser (Dashboard SPA)
  +-------------+ +-------------+ +-------------+ +------------------+
  |  Pipelines  | |   Agents    | |  Datasets   | | Jobs / Plugins / |
  |    Page     | |    Page     | |    Page     | | Storage/Settings |
  +------+------+ +------+------+ +------+------+ +--------+---------+
         |               |               |                  |
         +---------------+---------------+------------------+
                         |
             Supabase Client SDK (supabase-js)
        Auth | Realtime | Query | Storage | Functions
                         |
                    HTTPS / WebSocket
                         v
                     Supabase Backend
  +-------------------+ +------------------+ +----------------------+
  |    PostgreSQL     | |    Realtime      | |  Edge Functions (73) |
  |   (33 tables)    | |   (WebSocket)    | |  (API endpoints)     |
  |   + RLS           | |   + SSE stream   | |                      |
  |   + Functions     | |   + Polling      | |  run_pipeline        |
  +--------+----------+ +--------+---------+ |  advance_pipeline    |
           |                    |            |  claim_jobs_for_device|
           v                    v            |  list_plugins_for_org |
  +--------------------------------+          |  get_dataset_output   |
  | Storage (S3-compatible)        |          |  ...and 68 more      |
  | - Dataset files                |          +----------------------+
  | - Plugin binaries              |
  | - Merged outputs               |
  +--------------------------------+
```

### Tech Stack (Recommended)

| Layer | Technology | Why |
|:------|:-----------|:----|
| Framework | Vanilla HTML/JS or React/Vue/Svelte | Any SPA works; Supabase SDKs for all |
| Supabase SDK | @supabase/supabase-js v2 | Direct DB queries, Realtime, Auth |
| Auth | Supabase Auth (email+password or magic link) | Built-in JWT, RLS-compatible |
| Real-time | Supabase Realtime (WebSocket) + agent_stream SSE | Live job/device updates |
| CSS | Tailwind CDN or plain CSS | No build step for quick start |
| Hosting | Static file or Supabase Storage | No backend server needed |

### Connection Configuration

```env
SUPABASE_URL=https://ivwghcveytrkwqxxdtak.supabase.co
SUPABASE_ANON_KEY=<your-anon-key-from-supabase-dashboard>
SUPABASE_SERVICE_ROLE_KEY=<keep-secret-not-for-browser>
```

The dashboard uses the **anon key** with the user's JWT token (from Supabase Auth login). RLS policies enforce org-level access. Never expose the service_role key in the browser.

---

## 2. Authentication & Authorization

### Two Auth Paths

| Who | Auth Method | Token | Used For |
|:----|:------------|:------|:---------|
| **Dashboard user** (human) | Supabase Auth email+password or magic link | JWT in Authorization header | Dashboard UI, queries through supabase-js |
| **Agent device** (Go binary) | x-agent-token header (HMAC-SHA256 of device token) | Pre-issued token, stored in devices table | Agent Edge Functions (claim_jobs, agent_stream, etc.) |

### Dashboard Login Flow

1. User opens dashboard -> sees login form
2. User enters email+password (or clicks magic link)
3. Supabase Auth returns JWT
4. All subsequent supabase-js calls include JWT in Authorization header
5. RLS policies filter data by `org_id` (derived from `org_members` table)

### Org Resolution

```sql
-- The dashboard queries org_members to get the user's org_id
SELECT org_id FROM org_members WHERE user_id = auth.uid();
```

### Edge Function Auth for Dashboard

For Edge Functions that need to be called from the dashboard (not agent), use:

```typescript
// Client-side call with user's JWT
const { data, error } = await supabase.functions.invoke("run_pipeline", {
  body: { dataset_id, pipeline_template_id, org_id }
});
// The Edge Function reads the user's JWT from Authorization header
// and resolves org_id from org_members
```

**Important:** Many Edge Functions currently authenticate via `x-agent-token` (for device auth). For dashboard use, you have two options:
1. Call Edge Functions with the service_role key (from a backend proxy)
2. Query Supabase tables directly via supabase-js with RLS

**Recommended for dashboard:** Query tables directly via supabase-js (with user's JWT + RLS). Use Edge Functions only for actions that need service_role (running pipelines, registering devices, etc.).

---

## 3. Dashboard Pages & Data Sources

### Page 1: Home / Overview Dashboard

```
+--------------------------------------------------------------+
|  SentraZero Dashboard                    [User] [Logout]     |
+--------------------------------------------------------------+
| +--------+ +---------+ +---------+ +--------+                |
| | Active | | Running | | Success | | Agents |                |
| | Agents | |  Jobs   | | Rate    | | Online |                |
| |   3    | |    2    | |  100%   | |   3/4  |                |
| +--------+ +---------+ +---------+ +--------+                |
|                                                              |
| Recent Pipeline Runs                                         |
| +--------------------------------------------------------+  |
| | Execution ID | Pipeline | Status | Duration | Agent    |  |
| | 0145fec7...  | Dup V1   | Done   | 14.4s    | sentra   |  |
| | 1368d977...  | Scrape   | Done   | 41.0s    | vcnsentra|  |
| | 931dd7fe...  | Cat Img  | Done   | 81.1s    | sentra   |  |
| +--------------------------------------------------------+  |
|                                                              |
| Agent Health                               [See All]         |
| +--------------------------------------------------------+  |
| | Device     | Status  | Jobs  | CPU  | Mem  | Uptime    |  |
| | sentra     | ONLINE  | 0     | 45%  | 55%  | 2h 14m    |  |
| | vcnsentra  | ONLINE  | 0     | 30%  | 74%  | 3h 01m    |  |
| | sentrazero | ONLINE  | 0     | 22%  | 56%  | 2h 14m    |  |
| | Arpit.local| STOPPED | -     | -    | -    | -         |  |
| +--------------------------------------------------------+  |
+--------------------------------------------------------------+
```

**Data Sources:**

| Card | Supabase Query |
|:-----|:---------------|
| Active Agents | `devices.select('*', {count: 'exact'}).in('status', ['online','available','busy'])` |
| Running Jobs | `agent_jobs.select('*', {count: 'exact'}).in('status', ['assigned','running'])` |
| Success Rate | `executions.select('status').neq('status', 'running')` (calc % completed) |
| Recent Runs | `executions.select('*, pipeline_templates!inner(name)').order('created_at', {ascending: false}).limit(10)` |
| Agent Health | `devices.select('*').order('last_heartbeat', {ascending: false})` |

**RLS:** All queries filtered by `org_id = (SELECT org_id FROM org_members WHERE user_id = auth.uid())`

---

### Page 2: Pipeline Executions

```
Pipeline Runs                              [Filter: All | Running | Done | Failed]
+------------------------------------------------------------------------------+
| Execution ID | Pipeline    | Dataset | Steps | Status  | Duration | Agent   |
| 0145fec7...  | Dup Detect  | 19 rows | 2/2   | DONE    | 14.4s    | sentra  |
| 1368d977...  | Sc Compare  | 5 SKUs  | 2/2   | DONE    | 41.0s    | vcnsentra|
| 931dd7fe...  | Cat Image   | 202 imgs| 3/3   | DONE    | 81.1s    | sentra  |
| 1b667dd0...  | Sc Compare  | 9 URL   | 2/2   | CHUNK   | 64.8s    | Arpit   |
+------------------------------------------------------------------------------+

Click on a row -> Execution Detail

Execution Detail: 0145fec7-92f8-4540-a32d-15c35fc83a0d
+------------------------------------------------------------------------------+
| Overview:                                                                    |
| Pipeline: Walmart Duplicate Detection v1 | Dataset: baselining_19rows_csv    |
| Status: DONE | Total Duration: 14.4s | Agent: sentra (d2a7c7e7)             |
|                                                                              |
| Steps:                                                                       |
| Step | Plugin           | Status | Duration | Output                         |
| 0    | walmart_search   | DONE   | 8.2s     | 19 candidates found           |
| 1    | dup_classify     | DONE   | 6.2s     | 19/19 classified              |
|      | Merge            | DONE   | 1.75s    | baselining19rowscsv_merged.csv |
|                                                                              |
| Output File: s3://datasets/bd1d8e8e-.../baselining19rowscsv_merged.csv (6 KB)|
+------------------------------------------------------------------------------+
```

**Data Sources:**

| Element | Query |
|:--------|:------|
| Execution list | `executions.select('*, pipeline_templates(name), datasets(name)').order('created_at', {ascending: false})` |
| Execution detail | `executions.select('*').eq('id', executionId).single()` |
| Execution steps | `execution_steps.select('*').eq('execution_id', executionId).order('step_index')` |
| Agent jobs for exec | `agent_jobs.select('*').eq('execution_id', executionId)` |
| Dataset info | `datasets.select('*').eq('id', datasetId).single()` |

**Status Values:**

| executions.status | Icon | Meaning |
|:------------------|:-----|:--------|
| `running` | spinner | Pipeline is actively executing |
| `completed` | green check | All steps completed successfully |
| `completed_with_warnings` | yellow warning | Steps completed with degraded results |
| `failed` | red X | Pipeline failed |

| execution_steps.status | Meaning |
|:-----------------------|:--------|
| `pending` | Not yet started |
| `running` | Actively executing |
| `completed` | Step finished successfully |
| `failed` | Step errored |
| `failed_continuing` | Step failed but pipeline continues |
| `skipped` | Step was skipped |
| `awaiting_approval` | Requires manual approval |

---

### Page 3: Agent Fleet

```
Agent Fleet                          [Filter: Online | Offline | Busy | All]
+------------------------------------------------------------------------------+
| Device     | Status  | Arch  | CPU  | Mem Free | Jobs | Last Heartbeat      |
| sentra     | ONLINE  | amd64 | 45%  | 0.55 GB  | 0    | Jul 19 18:11        |
| vcnsentra  | ONLINE  | amd64 | 30%  | 4.29 GB  | 0    | Jul 19 18:11        |
| sentrazero | ONLINE  | amd64 | 22%  | 0.52 GB  | 0    | Jul 19 13:21        |
| Arpit.local| STOPPED | amd64 | -    | -        | -    | Jul 19 17:30 (last) |
+------------------------------------------------------------------------------+

Click on a device -> Device Detail

Device: sentra (d2a7c7e7-45f0-4e6a-8f46-8a923d5cf01f)
+------------------------------------------------------------------------------+
| Overview:                                                                    |
| Status: ONLINE | Org: 5236b19e-... | Last Heartbeat: Jul 19 18:11            |
| Arch: amd64 | OS: linux | Python: 3.x | Node: 18.x                          |
|                                                                              |
| Resources:                                                                   |
| CPU: 2 cores (45% used)  |  Memory: 0.93 GB total / 0.55 GB free             |
| GPU: None                |  Disk: -                                          |
| Network Latency: 1.49 ms |  Active Workers: 0                               |
|                                                                              |
| Recent Jobs:                                                                 |
| Job ID  | Type   | Pipeline   | Status | Duration | Date                    |
| ...     | merge  | Dup V1     | DONE   | 1.75s    | Jul 19 18:11            |
| ...     | process| Cat Img    | DONE   | 81.1s    | Jul 19 13:22            |
|                                                                              |
| Metrics History:                                                             |
| [Line chart: CPU %, Memory %, Active Workers over last 24h]                  |
+------------------------------------------------------------------------------+
```

**Data Sources:**

| Element | Query |
|:--------|:------|
| Device list | `devices.select('*').order('last_heartbeat', {ascending: false})` |
| Device detail | `devices.select('*').eq('id', deviceId).single()` |
| Device metrics history | `agent_metrics.select('*').eq('device_id', deviceId).order('created_at', {ascending: false}).limit(50)` |
| Device jobs | `agent_jobs.select('*').eq('agent_id', deviceId).order('created_at', {ascending: false}).limit(20)` |

**Status Mapping:**

| devices.status | Dashboard Display |
|:---------------|:------------------|
| `online` | GREEN badge |
| `available` | GREEN badge |
| `busy` | YELLOW badge (with 'N jobs') |
| `offline` | GRAY badge |
| `error` | RED badge |
| `draining` | ORANGE badge |

---

### Page 4: Datasets

```
Datasets                                     [Filter: Merged | Processing | All]
+------------------------------------------------------------------------------+
| Dataset ID  | Name            | Files | Size   | Status   | Created          |
| 05df69f0..  | cat-images      | 202   | 14 MB  | MERGED   | Jul 17 2026      |
| bd1d8e8e..  | baselining_csv  | 1     | 2 KB   | MERGED   | Jul 17 2026      |
| 72e32858..  | validation_csv  | 1     | 1 KB   | MERGED   | Jul 17 2026      |
+------------------------------------------------------------------------------+

Click on a dataset -> Dataset Detail

Dataset: cat-images-fresh-scan (05df69f0-...)
+------------------------------------------------------------------------------+
| Overview:                                                                    |
| Status: MERGED | Type: image/jpg | File Count: 202 | Total Size: 14.26 MB    |
| Storage: S3 (supabase.co) | Source Path: datasets/05df69f0-.../              |
|                                                                              |
| Pipeline Runs on this Dataset:                                               |
| Execution ID  | Pipeline    | Status | Duration | Date                      |
| 931dd7fe...   | Cat Image   | DONE   | 81.1s    | Jul 19 2026               |
| 5249d78c...   | Cat Image   | DONE   | 27.1s    | Jul 17 2026               |
|                                                                              |
| Merged Output:                                                               |
| File: cat-images-fresh-scan_merged.csv (846 KB)                              |
| Path: s3://datasets/05df69f0-.../cat-images-fresh-scan_merged.csv           |
| [Download] [Preview]                                                         |
+------------------------------------------------------------------------------+
```

**Data Sources:**

| Element | Query |
|:--------|:------|
| Dataset list | `datasets.select('*').order('created_at', {ascending: false})` |
| Dataset detail | `datasets.select('*').eq('id', datasetId).single()` |
| Runs on dataset | `executions.select('*, pipeline_templates(name)').eq('dataset_id', datasetId)` |
| Dataset output | Edge Function `get_dataset_output` or direct Storage check |

**Datasets status workflow:**
```
registered -> scanning -> scanned -> chunked -> processing -> merge_pending -> merging -> merged
                                                                                             |
                                                                                           failed
```

---

### Page 5: Jobs Queue

```
Jobs Queue                                   [Filter: Pending | Running | Failed | Dead Letter]
+------------------------------------------------------------------------------+
| Job ID   | Type    | Status     | Agent    | Pipeline  | Created    | Duration|
| ...      | process | RUNNING    | sentra   | Cat Img   | 13:22:01   | 45.2s   |
| ...      | merge   | ASSIGNED   | vcnsentra| Sc Compare| 18:10:00   | -       |
| ...      | scan    | PENDING    | -        | -         | 13:21:00   | -       |
| ...      | process | COMPLETED  | vcnsentra| Sc Compare| 18:10:01   | 39.1s   |
| ...      | process | FAILED     | Arpit    | Sc Compare| 22:02:00   | 12.3s   |
| ...      | process | DEAD       | -        | -         | 15:21:00   | -       |
+------------------------------------------------------------------------------+

Job Status Distribution:
[PENDING: 3] [ASSIGNED: 1] [RUNNING: 2] [COMPLETED: 145] [FAILED: 5] [DEAD: 2]

Dead Letter Queue:
+------------------------------------------------------------------------------+
| Job ID   | Type    | Error          | Retries | Last Attempt | Original Payload|
| ...      | process | Rate limited   | 3/3     | Jul 15 16:52 | {...}           |
| ...      | scan    | Timeout        | 3/3     | Jul 15 15:21 | {...}           |
+------------------------------------------------------------------------------+
```

**Data Sources:**

| Element | Query |
|:--------|:------|
| All active jobs | `agent_jobs.select('*').not('status', 'eq', 'completed').order('created_at', {ascending: false})` |
| Dead letter | `agent_jobs.select('*').eq('dead_lettered', true).order('created_at', {ascending: false})` |
| Job status counts | `agent_jobs.select('status')` (aggregate client-side or use a custom RPC) |
| Job detail | `agent_jobs.select('*, executions!inner(id, status), execution_steps!inner(step_index, plugin_id)').eq('id', jobId).single()` |

**agent_jobs.status values:**
```
pending -> assigned -> running -> completed
                                  -> failed -> (retry) -> assigned
                                                       -> dead (dead_lettered=true)
```

---

### Page 6: Pipeline Templates

```
Pipeline Templates
+------------------------------------------------------------------------------+
| Template ID | Name                     | Steps | Created By | Last Run       |
| ba416317..  | Cat Image Pipeline       | 3     | system     | Jul 19 2026    |
| 94211941..  | Walmart Dup Detect v1    | 2     | system     | Jul 19 2026    |
| 36b0c68e..  | Walmart Scrape & Comp v2 | 2     | system     | Jul 19 2026    |
+------------------------------------------------------------------------------+

Click to expand -> Template Detail

Pipeline: Walmart Duplicate Detection v1 (94211941-...)
+------------------------------------------------------------------------------+
| Steps Configuration:                                                         |
| Step 0: walmart_search.py v1.3.0                                            |
|   Config: { "olostep_api_key": "...", "max_candidates": 3, ... }            |
| Step 1: dup_classify.py v1.1.0                                              |
|   Config: { "candidate_threshold": 0.3, "dup_id_threshold": 0.5 }           |
|                                                                              |
| Execution History: 2 runs (1 local, 1 cloud)                                |
| Latest: 0145fec7 - 14.4s - 19/19 success                                    |
+------------------------------------------------------------------------------+
```

**Data Sources:**

| Element | Query |
|:--------|:------|
| All templates | `pipeline_templates.select('*').order('created_at', {ascending: false})` |
| Template detail | `pipeline_templates.select('*').eq('id', templateId).single()` |
| Template executions | `executions.select('*').eq('pipeline_template_id', templateId).order('created_at', {ascending: false}).limit(20)` |

The `steps` column is a JSONB array. Each element has:
```json
{
  "plugin_id": "uuid",
  "plugin_name": "scrape.py",
  "plugin_version": "1.3.2",
  "config": { ... },
  "runtime_override": null
}
```

---

### Page 7: Plugin Registry

```
Plugin Registry
+------------------------------------------------------------------------------+
| Plugin Name   | Version | Type    | Source   | Enabled | Rollout | Trusted  |
| scrape.py     | 1.3.2   | python  | built-in | YES     | 100%    | YES      |
| compare.py    | 1.4.0   | python  | built-in | YES     | 100%    | YES      |
| image_load    | 1.0.0   | python  | built-in | YES     | 100%    | YES      |
| image_resize  | 1.0.0   | python  | built-in | YES     | 100%    | YES      |
| image_embed   | 1.0.0   | python  | built-in | YES     | 100%    | YES      |
| walmart_search| 1.3.0   | python  | built-in | YES     | 100%    | YES      |
| dup_classify  | 1.1.0   | python  | built-in | YES     | 100%    | YES      |
+------------------------------------------------------------------------------+

Click on a plugin -> Plugin Detail

Plugin: scrape.py v1.3.2
+------------------------------------------------------------------------------+
| Metadata:                                                                     |
| Plugin ID:  ... | Group: builtin | Language: python                          |
| Network: YES | Timeout: 300s | Memory: 512 MB                               |
| Dependencies: pandas, beautifulsoup4, lxml, requests, curl_cffi             |
|                                                                              |
| Execution History:                                                            |
| Job ID  | Device    | Status  | Duration | Date                             |
| ...     | vcnsentra | DONE    | 25.3s    | Jul 19 2026                      |
| ...     | Arpit.loc | DONE    | 30.1s    | Jul 18 2026                      |
|                                                                              |
| Signing Key: ed25519:#...                                                    |
| Manifest: [View JSON]                                                        |
+------------------------------------------------------------------------------+
```

**Data Sources:**

| Element | Query |
|:--------|:------|
| All plugins | Edge Function `list_plugins_for_org` (joins plugins + org_plugins tables) |
| Plugin detail | `plugins.select('*').eq('id', pluginId).single()` |
| Org plugin config | `org_plugins.select('*').eq('plugin_id', pluginId)` |
| Plugin exec history | `plugin_execution_history.select('*').eq('plugin_id', pluginId).order('started_at', {ascending: false}).limit(20)` |

**Tables involved:**

- `plugins` - Global plugin registry (name, version, language, dependencies, manifest)
- `org_plugins` - Org-specific settings (enabled, rollout_percentage)
- `plugin_signing_keys` - Ed25519 signing keys for verification
- `plugin_execution_history` - Execution records per plugin

---

### Page 8: Storage & Settings

```
Storage Configurations
+------------------------------------------------------------------------------+
| Config ID | Bucket    | Provider | Region  | Type      | Default | Status   |
| ...       | datasets  | supabase | auto    | S3        | YES     | CONNECTED|
| ...       | plugins   | supabase | auto    | S3        | NO      | CONNECTED|
+------------------------------------------------------------------------------+

Org Settings
+------------------------------------------------------------------------------+
| Org ID: 5236b19e-...  | Name: SentraZero  | Plan: enterprise                |
| Members: 2  | Devices: 4  | Claim Code: SENTRA2026                         |
|                                                                              |
| Quotas:                                                                      |
| Max Devices: 100 | Max Concurrent Jobs: 500 | Max Storage: 1000 GB          |
| Max Plugins: 50  | Max Members: 10                                          |
+------------------------------------------------------------------------------+
```

**Data Sources:**

| Element | Query |
|:--------|:------|
| Storage configs | `org_storage_configs.select('*').eq('org_id', orgId)` |
| Org info | `orgs.select('*').eq('id', orgId).single()` |
| Org members | `org_members.select('*, users!inner(email)').eq('org_id', orgId)` |
| Quotas | `plan_limits.select('*').eq('plan_name', orgPlan)` |

---

## 4. Real-Time Subscriptions

### Supabase Realtime (WebSocket)

The dashboard subscribes to changes on key tables for live updates:

```typescript
// Connect to Realtime
const channel = supabase.channel('dashboard-live')

// Listen for new/updated executions
channel.on(
  'postgres_changes',
  { event: '*', schema: 'public', table: 'executions', filter: `org_id=eq.${orgId}` },
  (payload) => { /* update executions list */ }
)

// Listen for job status changes
channel.on(
  'postgres_changes',
  { event: '*', schema: 'public', table: 'agent_jobs', filter: `org_id=eq.${orgId}` },
  (payload) => { /* update jobs queue */ }
)

// Listen for device heartbeats
channel.on(
  'postgres_changes',
  { event: '*', schema: 'public', table: 'devices', filter: `org_id=eq.${orgId}` },
  (payload) => { /* update agent fleet */ }
)

// Listen for new metrics
channel.on(
  'postgres_changes',
  { event: 'INSERT', schema: 'public', table: 'agent_metrics', filter: `org_id=eq.${orgId}` },
  (payload) => { /* update metrics charts */ }
)

channel.subscribe()
```

### Polling Fallback

If Realtime is unavailable (firewall, corporate proxy), fall back to polling:

| Table | Poll Interval | Reason |
|:------|:-------------|:-------|
| `executions` | 10s | Low frequency updates |
| `agent_jobs` | 5s | Medium frequency |
| `devices` | 15s | Heartbeat updates |
| `agent_metrics` | 30s | Historical data, not live critical |

### Agent SSE Stream (for device status)

The `agent_stream` Edge Function provides SSE (Server-Sent Events) for agent devices. For the dashboard, Supabase Realtime is the preferred channel.

---

## 5. Key Queries & Edge Functions

### Direct Supabase Queries (via supabase-js)

These are the primary data access patterns for the dashboard:

```typescript
const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  global: { headers: { Authorization: `Bearer ${jwt}` } }
})
const orgId = '5236b19e-...' // resolved from org_members
```

**Dashboard Summary:**
```typescript
// Summary cards
const { count: activeAgents } = await supabase
  .from('devices')
  .select('*', { count: 'exact', head: true })
  .in('status', ['online', 'available', 'busy'])
  .eq('org_id', orgId)

const { count: runningJobs } = await supabase
  .from('agent_jobs')
  .select('*', { count: 'exact', head: true })
  .in('status', ['assigned', 'running'])
  .eq('org_id', orgId)

// Recent 10 executions with pipeline name
const { data: recentExecs } = await supabase
  .from('executions')
  .select('id, status, current_step_index, total_steps, created_at, completed_at, pipeline_templates(name), datasets(name)')
  .eq('org_id', orgId)
  .order('created_at', { ascending: false })
  .limit(10)
```

**Execution Detail:**
```typescript
// Execution + steps
const { data: exec } = await supabase
  .from('executions')
  .select('*, pipeline_templates(*), datasets(*)')
  .eq('id', executionId)
  .single()

const { data: steps } = await supabase
  .from('execution_steps')
  .select('*')
  .eq('execution_id', executionId)
  .order('step_index')
```

**Device Detail:**
```typescript
const { data: device } = await supabase
  .from('devices')
  .select('*')
  .eq('id', deviceId)
  .single()

const { data: metrics } = await supabase
  .from('agent_metrics')
  .select('*')
  .eq('device_id', deviceId)
  .order('created_at', { ascending: false })
  .limit(50)
```

### Edge Functions (for write operations)

Edge Functions that create/modify data (need service_role):

| Function | Method | Purpose | Dashboard Usage |
|:---------|:-------|:--------|:----------------|
| `run_pipeline` | POST | Trigger a pipeline execution | "Run Pipeline" button |
| `advance_pipeline` | POST | Advance a pipeline to next step | Internal, not direct |
| `register_device` | POST | Register a new agent device | "Add Agent" form |
| `register_plugin` | POST | Register a new plugin | "Upload Plugin" form |
| `set_plugin_trust` | POST | Toggle plugin trust | Plugin management |

For dashboard to call these with user auth:

```typescript
// Option 1: Call with user's JWT (function must support user auth)
const { data, error } = await supabase.functions.invoke('run_pipeline', {
  body: { dataset_id, pipeline_template_id, org_id }
})

// Option 2: Use a backend proxy (adds service_role key server-side)
// POST /api/proxy -> adds service_role key -> calls Edge Function
```

**Note:** Many Edge Functions currently expect `x-agent-token` auth (device-level). For the dashboard, either:
1. Modify Edge Functions to also accept `Authorization: Bearer <user-jwt>` and resolve via `org_members`
2. Call with service_role key from a trusted backend proxy
3. Query DB directly via supabase-js for reads, use a thin API layer for writes

**Recommended approach for E2E testing:** Query DB directly for reads (simpler, uses RLS) and call Edge Functions via the service_role key (from a secure context or .env) for writes.

---

## 6. RLS Policies for Dashboard

### Current RLS State

From the schema, most tables have FORCE ROW LEVEL SECURITY enabled but the policies primarily use `org_id` filtering:

```sql
-- Typical RLS pattern (from 20260614000003_policies.sql)
CREATE POLICY "Users can access their org's data"
  ON executions
  FOR ALL
  USING (
    org_id IN (
      SELECT org_id FROM org_members WHERE user_id = auth.uid()
    )
  );
```

### RLS for Dashboard Access

For the dashboard to work correctly, ensure RLS policies exist for:

| Table | Required Policy |
|:------|:----------------|
| `executions` | SELECT WHERE org_id IN (user's orgs) |
| `execution_steps` | SELECT via JOIN to executions |
| `agent_jobs` | SELECT WHERE org_id IN (user's orgs) |
| `devices` | SELECT WHERE org_id IN (user's orgs) |
| `agent_metrics` | SELECT via device_id JOIN to devices |
| `datasets` | SELECT WHERE org_id IN (user's orgs) |
| `pipeline_templates` | SELECT WHERE org_id IN (user's orgs) |
| `plugins` | SELECT (global, no restriction) |
| `org_plugins` | SELECT WHERE org_id IN (user's orgs) |
| `org_members` | SELECT WHERE user_id = auth.uid() |
| `orgs` | SELECT WHERE id IN (user's orgs) |
| `org_storage_configs` | SELECT WHERE org_id IN (user's orgs) |

### Verify RLS with a Test Query

```sql
-- Run in Supabase SQL Editor to verify RLS
SELECT * FROM executions LIMIT 5; -- Should only show your org's executions
SELECT * FROM devices LIMIT 5;    -- Should only show your org's devices
SELECT * FROM agent_jobs LIMIT 5; -- Should only show your org's jobs
```

If the dashboard is getting empty results but you know data exists, RLS policies may need to be added.

---

## 7. Quick Start: Single HTML File

### Option A: Standalone HTML (no build step)

Create `dashboard.html` in the repo root:

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>SentraZero Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-50">
  <div id="app" class="max-w-7xl mx-auto p-6">
    <!-- Login Form -->
    <div id="login-form" class="max-w-md mx-auto mt-20">
      <h1 class="text-2xl font-bold mb-6">SentraZero Dashboard</h1>
      <input id="email" type="email" placeholder="Email" class="w-full p-2 border rounded mb-2">
      <input id="password" type="password" placeholder="Password" class="w-full p-2 border rounded mb-2">
      <button onclick="login()" class="w-full bg-blue-600 text-white p-2 rounded">Sign In</button>
    </div>

    <!-- Dashboard Content (shown after auth) -->
    <div id="dashboard" class="hidden">
      <!-- Summary Cards -->
      <div class="grid grid-cols-4 gap-4 mb-8" id="summary-cards"></div>
      <!-- Recent Executions -->
      <div class="bg-white rounded-lg shadow p-6 mb-8">
        <h2 class="text-xl font-semibold mb-4">Recent Pipeline Runs</h2>
        <table class="w-full" id="executions-table"></table>
      </div>
      <!-- Agent Health -->
      <div class="bg-white rounded-lg shadow p-6">
        <h2 class="text-xl font-semibold mb-4">Agent Fleet</h2>
        <table class="w-full" id="agents-table"></table>
      </div>
    </div>
  </div>

  <script>
    const SUPABASE_URL = 'https://ivwghcveytrkwqxxdtak.supabase.co'
    const SUPABASE_ANON_KEY = 'YOUR_ANON_KEY' // <-- REPLACE ME

    let supabase, orgId

    async function login() {
      const email = document.getElementById('email').value
      const password = document.getElementById('password').value

      supabase = window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY)

      const { data, error } = await supabase.auth.signInWithPassword({ email, password })
      if (error) { alert(error.message); return }

      // Resolve org_id
      const { data: members } = await supabase
        .from('org_members')
        .select('org_id')
        .eq('user_id', data.user.id)
        .limit(1)

      if (members?.length) {
        orgId = members[0].org_id
        document.getElementById('login-form').classList.add('hidden')
        document.getElementById('dashboard').classList.remove('hidden')
        loadDashboard()
      }
    }

    async function loadDashboard() {
      // Summary cards
      const [agents, jobs, execs] = await Promise.all([
        supabase.from('devices').select('*', { count: 'exact', head: true }).in('status', ['online','available','busy']).eq('org_id', orgId),
        supabase.from('agent_jobs').select('*', { count: 'exact', head: true }).in('status', ['assigned','running']).eq('org_id', orgId),
        supabase.from('executions').select('*', { count: 'exact', head: true }).eq('org_id', orgId)
      ])

      document.getElementById('summary-cards').innerHTML = `
        <div class="bg-white rounded-lg shadow p-4 text-center">
          <div class="text-3xl font-bold">${agents.count ?? 0}</div>
          <div class="text-gray-500">Active Agents</div>
        </div>
        <div class="bg-white rounded-lg shadow p-4 text-center">
          <div class="text-3xl font-bold">${jobs.count ?? 0}</div>
          <div class="text-gray-500">Running Jobs</div>
        </div>
        <div class="bg-white rounded-lg shadow p-4 text-center">
          <div class="text-3xl font-bold">${execs.count ?? 0}</div>
          <div class="text-gray-500">Total Executions</div>
        </div>
        <div class="bg-white rounded-lg shadow p-4 text-center">
          <div class="text-3xl font-bold">${new Date().toLocaleTimeString()}</div>
          <div class="text-gray-500">Last Updated</div>
        </div>
      `

      // Recent executions
      const { data: recentExecs } = await supabase
        .from('executions')
        .select('id, status, current_step_index, total_steps, created_at, completed_at, pipeline_templates(name), datasets(name)')
        .eq('org_id', orgId)
        .order('created_at', { ascending: false })
        .limit(10)

      if (recentExecs) {
        document.getElementById('executions-table').innerHTML = `
          <thead><tr class="text-left text-gray-500">
            <th class="p-2">Execution ID</th>
            <th class="p-2">Pipeline</th>
            <th class="p-2">Dataset</th>
            <th class="p-2">Steps</th>
            <th class="p-2">Status</th>
            <th class="p-2">Created</th>
          </tr></thead>
          <tbody>
            ${recentExecs.map(e => `
              <tr class="border-t">
                <td class="p-2 font-mono text-sm">${e.id.substring(0,8)}...</td>
                <td class="p-2">${e.pipeline_templates?.name ?? '-'}</td>
                <td class="p-2">${e.datasets?.name ?? '-'}</td>
                <td class="p-2">${e.current_step_index}/${e.total_steps}</td>
                <td class="p-2">
                  <span class="px-2 py-1 rounded text-sm ${statusColor(e.status)}">${e.status}</span>
                </td>
                <td class="p-2 text-sm">${new Date(e.created_at).toLocaleString()}</td>
              </tr>
            `).join('')}
          </tbody>
        `
      }

      // Agent fleet
      const { data: devices } = await supabase
        .from('devices')
        .select('name, status, arch, cpu_usage_percent, memory_free_gb, total_memory_gb, last_heartbeat, active_workers')
        .eq('org_id', orgId)
        .order('last_heartbeat', { ascending: false })

      if (devices) {
        document.getElementById('agents-table').innerHTML = `
          <thead><tr class="text-left text-gray-500">
            <th class="p-2">Name</th>
            <th class="p-2">Status</th>
            <th class="p-2">Arch</th>
            <th class="p-2">Workers</th>
            <th class="p-2">Memory</th>
            <th class="p-2">Last Heartbeat</th>
          </tr></thead>
          <tbody>
            ${devices.map(d => `
              <tr class="border-t">
                <td class="p-2 font-semibold">${d.name}</td>
                <td class="p-2">
                  <span class="px-2 py-1 rounded text-sm ${deviceStatusColor(d.status)}">${d.status.toUpperCase()}</span>
                </td>
                <td class="p-2 font-mono text-sm">${d.arch ?? '-'}</td>
                <td class="p-2">${d.active_workers ?? 0}</td>
                <td class="p-2">${d.memory_free_gb?.toFixed(2) ?? '-'} GB</td>
                <td class="p-2 text-sm">${d.last_heartbeat ? new Date(d.last_heartbeat).toLocaleString() : '-'}</td>
              </tr>
            `).join('')}
          </tbody>
        `
      }

      // Set up realtime subscription
      setupRealtime()
    }

    function statusColor(status) {
      const colors = { completed: 'bg-green-100 text-green-800', running: 'bg-blue-100 text-blue-800', failed: 'bg-red-100 text-red-800' }
      return colors[status] ?? 'bg-gray-100 text-gray-800'
    }

    function deviceStatusColor(status) {
      const colors = { online: 'bg-green-100 text-green-800', available: 'bg-green-100 text-green-800', busy: 'bg-yellow-100 text-yellow-800', offline: 'bg-gray-100 text-gray-800', error: 'bg-red-100 text-red-800' }
      return colors[status] ?? 'bg-gray-100 text-gray-800'
    }

    function setupRealtime() {
      const channel = supabase.channel('dashboard-live')
      channel.on('postgres_changes',
        { event: '*', schema: 'public', table: 'executions', filter: `org_id=eq.${orgId}` },
        () => { /* Re-fresh relevant section */ }
      )
      channel.on('postgres_changes',
        { event: '*', schema: 'public', table: 'devices', filter: `org_id=eq.${orgId}` },
        () => { /* Refresh agent fleet */ }
      )
      channel.subscribe()
    }
  </script>
</body>
</html>
```

### Option B: React Quick Start

For a more maintainable version, use React with Vite:

```bash
npm create vite@latest sentra-dashboard -- --template react-ts
cd sentra-dashboard
npm install @supabase/supabase-js
npm run dev
```

Then structure the app as:
```
src/
  supabase.ts       - Supabase client initialization
  Auth.tsx          - Login/logout component
  Dashboard.tsx     - Home/summary page
  Executions.tsx    - Pipeline runs list + detail
  Agents.tsx        - Fleet view + detail
  Datasets.tsx      - Dataset list + detail
  Jobs.tsx          - Job queue + dead letter
  Plugins.tsx       - Plugin registry
  Templates.tsx     - Pipeline templates
  Settings.tsx      - Storage + org settings
  realtime.ts       - Realtime subscription setup
```

---

## 8. E2E Test Checklist

### Prerequisites

- [ ] Access to Supabase dashboard (URL + anon key)
- [ ] A dashboard user account in Supabase Auth (email+password)
- [ ] The user is linked to an org via `org_members` table
- [ ] Known org_id from the pipeline-metrics document: `5236b19e-...`
- [ ] Data exists in the tables (executions, devices, agent_jobs, etc.)

### Authentication Tests

- [ ] Login page renders at dashboard URL
- [ ] Invalid credentials show error message
- [ ] Valid credentials redirect to dashboard
- [ ] JWT token is stored (localStorage or cookie)
- [ ] Page refresh maintains session (auto-login from stored token)
- [ ] Logout clears session and returns to login
- [ ] Unauthenticated access redirects to login

### Dashboard / Home Page Tests

- [ ] Summary cards show correct counts:
  - Active Agents = number of devices with status IN ('online','available','busy')
  - Running Jobs = number of agent_jobs with status IN ('assigned','running')
  - Total Executions = count of executions for this org
- [ ] Recent pipeline runs table populates with data
- [ ] Status badges display correct colors (green=completed, blue=running, red=failed)
- [ ] Agent fleet table shows all registered devices
- [ ] Device status colors are correct (green=online, yellow=busy, gray=offline, red=error)
- [ ] Page loads within 3 seconds

### Pipeline Executions Page Tests

- [ ] Execution list shows all executions for the org (not other orgs)
- [ ] Filtering by status works (running, completed, failed)
- [ ] Clicking an execution row shows execution detail
- [ ] Execution detail shows:
  - Pipeline template name
  - Dataset name
  - Total duration
  - Agent that ran it (from agent_jobs)
  - Step-by-step breakdown with timing
- [ ] Step list shows correct status for each step
- [ ] Merge job is shown if applicable
- [ ] Output file path/link is displayed for completed executions
- [ ] Timestamps are in readable format

### Agent Fleet Page Tests

- [ ] Device list shows all devices for the org
- [ ] Status indicators update live via Realtime subscription
- [ ] Clicking a device shows device detail with:
  - Specs (CPU cores, RAM, OS, arch)
  - Current metrics (CPU %, memory free, active workers)
  - Recent jobs on this device
- [ ] Stopped/offline devices are clearly marked
- [ ] Device Arch field displays correctly (amd64, ARM, etc.)

### Dataset Page Tests

- [ ] Dataset list shows all datasets for the org
- [ ] Status labels match the dataset lifecycle (registered -> scanning -> ... -> merged)
- [ ] File count and size are correct
- [ ] Merge status is shown (merged/not merged)
- [ ] Pipeline runs on each dataset are listed
- [ ] Output file name matches expected pattern

### Jobs Queue Page Tests

- [ ] Jobs list shows all jobs for the org
- [ ] Status distribution chart/numbers are correct
- [ ] Dead letter queue shows dead-lettered jobs
- [ ] Filter by job type (scan, process, merge, etc.) works
- [ ] Job detail shows full payload (if accessible)

### Realtime Updates Tests

- [ ] New execution appears in the list without page refresh
- [ ] Device status changes update in real-time
- [ ] New jobs appear in the queue without page refresh
- [ ] Realtime connection indicator shows "Connected"
- [ ] On connection loss, fallback polling works
- [ ] On reconnection, missed updates are synced

### Plugin Registry Tests

- [ ] Plugin list loads from `list_plugins_for_org` Edge Function
- [ ] Built-in vs org plugins are correctly labeled
- [ ] Enabled/disabled status toggle works (if implemented)
- [ ] Rollout percentage is displayed
- [ ] Plugin version and type (python/node/native) are shown

### Pipeline Template Tests

- [ ] Template list loads correctly
- [ ] Template detail shows step configuration in formatted JSON
- [ ] Execution history per template is accurate

### Storage & Settings Tests

- [ ] Storage configuration loads (if any)
- [ ] Org information displays correctly
- [ ] Member list shows org members
- [ ] Quota information is correct

### Error Handling Tests

- [ ] Network errors show user-friendly messages
- [ ] Empty states show "No data" messages (not blank pages)
- [ ] Loading states are shown during data fetch
- [ ] Invalid UUIDs don't crash the page
- [ ] RLS violations return empty arrays (not errors)
- [ ] 401/unauthorized redirects to login

### Performance Tests

- [ ] Dashboard page loads in < 3s with initial data
- [ ] Execution list with 100+ rows renders smoothly
- [ ] Realtime updates don't cause UI jank
- [ ] No unnecessary re-renders on subscription events

### Cross-Browser Tests

- [ ] Chrome latest
- [ ] Firefox latest
- [ ] Safari latest
- [ ] Mobile Safari / Chrome (responsive layout)

### E2E Test Data Verification

Verify the dashboard shows the SAME data as the pipeline-metrics document:

| Metric | Expected | Dashboard Shows |
|:-------|:---------|:----------------|
| Total executions for Walmart Scrape | 22 | [ ] |
| Total executions for Cat Image | 9 | [ ] |
| Total executions for Walmart Dup | 2 | [ ] |
| Devices registered | 4 (sentra, vcnsentra, sentrazero, Arpit.local) | [ ] |
| Active agents | 3 (sentra, vcnsentra, sentrazero) | [ ] |
| Stopped agent | 1 (Arpit.local) | [ ] |
| Completed executions with merge | All 3 latest runs | [ ] |
| Merged output files | 3 files in S3 | [ ] |
| Plugin count | 7 plugins | [ ] |
| Pipeline templates | 3 templates | [ ] |

---

## Quick Reference: SQL Queries for Dashboard Testing

Run these in the Supabase SQL Editor to verify data exists:

```sql
-- 1. Check your org_id
SELECT id, name FROM orgs LIMIT 5;

-- 2. Check how many users are in org_members
SELECT * FROM org_members LIMIT 10;

-- 3. Check executions count by status
SELECT status, COUNT(*) FROM executions GROUP BY status;

-- 4. Check devices with their status
SELECT name, status, arch, last_heartbeat, cpu_usage_percent, memory_free_gb
FROM devices ORDER BY last_heartbeat DESC;

-- 5. Check agent_jobs distribution
SELECT status, COUNT(*) FROM agent_jobs GROUP BY status;

-- 6. Check dead-lettered jobs
SELECT COUNT(*) FROM agent_jobs WHERE dead_lettered = true;

-- 7. Check datasets with status
SELECT id, name, status, file_count, total_size_gb, merged_at
FROM datasets ORDER BY created_at DESC;

-- 8. Check pipeline templates
SELECT id, name, jsonb_array_length(steps) as step_count
FROM pipeline_templates;

-- 9. Check plugins
SELECT name, version, language FROM plugins ORDER BY name;

-- 10. Latest 5 executions with details
SELECT e.id, e.status, e.current_step_index, e.total_steps,
       e.created_at, e.completed_at,
       pt.name as pipeline_name,
       d.name as dataset_name
FROM executions e
LEFT JOIN pipeline_templates pt ON pt.id = e.pipeline_template_id
LEFT JOIN datasets d ON d.id = e.dataset_id
ORDER BY e.created_at DESC LIMIT 5;
```

---

## Supabase Client Configuration

```typescript
// supabase.ts - Client initialization
import { createClient } from '@supabase/supabase-js'

const SUPABASE_URL = 'https://ivwghcveytrkwqxxdtak.supabase.co'
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...' // from Supabase dashboard

export const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: {
    autoRefreshToken: true,
    persistSession: true,
    storageKey: 'sentrazero-dashboard-auth',
  },
  realtime: {
    params: {
      eventsPerSecond: 10,
    },
  },
})

// Auth helpers
export async function signIn(email: string, password: string) {
  const { data, error } = await supabase.auth.signInWithPassword({ email, password })
  if (error) throw error
  return data
}

export async function signOut() {
  await supabase.auth.signOut()
}

export async function getCurrentUser() {
  const { data: { user } } = await supabase.auth.getUser()
  return user
}

export async function getOrgId(): Promise<string | null> {
  const user = await getCurrentUser()
  if (!user) return null

  const { data } = await supabase
    .from('org_members')
    .select('org_id')
    .eq('user_id', user.id)
    .limit(1)
    .single()

  return data?.org_id ?? null
}
```

---

> **SentraZero Dashboard** — Observe your self-hosted compute fleet in real time.
> *Build against Supabase URL: https://ivwghcveytrkwqxxdtak.supabase.co*
> *Org ID: 5236b19e-...*
