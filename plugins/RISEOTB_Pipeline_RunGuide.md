# RISEOTB Pipeline — How to Run Plugins via the Sentra Binary

## Overview

SentraZero supports running multiple 2-step pipelines. Each pipeline always starts with **Scrape** (step 0) and ends with one of: **Coverage**, **Compare**, or **Dedup** (step 1).

**Example pipelines:**
- Pipeline A: Scrape → Coverage
- Pipeline B: Scrape → Compare
- Pipeline C: Scrape → Dedup

## Plugins in This Pipeline

| Plugin | File | Network | Purpose |
|---|---|---|---|
| `plugin_dedup` | `dedup.py` | No | Remove duplicate products by configurable field similarity |
| `plugin_scrape` | `scrape.py` | Yes | Scrape product attributes from URLs (or search platforms in TAC mode) |
| `plugin_coverage` | `coverage.py` | Yes | Check and fill platform coverage gaps per product |
| `plugin_compare` | `compare.py` | Yes | Compare two URL/attribute columns for match decisions |

A typical RISEOTB pipeline order is: **Scrape → Coverage → Compare → Dedup**

---

## Step 1 — Install Plugin Files

The Sentra binary expects plugins at a specific filesystem path with a platform subfolder:

```
~/.sentra/plugins/<plugin_name>/<os>-<arch>/
    <plugin_name>.json     ← manifest
    <plugin_file>.py       ← plugin binary
```

For a Linux amd64 machine, install each plugin like this:

```bash
PLUGIN_BASE=~/.sentra/plugins
PLATFORM=linux-amd64

for PLUGIN in plugin_dedup plugin_scrape plugin_coverage plugin_compare; do
  mkdir -p "$PLUGIN_BASE/$PLUGIN/$PLATFORM"
done

# Copy manifests and scripts (from extracted plugins-RISEOTB.zip)
cp plugins/dedup/dedup.json    "$PLUGIN_BASE/plugin_dedup/$PLATFORM/plugin_dedup.json"
cp plugins/dedup/dedup.py      "$PLUGIN_BASE/plugin_dedup/$PLATFORM/dedup.py"

cp plugins/scrape/scrape.json  "$PLUGIN_BASE/plugin_scrape/$PLATFORM/plugin_scrape.json"
cp plugins/scrape/scrape.py    "$PLUGIN_BASE/plugin_scrape/$PLATFORM/scrape.py"

cp plugins/coverage/coverage.json "$PLUGIN_BASE/plugin_coverage/$PLATFORM/plugin_coverage.json"
cp plugins/coverage/coverage.py   "$PLUGIN_BASE/plugin_coverage/$PLATFORM/coverage.py"

cp plugins/compare/compare.json "$PLUGIN_BASE/plugin_compare/$PLATFORM/plugin_compare.json"
cp plugins/compare/compare.py   "$PLUGIN_BASE/plugin_compare/$PLATFORM/compare.py"

# Lock down permissions (agent enforces 0700)
chmod -R 700 ~/.sentra/plugins
```

---

## Step 2 — Set Signing Keys in Environment

The agent's `LoadAndUpdatePlugin` function refuses to run any plugin unless its `signature` field passes Ed25519 verification. The signing key must be present as an environment variable named `PLUGIN_SIGNING_KEY_<UPPERCASE_KEY_ID>`:

```bash
# One env var per plugin key_id (from each plugin's .json manifest)
export PLUGIN_SIGNING_KEY_KEY-SCRAPE-001=<base64-ed25519-public-key>
export PLUGIN_SIGNING_KEY_KEY-COMPARE-001=<base64-ed25519-public-key>
export PLUGIN_SIGNING_KEY_KEY-COVERAGE-001=<base64-ed25519-public-key>
export PLUGIN_SIGNING_KEY_KEY-DEDUP-001=<base64-ed25519-public-key>
```

> **Important:** The placeholder values `base64-ed25519-signature-here` in the current `.json` manifests are not real signatures. Real Ed25519 private keys must sign the manifest fields (`name|version|filename|checksum`) and the corresponding public keys must be registered in `plugin_signing_keys` table in Supabase **and** exported into env.

To skip signature enforcement during local development only, set `trusted: true` in each manifest (already set) — the agent skips `VerifyPluginIntegrity` for trusted bundled plugins. For production, proper signatures are mandatory.

---

## Step 3 — Bootstrap the Agent

```bash
# Option A: Claim with a new claim code from the Sentra dashboard
./sentra claim --code <CLAIM_CODE> --name "riseotb-worker-01"

# Option B: Set env vars directly (CI / headless environments)
export BACKEND_URL=https://pqcwgkqrblugplpcaxcy.supabase.co
export BACKEND_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
export CLAIM_CODE=<your-claim-code>
export AGENT_NAME=riseotb-worker-01
export AGENT_ENVIRONMENT_TYPE=linux
export AGENT_STORAGE_TYPE=s3      # or "local" for dev
export MAX_CONCURRENCY=4
export HEALTH_CHECK_PORT=8080

./sentra start
```

On first run, the binary calls `/functions/v1/claim_device`, stores the encrypted config at `~/.sentra/.sentra_config.enc`, then calls `/functions/v1/agent_health_policy` to get `max_workers` and the Redis URL.

---

## Step 4 — Register Plugins in the Sentra Backend

Before a pipeline can reference a plugin by ID, it must exist in the `plugins` table. Use the `register_plugin` edge function from the dashboard or via curl:

```bash
curl -X POST https://pqcwgkqrblugplpcaxcy.supabase.co/functions/v1/register_plugin \
  -H "Authorization: Bearer <USER_JWT>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "plugin_dedup",
    "version": "1.0.0",
    "language": "python",
    "plugin_type": "client",
    "storage_path": "plugins/plugin_dedup/linux-amd64/dedup.py",
    "checksum": "sha256-dfea2d2804908181ee22610b54d07795813e96d3682d1650fbea215b002c0278",
    "trusted": true,
    "network": false,
    "resources": {
      "memory_mb": 512,
      "cpu_seconds": 60,
      "timeout_seconds": 300,
      "cpu_limit": 1.0
    },
    "description": "Universal deduplication plugin"
  }'
```

Repeat for `plugin_scrape`, `plugin_coverage`, and `plugin_compare` with their respective checksums and `"network": true`.

---

## Step 5 — Create a Pipeline Template

Create a pipeline template in the `pipeline_templates` table (or via the dashboard). The `steps` array defines the execution order and plugin configuration for each step.

```json
{
  "name": "Scrape → Coverage",
  "steps": [
    {
      "type": "process",
      "plugin_id": "<uuid-of-plugin_scrape>",
      "config": {
        "url_columns": ["walmart_url", "amazon_url"],
        "platforms": { "walmart_url": "walmart", "amazon_url": "amazon" },
        "request_timeout": 30
      }
    },
    {
      "type": "process",
      "plugin_id": "<uuid-of-plugin_coverage>",
      "config": {
        "walmart_url_column": "walmart_url",
        "platforms_column": "platforms",
        "request_timeout": 30
      }
    }
  ]
}
```

For **Scrape → Compare**: replace step 1 with `plugin_compare`.
For **Scrape → Dedup**: replace step 1 with `plugin_dedup`.

---

## Step 6 — Upload Dataset and Trigger the Pipeline

```bash
# 1. Upload your CSV as a dataset (via the Sentra dashboard or API)
# The dataset must reach status 'scanned' or 'chunked' before the pipeline can run.

# 2. Call run_pipeline with the dataset_id and template_id
curl -X POST https://pqcwgkqrblugplpcaxcy.supabase.co/functions/v1/run_pipeline \
  -H "x-relay-key: <RELAY_WEBHOOK_SECRET>" \
  -H "x-org-id: <YOUR_ORG_ID>" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_id": "<uuid>",
    "pipeline_template_id": "<uuid>",
    "created_by": "<user-uuid>"
  }'
```

This calls the `activate_pipeline` database function which:
1. Creates an `executions` row (`status = 'running'`, `current_step_index = 0`)
2. Creates one `execution_steps` row and one `agent_jobs` row per step
3. The Sentra binary picks up jobs via realtime subscription or SSE polling

---

## Step 7 — How Each Plugin Receives Its Job

The binary calls `ExecuteJob` which deserializes the `agent_jobs.payload` JSONB and passes it to the sandboxed plugin process via **stdin** as JSON in this shape:

```json
{
  "job_id": "<uuid>",
  "org_id": "<uuid>",
  "dataset_id": "<uuid>",
  "execution_id": "<uuid>",
  "step_index": 0,
  "chunk_id": "<uuid>",
  "chunk_index": 0,
  "input_path": "/path/to/input/chunk.csv",
  "output_path": "/path/to/output/chunk_processed.csv",
  "payload": {
    "input_path": "/path/to/input/chunk.csv",
    "output_path": "/path/to/output/chunk_processed.csv",
    "<configurable_field>": "<value from pipeline step config>"
  }
}
```

Each plugin reads from `sys.stdin` with `json.load(sys.stdin)` and accesses its config via `input_data["payload"]`.

---

## Plugin-Specific Payload Configuration

### plugin_dedup

```json
{
  "payload": {
    "input_path": "/data/products.csv",
    "output_path": "/data/products_deduped.csv",
    "match_fields": ["title", "brand", "upc"],
    "similarity_threshold": 0.9,
    "id_column": "product_id",
    "variant_fields": ["color", "size", "pack"],
    "variant_title_threshold": 0.8
  }
}
```

### plugin_scrape (URL scrape mode)

```json
{
  "payload": {
    "input_path": "/data/products_deduped.csv",
    "output_path": "/data/products_scraped.csv",
    "url_columns": ["walmart_url", "amazon_url"],
    "platforms": { "walmart_url": "walmart", "amazon_url": "amazon" },
    "request_timeout": 30,
    "mock_mode": false
  }
}
```

### plugin_scrape (TAC coverage search mode — triggered when `platforms_column` is set)

```json
{
  "payload": {
    "input_path": "/data/products_deduped.csv",
    "output_path": "/data/products_tac.csv",
    "platforms_column": "platforms",
    "title_column": "product_title",
    "walmart_url_column": "walmart_url",
    "request_timeout": 30
  }
}
```

### plugin_coverage

```json
{
  "payload": {
    "input_path": "/data/products_scraped.csv",
    "output_path": "/data/products_coverage.csv",
    "walmart_url_column": "walmart_url",
    "platforms_column": "platforms",
    "request_timeout": 30,
    "mock_mode": false
  }
}
```

### plugin_compare

```json
{
  "payload": {
    "input_path": "/data/products_coverage.csv",
    "output_path": "/data/products_compared.csv",
    "columns_to_compare": ["walmart_url", "amazon_url"],
    "compare_method": "similarity",
    "exact_match_threshold": 0.9,
    "probable_match_threshold": 0.7,
    "title_similarity_cutoff": 0.8
  }
}
```

---

## Mock Mode (Local Testing Without Live Scraping)

Set the env var before starting the agent or running the script manually:

```bash
export RISEOTB_MOCK_MODE=true
python3 plugins/scrape/scrape.py <<EOF
{
  "payload": {
    "input_path": "test_data/input/test_amazon_coverage.csv",
    "output_path": "/tmp/test_out.csv",
    "url_columns": ["walmart_url"],
    "platforms": {"walmart_url": "walmart"}
  }
}
EOF
```

Mock mode is supported in both `scrape.py` and `coverage.py` via the `mock_mode` payload field or `RISEOTB_MOCK_MODE` env var.

---

## Pipeline Advancement

After each step completes, the agent calls `complete_job`, which triggers `advance_pipeline`. That function:
1. Checks no unfinished jobs remain for the current `step_index`
2. Marks the `execution_steps` row `completed`
3. Increments `current_step_index`
4. Calls `plan_dataset_chunks` for the next step to create new `agent_jobs`

This continues until `current_step_index >= total_steps`, at which point the execution is marked `completed`.

---

## Env Var Quick Reference

| Variable | Required | Description |
|---|---|---|
| `BACKEND_URL` / `SENTRA_BACKEND_URL` | Yes | Supabase project URL |
| `BACKEND_ANON_KEY` / `SENTRA_BACKEND_ANON_KEY` | Yes | Supabase anon key |
| `CLAIM_CODE` | Yes (first run) | Device claim code from dashboard |
| `AGENT_NAME` | No | Display name for this device |
| `AGENT_ENVIRONMENT_TYPE` | No | `linux` / `local` (default: `local`) |
| `AGENT_STORAGE_TYPE` | No | `s3` / `local` (default: `local`) |
| `MAX_CONCURRENCY` | No | Number of parallel jobs (default: `nCPU / 2`) |
| `HEALTH_CHECK_PORT` | No | HTTP health check port (default: `8080`) |
| `PLUGIN_SIGNING_KEY_<KEY_ID>` | Yes (prod) | Base64 Ed25519 public key per plugin |
| `RISEOTB_MOCK_MODE` | No | `true` to skip live HTTP in scrape.py |
| `RELAY_WEBHOOK_SECRET` | Yes (edge) | Internal webhook auth for edge functions |
| `SENTRA_SANDBOX_BASE` | No | Override sandbox base path (default: `~/.sentra/sandbox`) |
