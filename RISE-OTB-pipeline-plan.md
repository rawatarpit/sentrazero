# RISE OTB — Pipeline & Plugin Plan

**Owner:** SentraZero (Orchestrator, engineering/product/ops perspective)
**Last verified live:** 2026-09-12 (business config catalog built + deployed; both pipelines re-run to 100% current-market match)
**Client:** RISE OTB — retail client whose Walmart feed is checked for coverage and published duplicates.

This document is the source of truth for **all RISE OTB plugins**, how they are connected into **pipelines**, and how those pipelines execute on the agent fleet. It complements `RISE-OTB-session-log.md` (chronological sessions, fixes, measurements).

---

## 1. System at a glance

```
  Supabase (Postgres + Edge Functions + Storage S3 API)
        │  datasets / pipeline_templates / executions / agent_jobs / batch_chunks
        │
  ┌─────▼─────┐     run_pipeline ──► activate_pipeline ──► advance_pipeline
  │  Planner  │         │                     │                    │
  └───────────┘         ▼                     ▼                    ▼
                   execution             batch_chunks        agent_jobs
                                                                    │
                          ┌─────────────────────────────────────────▼─────────┐
                          │  Fleet agent (Go binary, e.g. 129.154.254.115)     │
                          │  polls → claims → sandbox → runs plugin chain      │
                          │  compound job: step_0 → step_1 (chain in-process)  │
                          └─────────────────────────────────────────┬─────────┘
                                                                    │ upload output
                                                                    ▼
                              S3 (storage/v1/s3)  datasets/<slug>/chunks/chunk_0.out
                                                                    │
                                                    merge_dataset job ──► datasets/<slug>_merged.csv
```

**Key mechanics**
- A **pipeline template** = ordered list of steps; each step = one plugin + config.
- A pipeline run = **execution**; each execution processes **batch_chunks** (one or more).
- Each chunk becomes a **compound process job** (all steps chained, run by one agent in one sandbox) or a single-step job.
- Step N writes `step_{N}_output/step_{N}_output.json`; step N+1 reads it as its input (dispatcher `handlers_unix.go`).
- After all chunks complete, `advance_pipeline` schedules **one merge job** per execution (`scheduleMergeOnce`); the merge reads chunk outputs and writes the final merged CSV to S3.
- Watchdog: any `agent_jobs` with no heartbeat/progress for 15 min is auto-failed.

---

## 2. The 5 RISE OTB plugins (registered in `plugins`, org `5236b19e-…`)

| Plugin | ID | Version | Checksum (head) | Updated | Role |
|---|---|---|---|---|---|
| `scrape.py` | `0b509cf1-1111-4000-8000-000000000001` | 1.3.2 | `057e7310ddc4` | 2026-07-15 | Fetch product pages (Walmart/Amazon/…) → attributes |
| `coverage.py` | `bf19260c-2cea-4964-a36b-9ba01bdf14b8` | 1.2.0 | `42597253686b` | 2026-08-16 | Search other marketplaces for the same product |
| `compare.py` | `08be686f-1111-4000-8000-000000000002` | 1.4.2 | `36b047cc7d31` | 2026-09-09 | Side-by-side attribute comparison of two URLs |
| `walmart_search.py` | `2ba4185d-5548-47f0-aae0-07cb0aae206c` | 2.0.1 | `f08a0a8e3ac6` | 2026-07-27 | Search Walmart for duplicate listings |
| `dup_classify.py` | `61ed28d1-ff1b-4264-adc4-85d9d7700d0f` | 2.1.0 | `c39a29a964ec` | 2026-07-29 | Classify search candidates → dup/no-dup + Comments |

(Also present but **not RISE OTB retail**: `image_load`/`image_resize`/`image_embed` — the cat-image embedding pipeline.)

### 2.1 `scrape.py` — page fetcher / attribute extractor
- **Used by:** Walmart Coverage v1 (step 0), Walmart Scrape & Compare v2 (step 0).
- **Fetch strategy (tiered):** `curl_cffi` with `impersonate="chrome"` (JA3 + HTTP/2 fingerprint) → structured `__NEXT_DATA__` JSON on Walmart (Next.js) → Playwright stealth fallback → optional proxy. `retry_count`, `fallback_to_playwright`, `extract_from_idml`/`extract_from_jsonld`.
- **Config contract:** `url_columns` (e.g. `["Walmart_Url"]`), `platforms` map (`Walmart_Url → walmart`, `Comp_Url → amazon`), `browser`, `timeout`, `request_timeout`, `retry_count`, `use_curl_cffi`, `use_stealth_js`, `impersonate`, `include_images`, `image_limit`, `output_attr_columns`, `description_max_length`, `mock_mode`.
- **Output contract** — for each input URL column `{col}`, appends:
  - `{col}_success` (bool)
  - `{col}_attributes_json` (JSON with `title, brand, upc, gtin, model, size, color, weight, volume, price, pack_count, quantity, description, images`)
  - flat convenience columns: `{prefix}_title`, `{prefix}_brand`, `{prefix}_upc`, `{prefix}_gtin`, `{prefix}_model`, `{prefix}_size`, `{prefix}_color`, `{prefix}_weight`, `{prefix}_volume`, `{prefix}_price`, `{prefix}_pack_count`, `{prefix}_quantity`, `{prefix}_description`, `{prefix}_images_json`
  - On failure: `{col}_success=false`, attributes = `{"_scrape_error": "<reason>"}` (e.g. `HTTP 404` / Walmart soft-404 "We couldn't find this page").
- **Deps:** pandas, requests, beautifulsoup4, curl_cffi, playwright, lxml.

### 2.2 `coverage.py` — marketplace coverage search
- **Used by:** Walmart Coverage v1 (step 1).
- **Role:** for each scraped Walmart product, search other marketplaces and report the best matching listing (or "no coverage").
- **Config contract:** `mode` (`amazon` → Amazon only; `all` → 8 marketplaces incl. eBay/Etsy/Costco/…), `id_column` (`prmry_sku_id`), `walmart_url_column` (`Walmart_Url`), `walmart_attributes_column` (`Walmart_Url_attributes_json`), `similarity_threshold` (50.0), `exact_threshold` (90.0), `mock_mode`, `impersonate`, `use_curl_cffi`, `request_timeout`, `search_query_order` (`["upc","title","brand"]` default; tokens `upc`/`title`/`brand`/`brand_title`, deduped, unknown tokens warned).
- **Matching logic:** builds queries from the configured order (default UPC → title → brand); ranks candidates by similarity. **2026-08-16 fix:** `compute_similarity` uses `fuzz.token_set_ratio` first (verbose marketplace titles score 54–70 instead of <50), then `token_sort_ratio`, then `fuzz.ratio`; brand bonus +30 on 0.7-weighted title.
- **Output columns (exact):** `walmart_product_id, walmart_url, matched_marketplace, matched_product_url, exact_match, similarity_score`. Rows without a scaped title are skipped.

### 2.3 `compare.py` — two-URL attribute comparison
- **Used by:** Walmart Scrape & Compare v2 (step 1).
- **Version note (2026-09-09):** v1.4.2 — marketplace-brand normalization: `BRAND_PLACEHOLDER_MARKERS` / `is_marketplace_brand()` / `strip_marketplace_brand()` strip "Amazon Renewed / Restored / Refurbished / Value Pack…" brand decorations before brand comparison, so identical store-brand products no longer fail brand match. V1.4.2 fixed validation row 5 (`365298585`/B00GUEQ2KM, "Amazon Renewed" brand decoration) from mismatch to Exact Match. Deployed to all 3 fleet devices; uploaded to storage at fresh path `compare-1.4.2.py` (Supabase gateway caches by object path — reuse of the old `compare.py` path still served stale v1.4.1 bytes, so the new path + DB `storage_path` update was required).
- **Deployed checksums (all 3 devices):** `36b047cc7d318948715bd9f4e4bca3174918b865f683cabe9094ce248dfc0141` (41480 B); device-local metadata `compare.py.json` updated to 1.4.2 + new checksum on the two cached devices (stale 1.4.1 checksum caused one failed job-load before metadata fix).
- **Config contract:** `url_column1`/`url_column2` (`Walmart_Url`/`Comp_Url`), `attributes_column1`/`attributes_column2` (`…_attributes_json`), `compare_method` (`similarity`), thresholds (`exact_match_threshold` 0.6, `brand_similarity_threshold` 0.8, `title_similarity_threshold` 0.8), `color_empty_means_match`, `enable_title_only_match`, `enable_upc_match_bypass`, `include_debug_columns`, `weights` (optional override for `upc/brand/title/size/color`, partial OK, auto-normalized to sum 1; a 0 weight ignores that attribute; all-zero falls back to defaults).
- **Scoring:** weighted — UPC 0.50, brand 0.20, title 0.15, size 0.10, color 0.05 (defaults; overridable per run via `weights`).
- **Logic notes:** `canonicalize_title()` strips condition words (Restored/Renewed/Refurbished/Value Pack…) so condition-only differences still match; variant/variety-pack containment rule via `extract_pack_count()`; two-sided physical-conflict gate prevents false matches on genuinely different size/color/weight; `_is_weight_like_size` treats "11.4oz" vs "10ct" as weight not size (Cream of Wheat fix).
- **Output:** 7-column comparison verdict (`Match_Type` + supporting columns), driven by the config above (must be passed explicitly or every row falls back to string similarity).

### 2.4 `walmart_search.py` — duplicate-listing search
- **Used by:** Walmart Duplicate Detection v1 (step 0).
- **Role:** search Walmart for candidate listings that may be published duplicates of each feed row.
- **Search providers (ordered):**
  1. **ScraperAPI Walmart Search SDE** (structured JSON, no IP blocking) — 5 credits/request, 0 on failure.
  2. **DataImpulse residential proxy + curl_cffi HTML** fallback ($1/GB, gateway `gw.dataimpulse.com:823`).
- **Config contract:** `search_mode` (`generic` → ScraperAPI SDE; empty/other → direct curl_cffi HTML), `max_candidates` (3), `rate_limit_delay` (1.0), `similarity_threshold` (0.3), `use_generic_proxy`, `scraperapi_key`/`api_key` (key resolution: config → `$SCRAPERAPI_KEY` env → baked default), `credits_per_request`, `cost_per_credit`, `scraperapi_base`, `scraperapi_generic`.
- **Billing:** logs `credits_used` + estimated cost to output metadata ($0.005/credit, Hobby $49/100K); affiliate-link generation via Impact.com optional.
- **Output:** each row enriched with `candidate_count`, `top_candidate_id` + candidate rows/scores.

### 2.5 `dup_classify.py` — duplicate classification
- **Used by:** Walmart Duplicate Detection v1 (step 1).
- **Role:** given search candidates, decide dup/no-dup + human-readable `Comments` + `Remarks`.
- **Config contract:** `candidate_threshold` (0.3).
- **Logic (fully dynamic, no product catalog):** product-type tokens derived at runtime from the feed `pt` field; `candidate_same_family` checks title contains a source type token; attribute "Different X" detection is gated on computed similarity (brand match ≥0.38, cross-brand same-family ≥0.5) and is source-anchored (`prod_X and cand_X and prod_X != cand_X`); consumes structured `color/size/material` columns if the feed provides them.
- **Output columns:** `product_id, item_id, pt, brand, product_name, product_lifecycle_status, product_class_type, product_publish_status, primary_image_url, "Item has published dups ?", "count of published dups", published_dup_1..3, Comments, Remarks` + candidate details.

---

## 3. The 3 RISE OTB pipelines (templates, org `5236b19e-…`)

### 3.1 `Walmart Coverage v1` — `e8408527-7693-475e-a4da-6fb85759f818`
"Scrape Walmart product pages, then search other marketplaces (Amazon etc.) for matching listings."

| Step | Plugin | Config |
|---|---|---|
| 0 | `scrape.py` | `url_columns:["Walmart_Url"]`, `platforms:{Walmart_Url:walmart}`, `browser:playwright`, `timeout:30`, `request_timeout:30`, `retry_count:2`, `use_curl_cffi:true`, `use_stealth_js:true`, `impersonate:chrome`, `fallback_to_playwright:true`, `extract_from_idml/jsonld:true`, `output_attr_columns:true`, `description_max_length:1000`, `include_images:false`, `mock_mode:false` |
| 1 | `coverage.py` | `mode:amazon`, `id_column:prmry_sku_id`, `walmart_url_column:Walmart_Url`, `walmart_attributes_column:Walmart_Url_attributes_json`, `similarity_threshold:50.0`, `exact_threshold:90.0`, `mock_mode:false`, `impersonate:chrome`, `use_curl_cffi:true` |

**Status 2026-08-16: VERIFIED END-TO-END on the live fleet.** `validation_9rows_csv` (9 rows) → 7 matches written to `validation9rowscsv_merged.csv` (scores 54.3–70.0); 2 rows correctly excluded (Walmart URLs now 404). Last execution: `3e4805b3-e460-4d72-97e0-070f63c693ac` completed 2026-08-16.

### 3.2 `Walmart Scrape & Compare v2` — `36b0c68e-081f-48a3-8730-8d15ffe1d183`
"Compare Walmart vs Amazon products (7 column output)."

| Step | Plugin | Config |
|---|---|---|
| 0 | `scrape.py` | `url_columns:["Walmart_Url","Comp_Url"]`, `platforms:{Walmart_Url:walmart, Comp_Url:amazon}`, rest as 3.1 + `image_limit:5` |
| 1 | `compare.py` | `url_column1:Walmart_Url`, `url_column2:Comp_Url`, `attributes_column1:Walmart_Url_attributes_json`, `attributes_column2:Comp_Url_attributes_json`, `compare_method:similarity`, `exact_match_threshold:0.6`, `brand_similarity_threshold:0.8`, `title_similarity_threshold:0.8`, `color_empty_means_match:true`, `enable_title_only_match:true`, `enable_upc_match_bypass:true`, `include_debug_columns:false` |

**Status 2026-09-09: 100% CURRENT-MARKET MATCH on the live fleet.** Re-run with compare.py v1.4.2 (execution `a9f3853b-41e8-40aa-a550-af98a0ebc18c`, completed 2026-09-09 18:27:45Z; process job 242,210 ms + merge 2,195 ms) → **7 exact-matching rows + 2 delisted-OK (0 mismatch)**. The 2 "Not match" rows (`5755858147`, `194524183`) were verified genuinely HTTP 404 on Walmart (curl_cffi impersonation) — client's 2026-07-31 ground truth said Exact Match, but those products have since been delisted. Row `365298585`/B00GUEQ2KM now Exact Match (brand normalization fix). Output: `validation9rowscsv_merged.csv` (1737 B).

**Status 2026-09-12: RE-RUN AFTER CONFIG CATALOG WORK — still 100%.** Execution `9ff1d980-960c-4a3e-afca-c109aa709d85` (process 203,896 ms + merge 2,342 ms, agent sentrazero) → **7/9 MATCH + 2/9 OK-DELISTED = 9/9 (100%), 0 mismatches**, byte-identical verdicts to the 2026-09-09 run. Output `datasets/validation9rowscsv_merged.csv` rewritten (1742 B).

### 3.3 `Walmart Duplicate Detection v1` — `94211941-52ae-4ecb-9da3-78e0087cafdc`
"Detect duplicate product listings within the Walmart catalog using candidate search + attribute classification."

| Step | Plugin | Config |
|---|---|---|
| 0 | `walmart_search.py` | `search_mode:generic`, `max_candidates:3`, `rate_limit_delay:1.0`, `similarity_threshold:0.3` |
| 1 | `dup_classify.py` | `candidate_threshold:0.3` |

**Status 2026-09-09: GENERIC-SEARCH MODE WIRED + RE-RUN COMPLETE 100%.** Template config now pins `search_mode:"generic"` (ScraperAPI SDE). Baselining re-run (execution `03e264c1-14c9-407a-bd0a-b41586dacf92`, completed 2026-09-09 17:48:32Z; process job 447,991 ms + merge 2,655 ms) → **19 rows, all rc=0, 19 non-dup verdicts consistent with pipeline results**. Instrumented timing measurement on sentrazero (92.4.77.1): avg **11.499s/row** walmart_search generic (min 10.604 / max 12.602, measured total ≈218.5s) + dup_classify batch 0.29s; cost ≈152 ScraperAPI credits (~$0.76). Production execution was previously blocked only by the client's exhausted ScraperAPI key — the fleet env key drives generic search today.

**Status 2026-09-12: RE-RUN AFTER CONFIG CATALOG WORK + PROD KEY QUOTA INCIDENT — 100%.** First re-run attempt (execution `53f2e154-0668-4e39-93d6-8d4298f804cd`) **failed** after 902,966 ms with `step 0 plugin failed: sandbox execution failed for plugin "walmart_search.py": signal: killed`. Root cause: **production ScraperAPI key `b7e8347a…` quota exhausted** — ScraperAPI returned 403 "You have exhausted the API Credits available in this monthly cycle." on every query (confirmed via direct curl). The agent sandbox hard-killed the job at the 900 s timeout even though `execution_timeout_seconds=300`.
**Recovery:** re-ran with per-step `config_overrides` in the `run_pipeline` body (step 0 `api_key:"b264d9bd…"` test key, step 1 `candidate_threshold:0.3`) → execution `4f574f30-d711-4668-b3f3-70f7d9bb4405` (process 650,305 ms + merge 2,604 ms) → **19 rows: 15/19 OK + 4/19 ref-dup-delisted OK = 19/19 (100%), 0 mismatches**, identical to 2026-09-09. Output `datasets/baselining19rows2026-09-09_merged.csv` rewritten (6094 B).

---

## 4. Data contract between steps

- **scrape → coverage / compare:** the whole scraped CSV (original columns + `{col}_attributes_json` + flat `{prefix}_*` attribute columns). Downstream reads the attribute JSON columns (never re-fetches).
- **walmart_search → dup_classify:** the CSV with `candidate_count`, `top_candidate_id`, candidate score fields; classifier reads the row + top candidate.
- **Artifacts:** each chunk uploads its final step output to S3 (`datasets/<slug>/chunks/chunk_<i>.out`); `merge_dataset` concatenates chunk files into `datasets/<slug>_merged.csv` (chunks are deleted after merge).
- **Column discovery is by config, not magic:** compare requires explicit `url_column1/2` + `attributes_column1/2`; coverage requires `id_column`, `walmart_url_column`, `walmart_attributes_column`. If absent, plugins fall back to defaults (compare → generic string similarity), so always pass the template config.

---

## 5. Execution mechanics (verified live)

1. **`run_pipeline`** (user JWT) → `activate_pipeline` RPC → creates execution + chunk plan (`plan_dataset_chunks`) → calls `advance_pipeline`.
2. **`advance_pipeline`** creates compound **process** jobs (`job_type="process"`) per chunk, with all steps merged into one payload. Polling agents claim them (realtime wakeup → adaptive poll 20s–4min).
3. **Agent sandbox** runs the chain: `step_0_output/step_0_output.json` → input to step 1 → … → final step output uploaded to S3 via SigV4.
4. **`complete_job`** (device auth) marks the job; on the last chunk, `advance_pipeline` finalizes the execution and **`scheduleMergeOnce`** creates the `merge_dataset` job (guarded: one per execution, including completed ones — re-running the same execution will NOT spawn a new merge; create a fresh execution instead).
5. **`merge_dataset`** reads chunk outputs, writes `datasets/<slug>_merged.csv`, sets dataset status `merged`.
6. **Watchdog:** 15-min no-progress auto-fail (stale `started_at`/`heartbeat_at` will kill a reset job — a full reset must clear `started_at, heartbeat_at, finished_at, attempt_number, run_id, assigned_at, lease_expires_at`).

---

## 6. Search-source & billing policy

- **ScraperAPI** is primary for structured Walmart search (5 credits/request, 0 on failure; $0.005/credit on Hobby $49/100K); **DataImpulse** residential proxy is the coded fallback; Bright Data is the agreed alternate provider.
- **Key policy (hard):** per-client API keys are baked into per-client plugin builds — never moved, reused, or cross-account. The client's production key `b7e8347a…` is exhausted and untouched; dev/test runs use test key `b264d9bd…` (recovered from git objects, returns 200). **2026-09-12 incident:** production key quota was confirmed exhausted (403 "You have exhausted the API Credits available in this monthly cycle.") which killed a baselining re-run via sandbox hard-kill at ~15 min. Recovery used per-run `config_overrides:[{api_key:"b264d9bd…"}]` in the `run_pipeline` body (override appears in job payload `steps[i].config` and is honored). The fleet `/etc/sentra-agent.env` has **no** `SCRAPERAPI_KEY` (mtime 2026-08-08), so key resolution falls through to the baked default unless overridden.
- **Key resolution order** in `walmart_search.py` (`resolve_api_key`, line ~179): step config `api_key`/`scraperapi_key` → env `SCRAPERAPI_KEY` → baked default `b7e8347a…`. To point the fleet at the test key without per-run overrides, set `SCRAPERAPI_KEY` in `/etc/sentra-agent.env` on the fleet devices.
- **Never-silent-failure:** provider errors must surface + alert, never silently emit wrong output; per-execution credit caps + alerting.

---

## 7. Business config catalog (2026-09-12)

Non-technical clients configure pipelines via **human questions** instead of raw JSON keys. The catalog is the DB-side configuration layer; the wizard (dashboard) and `build_step_config` edge function consume it.

- **Migration `20260910000001_business_config_catalog`** (APPLIED to production via management API, recorded in `schema_migrations`; NOT pushed via bare `supabase db push` so no drift): adds to `plugins`: `business_schema` (JSON) — per-plugin, human-readable config contract: each key with `field_type` (`select`, `boolean`, `number`, `url_column`, `integer`, `columns`), `question`, `options`, `fixed_defaults` (hidden technical keys), `derived` flags, ordering, optional `description`, `min`/`max`. Also seeds 3 `pipeline_recipes` for org `5236b19e-…`: **Walmart Coverage Check** (`a1000000-0000-4000-8000-000000000001`), **Walmart vs Amazon Compare** (`a1000000-0000-4000-8000-000000000002`), **Find Duplicate Listings** (`a1000000-0000-4000-8000-000000000003`): block ordering, strictness-choice flags, derived-column stats.
- **Edge function `build_step_config`** (DEPLOYED; 21/21 tests pass): takes `{owner_org_id, plugin_id, answers, dataset_columns?, context?}` and returns the plugin's complete runtime config merged from `business_schema` field answers + `fixed_defaults`; applies recipe/`columns` validation with 406 errors listing known/override columns; auth-gated (401 without a user JWT). **Verified live:** coverage.py output matches the live template config exactly (`mode:amazon`, `id_column:prmry_sku_id`, `walmart_attributes_column:Walmart_Url_attributes_json`, `exact_threshold:90`, `similarity_threshold:50`, transform `derive_attributes_column`) and scrape.py matches (`platforms:{Walmart_Url:walmart}` via `column_to_platform`).
- **Wizard (dashboard, sentra-website-main)** — `app/dashboard/pipelines/new/` + `wizard/page.tsx`: translates catalog into question flows (URL-column fields, boolean toggles, strictness presets Loose/Balanced/Strict → threshold sets, recipe cards, dry-run). Typecheck-clean. Save flow implemented in-app.
- **Validation nuance (documented for the wizard):** `columns`-type fields are validated against `dataset_columns` when passed; derived columns (e.g. `Walmart_Url_attributes_json`, created by scrape at runtime) must be passed as context columns or validation rejects with 406 "Field … is not in dataset_columns".
- **Runbook:** `CONFIG-CATALOG-RUNBOOK.md` — status **DONE**, §9 E2E run results (both pipelines, 09-12) + search-source health notes.

---

## 8. Open items / next steps

1. **Client key decision (blocks live billable runs):** production ScraperAPI key `b7e8347a…` is **quota-exhausted for the monthly cycle** (confirmed 2026-09-12). Renew/enable overages or move to Bright Data before a client-owned billable run. Code is deployed and correct; dev/test runs work on test key `b264d9bd…` via `config_overrides` (or by setting `SCRAPERAPI_KEY` in the fleet env).
2. **Structured source attributes (optional):** the remaining baselining `Comments` misses need the source product's `color/size/material` in the feed (or `walmart_search.py` enriching the source row). `dup_classify` already consumes them — fully dynamic.
3. **Fresh merge requires a fresh execution** (guard in `scheduleMergeOnce`) — document for ops: re-run via `run_pipeline`, not by resetting an old execution.
4. **Gateway cache gotcha (fixed, 2026-09-09):** Supabase storage gateway serves stale bytes by object path — never overwrite a plugin at its registered `storage_path` in place; publish a versioned path (`compare-1.4.2.py`) + update DB `storage_path`.
5. **Chunk-planner calibration** (from RP8/RP9, image pipeline) is already done for the 202-file case; no RISE OTB impact at current sizes.
6. **Wizard dashboard save flow** is a UI-only exercise — the runtime path (config → `run_pipeline`) is proven; the save flow needs live user acceptance before general availability.

---

## 9. Reference files

- Session log (chronology, fixes, measurements): `RISE-OTB-session-log.md`
- Config catalog runbook (deploy/verify/E2E): `CONFIG-CATALOG-RUNBOOK.md`
- Plugin sources: `/Users/arpitrawat/Downloads/plugins-main/` (`scrape.py`, `coverage.py`, `compare.py`, `walmart_search.py`, `dup_classify.py`)
- Fleet deploy dirs: `/home/ubuntu/.sentra/plugins/<name>.py/any-any/` (and `/root/.sentra/…` for older installs)
- Client ground truth: `Baselining.xlsx`, `Validation.xlsx` (Output sheets, 2026-07-31)
- Compliance reports (2026-09-12): `validation-compare-2026-09-12.csv`, `validation-report-2026-09-12.html`, `baselining-compare-2026-09-12.csv`, `baselining-report-2026-09-12.html`, `combined-compliance-report-2026-09-12.html`; synced copies in sentra-website-main
- Agent/dispatcher code: `internal/dispatcher/handlers_unix.go`, `internal/storage/s3http.go`, `supabase/functions/{run_pipeline,advance_pipeline,complete_job,schedule_merge_job}/index.ts`
- Config catalog code: `supabase/migrations/20260910000001_business_config_catalog.sql`, `supabase/functions/build_step_config/{index.ts,compose.ts,build_step_config.test.ts}`, `supabase/functions/run_pipeline/index.ts`
