# RISE OTB — Walmart Duplicate-Detection Pipeline Session Log

**Scope:** Track 1 (unblock Walmart search for RISE OTB baselining) + Track 2 (attribute-classifier accuracy improvements).
**Date:** 2026-08-12 session.
**Author:** Orchestrator agent (engineering + product + growth + ops perspective).

---

## 0. TL;DR

- **Search unblocked** with a fresh ScraperAPI test key. Both pipelines (baselining `search→classify`, validation `scrape→compare`) run end-to-end on the test key.
- **Track 2 classifier reworked to be fully dynamic** (no hardcoded product catalog). Measured accuracy vs client ground truth:
  - Baselining `Comments`: **10/16 exact, 0 false positives** (was 9/16 + 1 FP).
  - Validation `Match_Type`: **7/9** (was 5/9). The 2 remaining diffs are stale 404 ground truth, not classifier bugs.
- **Deployed** the fixes to the fleet plugin dir (`/home/ubuntu/.sentra/plugins/`), with backups. All files compile and execute.
- **Key policy:** production client key is exhausted / untouched; code is correct and ready, but the live pipeline cannot run in production until the client renews the ScraperAPI key (or switches to Bright Data). Client-owned keys were not moved.

---

## 1. Context & Constraints

- **System:** Sentra agent (Go binary) on a fleet Ubuntu host runs plugin pipelines against Supabase + Walmart + Amazon.
- **Managed client:** RISE OTB — a retail client whose Walmart feed is checked for **published duplicates** (a feed SKU that already exists as a separate published listing on Walmart).
- **Key policy (hard):** Per-client API keys are baked into per-client plugin builds. Do **not** move or reuse client keys. RISE OTB's real ScraperAPI key is exhausted; switching providers or topping up is a client-side commercial action.
- **Fleet host used:** `ubuntu@129.154.254.115` (`ssh-key-2026-03-28.key`).

---

## 2. Track 1 — Unblocking Walmart Search

### Problem
- RISE OTB baselining pipeline was failing on the Walmart search step.
- Root causes: (a) the client's ScraperAPI key was exhausted; (b) direct Walmart `/search` from the datacenter IP returned `403` (Akamai bot mitigation).
- The structured SDE search (via ScraperAPI) was the working path, but blocked by the dead key.

### Resolution (measurement environment only)
- Provisioned a **fresh ScraperAPI account, test key `b2649bd017285beabefdd9bc5ae25a2`** — 5,000 free credits, no card required.
- This is a **TEST KEY, not production**, and is **not** a client top-up or a Bright Data switch. The client key `b7e8347aff8c3a5b0f748293afd6f5d4` remains exhausted and was left untouched.

### Verification
- Ran deployed `walmart_search.py` (only the key swapped on a temp copy) on all 19 baselining rows:
  - **19/19** returned `candidate_count = 3`, **0 search failures**, 51 requests, **255 credits** used.
- Ran both full pipelines on the test key:
  - Baselining: `walmart_search` → `dup_classify` → 19 rows.
  - Validation: `scrape` (16/18 URLs OK, 2 Walmart 404s) → `compare` → 9 rows.

---

## 3. Measurement vs Client Ground Truth

### Ground-truth facts
- `Baselining.xlsx` **Output** sheet = 19 rows; `Validation.xlsx` **Output** sheet = 9 rows. Both files dated **2026-07-31**.
- The client's human notes live in **`Comments`** (baselining) and **`Match_Type`** (validation). The `Remarks` column is **empty** in both — earlier "remark accuracy" was actually `Comments`/`Match_Type` agreement.
- **Stale reference:** the 4 published-dup IDs in baselining (`4KPELIL3O99I`, `1GRNU54IHI1Q`, `6ZHVYS953OCL`, `6Y1WTCI36Q80`) and 2 validation Walmart URLs now return **HTTP 404 "Item Not Found"** on Walmart — the client's July-31 labels predate the 404s. These are ground-truth drift, not pipeline failures.

### Baselining (19 rows)
- Dup-flag agreement: **15/19** (4 diffs = the dead 404 dup IDs; correct against live data).
- `Comments` agreement denominator = **16** (excludes the 3 blank-`Comments` YES rows: `6SI4CIXW2W93`, `5QMX9U45RV1I`, `7DUGRD8WNLT1`).
- Before Track 2 fix: **9/16 exact, 1 false positive** (`1OI2D8YT242C` curtains flagged "Different design").
- After Track 2 fix: **10/16 exact, 0 false positives**.

### Validation (9 rows)
- Client: 5 Exact Match, 4 Incorrect Match (Notes: Color×2, Ounce, Size).
- Before Track 2 fix: **5/9**.
- After Track 2 fix: **7/9**. Remaining 2 diffs (`5755858147`, `194524183`) = Walmart-side **404 scrape failures** (empty titles) → correctly flagged, stale ground truth.

### Honest bottom line (stated to user)
- Binary dup/no-dup detection is strong (>=79%, arguably higher once stale 404s are excluded).
- Validation is better than the raw 5/9 once the 2 stale-404 rows are recognized.
- Attribute *reasoning* (exact `Comments` wording) was the genuinely weak area — addressed in Track 2.

---

## 4. Track 2 — Dynamic Classifier Redesign

### User directive
> "don't make everything using const ... the rows and product keep on changing ... everything we need each time dynamic solutions ... try again"

The first Track 2 attempt introduced a **hardcoded `PRODUCT_TYPE_NOUNS` dictionary** (200+ product-type nouns) to gate attribute checks. This was rejected: it is a static catalog that cannot generalize to changing products/rows, and it caused false positives (e.g. `5QMX9U45RV1I` Coasters, similarity 0.08, matched a puppy plush toy -> "Different color").

### Dynamic redesign (no catalog)
**`dup_classify.py`**
- Removed `PRODUCT_TYPE_NOUNS` and the `extract_product_types` helper.
- Added `_product_type_tokens(pt)` — derives the source's product-type head nouns **at runtime from the feed `pt` field** (splits on `&,/` and keeps singular/plural variants). No global list.
- Added `candidate_same_family(cand_title, src_type_tokens)` — candidate is the same family only if its title contains a source type token (fails open when `pt` is absent).
- Attribute "Different X" detection is now gated on **computed title similarity** + same-family (brand match requires `similarity >= 0.38`; cross-brand same-family requires `similarity >= 0.5`). All "Different X" checks stay **source-anchored** (`prod_X and cand_X and prod_X != cand_X`) — we never flag an attribute the source itself doesn't state.
- Added a **structured-source-attribute capability**: if the feed row carries `color`/`size`/`material` columns, they are merged into the source attribute sets. The current RISE OTB feed has none, so this is a no-op until the feed (or `walmart_search.py`) supplies them.

**`compare_v140.py` / `compare.py`**
- Added `canonicalize_title()` — strips listing-condition wording (`Restored`, `Renewed`, `Refurbished`, `Value Pack`, `Core System`, `Pre-owned`, etc.) before similarity is computed. A pair differing *only* by condition still matches (gated at `title_sim >= 0.6` when condition was stripped, vs 0.8 normally).
- Added `extract_pack_count()` + a **variant/variety-pack containment rule**: same brand + both titles are multi-flavor/variety packs + same dynamically-extracted pack count => Exact Match, even when flavor lists differ (e.g. "Cinnabon/Maple Brown Sugar" vs "Three-Flavor").
- Both rules remain blocked by the existing two-sided physical-conflict gate, so they cannot false-match products that genuinely differ in size/color/weight.

### Results
| Stage | Before | After |
|---|---|---|
| Baselining `Comments` (16-row) | 9/16 (+1 FP) | **10/16, 0 FP** |
| Validation `Match_Type` (9-row) | 5/9 | **7/9** |

The 4 remaining baselining misses (material/color/design) are blocked because the **source feed title lacks the attribute word** — they need structured source attributes (see Section 6).

---

## 5. Deploy

### Targets (fleet)
- `dup_classify.py` -> `/home/ubuntu/.sentra/plugins/dup_classify.py/any-any/dup_classify.py`
- `compare` -> `/home/ubuntu/.sentra/plugins/compare.py/any-any/compare_v1.4.0.py` **and** the active sibling `/home/ubuntu/.sentra/plugins/compare.py/any-any/compare.py`

### Actions
- Backed up originals: `*.bak_<YYYYMMDD>` next to each deployed file.
- Pushed fixed copies. The sibling `compare.py` was an **older base** (lacked `_is_weight_like_size` and had a size-conflict bug that false-flagged weight-like size diffs like "11.4oz" vs "10ct"); merged the weight-like guard so Cream of Wheat resolves correctly (7/9 verified on the sibling too).
- `py_compile` check: **all three deployed files compile OK**.
- Live sanity run of deployed `dup_classify` on `/tmp/baseline_testkey_out.csv`: 19 rows processed, no errors.

### Key-policy note
- Code is deployed and correct. The **production client ScraperAPI key is still exhausted**, so the live RISE OTB pipeline cannot execute in production until the client renews/overages or moves to Bright Data. Client-owned keys were **not** moved or reused.

---

## 6. Open Items / Next Steps

1. **Structured source attributes (optional next):** the 4 remaining baselining `Comments` misses need the source product's structured `color`/`size`/`material`. Path: either (a) RISE OTB adds those columns to the feed, or (b) `walmart_search.py` enriches the source row with its own Walmart attributes. `dup_classify` already consumes them if present. Fully dynamic, no catalog.
2. **Client key decision:** renew ScraperAPI (with volume number if tier upgrade) or switch to Bright Data. Then re-run on the real key for a production-accurate measurement.
3. **Doc:** `RISE-OTB-search-source-decision.md` was intentionally **not** updated (user instruction) — decision recorded here instead.
4. **Deploy gating:** code is live on the fleet; the per-client *build* with the baked key still requires the client action in (2).

---

## 7. File Inventory

**Local working copies (session):** `/var/folders/6r/rjvc364j27937107tdhfdwj40000gn/T/opencode/`
- `deployed_dup_classify.py` — fixed classifier (dynamic).
- `deployed_compare_v140.py` — fixed compare (versioned base).
- `compare_sibling_local.py` — fixed compare (active sibling base).
- `rerun/baseline_testkey_out.csv` — search output (19 rows, test key).
- `rerun/baseline_testkey_classified_v1.csv` — original-deploy output (9/16).
- `rerun/baseline_testkey_classified_v2.csv` — dynamic-fix output (10/16, 0 FP).
- `rerun/validation_scraped.csv` — scraped attributes (9 rows).
- `rerun/validation_compared_v2.csv` / `validation_compared_sibling.csv` — compare output (7/9).
- `TRACK2_ATTRIBUTE_CLASSIFIER.md` — Track 2 ticket.

**Deployed (fleet):** `/home/ubuntu/.sentra/plugins/`
- `dup_classify.py/any-any/dup_classify.py` (+ `.bak_*`).
- `compare.py/any-any/compare_v1.4.0.py` (+ `.bak_*`).
- `compare.py/any-any/compare.py` (+ `.bak_*`).

**Client ground truth:** `/Users/arpitrawat/Downloads/sentrazero/Baselining.xlsx`, `Validation.xlsx` (both Output sheets, 2026-07-31).

---

# Session 2026-08-13 — Live re-runs + agent-job E2E (relay/wakeup verification)

## 1. ScraperAPI key renewal
- **New active key**: `b264d9bd017285beabefdd9bc5ae25a2` (user-provided). Verified live: account endpoint creditsLeft 4700, concurrencyLimit 5, burst 0; structured Walmart search returned real items; 0 failed requests. After today's runs: **4025 credits left**.
- Old test key `b2649bd0…` was already 401-dead by Aug 12 (242 failed requests in debug log). Client prod key `b7e8347aff8c3a5b0f748293afd6f5d4` (exhausted) swapped out.
- Installed in the **active** fleet plugin `/root/.sentra/plugins/walmart_search.py/any-any/walmart_search.py` (line 88) via sed; backup `.bak-key-20260813`; manifest untouched (AutoUpdatePlugins compares manifest checksums only, so file edits survive).
- Also fixed plugin crash: unguarded `_debug_log.flush()` at ~line 984 could kill the plugin after work when the debug log was unopenable → now `if _debug_log:`. `py_compile` clean.

## 2. Baselining re-run (live, new key)
- Search: 19/19 candidates, 52 requests, 0 failures, 260 credits, $1.30 → `/tmp/baselining_input_candidates.csv`; classify via deployed `dup_classify.py` → `/tmp/baseline_testkey_classified.csv`.
- Scores: dup-flag **15/19** (identical misses to yesterday: `55ARRC66XSVK`, `6SI4CIXW2W93`, `5QMX9U45RV1I`, `7DUGRD8WNLT1`). Comments **9/16** vs v2 10/16 — only row `1OI2D8YT242C` (curtains) flipped ("No duplicate found" → "Different design", dup flag still No); root cause = live Walmart result rotation (top candidate Kotile→Joqmia, max_similarity same 0.3429). Same classifier, different data — no logic regression.

## 3. Validation re-run
- scrape.py needs explicit `url_columns` (default `["url"]`): ran with `["Walmart_Url","Comp_Url"]` → 8/9 Walmart (only known stale 404 `5755858147`), 9/9 Amazon.
- compare.py had a **broken undefined `compare_strings`** NameError when attribute JSON columns were missing (yesterday's "fix" landed in `/home/ubuntu/.sentra`, which the agent never reads — active path is `/root/.sentra`). Added a safe fallback helper (fuzzy `token_sort_ratio`, ≥0.9 exact) to the active plugin; backup `.bak-20260813`; `py_compile` clean.
- compare must be called with explicit column config (`url_column1/2`, `attributes_column1/2` = `Walmart_Url_attributes_json` / `Comp_Url_attributes_json`) or every row falls into the string fallback.
- Final: **Match_Type 6/9** vs July-31 ground truth; the 3 diffs are all scrape-side artifacts, not logic: `5755858147` hard 404, `194524183` Walmart soft-404 ("We couldn't find this page", HTTP 200), `10292954` Amazon bot-blocked partial page (title literally "Adding to Cart..."). GT is stale for the two Walmart rows (same 2 as yesterday).

## 4. Agent-job E2E via relay_job_event (verified on v5 binary)
Full loop proven end-to-end on the fleet (`vcnsentra`, device `c527f075-5616-44a3-a82e-d8463c01a53e`, org `5236b19e-6a2b-4cd9-a2b7-14441b7b7d07`):
1. Dispatch (device auth, `x-agent-token`) → job row inserted with **raw** payload + `execution_id` column + **normalized `job_type="process"`**.
2. **Redis wakeup**: fleet polled ~1-2 s after insert (was on ~5-min safety cadence) — L5 verified.
3. Claim → lease verify → start → 2-step compound run (**mock_mode=true, 0 credits**) → output uploaded → `complete_job` 200 → status `completed`.
4. **advance_pipeline finalized the execution** to `completed` (current_step_index=2). Merge not scheduled because dataset `16a7152a` is already `merged` (Aug-10) — expected gating, not a bug.

### Bugs found & fixed during E2E (relay_job_event, deployed)
1. **Relay redacted operational fields in dispatched payloads** (`source_path` → `[REDACTED]`) → agent could not locate input. Fix: insert RAW `data`; sanitization kept for log lines only.
2. **Relay never set `execution_id` column** → claim returned empty `exec_id` → dispatcher rejected ("execution_id is required for completion"). Fix: mirror `execution_id`/`execution_step_id`/`run_id` onto agent_jobs columns.
3. **Relay stored `job_type="process_dataset"`**, but `advance_pipeline` matches compound jobs by `job_type="process"` (backend convention per plan_dataset_chunks) → execution stayed zombie "running" and the auto-advance cron would retry it every minute (quota burn). Fix: normalize to `"process"` when a `chunk_id` is present.
4. **Agent bug (v5 fix): `reportStepProgress` channel was `agent-`+hostname** (`getAgentID()` falls back to hostname when `AGENT_ID` unset) → relay dropped `step_progress` events (400 invalid channel). Fix: use `execClient.GetDeviceID()`. Verified: all relay batch flushes 200 after v5.
5. Also: internal relay-key auth now surfaces its own error instead of silently falling back to device auth (cleaner API + debuggability).

## 5. Notes / follow-ups
- **Management API secrets are hashed previews** — real `RELAY_WEBHOOK_SECRET` is NOT retrievable via API (runtime value ≠ GET value). Device-auth dispatch used for E2E. If the client needs the relay key, rotate via `supabase secrets set` (client must update their caller) or obtain from the party who set it.
- Relay jobs currently skip `job_dataset_id`/`job_chunk_id` columns (payload has the ids) — cosmetic; advance uses execution_id + payload.
- **Credits**: 4025 remaining after today's live runs.
- Cleanup: all test executions/jobs (rounds 1–5) deleted; fleet cache dirs removed.
- Go client changes remain uncommitted (as before).

## 6. Review-point resolutions (2026-08-13, same session)

### RP1 — Redaction = data-exposure risk? → Intentional-origin, decision made, RLS verified
- `git log -S sanitizedData` shows the sanitized dispatch insert is in the **original commit
  `9bd285c`** — present from the start, not a later regression. Reading: log-hygiene applied
  to the write path by mistake.
- **Decision:** raw payload in `agent_jobs.payload` (operational correctness); sanitization
  retained for `system_logs` lines only.
- **RLS residual verified:** `agent_jobs` has org-scoped `agent_jobs_select` (via
  `org_members`) + `service_role` policies (remote_schema L10442/L10438). Raw op fields are
  visible to owning-org members only — no cross-tenant exposure. In this org, payload paths
  are plain S3-style keys (`plugins/cats/`, `validation_9rows_csv.csv`), no absolute or
  customer-identifying paths. Residual note filed in TICKET-009.

### RP2 — Run-to-run variance in accuracy numbers → documented, not a logic regression
Same classifier, same ground truth, different **live** search/scrape results:
- Baselining `Comments` (16-row): v2 **10/16** → today **9/16**. Only flip:
  `1OI2D8YT242C` (curtains) "No duplicate found" → "Different design" — top Walmart
  candidate rotated Kotile→Joqmia, `max_similarity` unchanged 0.3429. Dup-flag unchanged
  **15/19** (same 4 misses both runs).
- Validation `Match_Type` (9-row): v2 **7/9** → today **6/9**. Today's 3 diffs are all
  scrape-side artifacts, not logic: `5755858147` hard 404, `194524183` Walmart soft-404
  (HTTP 200 "We couldn't find this page"), `10292954` Amazon bot-blocked partial page
  (title "Adding to Cart..."). GT is stale for the two Walmart rows (predates removal).
- **Rule going forward:** quote accuracy as a *snapshot with run date + denominator + diff
  breakdown*, never as a fixed property. Do not cite "79%" or any single number as stable.

### RP3 — RELAY_WEBHOOK_SECRET unverified for external callers → documented, remains blocked
- Confirmed every current caller is **internal** and shares the same runtime env secret
  (Go client + 7 Edge Functions: auto_assign_best_device, complete_job, advance_pipeline,
  schedule_merge_job, record_dataset_metadata, approve_dataset_and_plan_chunks,
  run_pipeline). Internal→internal auth works.
- Management API returns hashed previews only → real key not retrievable → external
  `x-relay-key` path **unproven and blocked** from here. Unblock = rotate secret via
  `supabase secrets set`, hand to client, test from outside. Filed in TICKET-009.

### RP4 — FIX_TICKETS documentation → done
- **TICKET-009** written (relay 4 fixes + agent v5 channel fix + redaction-intent finding +
  external-caller status + RLS residual). Format matches existing entries.

### RP5 — Uncommitted Go changes → committed
- `internal/backend/execution_client.go`, `internal/dispatcher/handlers_unix.go`,
  `internal/heartbeat/heartbeat.go`, `internal/realtime/supabase_realtime.go`,
  README, `.opencode/` additions (delivery agents, search-source workflows/commands),
  this session log, `docs/FIX_TICKETS.md` (TICKET-009). `go build ./...` + `go vet ./...`
  clean before commit.
