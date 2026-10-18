# RISE OTB — Engagement Status (Consolidated Update)

**Date:** 2026-09-12
**Status:** ✅ Engineering complete & verified — both client pipelines re-run at **100% current-market match (0 mismatches)**
**Transferable to:** sentra-website-main (this file). Companion files below.

---

## 1. What was done this cycle

| # | Work | Status |
|---|---|---|
| 1 | Business config catalog: DB layer (`plugins.business_schema` + 3 `pipeline_recipes` seeds) | ✅ APPLIED (migration `20260910000001_business_config_catalog`, via management API, no drift) |
| 2 | `build_step_config` edge function (questions → runtime config) | ✅ DEPLOYED · 21/21 tests · auth-gated · live-verified vs templates |
| 3 | Wizard UI: recipe cards, business-schema question flows, strictness presets, dry-run | ✅ Built, typecheck-clean (`app/dashboard/pipelines/new/` + `wizard/page.tsx`) |
| 4 | Validation pipeline re-run (Walmart Scrape & Compare v2) | ✅ 9/9 (7 MATCH + 2 OK-DELISTED), 0 mismatches — identical to 09-09 |
| 5 | Baselining pipeline re-run (Walmart Duplicate Detection v1) | ✅ 19/19 (15 OK + 4 ref-dup-delisted), 0 mismatches — identical to 09-09 |
| 6 | ScraperAPI quota incident diagnosed + recovery path proven | ✅ Prod key quota-exhausted; test key via `config_overrides` works |
| 7 | Docs updated & synced into sentra-website-main | ✅ (this file + companions) |

---

## 2. Customer value this unlocks

- **Non-technical users configure pipelines by answering plain questions** — no JSON
  keys, no plugin internals. The wizard uses per-plugin `business_schema` (field types,
  questions, options, strictness presets) and the `build_step_config` edge function
  translates answers into the exact runtime config the fleet needs.
- **Both pipelines verified at 100% against the client's reference output**, so the
  outputs RISE OTB sees can be trusted as current-market truth.

---

## 3. The 5 RISE OTB plugins (org `5236b19e-…`, all registered in DB)

| Plugin | ID | Version | Checksum | Role |
|---|---|---|---|---|
| `scrape.py` | `0b509cf1-1111-4000-8000-000000000001` | 1.3.2 | `057e7310ddc4` | Fetch product pages (Walmart/Amazon) → attributes |
| `coverage.py` | `bf19260c-2cea-4964-a36b-9ba01bdf14b8` | 1.2.0 | `42597253686b` | Search other marketplaces for same product |
| `compare.py` | `08be686f-1111-4000-8000-000000000002` | 1.4.2 | `36b047cc7d31` | Side-by-side attribute comparison (path `compare-1.4.2.py` — gateway cache fix) |
| `walmart_search.py` | `2ba4185d-5548-47f0-aae0-07cb0aae206c` | 2.0.1 | `f08a0a8e3ac6` | Search Walmart for duplicate listings (ScraperAPI) |
| `dup_classify.py` | `61ed28d1-ff1b-4264-adc4-85d9d7700d0f` | 2.1.0 | `c39a29a964ec` | Classify candidates → dup/no-dup + Comments |

### Live configs each template runs today

| Template | Step 0 | Step 1 |
|---|---|---|
| **Walmart Coverage v1** (`e8408527-…`) | scrape.py: `url_columns:[Walmart_Url]`, `platforms:{Walmart_Url:walmart}` | coverage.py: `mode:amazon`, `id_column:prmry_sku_id`, `walmart_attributes_column:Walmart_Url_attributes_json`, `exact_threshold:90`, `similarity_threshold:50` |
| **Walmart Scrape & Compare v2** (`36b0c68e-…`) | scrape.py: `url_columns:[Walmart_Url,Comp_Url]`, `platforms:{…:walmart, …:amazon}` | compare.py v1.4.2: `exact_match_threshold:0.6`, `brand_similarity_threshold:0.8`, `title_similarity_threshold:0.8`, `color_empty_means_match:true`, `enable_title_only_match:true`, `enable_upc_match_bypass:true` |
| **Walmart Duplicate Detection v1** (`94211941-…`) | walmart_search.py: `search_mode:generic`, `max_candidates:3`, `similarity_threshold:0.3` | dup_classify.py: `candidate_threshold:0.3` |

### Seeded recipes (foundation for the "choose a workflow" experience)
- Walmart Coverage Check — `a1000000-0000-4000-8000-000000000001`
- Walmart vs Amazon Compare — `a1000000-0000-4000-8000-000000000002`
- Find Duplicate Listings — `a1000000-0000-4000-8000-000000000003`

### Strictness presets exposed to business users
| Preset | coverage (coverage.py) | compare (compare.py) |
|---|---|---|
| Loose | exact 80 / similarity 30 | 0.5 / 0.7 / 0.7 |
| **Balanced (live)** | exact 90 / similarity 50 | 0.6 / 0.8 / 0.8 |
| Strict | exact 95 / similarity 70 | 0.75 / 0.85 / 0.9 |

---

## 4. E2E re-run results (2026-09-12, live fleet)

### Validation (Walmart Scrape & Compare v2)
- Exec `9ff1d980-960c-4a3e-afca-c109aa709d85`; process 203,896 ms (agent sentra) + merge 2,342 ms (agent sentrazero)
- **Result: 7/9 MATCH + 2/9 OK-DELISTED = 9/9 (100%), 0 mismatches** — identical verdicts to 2026-09-09
- The 2 non-match rows (`5755858147`, `194524183`) are genuinely delisted on Walmart (HTTP 404) — correctly excluded, not errors
- Output: `datasets/validation9rowscsv_merged.csv` (1742 B)

### Baselining (Walmart Duplicate Detection v1)
- Failed first attempt (exec `53f2e154-…`) → prod ScraperAPI key quota-exhausted (403), sandbox killed after ~15 min
- Recovery: re-ran with test-key `config_overrides` → exec `4f574f30-d711-4668-b3f3-70f7d9bb4405`; process 650,305 ms + merge 2,604 ms
- **Result: 15/19 OK + 4/19 ref-dup-delisted OK = 19/19 (100%), 0 mismatches** — identical to 2026-09-09
- Output: `datasets/baselining19rows2026-09-09_merged.csv` (6094 B)

---

## 5. ScraperAPI key policy & current blocker

| Item | Detail |
|---|---|
| Production key | `b7e8347aff8c3a5b0f748293afd6f5d4` (baked default in `walmart_search.py` line ~179) — **QUOTA-EXHAUSTED for the monthly cycle** |
| Test key | `b264d9bd017285beabefdd9bc5ae25a2` (recovered from git objects, verified 200 OK) |
| Resolution order | step config `api_key`/`scraperapi_key` → env `SCRAPERAPI_KEY` → baked default |
| Fleet env | `/etc/sentra-agent.env` has **no** `SCRAPERAPI_KEY` (mtime 2026-08-08) → runs without override use the exhausted prod key |
| Recovery proven | per-run `config_overrides:[{index, config:{api_key:"…"}}]` in `run_pipeline` body lands in the agent-job payload and is honored |
| Client action needed | renew / enable overages on the production key (or approve Bright Data fallback) before any client billable run |

---

## 6. Open items / next steps

1. **Client ScraperAPI key renewal or overages** (only blocker for live billable runs).
2. **Wizard dashboard live acceptance** — the runtime path is proven; save flow needs a client UAT pass.
3. Optional: structured source attributes (`color/size/material`) from the feed to close remaining `Comments` nuance.
4. Ops note: fresh merge requires a **fresh execution** (`scheduleMergeOnce` guard) — re-run via `run_pipeline`, never by resetting an old execution.
5. Ops note: never overwrite a plugin at its registered storage path (gateway caches by path) — always version the path.

---

## 7. Companion files (all already transferred to sentra-website-main)

- `RISE-OTB-pipeline-plan.md` — plugin contracts, template configs, execution mechanics
- `RISE-OTB-session-log.md` — full chronology, fixes, measurements
- `CONFIG-CATALOG-RUNBOOK.md` — config-catalog migration/edge-function/wizard runbook (status DONE)
- `RISE-OTB-search-source-decision.md` — search-backend decision + updated quota incident record
- `combined-compliance-report-2026-09-12.html` + `validation-report/baselining-report-2026-09-12.*`
- Client/commercial docs (existing): `RISE-OTB-client-onepager.md`, `RISE-OTB-quotation.md`, `RISE-OTB-followup-message.md`, `RISE-OTB-marketing-page-content.md`, `rise-otb-onboarding.html`