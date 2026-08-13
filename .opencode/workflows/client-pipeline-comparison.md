# Client Pipeline Comparison Workflow

Purpose: validate a managed-client pipeline run against the client's reference
output and drive it to acceptance.

## Team

1. Solutions Engineer — owns the run and the comparison
2. QA Engineer — verifies the comparison methodology
3. Backend Engineer — fixes pipeline issues found

## Process

1. Pre-run: verify search/API path health (quota, keys) — use
   `engineering/search-source-check`
2. Run the client pipeline
3. Download merged output (S3) and the client reference xlsx
4. Compare row-by-row (all columns)
5. Classify every diff:
   - Search-source failure (quota 403 → zero candidates)
   - Transient anti-bot soft-block
   - Logic/version nuance (e.g., comparator flags size+color)
6. Root-cause before changing anything — no guessing
7. Fix minimally, re-run, re-compare
8. Re-run until acceptance criteria are met (e.g., published duplicates
   detected with matching reference IDs; remark rows reproduced; zero-candidate
   warning only when genuinely zero candidates)

Never emit a confident-looking wrong answer ("No duplicate found") when the
search path returned nothing.
