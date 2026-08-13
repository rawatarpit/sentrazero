# Search-Source Change Workflow

Purpose: evaluate and ship changes to search/API providers for client
pipelines.

## Team

1. Solutions Engineer — owns provider wiring
2. Security Engineer — key handling
3. Impact Analyzer — consequences for client pipelines and billing

## Process

1. State the problem (e.g., quota exhaustion, success rate, cost)
2. Evaluate providers:
   - Billing model: quota-capped (hard 403 at exhaustion) vs pay-per-success
   - Reliability benchmark evidence (independent, not vendor-only)
   - ToS / legal risk
3. Decide primary + fallback (fallback is coded auto-failover, not manual)
4. Apply key policy: client-owned keys baked into per-client plugin builds;
   rotation = client renewal + platform redeploy
5. Wire alerting: zero-candidate warning + proactive alert + per-execution
   credit cap
6. Meter usage for billing pass-through (cost-plus-margin)
7. Re-run client baselining/validation and compare vs client reference
8. Update the decision record (e.g., RISE-OTB-search-source-decision.md) in
   place
