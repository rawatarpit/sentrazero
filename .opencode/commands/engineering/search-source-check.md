# Search-Source Health Check

Before any client pipeline run, verify the search/API path is healthy:

1. Confirm the provider key/quota is valid (no 403 / quota-exhaustion)
2. Confirm provider credentials match the client (per-client key)
3. Probe 1-2 live lookups and confirm real results (not 403)
4. Report: healthy / degraded (with reason) / failed (block the run)

If failed: do not run the client pipeline. Alert. Resolve via the
`search-source-change` workflow (client renewal + key redeploy).
