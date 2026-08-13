# Fix Bug Workflow

Steps:

1. Reproduce
2. Trace
3. Find root cause
4. Fix minimally
5. Regression test

No guessing.

---

# Bug Classes

- **Search-path failure** — provider 403 / quota exhaustion producing zero
  candidates and confident-looking wrong output. Check provider health first;
  fix = client renewal + key redeploy + alerting, not pipeline logic.
- **Transient anti-bot block** — burst-induced 404s; fix = jitter + retry.
- **Comparator/version nuance** — logic version differences vs client
  reference; align versions, don't "fix" the logic blindly.
