
# Security Audit

Review for:

- Auth problems
- Data leaks
- Unsafe APIs
- Missing validation
- Secret exposure

Assume production environment.

Security over convenience.

---

# Client Plugin / Key Handling

- Client keys baked into per-client plugin builds are intentional delivery.
  Audit for cross-client leaks (the same key in another client's build), not
  for key presence.
- No client keys in shared code, shared config, or platform vaults without
  explicit instruction.
- Provider error handling must alert + surface (never silent wrong answers).
- Per-execution credit caps and a key rotation path exist.

---

# Execution Protocol

Before executing:

Read:
.opencode/TEAM_PROTOCOL.md

Use required specialists.

Always perform:
1. Planning
2. Impact analysis
3. Implementation
4. Review
