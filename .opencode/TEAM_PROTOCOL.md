# OpenCode Team Protocol

All agents operate as one engineering team.

No agent works alone.

## Default Flow

Request
 ↓
Product Manager
 ↓
Architect
 ↓
Impact Analyzer
 ↓
Specialist Agent
 ↓
QA Engineer
 ↓
Code Reviewer
 ↓
Release Manager


## Collaboration Rules

Before coding:

Consult:
- architect
- impact-analyzer


For frontend changes:

Include:
- frontend-engineer
- ui-designer
- ux-designer


For client pipeline / delivery changes:

Include:
- solutions-engineer
- qa-engineer
- backend-engineer


For search-source / API provider decisions:

Include:
- solutions-engineer
- security-engineer
- impact-analyzer


For backend changes:

Include:
- backend-engineer
- database-engineer
- security-engineer


For database changes:

Mandatory:
- database-engineer
- impact-analyzer
- security-engineer


For AI features:

Include:
- ai-architect
- evaluation-engineer


For landing pages:

Include:
- brand-strategist
- copywriter
- conversion-specialist
- seo-specialist


Before completion:

Mandatory review by:

- qa-engineer
- code-reviewer
- release-manager


## Agent Binary Awareness

This project has a dual-backend architecture:

1. **Go Agent Binary (`sentra-agent`)** — runs on user machines, manages worker pools, executes plugins, handles local resources (CPU, RAM, GPU, disk).
2. **Supabase Backend (Edge Functions + PostgreSQL)** — job queue, device registry, pipeline orchestration, plugin storage, auth.

Changes often touch both sides. Always check:
- Does the Go agent need to be updated (new handler, changed API contract)?
- Do Edge Functions need updates (new endpoint, changed response format)?
- Are running agents compatible with backend changes?
- Is there a migration path for existing devices?

## Rule

If a change affects another domain,
that specialist must be considered.

Think in systems, not files.
