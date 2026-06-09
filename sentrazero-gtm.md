# SentraZero — Go-to-Market Strategy

**Prepared:** June 2026
**Status:** Pre-Launch (Product Built, GTM Not Started)

---

## Executive Summary

SentraZero is a **distributed data processing platform** that turns idle machines into a processing cluster. It's fully built (22 Go packages, 55+ edge functions, 51 database tables, complete dashboard, agent binary, plugin system). What's missing is the billing integration, plan gating, and go-to-market motion.

This document outlines the complete GTM strategy to take SentraZero from zero to **$1.79M ARR in 12 months** and **$49.8M ARR by Year 3**.

---

## 1. GTM Strategy: Two-Phase Motion

### Phase 1: Product-Led Growth (Months 1–3)

**Goal:** Self-serve acquisition funnel. Free → Value → Upgrade.

| Component | Detail |
|-----------|--------|
| **Signup** | Self-serve at app.sentra.sh. Email + password. Free plan activated immediately. |
| **Activation** | Install agent: `curl -fsSL https://sentra.sh/install \| bash` with claim code from dashboard. |
| **First Value** | Upload a CSV, run a pipeline. Time-to-first-pipeline < 5 minutes. |
| **Upgrade Trigger** | Hits free limit (2nd agent, 4th team member). In-app banner → Stripe Checkout. |
| **Payment** | Stripe Checkout embedded in dashboard. Monthly or annual (20% discount). |

**Key Metric:** Time-to-first-pipeline (target: < 5 min). This is the "Aha!" moment.

### Phase 2: Sales-Led Growth (Months 4–6+)

**Goal:** Capture enterprise accounts with compliance requirements.

| Component | Detail |
|-----------|--------|
| **Trigger** | Accounts that evaluate Team plan but need on-premise control plane, SSO, or SLA. |
| **Motion** | 30-day guided POC with onboarding call. Dedicated Slack channel. |
| **Marketplace** | AWS, Azure, GCP marketplace listings (15–20% rev share, but enterprise discovery). |
| **Contract** | Annual contracts ($2,500–10,000+/mo). 20% discount vs monthly. |

---

## 2. Target ICP

### Ideal Customer Profile

| Attribute | Value |
|-----------|-------|
| **Company Size** | 10–500 employees |
| **Revenue** | $10M–$500M |
| **Data Volume** | 1–50 TB processed per month |
| **Infrastructure** | Mix of on-premise NAS + cloud storage (S3, Azure Blob, GCS) |
| **Compliance** | GDPR, HIPAA, SOC 2, CCPA — needs data sovereignty |
| **Buyer** | Head of Data Engineering, CTO, Security/Privacy Lead |
| **Budget** | $500–5,000/mo for data processing tools |

### Buyer Personas

| Persona | Pain | Solution | Budget |
|---------|------|----------|--------|
| **Data Engineer** | Brittle Python scripts, no parallelism, no monitoring | Distributed pipeline engine, auto-scaling, observability | $500–2,000/mo |
| **Security/Privacy Lead** | Data cannot leave VPC, need auditable processing | Edge-native processing, plugin signing, audit logs | $2,000–5,000/mo |
| **ML Engineer** | Training data prep is slow, non-reproducible | Versioned pipelines, parallel chunk processing | $200–1,000/mo |

### Anti-ICP (Not a fit)

- Companies with < 1 TB data (too small — Free plan works indefinitely)
- Companies already fully on a single cloud with no compliance concerns (weak differentiator)
- Companies needing real-time streaming (SentraZero is batch-oriented)
- Individual developers (no team budget)

---

## 3. Pricing & Packaging

### Tier Structure

| Feature | Free | Team ($799/mo) | Enterprise (Custom) |
|---------|------|----------------|---------------------|
| **Agent devices** | 1 | 10 | Unlimited |
| **Concurrent jobs** | 50 | 500 | Unlimited |
| **Plugins** | 5 | 50 | Unlimited |
| **Team members** | 3 | 25 | Unlimited |
| **Support** | Community | Email + Slack | Dedicated engineer |
| **Execution history** | 7 days | 90 days | Unlimited |
| **Custom storage** | — | ✅ | ✅ |
| **SSO (SAML/OIDC)** | — | ✅ | ✅ |
| **Audit logs export** | — | ✅ | ✅ |
| **On-premise control plane** | — | — | ✅ |
| **SLA** | — | — | 99.9% |
| **Price** | $0 | $799/mo | $2,500–10,000+/mo |

### Add-on Pricing (Team Plan)

| Add-on | Price | Notes |
|--------|-------|-------|
| Extra agent (beyond 10) | $99/agent/mo | $79/agent at 25+ |
| Extra team member (beyond 25) | $19/user/mo | — |
| Priority support (4h response) | $299/mo | Included in Enterprise |
| Extra worker slots (beyond 500) | $49/slot/mo | — |

### Pricing Principles

1. **No per-row pricing.** Customers hate unpredictable bills that grow with data volume.
2. **Per-device licensing.** Ties cost to infrastructure, which is predictable.
3. **Generous free tier.** 1 agent, 5 plugins — enough to evaluate and get value.
4. **Clear upgrade triggers.** Hitting limits is the conversion moment.
5. **Annual discount (20%).** Incentivizes commitment without requiring it.

---

## 4. Marketing Channels

| Channel | Strategy | Cost | Expected Impact |
|---------|----------|------|-----------------|
| **Product Hunt** | Launch with demo video + interactive architecture page | $0 | 2,000–5,000 signups |
| **Hacker News** | "Show HN: We built a data processing platform that doesn't move your data" | $0 | 5,000–15,000 visits |
| **Content Marketing** | Architecture blog ("Why we built another ETL"), comparison posts, "Edge processing at scale" | $0 (time) | SEO growth, thought leadership |
| **Reddit** | r/dataengineering, r/devops, r/golang — share architecture, answer questions | $0 | 500–2,000 visits/post |
| **LinkedIn Ads** | Target data engineering managers at mid-market companies | $3,000–5,000/mo | 100–200 leads/mo |
| **Cloud Marketplaces** | AWS, Azure, GCP listings | 15–20% rev share | Enterprise discovery, $50K+ ACV |
| **Conferences** | KubeCon, Data Engineering Summit, Data + AI Summit | $5,000–15,000/event | Brand + enterprise contacts |
| **Discord / GitHub** | Community support, open-source plugin SDK | $0 | Community growth, word-of-mouth |
| **Direct Outreach** | 500 mid-market data leaders on LinkedIn (personalized) | $0 (time) | 20–50 intro calls |

### Launch Sequence (Month 3)

- **Week 1:** Product Hunt launch (Monday) + Hacker News post (Tuesday)
- **Week 2:** Architecture blog post + Reddit r/dataengineering post
- **Week 3:** LinkedIn ad campaign starts + first newsletter
- **Week 4:** Community Discord open + plugin SDK release

---

## 5. Content Marketing Plan

### Blog Topics (One per week for 3 months)

| Week | Topic | Target Keyword |
|------|-------|----------------|
| 1 | "Why we built another data processing platform" | data processing platform |
| 2 | "The hidden cost of data egress (and how to eliminate it)" | data egress costs |
| 3 | "Edge processing: when cloud isn't the answer" | edge data processing |
| 4 | "How we use pgvector for smart device-job matching" | pgvector cosine similarity |
| 5 | "Data sovereignty is becoming mandatory — here's what to do" | data sovereignty compliance |
| 6 | "Plugin signing with Ed25519: zero-trust code execution" | plugin signing Ed25519 |
| 7 | "macOS Seatbelt vs Linux cgroups: OS-level sandboxing compared" | OS sandbox comparison |
| 8 | "Chunking at scale: how to split 10TB datasets for parallel processing" | dataset chunking |
| 9 | "Fivetran vs. edge processing: the pricing showdown" | Fivetran alternative |
| 10 | "Building a distributed compute fleet from your dev laptops" | distributed computing cluster |
| 11 | "Auto-advancing pipelines: the architecture behind zero-touch data processing" | auto-advancing pipeline |
| 12 | "SentraZero Year 1: what we built, what we learned" | startup journey |

### Content Repurposing

- Each blog post → Twitter/X thread (5–10 tweets)
- Each technical post → Reddit r/dataengineering + r/programming
- Architecture content → LinkedIn carousel posts
- Comparison content → Landing page sections

---

## 6. Sales Process

### Self-Serve (PLG)

```
Signup → Free plan → Install agent → Upload dataset → Run pipeline → 
  → Hit limit → Banner CTA → Compare plans → Stripe Checkout → Team plan active
```

No sales calls. Entirely automated. Stripe handles payment, webhooks handle provisioning.

### Sales-Assist (Team Plan Inquiries)

```
Inbound interest (chat/email) → 15-min product tour → 
  → 7-day trial with extra limits → Decision → Stripe Checkout
```

One sales call. No custom pricing. Self-serve checkout.

### Enterprise (Custom)

```
Inbound / Marketplace → Discovery call → POC scope → 
  → 30-day POC with dedicated support → Security review → 
  → Legal / procurement → Annual contract signed
```

1–2 month sales cycle. Custom pricing. On-premise control plane.

---

## 7. Community Strategy

### Why Community Matters

SentraZero's core user is a developer or data engineer. Developers discover tools through:
1. Hacker News / Reddit (60%)
2. GitHub / open-source (25%)
3. Word-of-mouth (10%)
4. Ads / conferences (5%)

Community is the primary acquisition channel.

### Tactics

| Initiative | Detail | Timeline |
|------------|--------|----------|
| **Plugin SDK** | Open-source Go + Python + Node.js SDK for building plugins | Month 3 |
| **Sample Plugins** | GitHub repo with 10 example plugins (CSV transform, PII redact, JSON parse, etc.) | Month 3 |
| **Discord Server** | Community support, showcase, feedback channel | Month 3 |
| **GitHub Sponsors** | Not for revenue — for community credibility | Month 4 |
| **Template Library** | Community-contributed pipeline templates | Month 5 |
| **Plugin Marketplace** | Curated list of community plugins (not hosted — directory) | Month 6 |

---

## 8. Competitive Positioning

### Messaging Matrix

| For | Message |
|-----|---------|
| **Data Engineers** | "Process data where it lives. Turn your dev laptops into a distributed processing cluster with one command." |
| **Security Leads** | "Data never leaves your infrastructure. Ed25519-signed plugins. OS-level sandbox. Full audit trails." |
| **CTOs** | "Stop paying per-row for data processing. Predictable pricing. No data egress. Use hardware you already own." |
| **Investors** | "The anti-Fivetran. Distributed data processing at the edge. $24B TAM, product built, revenue-ready." |

### One-Liners for Different Contexts

| Context | One-Liner |
|---------|-----------|
| **Elevator pitch** | "Distributed data processing that runs on your existing hardware — no cloud, no egress, no per-row pricing." |
| **Technical** | "Go agent + Supabase backend + pgvector device matching. Ed25519-signed plugins in OS sandboxes. Auto-advancing pipelines." |
| **Pain-focused** | "You know how Fivetran charges per row and Databricks per DBU? We charge by compute capacity. Predictable. Fair. Edge-native." |
| **Comparison** | "Airbyte moves data to process it. We process data where it is. The difference is hours of egress time and thousands in bandwidth costs." |

---

## 9. Launch Checklist

### Pre-Launch (Month 1–2)

- [ ] Stripe integration: Checkout, webhooks, subscription management
- [ ] Plan gating: middleware + soft/hard limits
- [ ] Billing dashboard tab with invoice history
- [ ] Quota consumption tracking (real-time)
- [ ] Email drip campaign setup (day 3, 7, 14, 30)
- [ ] Landing page: sentra.sh (product page + pricing + docs)
- [ ] Product Hunt draft: tagline, images, demo video
- [ ] Blog: 4 pillar articles written
- [ ] Discord server ready
- [ ] Docs site: installation, quickstart, architecture

### Launch (Month 3 — Week 1)

- [ ] Product Hunt launch (Monday, midnight PT)
- [ ] Hacker News "Show HN" post (Tuesday, morning ET)
- [ ] r/dataengineering post (Wednesday)
- [ ] LinkedIn post from founder (Wednesday)
- [ ] Twitter/X thread (Wednesday)
- [ ] Launch newsletter sent (Thursday)
- [ ] Monitor + respond to all comments (all week)

### Post-Launch (Month 3–4)

- [ ] Blog: weekly content cadence begins
- [ ] LinkedIn ad campaign starts ($3K/mo)
- [ ] Outreach to 50 potential enterprise customers
- [ ] Plugin SDK open-source release
- [ ] Sample plugin repository
- [ ] 10 quickstart templates

### Scale (Month 4–6)

- [ ] AWS Marketplace application
- [ ] Azure Marketplace application
- [ ] GCP Marketplace application
- [ ] SOC 2 Type I (start process)
- [ ] Enterprise POC program formalized
- [ ] On-premise control plane (Docker Compose)
- [ ] Help Scout / Intercom for support

---

## 10. Success Metrics

### North Star Metric

**Active pipeline executions per day.** This measures real value delivery.

### Leading Indicators (Weekly)

| Metric | Target (Month 3) | Target (Month 6) |
|--------|------------------|------------------|
| Signups | 200/week | 500/week |
| Agent installations | 100/week | 300/week |
| First pipeline within 7 days | 40% of signups | 50% of signups |
| Free → paid conversion | 2% | 4% |
| Paid MRR added | $5K/week | $15K/week |
| NPS (paid users) | 40+ | 50+ |

### Lagging Indicators (Monthly)

| Metric | Target (Month 3) | Target (Month 6) |
|--------|------------------|------------------|
| Total signups | 2,500 | 5,000 |
| Paid customers | 100 | 300 |
| MRR | $20K | $100K |
| ARR | $240K | $1.2M |
| Monthly churn | <5% | <3.5% |
| LTV:CAC | 10x | 20x |

---

## 11. Budget Allocation (First 12 Months)

| Category | Monthly | Annual | Notes |
|----------|---------|--------|-------|
| Infrastructure (Supabase, S3, Vercel) | $2,000 | $24,000 | Scales with users |
| Stripe fees | 2.9% + $0.30 | ~$12,000 (est.) | Percent of revenue |
| Cloud marketplace fees | 15–20% rev share | ~$30,000 (est.) | Only on marketplace-sourced deals |
| LinkedIn ads | $3,000 | $36,000 | Months 3–12 |
| Conferences (2x) | $10,000 avg | $20,000 | KubeCon + Data Engineering Summit |
| Content (freelance) | $1,000 | $12,000 | Optional — can do in-house |
| SOC 2 readiness | One-time | $15,000 | Compliance certification |
| **Total** | | **~$149,000** | Excludes team salaries |

---

## 12. Risks & Mitigations

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| **Low free→paid conversion** | High | Medium | Team-only features (SSO, audit, custom storage). Limit banners at 80/100%. |
| **OSS competition (Airbyte/Prefect)** | Medium | High | Edge-native is genuine differentiator. Open-source agent or plugin SDK to build community. |
| **Long enterprise sales cycles** | High | Medium | PLG-first for 6 months. Enterprise is inbound upgrade path, not primary motion. |
| **Complexity churn** | High | Medium-High | Onboarding wizard, sample pipelines, quickstart datasets. Measure time-to-first-pipeline. |
| **Stripe outage / billing failure** | Medium | Low | Webhook idempotency. Graceful degradation (don't block active pipelines for billing issues). |
| **Compliance burden (SOC 2)** | High | Medium | Start SOC 2 process early (Month 4). Document everything. On-premise option for regulated customers. |

---

## 13. Key Partnerships

| Partner | Why | How |
|---------|-----|-----|
| **Supabase** | Current backend. Their marketplace / case study. | Apply for Supabase Partner Program. Co-marketing opportunity. |
| **Stripe** | Billing provider. Stripe Partner for reduced fees. | Apply for Stripe Partner Program. |
| **Cloud Marketplaces** | AWS, Azure, GCP — enterprise discovery channel. | Transactable listing with custom AMI/VHD. |
| **Data Engineering communities** | dbt, Airbyte, Prefect communities — our users are there. | Sponsor meetups, contribute to discussions, share architecture. |

---

## Appendix A: Marketing Copy

### Homepage Headlines

**Hero:** "Process Data Without Moving It"

**Sub:** Turn idle machines into a distributed processing cluster. One binary. One command. Zero data egress.

**CTA:** "Start Free →"

### Feature Highlights

- **Distributed by default** — Every machine runs an agent. Jobs are automatically matched to devices using vector similarity.
- **Zero-trust plugins** — Custom code in any language, Ed25519-signed, SHA256-verified, OS-sandboxed.
- **Auto-advancing pipelines** — Define steps, upload data, walk away. The system handles chunking, execution, merging.
- **Your infrastructure** — Runs on-premise, in VPC, or at the edge. Data never leaves your control.
- **Predictable pricing** — $799/mo for 10 agents. No per-row fees. No surprise bills.

### Taglines

- "The distributed data processing platform that runs on hardware you already own."
- "Fivetran without the egress. Spark without the cluster. Cloud without the lock-in."
- "One binary to turn every machine into a data processing node."
- "Data sovereignty isn't a feature — it's the architecture."

---

## Appendix B: Investor Narrative

### The Problem in One Paragraph

"Organizations spend millions moving data to cloud platforms for processing. Fivetran charges per row, Snowflake per credit, Databricks per DBU. A 10TB dataset costs $500–1,500 in egress fees every time you process it. Meanwhile, 60–80% of on-premise compute sits idle. SentraZero eliminates the movement and uses the hardware you already own."

### The Solution in One Paragraph

"SentraZero is a distributed data processing platform that runs on customer infrastructure. A single Go binary installs on any machine (Linux, macOS, Windows). Agents register with a Supabase control plane, receive jobs via Realtime WebSocket, and process data locally using signed, sandboxed plugins. Results are uploaded — data never leaves. Pricing is per-device, not per-GB."

### The Ask in One Paragraph

"We're seeking $1.5M in seed funding to take SentraZero to revenue. The product is fully built — 22 Go packages, 55+ edge functions, 51 database tables, complete dashboard, and agent fleet. We need Stripe billing integration, GTM execution, and team expansion. Target: $1.79M ARR by Month 12."

---

*End of GTM Document*
