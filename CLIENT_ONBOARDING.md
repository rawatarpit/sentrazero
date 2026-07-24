# SentraZero — Client Onboarding Guide

> **Purpose:** This document is the complete reference for how SentraZero presents itself to new clients. It covers three things:
>
> 1. **Agent Binary Distribution** — How clients download and run the agent
> 2. **Dashboard** — What clients see in their browser
> 3. **Onboarding Flow** — The end-to-end journey from signup to first pipeline run
>
> Give this to your website agent. It has the exact copy, layout, and UX for each section.

---

## Table of Contents

1. [Platform Overview](#1-platform-overview)
2. [Agent Binary Distribution](#2-agent-binary-distribution)
3. [Dashboard](#3-dashboard)
4. [Client Onboarding Flow](#4-client-onboarding-flow)
5. [Technical Details](#5-technical-details)
6. [Website Copy & Presentation](#6-website-copy--presentation)

---

## 1. Platform Overview

### What SentraZero Is

SentraZero is a **self-hosted data processing platform**. Clients run their own data through pipelines — deduplication, classification, enrichment — using their own infrastructure.

**Key architecture:**
- **Backend** (Supabase) — Hosted centrally. Manages auth, jobs, datasets, pipelines.
- **Dashboard** (Next.js) — Web UI for managing pipelines, viewing results, adding agents.
- **Agents** (Go binary) — Run on the client's own machines. Process data. Scale infinitely.

**The core value proposition:**
> "Your data never leaves your infrastructure. You control the compute. You scale by adding more agents to your own machines."

---

## 2. Agent Binary Distribution

### What Clients Need

Every client needs at least one agent running on their infrastructure. The agent is a single compiled Go binary — no dependencies, no Docker, no runtime.

### Available Binaries

| Platform | Architecture | Binary Name | Size |
|----------|-------------|-------------|------|
| Linux | amd64 (Intel/AMD) | `sentra-agent-amd64` | ~19 MB |
| Linux | arm64 (ARM) | `sentra-agent-arm64` | ~18 MB |

### How to Present on Website

Create a **"Download Agent"** or **"Get Started"** section with:

```
┌─────────────────────────────────────────────────┐
│                                                 │
│   Download SentraZero Agent                     │
│                                                 │
│   The agent runs on your infrastructure.        │
│   Your data never leaves your machines.         │
│                                                 │
│   ┌─────────────────────┐  ┌─────────────────┐  │
│   │  Linux amd64        │  │  Linux arm64    │  │
│   │  (Intel/AMD)        │  │  (ARM)          │  │
│   │  [Download]         │  │  [Download]     │  │
│   └─────────────────────┘  └─────────────────┘  │
│                                                 │
│   Or install via CLI:                           │
│   $ curl -fsSL https://sentra.zero/install.sh | │
│     bash                                        │
│                                                 │
└─────────────────────────────────────────────────┘
```

### Hosting the Binary

The binary files are at:
```
sentra-agent-amd64   (local build output)
sentra-agent-arm64   (local build output)
```

**Recommended hosting options (pick one):**

1. **GitHub Releases** — Upload binaries to a GitHub release. Clients download via direct URL.
   - Example URL: `https://github.com/yourorg/sentrazero/releases/latest/download/sentra-agent-amd64`
   
2. **S3 / R2 Bucket** — Upload to a public bucket.
   - Example URL: `https://releases.sentra.zero/sentra-agent-amd64`

3. **Your website's `/public` folder** — If using Next.js/Vercel, put in `public/agents/` and serve statically.

### Binary Checksums (provide for security)

Always provide SHA256 checksums alongside downloads:

```bash
# Generate checksums (run once, update when binary changes)
shasum -a 256 sentra-agent-amd64 sentra-agent-arm64
```

### Version Management

Currently there's one version. When you release updates:
1. Build new binaries locally (`go build -o sentra-agent-amd64 ./cmd/`)
2. Upload to hosting location
3. Update checksums
4. Agents auto-check for updates on restart (or you can push updates via the dashboard)

---

## 3. Dashboard

### What the Dashboard Is

The dashboard is a **Next.js 14 web application** that clients access via browser. It's the control center for:

- Viewing datasets and their status
- Running pipelines
- Managing agents (viewing health, adding new ones)
- Viewing execution results
- Managing team members

### Dashboard Tech Stack

- **Framework:** Next.js 14 (App Router)
- **Auth:** Supabase Auth (JWT cookie-based — users log in with email/password)
- **Database:** Supabase (PostgreSQL + RLS for multi-tenant isolation)
- **Styling:** Tailwind CSS + shadcn/ui components

### Dashboard Location

```
/Users/arpitrawat/Downloads/sentra-website-main/
```

### How to Present on Website

Create a **"Dashboard"** or **"Control Center"** section:

```
┌─────────────────────────────────────────────────┐
│                                                 │
│   Your Command Center                           │
│                                                 │
│   Manage pipelines, monitor agents, and view    │
│   results — all from one place.                 │
│                                                 │
│   ┌───────────────────────────────────────────┐ │
│   │                                           │ │
│   │   [Screenshot/Preview of Dashboard]       │ │
│   │                                           │ │
│   │   • Real-time agent health monitoring     │ │
│   │   • One-click pipeline execution          │ │
│   │   • Dataset management & results viewer   │ │
│   │   • Team member invitations               │ │
│   │                                           │ │
│   └───────────────────────────────────────────┘ │
│                                                 │
│   [Launch Dashboard]                            │
│                                                 │
└─────────────────────────────────────────────────┘
```

### Dashboard Deployment

The dashboard needs to be deployed so clients can access it via browser.

**Recommended: Vercel (free tier works)**

```bash
cd sentra-website-main
npx vercel --prod
```

Or connect the GitHub repo to Vercel for automatic deploys.

**Environment variables needed in Vercel:**
```
NEXT_PUBLIC_SUPABASE_URL=https://ivwghcveytrkwqxxdtak.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=<your-anon-key>
```

**Custom domain:**
- Point `app.sentra.zero` or `dashboard.sentra.zero` to the Vercel deployment
- Update CORS on Supabase Edge Functions to allow the new domain

### Dashboard Pages

| Page | Route | Description |
|------|-------|-------------|
| Login | `/auth/login` | Email + password login |
| Signup | `/auth/signup` | Create account (auto-creates org) |
| Dashboard Home | `/dashboard` | Overview — datasets, recent runs, agent status |
| Datasets | `/dashboard/datasets` | List all datasets, view status, trigger scans |
| Pipelines | `/dashboard/pipelines` | Pipeline templates, run pipelines |
| Agents | `/dashboard/agents` | View connected agents, get claim codes |
| Settings | `/dashboard/settings` | Org settings, team management |

---

## 4. Client Onboarding Flow

### The Journey (5 Steps)

```
Step 1: Sign Up
    ↓
Step 2: Download Agent
    ↓
Step 3: Get Claim Code
    ↓
Step 4: Run Agent on Your Infra
    ↓
Step 5: Run Your First Pipeline
```

### Step-by-Step Breakdown

#### Step 1: Sign Up

**What happens:**
1. Client visits the dashboard URL
2. Clicks "Sign Up"
3. Enters email + password
4. Account is created, an organization is auto-created
5. Client is logged in and sees the dashboard

**On the website, present as:**
```
Create Your Account
─────────────────────
1. Go to app.sentra.zero
2. Click "Sign Up"
3. Enter your email and password
4. You're in — your workspace is ready
```

#### Step 2: Download the Agent

**What happens:**
1. Client goes to the "Agents" page in the dashboard
2. Sees download links for their platform
3. Downloads the binary

**On the website, present as:**
```
Download the Agent
─────────────────────
1. In your dashboard, go to "Agents"
2. Click "Download Agent"
3. Choose your platform:
   - Linux amd64 (most common — Intel/AMD servers)
   - Linux arm64 (ARM servers, AWS Graviton, Apple Silicon VMs)
4. Save the binary to your server
```

#### Step 3: Get a Claim Code

**What happens:**
1. In the dashboard, client clicks "Add Agent"
2. A claim code is generated (one-time use)
3. Client copies the claim code

**On the website, present as:**
```
Generate a Claim Code
─────────────────────
1. In your dashboard, go to "Agents"
2. Click "Add New Agent"
3. Copy the claim code shown
4. This code links the agent to your account
```

#### Step 4: Run the Agent

**What happens:**
1. Client SSHs into their server
2. Runs the binary with the claim code
3. Agent registers itself, connects to the backend
4. Agent appears as "Online" in the dashboard

**On the website, present as:**
```
Run the Agent
─────────────────────
1. SSH into your server
2. Make the binary executable:
   chmod +x sentra-agent-amd64

3. Run it with your claim code:
   ./sentra-agent-amd64 --claim-code YOUR_CODE_HERE

4. The agent will:
   - Register with your account
   - Connect to SentraZero
   - Start listening for jobs

5. You'll see it appear as "Online" in your dashboard
```

**Advanced options:**
```bash
# Custom storage (S3)
./sentra-agent-amd64 \
  --claim-code YOUR_CODE \
  --storage-type s3 \
  --s3-endpoint https://s3.amazonaws.com \
  --s3-bucket your-bucket \
  --s3-region us-east-1

# Custom concurrency (process multiple jobs at once)
./sentra-agent-amd64 \
  --claim-code YOUR_CODE \
  --max-concurrency 4

# Run as systemd service (production)
sudo ./sentra-agent-amd64 \
  --claim-code YOUR_CODE \
  --service
```

#### Step 5: Run Your First Pipeline

**What happens:**
1. Client uploads a dataset (CSV, JSON, etc.)
2. System scans and chunks the data
3. Client selects a pipeline template
4. Clicks "Run Pipeline"
5. Agents process the data
6. Results are merged and available for download

**On the website, present as:**
```
Run Your First Pipeline
─────────────────────
1. Upload a dataset from the "Datasets" page
2. Wait for the scan to complete (~seconds)
3. Go to "Pipelines"
4. Select a pipeline template
5. Choose your dataset
6. Click "Run"
7. Watch progress in real-time
8. Download your results when complete
```

---

## 5. Technical Details

### Agent Registration Flow

```
Client runs agent with claim code
    ↓
Agent calls POST /functions/v1/claim_device
    { claim_code: "...", sysinfo: { hostname, cpu, memory, ... } }
    ↓
Backend validates claim code via validate_claim_secret() RPC
    ↓
Backend creates device record (org_id, name, status=online)
    ↓
Backend returns: { agent_token, device_id, backend_url, anon_key }
    ↓
Agent stores credentials locally (job_dedup.json)
    ↓
Agent starts polling POST /functions/v1/poll_state
    ↓
Agent appears as "Online" in dashboard
```

### Agent Job Lifecycle

```
Pipeline runs → creates pending agent_jobs
    ↓
Agent polls poll_state → gets assigned a job (status: pending → assigned)
    ↓
Agent calls start_job → status: running
    ↓
Agent processes data (reads from S3/mount, writes results)
    ↓
Agent calls complete_job → status: completed
    ↓
advance_pipeline checks → moves to next step or triggers merge
    ↓
Merge job created → merge agent reads all chunks → writes merged output
    ↓
Dataset status → merged
```

### Org Isolation

Every piece of data is scoped to an organization:
- **RLS policies** on all tables ensure users only see their org's data
- **Agents** only receive jobs from their org
- **Claim codes** are org-scoped — an agent registered to Org A can never see Org B's data

### Multi-Agent Scaling

Clients scale by adding more agents:

```
1 agent  = 1 concurrent job    (good for small datasets)
3 agents = 3 concurrent jobs   (good for medium workloads)
10 agents = 10 concurrent jobs (good for large-scale processing)
N agents = N concurrent jobs   (scales linearly)
```

Each agent is independent. No coordination needed. The backend handles job distribution automatically.

---

## 6. Website Copy & Presentation

### Hero Section (Homepage)

```
SentraZero
──────────
Self-hosted data processing that scales with your infrastructure.

Your data never leaves your machines.
You control the compute.
You add agents to scale.

[Get Started]  [View Docs]
```

### How It Works Section

```
How It Works
────────────

1. Deploy the Agent
   One binary. No dependencies. Runs on your infrastructure.
   
2. Upload Your Data
   CSV, JSON, Parquet, images, video, audio — we handle it all.
   
3. Run Pipelines
   Deduplicate, classify, enrich — with pre-built or custom pipelines.
   
4. Get Results
   Merged, clean output — ready for your next step.

[See it in action →]
```

### Features Section

```
Features
────────

🔒 Data Sovereignty
   Your data stays on your machines. We never see it.

⚡ Infinite Scale
   Add more agents to your infrastructure. Linear scaling.

🧩 Plugin System
   Python, Node, Go, Rust — run any code in sandboxed environments.

📊 Real-Time Monitoring
   Watch your pipelines run in the dashboard. Agent health at a glance.

👥 Team Collaboration
   Invite team members. Role-based access. Multi-tenant by default.

🔄 Auto-Scaling Pipelines
   Compound and step-by-step pipelines. Automatic merge. Zero config.
```

### Pricing Section (if applicable)

```
Pricing
───────

Free Tier
- 1 agent
- 3 pipeline runs/month
- Community support

Pro
- Unlimited agents
- Unlimited pipeline runs
- Priority support
- Custom pipeline templates

Enterprise
- Self-hosted dashboard option
- SSO / SAML
- Dedicated support
- SLA guarantee
```

### CTA Section

```
Ready to Process Data on Your Terms?
────────────────────────────────────

No vendor lock-in. No data leaving your infra.
Just clean, scalable data processing.

[Start Free]  [Talk to Us]
```

---

## Appendix A: Claim Code Lifecycle

```
Dashboard: "Add Agent" button
    ↓
Backend: generate_claim_secret() RPC
    ↓
Returns: claim_code (one-time use, expires in 24h)
    ↓
Client copies claim_code
    ↓
Agent: ./sentra-agent --claim-code <code>
    ↓
Backend: validate_claim_secret() → links agent to org
    ↓
Claim code is consumed (cannot be reused)
```

## Appendix B: Supported Data Types

| Type | Extensions | Processing |
|------|-----------|------------|
| Tabular | `.csv`, `.json`, `.parquet`, `.xlsx` | Row-level chunking, dedup |
| Images | `.jpg`, `.png`, `.webp`, `.gif` | Image similarity, classification |
| Video | `.mp4`, `.avi`, `.mov` | Frame extraction, dedup |
| Audio | `.mp3`, `.wav`, `.m4a` | Transcription, similarity |
| PDF | `.pdf` | Text extraction, chunking |
| Archives | `.zip`, `.tar.gz` | Auto-extraction, recursive processing |

## Appendix C: Pipeline Templates (Current)

| Template | Steps | Description |
|----------|-------|-------------|
| Dedup Pipeline | 2 | Deduplicate tabular data using similarity matching |
| Classification Pipeline | 1 | Classify data using ML plugins |
| Enrichment Pipeline | 2 | Enrich data with external sources |

## Appendix D: Agent CLI Flags

```
Usage:
  sentra-agent [flags]

Flags:
  --claim-code string       One-time claim code for registration
  --backend-url string      Backend URL (default: auto-detected from claim)
  --storage-type string     Storage backend: local, s3 (default: local)
  --s3-endpoint string      S3 endpoint URL
  --s3-bucket string        S3 bucket name
  --s3-region string        S3 region (default: us-east-1)
  --max-concurrency int     Max concurrent jobs (default: 1)
  --service                 Install as systemd service
  --version                 Print version and exit
```
