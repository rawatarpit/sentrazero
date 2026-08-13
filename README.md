# SentraZero — Self-Hosted Pipeline Orchestration

[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org)
[![OS](https://img.shields.io/badge/OS-Linux%20%7C%20macOS%20%7C%20Windows-blue?style=flat)]()
[![License](https://img.shields.io/badge/License-Proprietary-red?style=flat)]()

> SentraZero's agent + plugin SDK: deploy a single 12–15 MB Go binary anywhere — bare metal, VM, Raspberry Pi, Kubernetes — and start processing pipelines without your data ever leaving your infrastructure. Works across row-based (CSV) and file-based (image, video, PDF) datasets through the same plugin architecture.

---

## What is SentraZero?

**SentraZero is self-hosted pipeline orchestration for teams who can't send their data to a third party to process it — verified, not just promised.**

It is a single, self-hosted Go agent with embedded security (seccomp), cryptographic plugin verification (Ed25519), GPU-aware scheduling, and rich media auto-detection for images, video, PDFs, audio, and any binary files. Everything runs from one statically-linked binary (12–15 MB depending on server/compute architecture) — no Docker required, no runtime engines, no separate wrapper binaries.

The control plane (job scheduling, org management) is a managed service. Your **data and your plugins** run on your own hardware.

### Why it exists

| Pillar | What's true | How it's verified |
|--------|-------------|-------------------|
| **1. Verified data sovereignty** | Zero rows of client data touch the control plane database | Direct queries against production tables — `vector_store`, `step_outputs`, `plugin_execution_history` are empty after real runs |
| **2. Orchestration efficiency** | Compound-mode execution cuts multi-step pipelines from per-step scheduling to a single job | Measured before/after: a 3-step pipeline went from ~35 min to under 30 s (scheduling overhead, not compute) |
| **3. Rich media by default** | One platform for any data shape — rows, images, video, PDF, audio — auto-detected from content headers | Built-in scan extractors (CSV, JSON, Parquet, image EXIF, video, PDF, audio, archives) |
| **4. Sandboxed, signed, auditable** | Every plugin runs in a resource-limited sandbox with Ed25519 signature verification before execution | Embedded seccomp filter (~68 allowlisted syscalls, default-deny) + per-platform sandboxing |

---

## How It Works

```
                        ┌─────────────────────────────┐
                        │   Control Plane (managed)   │
                        │  orgs · jobs · scheduling   │
                        └──────────────┬──────────────┘
                                       │ HTTPS / SSE
                    ┌──────────────────┼──────────────────┐
                    ▼                  ▼                  ▼
            ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
            │   Agent      │  │   Agent      │  │   Agent      │
            │  (Go binary) │  │  (Go binary) │  │  (Go binary) │
            │  on your HW  │  │  on your HW  │  │  on your HW  │
            └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
                   │                  │                  │
                   └──────────────────┼──────────────────┘
                                      ▼
                        ┌─────────────────────────────┐
                        │   Your storage (S3 / local) │
                        │   raw data · plugin outputs │
                        └─────────────────────────────┘
```

1. **Register an agent** with a claim code from your control plane dashboard.
2. **Upload a dataset** (rows or files) to your own storage.
3. **Define a pipeline** — ordered steps, each a built-in handler or a plugin.
4. **Plugins execute on your hardware**, inside a sandbox, with your data.
5. **Results stay on your storage**; the control plane only sees metadata.

---

## Quick Start

### 1. Download the agent

| Platform | Download |
|----------|----------|
| Linux amd64 | [sentra-agent-linux-amd64](https://github.com/rawatarpit/sentrazero/releases/download/v1.0.0/sentra-agent-linux-amd64) |
| Linux arm64 | [sentra-agent-linux-arm64](https://github.com/rawatarpit/sentrazero/releases/download/v1.0.0/sentra-agent-linux-arm64) |

Verify the checksum before running:

```bash
curl -LO https://github.com/rawatarpit/sentrazero/releases/download/v1.0.0/sentra-agent-linux-amd64
# Download SHA256SUMS from the release page, then:
shasum -a 256 sentra-agent-linux-amd64   # compare against the release checksum
```

### 2. Run with a claim code

```bash
chmod +x sentra-agent-linux-amd64
./sentra-agent-linux-amd64 --claim-code <CLAIM_CODE>
```

Or via environment variable:

```bash
export SENTRA_CLAIM_CODE=<claim_code>
./sentra-agent-linux-amd64
```

The agent registers itself, receives configuration, and starts processing jobs. Claim codes are issued by your SentraZero control plane operator.

---

## Plugin SDK

Plugins extend the agent. They can be written in **Python, Node.js, Go, Rust, or native binaries**.

- **Manifest** — declare the plugin's runtime, resource limits, and capabilities.
- **Signing** — plugins are signed with **Ed25519** on upload and the signature is verified again on the agent before every execution. No unsigned plugin runs.
- **Sandboxing** — resource limits (memory, CPU time, timeout) are enforced per platform (see table below).
- **GPU routing** — plugins that declare a GPU requirement are routed to GPU-capable agents.

```json
{
  "name": "example-plugin",
  "version": "1.0.0",
  "runtime": "python",
  "entry": "main.py",
  "resource_limits": { "memory_mb": 512, "cpu_seconds": 60, "timeout_s": 300 },
  "gpu": false
}
```

---

## Cross-Platform Sandboxing

The agent is a true cross-platform binary — compile once per platform, run anywhere. Each OS gets platform-native sandboxing, not a least-common-denominator approach:

| Feature | Linux | macOS | Windows |
|---------|-------|-------|---------|
| **Syscall filter** | Seccomp BPF (~68 syscalls, default-deny) | — | — |
| **Namespace isolation** | User/PID/Mount/UTS/IPC/Net | — | — |
| **Process isolation** | Namespace clone | Seatbelt (Apple sandbox) | Job Objects |
| **Network isolation** | Network namespace | Seatbelt `(deny network*)` | Windows Firewall block rule |
| **Memory limit** | cgroup v2 `memory.max` + rlimit | rlimit | Job Object `ProcessMemoryLimit` |
| **CPU limit** | cgroup v2 `cpu.max` + rlimit | rlimit | Job Object CPU quotas |
| **GPU access** | Device node passthrough | Metal / IOAccelerator | — |
| **Ed25519 plugin verification** | Yes | Yes | Yes |

Linux has the most comprehensive sandbox (seccomp + namespaces + cgroups). macOS uses Seatbelt's default-deny profiles; Windows uses Job Objects plus a firewall rule when network access is disabled.

---

## Building from Source

Requires **Go 1.25+**.

```bash
git clone https://github.com/rawatarpit/sentrazero.git
cd sentrazero
make build-all
```

Cross-compile for all supported platforms with the included Makefile targets. The build produces a single static binary per platform (~13–14 MB stripped).

---

## Configuration

Public environment variables (all optional unless noted):

| Variable | Purpose |
|----------|---------|
| `SENTRA_CLAIM_CODE` | Device registration claim code (or pass `--claim-code`) |
| `SENTRA_API_URL` | Override the control plane API URL |
| `SENTRA_LOG_LEVEL` | Log verbosity (`debug`, `info`, `warn`, `error`) |

Secrets are never read from the environment of the plugin sandbox — `SENTRA_SECRET`, `SENTRA_API_KEY`, and cloud credentials are blocked from plugin processes by default.

---

## Verified Performance Snapshot

Measured on live production agents (v1.0.0, x86_64 and ARM64). Reproducible; methodology documented in the release notes.

| Metric | Value |
|--------|-------|
| Agent idle RAM | **~15 MB** RSS |
| Binary size | **13–14 MB** (static, stripped) |
| Python plugin warm start | **~0.11 s** |
| Python plugin throughput | **~5.7 jobs/s** |
| Native subprocess start | **~1–2 ms** |
| Sandbox namespace creation | **~5–10 ms** |
| Control-plane RTT (same region) | **<2 ms** |
| Cold startup to operational | **<100 ms** |

**Honest limitations:**
- Sandboxing is strongest on Linux; macOS and Windows rely on OS-native mechanisms with no syscall filtering equivalent.
- Measured throughput is per single agent; scale linearly by adding agents.
- These are platform numbers, not plugin business-logic results. Plugin accuracy/reliability varies per plugin.

---

## Security & Privacy

- **Verified sovereignty** — the control plane stores only metadata; verified by direct database queries.
- **Signed plugins** — Ed25519 signatures verified on the agent before every execution.
- **Sandboxed execution** — resource limits and syscall filtering enforced per platform.
- **No data at rest in the control plane** — raw data and outputs live on your storage.
- This repo intentionally does **not** contain control-plane internals, database schema, or any credentials.

---

## License

Proprietary. The agent binary is distributed via GitHub Releases for evaluation; the control plane and plugin SDK remain closed-source. Contact the maintainers for deployment and licensing.
