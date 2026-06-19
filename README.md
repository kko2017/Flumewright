# Flumewright

> A distributed message bus — a high-throughput PubSub infrastructure built on C# / .NET 8 / gRPC.

![C#](https://img.shields.io/badge/C%23-239120?logo=csharp&logoColor=white)
![.NET 8](https://img.shields.io/badge/.NET%208-512BD4?logo=dotnet&logoColor=white)
![gRPC](https://img.shields.io/badge/gRPC-2496ED?logo=grpc&logoColor=white)
![Protobuf](https://img.shields.io/badge/Protobuf-EA4C89?logo=protobuf&logoColor=white)

Flumewright is a high-throughput message bus where a standalone **Broker** process mediates
communication between **Publisher** and **Subscriber** processes over gRPC, following the
publish-subscribe pattern. It is built on a **Kafka-style log model**: publish appends to a
per-partition append-only log, and subscribers consume by pulling at their own offset.

> **Status:** Phase 1 in progress. M1 (gRPC contract + broker host + pub→sub) and M2
> (partitioning + append-only log + offset-based consumption) are complete. Transport security
> (mTLS), consumer groups, offset-commit acks, and 100K-scale streaming are upcoming milestones —
> see [Roadmap](#-roadmap). Features below are marked ✅ done or 🔜 planned accordingly.

---

## 🤖 Built with AI — under human control

Flumewright was built using an **agent-assisted workflow** (Claude for planning, design, and code
review; Gemini CLI for implementation), with the human owning all architecture, verification, and
decisions. Work was delegated in small, scoped, individually-verified steps; every design choice and
fix is recorded; total tooling cost was kept **under $60/month**.

This is *assisted*, not autonomous: the AI never pushes to remote, never decides scope, and every
proposal is approved or rejected by a human. Hand code-review caught real defects the AI had reported
as passing tests (see the decision-and-fix log).

See **[AI Collaboration & Cost Strategy](docs/ai-collaboration.md)** for the full model — control
mechanisms, verification discipline, and cost strategy.

---

## ✨ Features

**Working today (M1–M2):**
- ✅ **Process isolation** — the broker runs as an independent process; clients connect over gRPC
- ✅ **Log-based delivery** — publish appends to a per-partition append-only log; messages are retained
  (for the process lifetime in Phase 1) rather than pushed-and-forgotten
- ✅ **Partitioning & routing** — a topic is split into N partitions; a `partition_key` routes by stable
  hash (same key → same partition → per-partition ordering), and keyless messages spread round-robin
- ✅ **Offset-based consumption** — subscribers pull by holding their own offset (cursor); fan-out is many
  subscribers reading the same log at their own offsets
- ✅ **Extensibility** — the broker treats payloads as opaque bytes, so clients may use any `.proto` they like

**Planned (later milestones):**
- 🔜 **At-least-once delivery via offset commit** — durable consumer progress + redelivery (M3)
- 🔜 **Consumer groups** — partition assignment across group members for load balancing (M3)
- 🔜 **mTLS security** — mutual certificate verification (M4)
- 🔜 **100K-scale throughput** — streaming publish + batching + backpressure (M5)
- 🔜 **Observability** — metrics and structured logging (M6; distributed tracing in Phase 2)
- 🔜 **Persistence & retention** — disk-backed log, eviction policy, replay/seek (Phase 2)

---

## 🏗️ Architecture

```
 ┌─────────────┐            gRPC              ┌──────────────────────────────┐
 │ Publisher A │ ──────(unary publish)──────▶ │           BROKER             │
 │ (proto X)   │                              │  ┌────────────────────────┐  │
 └─────────────┘                              │  │  gRPC Endpoint Layer    │  │
 ┌─────────────┐                              │  │  Router / Partitioner   │  │
 │ Publisher B │ ───────────────────────────▶ │  │  Topic → Partitions     │  │
 │ (proto Y)   │                              │  │   (append-only logs)    │  │
 └─────────────┘                              │  │  Auth (mTLS) ······ M4  │  │
 ┌─────────────┐                              │  │  Delivery (offset)      │  │
 │ Subscriber 1│ ◀──(server stream by offset)─ │  │  Metrics/Logging ·· M6  │  │
 │ (offset N)  │                              │  └────────────────────────┘  │
 └─────────────┘                              └──────────────────────────────┘

 Publish appends to a partition's log; each subscriber reads the log at its own offset.
 (mTLS, consumer groups, offset-commit acks, and streaming publish are upcoming — see Roadmap.)
```

See [docs/design/plan.md](docs/design/plan.md) for the full design.

---

## 🚀 Quick Start

### Prerequisites
- [.NET 8 SDK](https://dotnet.microsoft.com/download) (8.0.x)
- Git

### Build & Test
```bash
git clone https://github.com/<USERNAME>/Flumewright.git
cd Flumewright

dotnet restore
dotnet build --configuration Release

# Unit + integration tests (excludes load tests)
dotnet test --filter "Category!=Load"
```

### Run the Broker
```bash
dotnet run --project src/Flumewright.Broker
```
The broker starts a gRPC host (plaintext in Phase 1; mTLS arrives in M4). Publishers append to a
topic's partition log; subscribers stream messages from their offset.

### Run the Sample Publisher / Subscriber
```bash
# Terminal 1 — subscriber
dotnet run --project samples/SampleSubscriber

# Terminal 2 — publisher
dotnet run --project samples/SamplePublisher
```
The samples use the `Flumewright.Client` SDK (`FlumewrightPublisher` / `FlumewrightSubscriber`).
See the integration tests for further end-to-end usage.

---

## 📁 Project Structure

```
Flumewright/
├── src/
│   ├── Flumewright.Protocol/        # Fixed .proto + generated code (shared contract)
│   ├── Flumewright.Broker.Core/     # Routing/partitioning/delivery (pure, testable logic)
│   ├── Flumewright.Broker/          # Broker host (executable process)
│   ├── Flumewright.Client/          # Publisher/Subscriber SDK
│   ├── Flumewright.Security/        # mTLS / certificate utilities
│   └── Flumewright.Observability/   # Metrics / logging / tracing
├── samples/                    # Example publisher/subscriber processes
├── tests/
│   ├── Flumewright.UnitTests/
│   ├── Flumewright.IntegrationTests/
│   └── Flumewright.LoadTests/       # Performance/stress (Category=Load)
└── docs/                       # Design, learning, and guide documents
```

---

## 🔄 Development Workflow

- **Branches**: `main` (always green) + milestone branches (`feat/mN-...`)
- **Commits**: Conventional Commits (`feat:`, `fix:`, `test:` ...)
- **Validation**: pre-commit hook (build + unit tests) → CI (unit + integration) → merge to main
- **Releases**: push a `vX.Y.Z` tag → automated Release publishing

See [docs/guides/version-control-and-validation-guide.md](docs/guides/version-control-and-validation-guide.md) for the full rules.

### Workflows (GitHub Actions)
| Workflow | Trigger | Role |
|----------|---------|------|
| CI | push/PR → main | Build + unit/integration tests + dev artifact |
| Load Tests | manual / nightly | Performance & stress tests |
| Release | push `v*` tag | Release build + publish to Releases tab |

---

## 📚 Documentation

- [Execution Plan](docs/design/plan.md)
- [Message Bus & PubSub Study Notes](docs/learning/study-notes.md)
- [Version Control & Validation Guide](docs/guides/version-control-and-validation-guide.md)
- [AI Collaboration & Cost Strategy](docs/ai-collaboration.md)
- [M2 Design Note — Partition Log Model](docs/design/m2-partitioning.md)
- [Decision & Fix Log](docs/decisions/decision-and-fix-log.md)
- [Architecture Decision Records (ADR)](docs/decisions/)

---

## 📦 Releases / Downloads

- **Stable releases**: download per-version artifacts from the [Releases](https://github.com/<USERNAME>/Flumewright/releases) tab
- **Development builds**: Actions tab → latest CI run → download from Artifacts

---

## 🗺️ Roadmap

Phase 1 (in-memory, high-throughput, security) → Phase 2 (persistence & ops).

| Milestone | Scope | Status |
|-----------|-------|--------|
| M1 | gRPC contract + broker host + pub→sub passthrough | ✅ done |
| M2 | Partitioning + append-only log + offset-based consumption | ✅ done |
| M3 | Consumer groups + at-least-once via offset commit + redelivery/DLQ | 🔜 |
| M4 | mTLS (mutual certificates) | 🔜 |
| M5 | Streaming publish + batching + backpressure (100K) | 🔜 |
| M6 | Metrics + logging → tag `v0.1.0` | 🔜 |
| Phase 2 | Disk persistence, retention/eviction, replay/seek, tracing | 🔜 |

See the [Execution Plan](docs/design/plan.md) for the full design and the
[Decision & Fix Log](docs/decisions/decision-and-fix-log.md) for the reasoning behind key choices
(e.g. the push→log model switch, DEC-015).

---

## 📄 License

TBD (to be decided)
