# Flumewright

> A distributed message bus — a high-throughput PubSub infrastructure built on C# / .NET 8 / gRPC.

![C#](https://img.shields.io/badge/C%23-239120?logo=csharp&logoColor=white)
![.NET 8](https://img.shields.io/badge/.NET%208-512BD4?logo=dotnet&logoColor=white)
![gRPC](https://img.shields.io/badge/gRPC-2496ED?logo=grpc&logoColor=white)
![Protobuf](https://img.shields.io/badge/Protobuf-EA4C89?logo=protobuf&logoColor=white)

Flumewright is a high-throughput message bus where a standalone **Broker** process mediates
communication between **Publisher** and **Subscriber** processes over mTLS-secured gRPC,
following the publish-subscribe pattern.

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

- **Process isolation** — the broker runs as an independent process; clients connect over the network
- **High throughput** — handles bursts of 100K+ messages via multi-threading and gRPC streaming
- **Extensibility** — the broker treats payloads as opaque bytes, so clients may use any `.proto` they like
- **At-least-once delivery** — ack/nack based, with in-flight tracking and redelivery
- **mTLS security** — mutual certificate verification; only authorized clients may connect
- **Consumer groups & partitioning** — load balancing plus per-partition ordering
- **Observability** — metrics and structured logging (distributed tracing in Phase 2)

---

## 🏗️ Architecture

```
 ┌─────────────┐         mTLS / gRPC          ┌──────────────────────────────┐
 │ Publisher A │ ───────(bidi stream)───────▶ │           BROKER             │
 │ (proto X)   │                              │  ┌────────────────────────┐  │
 └─────────────┘                              │  │  gRPC Endpoint Layer    │  │
 ┌─────────────┐                              │  │  Auth (mTLS)            │  │
 │ Publisher B │ ───────────────────────────▶ │  │  Router / Partitioner   │  │
 │ (proto Y)   │                              │  │  Topic → Partitions     │  │
 └─────────────┘                              │  │  Delivery + Ack/Nack    │  │
 ┌─────────────┐                              │  │  Metrics / Logging      │  │
 │ Subscriber 1│ ◀──────(server stream)────── │  └────────────────────────┘  │
 │ (group G1)  │                              └──────────────────────────────┘
 └─────────────┘
```

See [docs/design/plan.md](docs/design/plan.md) for the full design.

---

## 🚀 Quick Start

> ⚠️ This section is filled in as features become functional. (Currently in scaffolding stage.)

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

### Run the Broker (planned)
```bash
dotnet run --project src/Flumewright.Broker
```

### Run the Sample Publisher / Subscriber (planned)
```bash
# Terminal 1
dotnet run --project samples/SampleSubscriber

# Terminal 2
dotnet run --project samples/SamplePublisher
```

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
- [Architecture Decision Records (ADR)](docs/decisions/)

---

## 📦 Releases / Downloads

- **Stable releases**: download per-version artifacts from the [Releases](https://github.com/<USERNAME>/Flumewright/releases) tab
- **Development builds**: Actions tab → latest CI run → download from Artifacts

---

## 📄 License

TBD (to be decided)
