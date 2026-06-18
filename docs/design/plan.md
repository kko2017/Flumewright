# Distributed Message Bus — Execution Plan v0.7

> Codename: **Flumewright** (short alias: **fw** — used in the proto package `fw.v1`, etc.)
> Stack: C# / .NET 8.0 (LTS) / gRPC (HTTP/2) / Protobuf
> Tooling: Google Antigravity CLI / GitHub + GitHub Actions

---

## 0. Document Map (Single Source of Truth)

The documents below collectively define the project's source of truth. Only the English versions
are committed (kept under `docs/`); Korean versions are for personal reference. When this plan
references another document, it points to the filenames below.

| Document | English file (repo) | Role |
|----------|---------------------|------|
| Execution Plan (this document) | `01-execution-plan.en.md` → `docs/design/plan.md` | Top-level reference for scope, architecture, roadmap |
| Study Notes | `02-study-notes.en.md` → `docs/learning/study-notes.md` | Learning material on PubSub / message bus concepts |
| Version Control & Validation Guide | `03-version-control-guide.en.md` → `docs/guides/version-control-and-validation-guide.md` | Branch, commit, and test-gate rules |
| Phase 0 Scaffolding Instructions | `04-phase0-scaffolding.en.md` | Step-by-step CLI commands for initial setup |
| CLI Master Instruction & CI/CD | `05-phase0-cli-master-instruction.en.md` | CLI command prompts + Release/dev-artifact workflows |
| README | `06-README.en.md` → `README.md` | Repository entry document |

> All document references in this (English) version point to `*.en.md`.

---

## 1. One-line Definition

A scalable, high-throughput (≥100K msg burst) message bus in which a standalone **Broker**
process mediates communication between **Publisher / Subscriber** processes over mTLS-secured gRPC,
following the publish-subscribe pattern.

Key non-functional requirements:
- Process isolation (broker runs independently, communicates over the network)
- Multi-threaded concurrency, handling 100,000 concurrent inbound messages
- Broker is agnostic to each client's `.proto` (extensibility)
- mTLS certificate-based transport security
- At-least-once delivery + ack/nack
- Unit tests + system tests (performance/stress)
- An incremental development process validated and committed step by step (Section 12)

---

## 2. Reference Models

| Candidate | Decision | Rationale |
|-----------|----------|-----------|
| **Apache Kafka** | **Primary model (confirmed)** | Partitioned **append-only log**, offset-based consumption, consumer groups, replay. This is the core architecture. Scoped to "Kafka-lite" |
| **Google Pub/Sub** | Pattern + delivery-guarantee influence | Same Pub/Sub pattern (topic/subscription, fan-out); at-least-once delivery. In a log model this is realized via **offset commit** (the Kafka form of ack), not push-style ack/nack |

> **Conclusion (model confirmed):** a lightweight self-hosted broker built on the **log model** —
> publish *appends* to a per-partition append-only log; subscribers *pull* by holding their own offset
> (cursor); fan-out is many subscribers reading the same log at their own offsets. This is the Kafka
> architecture, not the push-and-forget delivery of a classic broker.
>
> Both Kafka and Google Pub/Sub implement the **Pub/Sub pattern** (decoupled publishers/subscribers,
> topic-based fan-out); they differ in *how messages are retained and delivered*. The initial idea was to
> blend both delivery models, but they rest on different mechanics (log + pull vs push + buffer) and don't
> cleanly combine in one store. We commit to the **log/pull model** — offsets and partitions then carry
> real meaning (log position, replay), and at-least-once follows naturally from offset commit. The
> push-model concerns (per-subscriber buffers, drop policy, buffer backpressure) fall away.
> No dependency on external Kafka/PubSub — the goal is our own bus. (See 09 DEC-015.)

---

## 3. Core Design Decisions (Confirmed)

### 3.1 Persistence — Staged Approach
The three axes are **independent** and arranged as follows.

| Axis | Phase 1 | Phase 2 |
|------|---------|---------|
| Process isolation (separate broker + gRPC) | ✅ Included | — |
| High performance (100K, multi-threading, bidi streaming) | ✅ Included | Tuning |
| Message storage location | In-Memory **append-only log** (per-partition) | Disk persistence (WAL + segment, replay, restart survival) |

> Phase 1: The broker runs as a separate process; external pub/sub processes connect via mTLS gRPC
> and 100K messages are processed across threads. Messages are **appended to a per-partition in-memory
> log and retained** (Phase 1 keeps them for the process lifetime — no eviction policy yet). Subscribers
> read from the log by offset. On broker shutdown the in-memory log is lost (an intended Phase-1 constraint).
> Phase 2: Append-only disk records (WAL + segments) → restart survival + retention/eviction policy + replay.

### 3.2 Delivery Guarantee
- **At-least-once + ack/nack**
- Messages are tracked as "in-flight" until acked; redelivered on nack or ack timeout.
- Duplicates are possible → consumer-side idempotency is the user's responsibility. Exactly-once is out of scope.
- Repeatedly failing messages are isolated to a **Dead Letter Queue (DLQ)**.

### 3.3 Payload Opacity (Key to Extensibility)
The broker **never deserializes** user message content.
- The broker's gRPC interface is fixed (Publish/Subscribe/Ack, etc.).
- User payloads are wrapped as opaque `bytes` + metadata (headers).
- The broker routes using only the routing key (topic, partition key) and headers.
- Therefore the broker is unaffected by whichever `.proto` a Publisher/Subscriber uses.
- (Concept explained in `02-study-notes.en.md` Section 9. Rationale recorded in ADR 0001.)

### 3.4 Transport Patterns (gRPC)
| Operation | Pattern | Notes |
|-----------|---------|-------|
| Publish | client streaming / bidi | Batched send + flow control; no per-message unary |
| Subscribe | server streaming | Broker streams log records to the consumer from its current offset (cursor); the consumer advances through the partition log |
| Ack/Nack | message within bidi or a separate stream | Keyed by delivery_id |
| Admin (topic creation, etc.) | unary | Low-frequency control |

---

## 4. Included Features

| Feature | Phase 1 | Phase 2 | Notes |
|---------|---------|---------|-------|
| Partitioning | **M2** — topic→N partitions, hash/round-robin routing, **per-partition append-only log + per-partition offset**, subscribers consume by **pulling from their offset (cursor)**, parallel per-partition reads | (count change) | Per-partition ordering; a subscriber reads ALL partitions of a topic; partition count fixed per topic in Phase 1 |
| Consumer group | **M3** — in-group partition assignment + distribution (static) | Rebalancing maturity | Splits a topic across group members; built on top of M2's partitions, NOT part of M2 |
| mTLS certificate security | ✅ Required | Cert rotation / CRL | Mutual authentication |
| Schema Registry | Minimal interface only | Full implementation (validation/compatibility) | Payload stays opaque; schema_id offers optional type safety |
| Observability (Metrics/Tracing/Logging) | Basic metrics + structured logging | Distributed tracing (OpenTelemetry) | Prometheus-compatible metrics recommended. Logging: **Serilog** behind `Microsoft.Extensions.Logging` (`ILogger<T>`), rolling file sink (daily + size cap + retention). Wired at M6; revisit async sink (`Serilog.Sinks.Async`) at M5/M6 for the 100K hot path. Phase 0 only: `.gitignore` `logs/` and `*.log`. |

---

## 5. System Architecture (Phase 1)

```
 ┌─────────────┐         mTLS / gRPC          ┌──────────────────────────────┐
 │ Publisher A │ ───────(bidi stream)───────▶ │           BROKER             │
 │ (proto X)   │                              │  ┌────────────────────────┐  │
 └─────────────┘                              │  │  gRPC Endpoint Layer    │  │
 ┌─────────────┐                              │  │  Auth (mTLS) / AuthZ    │  │
 │ Publisher B │ ───────────────────────────▶ │  │  Router / Partitioner   │  │
 │ (proto Y)   │                              │  │  Topic → Partitions     │  │
 └─────────────┘                              │  │  (append-only logs)     │  │
 ┌─────────────┐                              │  │  Delivery + Ack/Nack    │  │
 │ Subscriber 1│ ◀──(server stream by offset)─ │  │  Metrics / Logging      │  │
 │ (group G1)  │                              │  └────────────────────────┘  │
 └─────────────┘                              └──────────────────────────────┘
```

### Component Responsibilities
- **gRPC Endpoint Layer**: connection acceptance, stream management.
- **Auth**: mTLS client certificate verification, (optional) per-topic authorization.
- **Router / Partitioner**: topic + partition key → target partition (hash-based; round-robin if no key).
- **Topic/Partition Store**: a **per-partition append-only log** (ordered records) + an offset counter. Publish appends; subscribers read by offset.
- **Delivery Manager**: streams log records to each subscriber from its offset (cursor); per-consumer-group partition assignment (M3); in-flight tracking + ack via offset commit / redelivery (M3).
- **Observability**: throughput/latency/log-depth/lag/in-flight metrics, structured logs.

---

## 6. Strategy for 100K Concurrent Inbound

| Technique | Description |
|-----------|-------------|
| Per-partition append-only log | Append is O(1) and lock-light; reads are sequential by offset |
| Backpressure (M5) | Flow control on the *publish* stream (server-streaming send) when a consumer's read lags — not a per-message drop. Real backpressure is M5 |
| Batching | Batch publish/deliver to reduce overhead |
| Partition parallelism | Partition count = unit of parallelism, scales with cores |
| Thread pool tuning | Dedicated read loops + ThreadPool, avoiding excessive async context switching |
| Object pooling / `ArrayPool` | Buffer reuse to reduce GC pressure |
| Zero/low-copy | Opaque payloads → minimal deserialization/copying |

Target: single broker, 8 cores, 100K msg burst. p99 latency to be **established as a baseline after measurement** (see Section 15).

---

## 7. Security (mTLS)

- Issue broker/Publisher/Subscriber certificates from a self-managed CA (for development); leave room for an external CA in production.
- Both broker and clients verify each other's certificates (mTLS).
- (Optional) Use the certificate's SAN/CN as a client identity → basis for per-topic ACLs.
- **Certificates/keys are never committed to the repository** (blocked via .gitignore, generated locally via certgen).
- Phase 2: certificate rotation, expiry handling, revocation (CRL/OCSP) consideration.

### 7.1 certgen — Mandatory Checklist for M4
> The certgen tool is **built and run at M4**, not in Phase 0. The only Phase 0 task is making
> `.gitignore` block certs/keys (see below). Tool form (.NET console tool vs. openssl script) is
> decided at M4; current leaning is a **.NET console tool** (zero extra dependency, cross-platform,
> reusable from integration-test fixtures). LLM-generated cert code frequently omits the items below,
> so each MUST be verified:
>
> 1. **CA (root):** `BasicConstraints(certificateAuthority: true)`. This is the dev self-managed CA that signs everything below.
> 2. **Broker server cert:** SAN **must** include `localhost` (plus any real hostname the broker listens on). Clients verify the server against the SAN.
> 3. **Publisher / Subscriber client certs:** ExtendedKeyUsage **must** include `clientAuth` (OID 1.3.6.1.5.5.7.3.1 is serverAuth; clientAuth is 1.3.6.1.5.5.7.3.2). The broker authenticates clients via this.
> 4. **Single chain of trust:** broker, publisher, and subscriber certs are **all signed by the same CA**, and that CA must be registered in the trust store of both the broker and the clients (mTLS is mutual).
>
> Output goes to a `.gitignore`d folder (e.g. `certs/`). **Phase 0 must add `certs/`, `*.pfx`, `*.pem`, `*.key`, `*.crt` to `.gitignore`** so keys can never be committed by accident.

---

## 8. Protocol Draft (Broker's Fixed Interface)

```protobuf
syntax = "proto3";
package fw.v1;

service MessageBus {
  rpc Publish(stream PublishEnvelope) returns (stream PublishAck);
  rpc Subscribe(SubscribeRequest) returns (stream DeliverEnvelope);
  rpc Acknowledge(stream AckRequest) returns (stream AckSummary);
  rpc Admin(AdminRequest) returns (AdminResponse);
}

message PublishEnvelope {
  string topic = 1;
  bytes  partition_key = 2;          // round-robin if absent
  map<string, string> headers = 3;   // content-type, schema_id, etc.
  bytes  payload = 4;                 // opaque — broker does not interpret
  string client_msg_id = 5;
}

message PublishAck {
  string client_msg_id = 1;
  string topic = 2;
  int32  partition = 3;
  int64  offset = 4;
  bool   accepted = 5;
  string error = 6;
}

message SubscribeRequest {
  string topic = 1;
  string group_id = 2;
  int32  max_in_flight = 3;
  StartPosition start = 4;           // LATEST / EARLIEST / OFFSET (Phase 2 replay)
}

message DeliverEnvelope {
  string topic = 1;
  int32  partition = 2;
  int64  offset = 3;
  map<string, string> headers = 4;
  bytes  payload = 5;
  string delivery_id = 6;
}

message AckRequest { string delivery_id = 1; bool nack = 2; }
```

> Clients serialize their own message `.proto` into `payload` and identify the type via `headers["schema_id"]`.
> The broker routes without ever inspecting the content.

---

## 9. Solution Structure

```
Flumewright.sln
├── src/
│   ├── Flumewright.Protocol/        # Fixed .proto + generated code
│   ├── Flumewright.Broker.Core/     # Routing/partitioning/delivery (pure logic, easy to test)
│   ├── Flumewright.Broker/          # Broker host (executable process)
│   ├── Flumewright.Client/          # Publisher/Subscriber SDK
│   ├── Flumewright.Security/        # mTLS / certificate utilities
│   └── Flumewright.Observability/   # Metrics/logging/tracing
├── samples/
│   ├── SamplePublisher/        # Example publisher with its own .proto
│   └── SampleSubscriber/       # Example subscriber with its own .proto
├── tests/
│   ├── Flumewright.UnitTests/        # [Category=Unit]
│   ├── Flumewright.IntegrationTests/ # [Category=Integration] cross-process e2e
│   └── Flumewright.LoadTests/        # [Category=Load] performance/stress
├── tools/
│   └── certgen/                # Dev CA/certificate generation scripts
├── docs/                       # Design, learning, guides, ADRs
└── .github/workflows/          # ci.yml / load.yml / release.yml
```

---

## 10. Testing & Validation Strategy

Tests are classified via `[Trait("Category", ...)]` into Unit / Integration / Load,
running different sets at each gate (Section 12.3). For per-gate rules, see `03-version-control-guide.en.md` Sections 5–6.

### 10.1 Unit
- Router/Partitioner: same key → same partition, distribution uniformity.
- Log/offset: append assigns increasing per-partition offsets; subscriber reads records in offset order from its cursor; per-partition offsets are independent.
- Delivery: in-flight removal on ack (offset commit), redelivery on nack/timeout, in-group distribution (M3).
- Tools: xUnit + FluentAssertions; prefer deterministic simulation for concurrency.

### 10.2 Integration / System
- Start broker + N publishers + M subscribers as separate processes, end-to-end including mTLS handshake.
- At-least-once verification: confirm all messages are eventually received after forced nack/disconnect.

> **Staging note (DEC-007):** the above is the *final-form* system test with mTLS and multiple clients.
> **M1's e2e integration test starts as a single message, in-process (real-port Kestrel)** — M1's bar is
> "one message through", not "prove physical process separation", and the separation requirement is already
> met by the architecture (standalone broker host + gRPC). True separate-process e2e arrives naturally at
> M4 (mTLS) / M5 (load); manual smoke is always available via the sample consoles.
> **Implementation note (M1 Step 5 done):** the in-process host is built directly via `WebApplication` in an
> `IAsyncLifetime` fixture (NOT `WebApplicationFactory`, which conflicts with the real-Kestrel `Program.cs`);
> bind `IPAddress.Loopback:0` for a dynamic h2c port. The `Http2UnencryptedSupport` switch is unnecessary on
> .NET 8. See decision-and-fix-log FIX-005 / DEC-008.

### 10.3 Performance / Stress
| Item | Measured |
|------|----------|
| Throughput | Messages per second (by payload size) |
| Latency | p50/p95/p99 end-to-end |
| Burst 100K | Loss/latency/queue depth under 100K concurrent inbound |
| Soak | Long-run leak/memory stability |
| Backpressure | Broker stability when publishers overrun |
| Fan-out | Scalability of concurrent push to many subscribers |
- Tools: BenchmarkDotNet (micro) + custom load generator (macro). Results recorded for regression tracking.

---

## 11. Documentation Policy

- Use a **repository `docs/` folder instead of a Wiki**. Versioned in the same commits/PRs as code,
  preventing code-doc drift and fitting the CLI workflow.
- Record milestone design decisions in `docs/design/mN-*.md`; capture significant decisions as **ADRs**
  (`docs/decisions/NNNN-*.md`). For the ADR template, see `05-phase0-cli-master-instruction.en.md` Section A.
- Bundle code and docs in the **same commit/PR**.
- Update the README "Quick Start" whenever a feature becomes functional.
- **Language policy:** all repository text is in **English** — README, docs, ADRs, commit messages,
  and code comments. Korean source drafts are kept privately, outside the repository, for personal reference only.

### docs/ Structure
```
docs/
├── README.md          # index
├── design/            # plan.md, mN-*.md
├── learning/          # study-notes.md
├── guides/            # version-control-and-validation-guide.md
└── decisions/         # ADRs (0001-opaque-payload.md, 0002-at-least-once.md ...)
```

---

## 12. Development Process (Version Control & Validation)

> Core principle: **one commit = one validated small change.** Do not build everything at once.
> See `03-version-control-guide.en.md` for full rules.

### 12.1 Branch Strategy
- `main` is always green. Work on `feat/mN-...` branches per milestone, then merge.
- Tag `vX.Y.Z` when a Phase completes.

### 12.2 Commit Rules
- Conventional Commits (`feat`/`fix`/`test`/`refactor`/`perf`/`docs`/`chore`).
- Feature → test → commit loop. Each commit builds and passes tests on its own.

### 12.3 Validation Gates (Two Layers)
| Gate | When | Runs |
|------|------|------|
| pre-commit hook (local) | on commit | build + Unit |
| GitHub Actions CI | push/PR → main | build + Unit + Integration |
| Load workflow | manual / nightly | Load |

### 12.4 GitHub Control Boundary ⭐
- **Only local Git operations are allowed for the CLI**: init/add/commit/branch/checkout/merge/tag/status/log/diff/config.
- **Remote operations are performed by the user**: push/pull/fetch/remote/gh, repo creation, branch protection, tag push.
- After local commits, the CLI reports "✅ Ready to push" and stops.
- Rationale: the user holds auth credentials and reviews commits/sensitive files before pushing — a final safety net.

---

## 13. CI/CD Workflows

| Workflow | Trigger | Role |
|----------|---------|------|
| `ci.yml` | push/PR → main | Build + Unit/Integration tests + **dev artifact upload** |
| `load.yml` | manual / nightly cron | Performance & stress tests |
| `release.yml` | **push `v*.*.*` tag** | Release build + packaging + publish to Releases tab |

- Full workflow YAML and the dev-artifact/Release supplements: see `05-phase0-cli-master-instruction.en.md` Section B and `04-phase0-scaffolding.en.md` Section 4.
- Action versions: `actions/checkout@v6`, `actions/setup-dotnet@v5`, `actions/cache@v4`,
  `actions/upload-artifact@v4`, `softprops/action-gh-release@v3`.
- **Dev build**: each main CI uploads the broker artifact to Actions Artifacts (download the latest dev build).
- **Release**: a tag push is the release signal. Artifacts published to the Releases tab. Actual server
  deployment (CD) is deferred until a deployment target is chosen (currently up to "publish to Releases").

---

## 14. Roadmap

### Phase 0 — Scaffolding (CLI-executed, stop before push)
> Step-by-step commands: `04-phase0-scaffolding.en.md`. The CLI command prompt: `05-phase0-cli-master-instruction.en.md` Section D.
- Solution/project structure, .gitignore/editorconfig/global.json/Directory.Build.props.
  - `.gitignore` blocks .NET build output **plus** certs/keys (`certs/`, `*.pfx`, `*.pem`, `*.key`, `*.crt`) and logs (`logs/`, `*.log`).
  - `global.json` pins .NET 8 SDK (`rollForward: latestFeature`); `Directory.Build.props` sets net8.0 / Nullable / `TreatWarningsAsErrors=true` / analyzers for all projects.
- README, docs/ structure + ADRs, ci.yml/load.yml/release.yml, pre-commit hook.
- `git init` + logically split commits → report "Ready to push" and stop.
- User: create repo → remote → push → branch protection → verify CI.

### Phase 1 — Core (In-Memory, High Performance, Security)
- M1: Fixed gRPC contract + broker host startup (plaintext) + simple Pub→Sub passthrough (initial slice; the store evolves to the log model in M2).
- M2: Topic/partition + **per-partition append-only log + offset-based consumption (pull by cursor)** + routing, parallel per-partition reads.
- M3: Consumer group distribution + at-least-once (**ack via offset commit**) + in-flight + redelivery + DLQ skeleton.
- M4: mTLS (mutual certificates), certgen tool. **Follow the Section 7.1 certgen checklist (CA BasicConstraints, server SAN, client EKU, single CA chain).**
- M5: Bidi/server streaming + batching + backpressure for 100K throughput.
- M6: Basic metrics/logging + first pass of unit/integration/load tests.
- Phase 1 complete → tag `v0.1.0`.

### Phase 2 — Persistence & Maturity
- Disk persistence (WAL + segment), replay (offset-based re-subscription).
- Full Schema Registry (registration/validation/compatibility).
- OpenTelemetry distributed tracing, certificate rotation/CRL.
- Consumer group rebalancing maturity, (consider) multi-instance/replication.

---

## 15. Open Questions

1. Start with a single broker instance vs. design for horizontal scaling/replication from the start (Phase 1 recommends single).
2. Partition count: fixed at topic creation vs. dynamically changeable.
3. Default ack timeout and max redelivery count (+ DLQ transition criteria).
4. Schema Registry: separate service vs. broker-embedded module.
5. Target performance: fixed numbers vs. post-measurement baseline (current plan uses baseline).
6. Dev/CI environment (OS, core count) and reference machine specs for load testing.
7. License choice (README currently TBD).
8. Actual deployment (CD) target — Docker image / cloud, etc. (currently up to publishing Releases).

---

## 16. Change Log

| Version | Changes |
|---------|---------|
| v0.1 | Initial draft — scope/architecture/technical design/roadmap |
| v0.2 | Added development process (version control, validation, GitHub boundary), documentation policy (docs/ + ADR, English-only repo text), CI/CD (CI/Load/Release + dev artifact), document map, and open questions (DLQ, license, CD target) |
| v0.3 | Added Section 7.1 — mandatory certgen checklist for M4 (CA BasicConstraints, broker SAN incl. localhost, client EKU clientAuth, single CA chain); leaning toward a .NET console tool; Phase 0 must .gitignore certs/keys. Cross-linked from the M4 roadmap item. Logging decision: Serilog behind MEL with rolling file sink (wired at M6, async sink revisited at M5/M6); Phase 0 .gitignores logs/ and *.log. Detailed Phase 0 Ch.3 root-config decisions (gitignore certs+logs, global.json SDK pin, Directory.Build.props warnings-as-errors). |
| v0.4 | Added staging note to §10.2 (DEC-007) — M1's integration test starts in-process (real-port Kestrel) with a single message; separate-process/mTLS e2e is the final form at M4/M5. M1's bar is "one message through". |
| v0.5 | M1 Step 5 done (e2e integration test passes → M1 functional phase complete). Refined §10.2 with the implementation note: the in-process host is built directly via `WebApplication` in an `IAsyncLifetime` fixture, not `WebApplicationFactory` (which conflicts with the real-Kestrel `Program.cs`); bind `IPAddress.Loopback:0` for a dynamic h2c port; the `Http2UnencryptedSupport` switch is unnecessary on .NET 8 (decision-and-fix-log FIX-005 / DEC-008). |
| v0.6 | M1 milestone closed (merged to main via merge commit; no tag — that is the Phase 1/M6 marker). M1 wrap: dev container adoption (DEC-009/010), pre-commit exec-bit fix (FIX-007), zoom-out review (DEC-011), main branch-protection ruleset (DEC-012). **Clarified the M2/M3 split in §4:** partitioning is M2 (topic→N partitions, routing, per-partition offset + bounded channel, parallel consume loops; a subscriber gets all partitions; count fixed per topic), consumer-group distribution is M3 (built on top of M2) — previously bundled as one "skeleton" row. Bounded-channel backpressure signaling stays in M5 (FIX-002). |
| v0.7 | **Delivery model confirmed: log/pull (Kafka-style), not push (DEC-015).** Both Kafka and Google Pub/Sub implement the Pub/Sub pattern; they differ in retention/delivery. The early idea of blending both delivery models proved incoherent in one store (log+pull vs push+buffer), so the broker is built on the **log model**: publish appends to a per-partition append-only log; subscribers pull by holding their own offset (cursor); fan-out is many subscribers reading the same log. Offsets/partitions now carry real meaning (log position, replay); at-least-once follows from offset commit (M3). The push-model artifacts (per-subscriber bounded channel, drop policy, buffer backpressure) are removed. **§3/§4/§5/§6/§10/§14 updated accordingly; M2 redefined** (append-only log + offset cursor pull, was bounded channel + consume loops). In-memory log is retained for the process lifetime in Phase 1; retention/eviction + disk persistence are Phase 2. |
