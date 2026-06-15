# Message Bus & PubSub Study Notes

> Concept study material compiled while building the Flumewright project.
> A collection of the core concepts needed to build a distributed message bus from scratch.
> Target stack: C# / .NET 8+ / gRPC / Protobuf

---

## Table of Contents

1. What is the PubSub pattern
2. What is a message bus
3. Reference models: Kafka vs Google Pub/Sub
4. Delivery guarantee: At-least-once + ack/nack
5. Schema Registry (type safety)
6. mTLS certificate-based security
7. Observability: Metrics / Tracing / Logging
8. Consumer Group & Partitioning
9. Payload opacity (key to extensibility)
10. gRPC transport patterns
11. High-throughput (100K+) techniques
12. Glossary
13. How the concepts fit together

---

## 1. What is the PubSub Pattern

**Publish-Subscribe** is a messaging pattern where the sender (Publisher) and receiver
(Subscriber) communicate **without knowing each other directly**.

- A Publisher merely publishes a message to a specific **topic**; it does not know who receives it.
- A Subscriber merely subscribes to topics of interest; it does not know who sent the message.
- A **broker** in the middle connects the two.

**Core value = decoupling.** Because publishers and subscribers are independent,
changing or scaling one side does not affect the other.

```
Publisher ──▶ [ Topic ] ──▶ Subscriber A
                   │
                   └───────▶ Subscriber B
```

Difference from direct calls (RPC):
- Direct call: A calls B "now" and waits for a response (tightly coupled, synchronous).
- PubSub: A fires and forgets. B processes at its own pace (loosely coupled, asynchronous).

---

## 2. What is a Message Bus

A **message bus** is a representative infrastructure implementing the PubSub pattern.
Multiple systems connect to a shared "bus" to exchange messages.
(Named by analogy with the hardware bus — many components communicate over a shared channel.)

What a message bus provides:
- Topic-based routing
- Many publishers / subscribers connected simultaneously
- Asynchronous delivery, buffering (temporarily holding messages)
- (Depending on implementation) persistence, ordering, redelivery, load balancing

**In-Memory vs distributed (process-isolated):**
- In-Memory bus: works only within the same process (e.g., MediatR). Simple but confined to one process.
- Distributed bus: the broker runs as a separate process/server, and clients in other processes
  connect over the network. → the form Flumewright targets.

---

## 3. Reference Models: Kafka vs Google Pub/Sub

| Aspect | Apache Kafka | Google Pub/Sub |
|--------|--------------|----------------|
| Form | Self-hostable open source | Fully managed cloud service |
| Storage model | Append-only **log** + offset | Queue-based, deleted after ack |
| Ordering | Guaranteed within a partition | When using an ordering key |
| Replay | Possible by rewinding offset | Limited |
| Load balancing | Consumer group | Subscription |
| Best learning point | log/partition/group/replay architecture | topic/subscription split, ack/nack semantics |

**Flumewright's choice:**
Build a lightweight self-hosted broker ("Kafka-lite") that takes Kafka's **architectural concepts**
(partitioned log, offset, consumer group) as the primary reference, while borrowing Pub/Sub's
**delivery semantics** (ack/nack, at-least-once) for the interface. No dependency on external Kafka/PubSub.

---

## 4. Delivery Guarantee: At-least-once + ack/nack

### The Problem
Just because the broker sent a message does not mean it was processed.
The subscriber could die while receiving, fail during processing, or the network could drop.
If the broker is unaware, messages silently vanish.

### Three Levels of Delivery Guarantee

| Level | Meaning | Loss | Duplicate | Cost |
|-------|---------|------|-----------|------|
| At-most-once | Fire and forget | Possible | None | Lowest |
| **At-least-once** | Redeliver until processing confirmed | None | **Possible** | Medium |
| Exactly-once | No loss, no duplicate | None | None | Highest (very hard) |

> Flumewright adopts **At-least-once**.

### ack / nack
- **ack** (acknowledge): "Processed. You may delete it."
- **nack** (negative acknowledge): "Failed to process. Please resend."

### Why Duplicates Happen
```
broker sends → subscriber finishes processing → network drops while sending ack
            → broker never receives ack → redelivers → subscriber processes the same message twice
```
Even if processing succeeded, a lost ack triggers redelivery.
So at-least-once inherently accepts duplicates.

### Idempotency — duplicate handling is the subscriber's responsibility
Make processing the same message twice yield the same result.
- Bad: "balance += 100" (twice → +200)
- Good: "if transaction ID X, set balance to 500" / record processed message IDs and skip duplicates

### Flumewright Behavior
```
broker delivers (assigns delivery_id) → subscriber processes → ack/nack
   ↑                                                              │
   └──── if no ack, redeliver after timeout ◀─────────────────────┘
```
- The broker tracks sent messages as **in-flight**.
- ack → remove from in-flight.
- nack → add to redelivery set immediately.
- ack timeout → assume dead, redeliver.
- **Dead Letter Queue (DLQ)**: messages exceeding max redelivery count are isolated to prevent
  infinite retries.

**Parameters to set:** ack timeout (wait duration), max redelivery count.

---

## 5. Schema Registry (Type Safety)

### The Tension
If the broker handles payloads only as opaque bytes (see Section 9), you gain extensibility but lose
the guarantee that the subscriber can correctly interpret what the publisher sent.

### The Problem
```
Publisher A:  OrderV1 { int id; string item }
Subscriber B: tries to read as OrderV2 { int id; string item; decimal price }
broker: just relays bytes — unaware of the mismatch
→ deserialization failure, or worse, "silent misinterpretation"
```
Protobuf tolerates some changes (adding fields), but type/semantic changes break subscribers in a chain.

### What a Schema Registry Does
A **central ledger that registers and version-manages message schemas (.proto definitions)**.
1. The publisher registers its schema and receives a **schema_id**.
2. It carries the schema_id in the message header (`headers["schema_id"] = "order-v3"`).
3. The subscriber learns the correct structure from schema_id and interprets accordingly.
4. **Enforces compatibility policy**: only allow new schema registrations that don't break existing
   subscribers (add field OK / remove field or change type rejected, etc.).

### The Key Point
The broker still does not inspect content.
Type safety is the responsibility of the **registry + both clients**, not the broker.
→ you keep the broker's simplicity (extensibility) and gain type safety.

### Flumewright Stages
- Phase 1: minimal — a convention of carrying schema_id in headers. No enforcement.
- Phase 2: full — registration/lookup API, compatibility validation.
  (Reference: Confluent Schema Registry in the Kafka ecosystem runs as a separate service.)

---

## 6. mTLS Certificate-based Security

### TLS vs mTLS
- **TLS** (as used in https): authenticates **one side**. The client verifies the server.
  The server doesn't care who the client is.
- **mTLS** (mutual TLS): **both sides** verify each other with certificates.

### Why a Message Bus Needs It
The broker is a central hub that many processes connect to.
You can't let any process connect to spray messages or eavesdrop.
mTLS provides two things at once:
1. **Encryption** — prevents eavesdropping in transit (TLS basics).
2. **Mutual authentication** — only legitimate clients/brokers holding certificates may connect.

### Certificate Structure
```
        CA (Certificate Authority)
        "Certificates I signed can be trusted"
           │ sign          │ sign          │ sign
           ▼               ▼               ▼
     Broker cert     Publisher cert   Subscriber cert
```
The CA is the root of trust. The broker and all clients hold certificates signed by the same CA
(or trust chain). On connection, each verifies "is your cert signed by a CA we trust?"
In development, we create our **own CA** to issue certificates (certgen tool).

### Bonus: certificate as identity
Use the cert's CN/SAN (e.g., "publisher-payments") as the client identity, forming the basis for
**access control (ACL)** such as "this client may only publish to the payments topic."

### Flumewright Stages
- Phase 1: self-managed CA + mutual cert verification. Dev certs via certgen.
- Phase 2: cert rotation (replace before expiry), revocation list (CRL/OCSP — invalidate stolen certs).

---

## 7. Observability: Metrics / Tracing / Logging

**Observability** = the ability to see what's happening inside the system from the outside.
Without it, a multi-threaded distributed system processing 100K is operating blind.
The "three pillars" each answer a different question.

### (1) Metrics — "how much? how many? how fast?"
Numeric time-series data aggregated. A health overview at a glance.
- e.g., messages/sec, queue depth, in-flight count, ack latency p99, drop count, connected clients.
- Use: dashboards, alarms. Standard: Prometheus.
- Analogy: a car dashboard.

### (2) Tracing — "where did this one message travel, and how?"
Follows the entire journey of a single request.
- e.g., publish(2ms) → queue wait(15ms) → deliver(1ms) → processing(40ms) → ack.
- A trace ID attached to each message follows it across process boundaries. Standard: OpenTelemetry.
- Analogy: parcel tracking.

### (3) Logging — "what exactly happened, and when?"
Records of individual events. Basis for post-hoc debugging.
- **Structured logging** matters: log as key-value (JSON), not prose, so it's searchable/filterable.
- e.g., `{level:WARN, partition:3, client:pub-A, event:nack_received, delivery_id:abc}`
- Analogy: a black box / ship's log.

### Using All Three Together
```
Metrics reveals "p99 latency spiked"
  → Tracing localizes "stuck at partition 3"
    → Logging confirms root cause "partition 3 subscriber keeps nacking"
```

### Flumewright Stages
- Phase 1: basic metrics (throughput/latency/queue-depth/in-flight) + structured logging.
  (.NET built-ins: `ILogger`, `System.Diagnostics.Metrics`)
- Phase 2: OpenTelemetry distributed tracing, Prometheus/dashboard integration.

---

## 8. Consumer Group & Partitioning

A mechanism solving scalability (load balancing) and ordering at once. Kafka's core idea.

### Partitioning
Split one topic into several **partitions**.
A message goes to a specific partition by the hash of its partition key.
```
Topic "orders" (3 partitions)
 ├─ Partition 0 : [m1] [m4] [m7] ...
 ├─ Partition 1 : [m2] [m5] [m8] ...
 └─ Partition 2 : [m3] [m6] [m9] ...
```
Two benefits:
1. **Parallelism/scalability** — one partition is consumed by one consumer at a time. With 3
   partitions, 3 consumers process concurrently. Partition count = unit of parallelism = the key
   means of handling a 100K burst.
2. **Per-partition ordering** — within a partition, messages are delivered in arrival order.
   Give order-sensitive messages the same partition key
   (e.g., events for the same order ID → same key → same partition → preserved order).
   Note: global order *across* partitions is not guaranteed (a trade-off).

### Consumer Group
Group several subscribers consuming the same topic.
The broker assigns partitions across the group without overlap.
```
Topic "orders" (3 partitions)     Consumer Group "billing"
 ├─ Partition 0 ──────────────▶ Subscriber A
 ├─ Partition 1 ──────────────▶ Subscriber B
 └─ Partition 2 ──────────────▶ Subscriber C
```
Rules:
- Within a group, a partition goes to only one member → members split the work (load balancing).
- If a member dies, another takes over its partition → **rebalancing** (automatic failover).
- If there are more members than partitions, the extras idle (partition count = parallelism ceiling).

### The Point That Separates PubSub's Two Patterns
```
                  ┌── Group "billing"   : members split the messages (work queue)
Topic "orders" ──┤
                  └── Group "analytics" : receives the same messages in full, separately (broadcast)
```
- **Different groups** each receive the same messages (fan-out / broadcast).
- **Within a group** they split them (work queue / load balancing).
→ "billing processes with load balancing" + "analytics receives the same data in full to aggregate"
  coexist on one topic.

### Flumewright Stages
- Phase 1: skeleton — per-partition queues, key→partition routing, in-group assignment+distribution
  (starting static).
- Phase 2: rebalancing maturity — dynamic reassignment on member join/leave, safe handover.

---

## 9. Payload Opacity (Key to Extensibility)

The core design decision that unlocks Flumewright's extensibility.

### The Problem
If publishers/subscribers each use a different .proto and the broker had to know them all,
the broker would need modification for every new message type → not extensible.

### The Solution: the broker doesn't know the content
The broker **never deserializes** user messages.
- The broker's gRPC interface is **fixed** (Publish/Subscribe/Ack, etc.).
- User payloads are wrapped as **opaque bytes** + metadata (headers).
- The broker routes using only routing keys (topic, partition key) and headers.

```
Publisher (proto X) ─┐
Publisher (proto Y) ─┼─▶ broker: { topic, key, headers, payload=<bytes> } ─▶ Subscriber
Publisher (proto Z) ─┘        (never looks at payload content)
```

→ The broker is unaffected by whichever .proto a publisher/subscriber uses.
The responsibility for type interpretation lies with both clients (+ Schema Registry).
**Low coupling = high extensibility.**

---

## 10. gRPC Transport Patterns

gRPC is HTTP/2-based and supports four communication patterns.
Pattern choice matters for high throughput.

| Pattern | Form | Use in the message bus |
|---------|------|------------------------|
| Unary | 1 request → 1 response | Low-frequency control (topic creation, etc., admin) |
| Server streaming | 1 request → N responses | **Subscribe** (broker pushes continuously to consumer) |
| Client streaming | N requests → 1 response | Batch upload |
| Bidi streaming | N requests ↔ N responses | **Publish** (batch + flow control), concurrent ack handling |

Core principle: **no per-message unary calls.**
Calling unary once per message for 100K collapses under connection/handshake overhead.
→ keep the connection open via streaming for bulk transfer + flow control.

---

## 11. High-throughput (100K+) Techniques

| Technique | Description |
|-----------|-------------|
| **Bounded Channels** (`System.Threading.Channels`) | Per-partition producer/consumer split queue, minimal lock contention |
| **Backpressure** | On queue limit, signal flow control back to the publish side → prevents unbounded buildup |
| **Batching** | Batch publish/deliver → reduces syscall/allocation overhead |
| **Partition parallelism** | Partition count = unit of parallelism, scales with cores |
| **Thread pool tuning** | Dedicated long-running consume loops + ThreadPool. Avoid excessive async context switching |
| **Object pooling / `ArrayPool`** | Buffer reuse to reduce GC pressure |
| **Zero/low-copy** | Opaque payload → no deserialization, minimal copying |

> **Backpressure** note: if production outpaces consumption, the queue grows unbounded → memory blowup
> → OOM. Backpressure flows a "slow down!" signal back to the producer to keep the system stable
> (like tightening a valve when a pipe over-fills).

---

## 11.5 .NET Implementation Concepts: DI & Resource Management (Dispose)

> Not message-bus "concepts", but the two key .NET tools used to **build** one — with how Flumewright
> actually uses them.

### Dependency Injection (DI)
**What:** a class receives its dependencies **injected** from outside (a container) rather than
creating them with `new`. .NET Core/8 ships a built-in `Microsoft.Extensions.DependencyInjection`
container — no extra library needed.

**The point is "depend on interfaces":** the consumer takes an interface; the concrete type is chosen
at registration.
```csharp
builder.Services.AddSingleton<ITopicStore, InMemoryTopicStore>();   // implementation chosen here
public MessageBusService(ITopicStore store) { ... }                 // consumer knows only the interface
```

**Three lifetimes:**
| Lifetime | Meaning | Example |
|----------|---------|---------|
| Singleton | one per process | `ITopicStore` (shared in-memory state) |
| Scoped | one per request/scope | per-request context |
| Transient | new every time | lightweight stateless helpers |

**Pros:** loose coupling (swap an implementation in one line) · testability (inject fakes) · automatic
lifetime management.
**Watch-outs:** a missing registration fails at **runtime** (compiler won't catch it). **Captive
dependency** — a Singleton must not capture a Scoped service (lifetime-mismatch bug).

**Flumewright usage:** depending on `ITopicStore` lets Phase 2 swap `InMemoryTopicStore` →
`DiskTopicStore` (WAL) **without touching consumers** — the mechanism behind "three independent axes"
(swap persistence alone). (Decision: 09 DEC-005)

### Resource management: GC and Dispose
**What GC does:** reclaims managed memory automatically.
**What GC doesn't:** unmanaged resources (file handles, sockets, certificates) aren't promptly cleaned
by GC → they need **explicit release**.

**The Dispose pattern:** a class holding such resources implements `IDisposable.Dispose()` (or async
`IAsyncDisposable.DisposeAsync()`), and the user calls it at the right time. Two mechanisms automate
*when*:
- **`using`** — `using var s = new DiskTopicStore();` → `Dispose()` at end of scope.
- **DI container auto-dispose** — if a registered implementation is `IDisposable`, the container calls
  `Dispose()` automatically at app shutdown (Singleton) / request end (Scoped). You don't call it yourself.

**When it's needed (signals):** ① the class holds an `IDisposable` member (`FileStream`,
`X509Certificate2`, …), or ② it grabs a native handle directly. Neither → no Dispose needed.

**Full pattern vs simplified:** the *full* `Dispose(bool) + ~Finalizer + GC.SuppressFinalize` pattern
is only needed when **holding a native handle directly**. If you merely hold .NET types that are
already `IDisposable`, a **simplified** form (no finalizer; `Dispose()` releases members; `sealed`
class) is enough. Over-using the full pattern only adds complexity.

**Flumewright usage (by milestone):**
- **M1 (now):** `InMemoryTopicStore` holds only managed channels → no `IDisposable`. Just complete the
  channel `Writer.Complete()` on unsubscribe for clean teardown (FIX-003).
- **M3:** redelivery timers / `CancellationTokenSource` → release via `IDisposable`.
- **M4:** `X509Certificate2` is `IDisposable` → must dispose.
- **Phase 2:** disk WAL `FileStream`/segment handles → disposal mandatory. `DiskTopicStore` implements
  `IDisposable`/`IAsyncDisposable` → **the DI container calls it at shutdown** (you build the Dispose;
  the container calls it).
(Decision: 09 DEC-006)

---

## 11.6 .NET implementation concepts: integration testing & gRPC networking

> Concepts learned by actually hitting them in M1 Step 5 (the integration test). Not message-bus "concepts"
> per se, but essential tools for **testing and network-connecting** a gRPC service. The process went
> through a 5-error cascade (09 FIX-005); each error was, in hindsight, a signal that one concept below was
> missing.

### ① Why gRPC requires HTTP/2 + h2c (plaintext HTTP/2)
**Concept:** gRPC uses **HTTP/2** as its transport (multiplexed streams, header compression, and
bidirectional streaming underpin gRPC's streaming RPCs). gRPC does not work over HTTP/1.1.

**What h2c is:** "HTTP/2 cleartext" — HTTP/2 **without TLS**. HTTP/2 is usually paired with TLS (browsers
only allow h2 over TLS), but plaintext h2c is possible for internal server-to-server traffic. Since there's
no TLS negotiation (ALPN) to agree on "HTTP/2 from here", **both server and client must declare "this is
h2c" explicitly**.
```csharp
// Server (Kestrel): this port speaks plaintext HTTP/2
options.Listen(IPAddress.Loopback, 0, l => l.Protocols = HttpProtocols.Http2);
```
**Conclusion on .NET 8:** the client (`GrpcChannel.ForAddress("http://...")`) recognizes h2c from the
`http://` scheme alone. Older runtimes needed the `AppContext.SetSwitch("...Http2UnencryptedSupport", true)`
switch, but on **.NET 8 it is unnecessary** (confirmed as 09 DEC-008). M1 is plaintext; once M4 moves to
mTLS, h2c disappears.

### ② Integration testing: in-memory TestServer vs real-port Kestrel
The standard tool for ASP.NET Core integration tests is `WebApplicationFactory<T>`, which by default spins
up an **in-memory `TestServer`** — it opens no real socket and handles requests in memory via an
`HttpMessageHandler` (fast, no port contention).

**The problem:** our SDK (`FlumewrightPublisher/Subscriber`) opens a socket on a **real port** via
`GrpcChannel.ForAddress("http://host:port")`. That doesn't match the in-memory handler. So the integration
test must run a **real-port Kestrel** for the SDK to connect as-is.

**The trap learned (09 FIX-005):** `WebApplicationFactory<Program>` runs our `Program.cs`, intercepts the
host builder, then tries to **overlay its in-memory TestServer**. But our `Program.cs` is already a
**complete real-Kestrel host**, so the two conflict and throw `InvalidCastException`
(KestrelServerImpl→TestServer). Waking it via `.Server` or `.Services` fails at the same place
(`EnsureServer()`).
→ **Resolution:** drop `WebApplicationFactory` and have the test fixture **build/start the host directly**
with `WebApplication.CreateBuilder()`. That avoids the in-memory path entirely.

**Why in-process still counts:** even without spawning a separate OS process, SDK→Kestrel traverses a real
TCP socket and the HTTP/2 stack. That's sufficient to prove "one message crosses the network" (09 DEC-007).
True process separation is handled by the M5 load tests.

### ③ Binding address vs connect-target address (0.0.0.0 ≠ 127.0.0.1)
**Concept:** what looks like the same "IP address" plays different roles.
- **`0.0.0.0` (IPv4) / `[::]` (IPv6) = "unspecified address":** when a server **listens**, this means
  "accept on all network interfaces of this machine". `ListenAnyIP` uses it. It is a **listen-only wildcard**.
- **`127.0.0.1` = loopback:** a **concrete connect-target** address meaning "this machine itself".

**The trap:** starting the server with `ListenAnyIP(0)` makes `IServerAddressesFeature` return
`http://0.0.0.0:{port}`. Using that as the gRPC client's target throws `0.0.0.0 cannot be used as a target
address` — because **"everywhere" cannot be a connection destination**. A client must aim at a specific spot.
→ **Resolution:** bind the server to `IPAddress.Loopback` (127.0.0.1) so the address comes back as
`http://127.0.0.1:{port}` the client connects to directly. (A defensive `0.0.0.0`→`127.0.0.1` replacement
can also be kept.)

### ④ Dynamic port (port 0)
**Concept:** binding with port **0** tells the OS to **auto-assign a free port** (an ephemeral port).
Especially useful in tests — a fixed port (5050) collides with parallel tests or an already-running broker,
whereas port 0 gets a fresh free port every time, eliminating contention. Read the actual assigned number
after startup via `IServerAddressesFeature`.

**The trap:** Kestrel **forbids** `ListenLocalhost(0)` (dynamic port + localhost). `localhost` resolves to
both IPv4 (`127.0.0.1`) and IPv6 (`::1`); with port 0, the two stacks would get **different ports**, making
"which port?" ambiguous. The error itself states the fix: bind `127.0.0.1:0` or `[::1]:0`.
→ **Resolution:** `options.Listen(IPAddress.Loopback, 0, ...)` — one stack (IPv4 loopback), dynamic port, no
ambiguity.

### ⑤ Test fixture lifecycle: IClassFixture + IAsyncLifetime
**Concept:** in xUnit, when multiple tests need to **share an expensive resource** (here, a started broker
host), use a fixture.
- **`IClassFixture<T>`:** all tests in a test class share a **single** `T` instance (created once per class),
  injected via the constructor. Avoids re-spinning the broker for every test.
- **`IAsyncLifetime`:** the interface for **async setup/teardown** of a fixture/test. xUnit auto-calls
  `InitializeAsync()` (before) and `DisposeAsync()` (after). Broker startup is async
  (`await _app.StartAsync()`), which a (synchronous) constructor can't do — so `InitializeAsync` is the right
  place. Teardown (`await _app.DisposeAsync()`) is async too.

```csharp
public sealed class BrokerAppFactory : IAsyncLifetime
{
    private WebApplication? _app;
    public string Address { get; private set; } = "";
    public async Task InitializeAsync() { /* build → StartAsync → read address */ }
    public async Task DisposeAsync()    { if (_app is not null) await _app.DisposeAsync(); }
}
```
This pattern is the clean container for ②'s "direct startup". (DEC-006's disposal habit applies to test
resources too.)

**Flumewright usage (by phase):** this in-process hosting fixture is the foundation of integration testing.
Reused in M3 (ack/nack e2e), extended in M4 (mTLS — loopback + certs), and possibly moved to a genuinely
separate process in M5 (load). (Incident/decisions: 09 FIX-005 · DEC-007 · DEC-008)

---

## 11.7 Dev environment: containers, Linux capabilities & Git file mode

**Why this is here:** development moved into a VS Code **dev container** (09 DEC-009). Three concepts
came up that are worth understanding, because each caused a concrete failure during setup. None is about
the message bus itself — they are about the *environment* a distributed system is built and tested in, and
they recur on any container-based or CI workflow.

### ① Dev container = reproducible, isolated dev environment
**Concept:** a dev container is a Docker container described by `.devcontainer/devcontainer.json` (committed
to the repo) that holds the exact toolchain — here `mcr.microsoft.com/devcontainers/dotnet:8.0`. The editor
attaches *into* the container, so everyone (and CI) builds in the same place.
- **Why it matters for this project:** CI already runs on `ubuntu-latest`. Developing in a Linux container
  makes **local == CI**, removing "works on my machine" drift. .NET 8 is cross-platform and all build/test is
  `dotnet` CLI, so nothing in the project had to change.
- **Key distinction:** the container isolates **what the agent can *do*** (processes, permissions), not
  **what files it sees**. With a **bind mount**, the workspace folder is the *same files* as on the host —
  edit in either place, it's one file. (A "Clone in Container Volume" setup would instead keep files inside
  the container.)

### ② Linux capabilities — fine-grained root powers
**Concept:** Linux splits "root's powers" into discrete **capabilities** (e.g. `SETUID`/`SETGID` to change
user/group, `SYS_PTRACE` to attach a debugger, `AUDIT_WRITE`). A container can drop them to shrink the attack
surface.
- **In this config:** `--cap-drop=ALL` drops everything, then `--cap-add=SYS_PTRACE` adds back only what
  .NET debugging needs. Combined with non-root `vscode`, this is the "limit a misbehaving agent" stance.
- **The trade-off we hit:** with all capabilities dropped, **`sudo` itself can't work** (it needs
  `SETUID`/`SETGID` to become root) → `sudo: unable to change to root gid: Operation not permitted`. The
  lesson: install OS packages at **container build time** (via a dev-container *feature*, which runs before
  the drop) rather than `sudo apt-get` at runtime. (09 DEC-009)

### ③ File ownership across a bind mount (UID mismatch)
**Concept:** Linux files are owned by a numeric **UID**. A bind-mounted host folder keeps its host
ownership/permissions inside the container. If the host UID ≠ the container user's UID, the container user
may not own "its own" files.
- **What broke:** host-created `obj/` markers were owned by UID 0 (root) while the container user is
  `vscode` (UID 1000), so `dotnet build` couldn't update them (`MSB3374 … Access … denied`), and `chmod`
  on a non-owned file failed (`Operation not permitted`). Deleting stale `bin/`/`obj/` (regenerated under
  `vscode`) fixed the build. (09 DEC-010)

### ④ Git file mode (100644 vs 100755) — the exec bit lives in the repo
**Concept:** Git tracks a minimal **file mode**: `100644` (normal) or `100755` (executable). The executable
bit is stored **in the repo index**, not just on your local filesystem — so a script's "runnable" state
travels with clones.
- **Why it bit us:** the `pre-commit` hook was committed as `100644`. **Git won't run a non-executable
  hook**, so the local build+test gate (03 §5.1) could have been silently skipped throughout M1 — even
  though `core.hooksPath` was set. Likely cause: a Windows host never recorded the exec bit in the index.
- **The fix (filesystem `chmod` failed due to ③):** `git update-index --chmod=+x .githooks/pre-commit`
  sets the **index** mode to `100755` regardless of file ownership, then commit. Now any clone on any OS
  restores the bit. (09 FIX-007)
- **General lesson:** "a gate that is *configured*" ≠ "a gate that *runs*." The same verify-don't-assume
  discipline from 08 applies to tooling, not just code.

**Flumewright usage:** this is environment knowledge, not runtime architecture — but it underpins every
build/test/commit from M2 on. The committed `devcontainer.json` is also a portfolio artifact (reproducible
environment). (Decisions: 09 DEC-009 · DEC-010 · FIX-007)

---

## 12. Glossary

| Term | Meaning |
|------|---------|
| Broker | The central server mediating publishers and subscribers |
| Topic | A logical channel/subject classifying messages |
| Partition | A physical split of a topic. The basis of parallelism and ordering |
| Offset | A message's sequence number (position) within a partition. The basis for replay |
| Consumer Group | A set of subscribers splitting consumption of the same topic |
| ack / nack | Process success/failure acknowledgment signals |
| In-flight | A message sent but not yet acked, i.e., "in processing" |
| Idempotency | The property that doing the same operation multiple times yields the same result |
| DLQ (Dead Letter Queue) | A place to isolate repeatedly failing messages |
| Backpressure | Flow control that slows production when consumption can't keep up |
| Replay | Rewinding to a past offset to re-consume messages |
| Rebalancing | Partition reassignment when consumer group membership changes |
| Schema Registry | A central ledger managing message schemas |
| mTLS | Mutual TLS where both sides verify each other with certificates |
| CA | The root of trust that signs/issues certificates (Certificate Authority) |
| Opaque payload | Opaque bytes whose content the broker does not interpret |
| Fan-out | Spreading one message to many subscribers (broadcast) |
| Dev container | A Docker-based, reproducible dev environment defined by `.devcontainer/` and committed to the repo |
| Linux capability | A discrete slice of root's privileges (e.g. SETUID, SYS_PTRACE) that a container can grant or drop |
| Bind mount | Mounting a host folder into a container so both see the *same* files (vs an isolated volume) |
| UID | Numeric Linux user id; a bind-mount UID mismatch can make the container user unable to write "its own" files |
| Git file mode | The exec bit Git stores in the index (`100644` normal / `100755` executable), carried across clones |

---

## 13. How the Concepts Fit Together

> Split load via **partitioning** and process it divided across a **consumer group**, while each
> message is delivered without loss via **at-least-once + ack/nack**, exchanged safely with only
> legitimate peers via **mTLS**, matched in format via the **Schema Registry**, made extensible via
> **payload opacity**, scaled to 100K via **gRPC streaming + high-throughput techniques**, and the
> whole flow is observed via **observability**.

Each concept is not an isolated feature but complements the trade-offs of the others:
- Type risk created by opaque payloads (↑extensibility) → mitigated by the Schema Registry.
- Duplicate risk created by at-least-once → mitigated by idempotency.
- Parallelism created by partitioning → leveraged by consumer groups.
- Buildup risk created by high throughput → mitigated by backpressure.
- Opacity created by distribution → mitigated by observability.

---

## Related Documents

- Execution Plan: `01-execution-plan.en.md`
