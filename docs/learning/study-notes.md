# Message Bus & PubSub Study Notes

> Concept study material compiled while building the Flumewright project.
> A collection of the core concepts needed to build a distributed message bus from scratch.
> Target stack: C# / .NET 8+ / gRPC / Protobuf

---

## Table of Contents

1. [What is the PubSub pattern](#1-what-is-the-pubsub-pattern)
2. [What is a message bus](#2-what-is-a-message-bus)
3. [Reference models: Kafka vs Google Pub/Sub](#3-reference-models-kafka-vs-google-pubsub)
4. [Delivery guarantee: At-least-once + ack/nack](#4-delivery-guarantee-at-least-once--acknack)
5. [Schema Registry (type safety)](#5-schema-registry-type-safety)
6. [mTLS certificate-based security](#6-mtls-certificate-based-security)
7. [Observability: Metrics / Tracing / Logging](#7-observability-metrics--tracing--logging)
8. [Consumer Group & Partitioning](#8-consumer-group--partitioning)
9. [Payload opacity (key to extensibility)](#9-payload-opacity-key-to-extensibility)
10. [gRPC transport patterns](#10-grpc-transport-patterns)
11. [High-throughput (100K+) techniques](#11-high-throughput-100k-techniques)
12. [Glossary](#12-glossary)
13. [How the concepts fit together](#13-how-the-concepts-fit-together)

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

**Flumewright's choice (log model confirmed):**
Both Kafka and Google Pub/Sub implement the **Pub/Sub pattern**; the table above shows they differ in *how
messages are stored and delivered*. Flumewright is built on the **log model** (Kafka-style): publish appends
to a per-partition append-only log; subscribers pull by holding their own offset (cursor); fan-out is many
subscribers reading the same log at their own offsets. This is a deliberate, confirmed choice — not a blend
of both delivery models (the two don't cleanly combine; log+pull vs push+buffer are different mechanics).
at-least-once is then realized via **offset commit** (the Kafka form of ack), not push-style ack of
individual messages. No dependency on external Kafka/PubSub. (See plan §3 and decision-and-fix-log DEC-015.)

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

### Flumewright Behavior (log model)
In a log model, "ack" is an **offset commit**: the subscriber tells the broker how far it has processed.
That commit IS the acknowledgment — it's how at-least-once is realized here, rather than acking each message
individually as in a push model.

**What value is committed — the Kafka convention (Flumewright follows it, DEC-023).** The committed offset
is the **next offset to read**, i.e. the *count* of records processed — not the offset of the last processed
record. So after handling records `0..N-1`, the consumer commits `N`. Resume then streams from `committed`
**directly** (no `+1`): the committed value already points at the first unprocessed record. Committing
`highWatermark` (the partition's message count) means "everything currently in the log is done"; an empty or
untouched partition is naturally `committed = 0`. The rejected alternative — committing the *last processed*
offset and resuming at `committed + 1` — was avoided because it needs a `+1` correction at both commit and
resume, doubling the chances of an off-by-one bug.
```
subscriber reads from its committed offset → processes → commits next offset (count processed)
   ↑                                                          │
   └── on crash/restart, resume from committed offset ◀───────┘  (records at/after committed re-read → at-least-once)
```
- The subscriber's **committed offset** is the durable "up to here is done" marker.
- If it crashes before committing, it resumes from the last commit and **re-reads** (possible duplicates →
  idempotency, above).
- nack / redelivery / **DLQ** (isolating messages that exceed a max-retry count): these are M3 concerns built
  on top of offset tracking. (Earlier drafts described per-message ack with `delivery_id` and an in-flight set —
  that was the push-model framing; the log model centers on the committed offset instead. See DEC-015.)
- Because the log is retained (Phase 1: process lifetime), a slow subscriber is never "dropped" — it simply
  has a lower committed offset and catches up at its own pace. (Contrast: a push model with a bounded buffer
  must drop or block when the buffer fills; the log model has no such dilemma.)

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

### Push vs Pull — the model Flumewright uses
There are two ways a broker gets messages to subscribers:
- **Push model** (e.g. classic brokers): the broker *pushes* each message into a per-subscriber buffer/queue.
  No retention — once delivered it's gone. A slow subscriber fills its buffer → the broker must **drop or
  block**. Late subscribers get nothing from before they connected.
- **Pull model** (Kafka, **Flumewright**): the broker *appends* messages to a per-partition **log** and keeps
  them. Each subscriber *pulls* by holding its own **offset** (a cursor = "next index I will read"). A slow
  subscriber just has a lower offset and catches up later — nothing is dropped. Fan-out = many subscribers
  reading the **same log** at their **own offsets**.

Flumewright uses the **pull/log model**. So "offset" is not a mere sequence label — it is the subscriber's
read position in the log, and the basis of replay and of at-least-once (via offset commit). (See DEC-015.)

### Partitioning
Split one topic into several **partitions**. Each partition is an **append-only log**.
A message goes to a specific partition by the hash of its partition key (round-robin if no key).
```
Topic "orders" (3 partitions)   — each partition is an ordered log; subscribers hold an offset per partition
 ├─ Partition 0 (log) : [off0:m1] [off1:m4] [off2:m7] ...
 ├─ Partition 1 (log) : [off0:m2] [off1:m5] [off2:m8] ...
 └─ Partition 2 (log) : [off0:m3] [off1:m6] [off2:m9] ...
```
Two benefits:
1. **Parallelism/scalability** — partitions are read independently/in parallel. Partition count = unit of
   parallelism = the key means of handling a 100K burst.
2. **Per-partition ordering** — within a partition log, records are in append order (by offset). Give
   order-sensitive messages the same partition key (same order ID → same key → same partition → preserved
   order). Global order *across* partitions is not guaranteed (a trade-off).

### How routing is decided — partition key present or not
There is **no separate flag**. The decision is made from the `partition_key` field itself: the publisher
either fills it or leaves it empty, and the broker branches on that.
```
PublishEnvelope.partition_key empty?
 ├─ NO  (key given)  → hash(key) % N  → same key always lands in the same partition  → ORDER preserved
 └─ YES (no key)     → round-robin     → spread evenly across partitions             → no ordering promise
```
- **The publisher decides per message, by the meaning of the data** — not the topic, and not a flag. The
  question is "must this message stay ordered relative to others?":
  - **Needs ordering / grouping → give a key.** Use the identifier of the thing whose events must stay in
    order: `partition_key = orderId` (an order's create/pay/ship events land together, in order), or
    `partition_key = userId` (one user's activity stays sequential).
  - **Order doesn't matter → omit the key.** It is then spread round-robin for even load (independent sensor
    readings, order-insensitive notifications).
- So **within one topic** some messages may carry a key and others may not — it is a per-message choice.
- **Ordering is guaranteed only within a partition, never across the topic.** The *only* way to get ordering
  is to route related messages to the same partition by giving them the same key. No key = a declaration that
  order is not needed.
- Implementation detail: "no key" = an empty `partition_key` (we branch on `IsEmpty`); we don't add a separate
  "has key" flag. (Kafka behaves the same: keyed record → hash-partitioned; null key → round-robin/sticky.)

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
- **M2 (partitioning):** topic→N partitions, key→partition routing (hash; round-robin if no key), each
  partition is a **per-partition append-only log with its own offset**; subscribers **pull by offset (cursor)**,
  partition logs read in parallel. A subscriber receives ALL of a topic's partitions at this stage. Partition
  count is fixed per topic. (No per-subscriber buffer/drop — that was the old push framing; see DEC-015.)
- **M3 (consumer group):** in-group partition assignment + distribution (static first) — group members
  split the topic's partitions; at-least-once via **offset commit**. Built ON TOP OF M2, a separate milestone.
- **Phase 2:** rebalancing maturity (dynamic reassignment), retention/eviction + disk persistence, replay.

### The log model's core: two bookmarks and commit (Kafka-style) ⭐ Key concept 🔒
This is the conceptual heart of the log/pull model — the thing that makes it different from a classic queue.

**A classic queue has one bookmark.** You read a message, it's removed, you process it, you read the next.
Read = remove. If you crash mid-processing, that message is gone (it left the queue). FIFO, one position.

**The log model has two separate bookmarks**, and they can diverge:
- **read offset** — "how far I've fetched." Held by the consumer, moves ahead.
- **committed offset** — "how far I've *safely finished*." Stored by the broker, trails behind. This is the
  source of truth for recovery.

Committed offset ≤ read offset, always. The gap between them is exactly "messages that will be redelivered
if I crash now."

**Why two, not one — the real reason is failure recovery, not speed.** Reading a message and finishing it
(e.g. writing to a DB) are separated in time, and the consumer can die in between. With one bookmark, the
position advances on read, so a crash loses the in-flight message (at-most-once). With two, you **commit
after processing** — a crash replays from the committed point, so unfinished work is redelivered
(at-least-once). The two bookmarks are what make "process, then commit" possible.

**Three independent separations the log model enables** (don't conflate them):
1. **publish ↔ consume (async decoupling)** — the API gets an "accepted" response the moment the broker
   appends to the log; actual processing happens later at the consumer's pace. (10M Instagram likes:
   answer "liked!" instantly, persist to DB slowly; the log is the buffer so a slow DB never blocks the API.)
2. **read ↔ commit (failure recovery)** — the two bookmarks above. *Reason: crashes, not speed.*
3. **batch read (throughput)** — fetch many ahead, process one at a time; read naturally runs ahead of
   commit. A bonus the two-bookmark split enables, not the reason for it.

**Commit model (M3a):** processing-then-**manual**-**batch** commit. The consumer explicitly commits the
next offset to read (the count processed) after a batch; the broker stores it per `(group, topic, partition)`.
Manual (not auto) because auto-commit tracks the *read* position and can lose in-flight work; manual commits
only what's actually done. Batch (not per-message) for throughput — at the cost of a larger replay window on
crash.

**at-least-once + idempotency = effectively-once.** Batch commit means a crash replays the
already-processed-but-not-committed messages → duplicates are inherent to at-least-once. The broker
guarantees "uncommitted is redelivered"; the **consumer** absorbs duplicates by being idempotent. Two ways:
- **upsert / overwrite** — the operation is naturally idempotent (`set like(user,post)=true`; `SET amount=500`).
  Re-applying yields the same result. (This is the "same id overwrites" case.)
- **dedup / reject** — the operation accumulates (`balance = balance + 100`), so overwriting won't help;
  record each message's unique id and **skip** if already processed.
Idempotency is the **consumer's** responsibility — the broker's contract is only at-least-once delivery.
Together they give "effectively-once."

**Consumer-group assignment rule (the heart of groups):** within one group, a partition is assigned to
**exactly one member**. If two members read the same partition, the group double-processes and their commits
collide. So one partition = one member; members split the partitions → load balancing. Consequence:
**partition count caps consumer parallelism** (more members than partitions → some sit idle). In M3a this
split is static (consumers declare which partitions); dynamic reassignment (rebalance) is M3c.

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

## 9.5 Distributed messaging semantics: delivery, ordering, failure

*(Learned while designing M3b — the failure path. The three concepts below are settled; the three
implementation details further down are placeholders to fill in once M3b is actually built.)*

### Order vs progress — you can't keep both once a message fails ⭐ Key concept
On the normal path, per-partition order and forward progress coexist trivially: process 0, 1, 2… in order,
commit as you go. The moment a message **fails**, they conflict, and you must pick:
- **Keep order (blocking retry):** retry the failed message in place; later messages wait behind it. The
  partition stops making progress while it retries — *head-of-line blocking*.
- **Keep progress (non-blocking retry):** move the failed message aside (to a retry topic) and continue with
  later messages. That message loses its place in the partition's order.

There is no third option that keeps both — that is the whole tension of failure handling in a log. Which to
choose depends on *why* it failed: a **transient** downstream outage tends to fail the next messages too, so
blocking (wait it out, keep order) is often better; a **poison** message (permanently bad) should be moved
aside so it doesn't block anything. A mature system offers both and lets the user choose per use case
(Kafka's ecosystem does exactly this).

### At-least-once duplication comes from *two operations not being atomic* ⭐ Key concept
At-least-once doesn't mean "duplicates randomly happen" — it means a specific, locatable thing: whenever
"do the work" and "record that it's done" are **two separate steps**, a crash *between* them replays the
work. Concretely: read offset k → publish it onward → commit k. If the process dies after publish but before
commit, resume re-reads k and publishes it again → duplicate. The only way to remove the duplicate is to make
the two steps **atomic** (a transaction binding output + offset) — that is what "exactly-once" requires, and
without it the floor is at-least-once. So the defense is not "try to avoid crashes" but **idempotency on the
consumer**: make re-processing the same message produce the same result. Recognizing "where are my two
non-atomic steps?" is the general skill — it locates exactly where duplicates can enter.

### The broker stays a broker — capability vs policy ⭐ Key concept
A recurring design line in this project: the broker provides **general mechanism**, not **specific policy**.
It appends to logs, serves offsets, treats payloads as opaque bytes (§9) — and, as confirmed in M3b, it does
**not** implement retry or dead-letter logic. Those are *policies* built on top of the general mechanism by
the client/SDK, using plain topics the broker already serves (a DLQ is just another topic someone publishes
to). This is the Kafka model, and it is why the broker stays small and reusable: every higher-level behavior
(retry, DLQ, ordering guarantees) is composed from the same few primitives rather than baked into the broker.
The skill is spotting the line between "mechanism everyone needs" (belongs in the broker) and "policy that
varies by user" (belongs above it).

### To fill in once M3b is implemented ⏳
*Placeholders — these are designed but not yet built; complete them from the code after M3b so the notes
record what was actually learned, not just planned:*
- **Error classification (transient vs poison) in practice** — how `RetryPolicy.shouldRetry` ended up
  drawing the line, and which failures were ambiguous. *(fill after M3b)*
- **Extension-point design: open the structure, defer the implementation** — how `RetryPolicy` was shaped to
  express single/multi-stage/blocking + delay while Phase 1 implemented only the simplest, and whether that
  shape actually held up when extended. *(fill after M3b / Phase 2)*
- **The delayed-retry trap** — why you can't just sleep a consumer to delay redelivery (it gets treated as
  dead and its partitions reassigned), and what due-time mechanism replaces it. *(fill when delayed backoff
  is built — Phase 2)*

---

## 10. gRPC Transport Patterns

gRPC is HTTP/2-based and supports four communication patterns.
Pattern choice matters for high throughput.

| Pattern | Form | Use in the message bus |
|---------|------|------------------------|
| Unary | 1 request → 1 response | Low-frequency control (topic creation, etc., admin) |
| Server streaming | 1 request → N responses | **Subscribe** (broker streams log records to the consumer from its offset) |
| Client streaming | N requests → 1 response | Batch upload |
| Bidi streaming | N requests ↔ N responses | **Publish** (batch + flow control), concurrent ack handling |

Core principle: **no per-message unary calls.**
Calling unary once per message for 100K collapses under connection/handshake overhead.
→ keep the connection open via streaming for bulk transfer + flow control.

---

## 11. High-throughput (100K+) Techniques

| Technique | Description |
|-----------|-------------|
| **Per-partition append-only log** | Append is O(1) and lock-light; reads are sequential by offset. Partitions are independent → parallel append/read |
| **Backpressure (M5)** | Flow control on the *publish* stream when a consumer's reads lag the log — a "slow down" signal to the producer, NOT a per-message drop. Real backpressure is an M5 concern |
| **Batching** | Batch publish/deliver → reduces syscall/allocation overhead |
| **Partition parallelism** | Partition count = unit of parallelism, scales with cores |
| **Thread pool tuning** | Dedicated long-running read loops + ThreadPool. Avoid excessive async context switching |
| **Object pooling / `ArrayPool`** | Buffer reuse to reduce GC pressure |
| **Zero/low-copy** | Opaque payload → no deserialization, minimal copying |

> **Backpressure** note: in a log model a slow consumer does not cause buildup the way a push buffer would —
> it simply lags (lower offset) and the retained log lets it catch up. Backpressure matters mainly on the
> *publish* side: if producers append faster than the system can sustain, M5 flows a "slow down!" signal back
> to the producer (like tightening a valve when a pipe over-fills). This is distinct from the push model,
> where a full per-subscriber buffer forces an immediate drop-or-block decision.

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

## 11.65 Test design: deterministic vs probabilistic assertions, and flaky tests 🔒

A lesson learned while adding the partition hash-distribution test (M2 zoom-out). It changed how I think
about what a test should assert.

### What a flaky test is
A **flaky test** is one that passes or fails **without the code changing** — green today, red tomorrow, green
again, all on the same commit. The key point: **the flakiness is usually caused by the test, not by a bug in
the code.** The code is fine; the test is asserting the wrong thing.

### Why flaky tests are damaging — they destroy CI trust
A test suite is only useful if a red result *means something*. When a test is flaky, red stops meaning "there
is a bug" and starts meaning "that flaky one again — just re-run it." Once people learn to ignore or re-run
red, they ignore the **real** failures too, and the CI gate is effectively dead: PRs get merged past a
suite no one believes. **A flaky test is worse than no test** — it adds noise and erodes the signal that the
whole point of CI depends on. So a flaky test is not a "strict, demanding test"; it is a **broken** test.

### The real rule: assert exactly the property the code guarantees
The mistake that creates flakiness is asserting **more than the code actually promises**. The fix is not
"loosen everything until it stops going red" — it is to match the assertion to the *kind* of property:

- **Deterministic property → assert it tightly (exactly).** The code guarantees an exact outcome every time,
  so the test should too. Loosening here would be a genuinely weak test.
  - Examples in Flumewright: "same partition key → same partition" (`ForKey(k)` is pure → assert equality
    exactly); "offsets are unique and contiguous under 1000 concurrent appends" (an invariant that must hold
    every run → assert exactly, no tolerance). A tight bound is *correct* here.
- **Probabilistic / statistical property → assert it as a generous range.** The code only promises a
  *tendency*, not an exact number, so demanding an exact number turns normal statistical variation into a
  failure — that is precisely how you manufacture a flaky test.
  - Example: hash distribution. Hashing 1000 distinct keys over 4 partitions will **never** land exactly
    250/250/250/250 — you get something like 248/251/263/238. That spread is the nature of statistics, not a
    bug. If the test asserted "each partition 245–255", a healthy run producing 263 would fail → flaky, and it
    would be **the test's fault**, not the code's. Asserting a **generous band** (each partition 150–350 for
    1000 keys) is not "loose" — it is the *accurate* expression of what the hash actually guarantees:
    "reasonably even, never all piled into one partition." It still catches the real failure mode (a broken
    hash dumping 900 keys into one partition) while ignoring meaningless jitter.

### The mental model
Ask: **"What does the code actually guarantee?"** Assert that, no more, no less.
- Guarantees an exact result → tight assertion (and a loose one would be a weakness).
- Guarantees only a tendency → ranged assertion (and a tight one would be a flaky bug *in the test*).

Avoiding flakiness is therefore not a goal in itself that you reach by "being lenient" — it is the natural
result of asserting the property the code truly has. A medical analogy: testing for "normal body temperature"
as *exactly* 36.5 °C would flag a perfectly healthy 36.7 as abnormal; the *accurate* test is a range
(36–37.5). Temperature naturally varies; so does a hash distribution. (Flumewright examples: 09 FIX-008 — an
integration test made robust with a bounded timeout instead of an unbounded wait — is the same idea on the
timing axis: assert "arrives within a bound", not "arrives at an exact instant".)

### Fake-green: the test that always passes and verifies nothing 🔒
A flaky test fails *sometimes* — at least it draws attention. A **fake-green** test is the opposite and more
insidious: it passes *every* time while not actually exercising what it claims. It asserts something, but the
assertion is hollow, so it gives false confidence that a behavior is covered when it is not. Because it never
goes red, nothing ever points at it. (Flumewright hit several of these at once — see 09 FIX-012.)

The hardest case is a **concurrency test that creates no concurrency.** Two ways it happened here, both
subtle:
- **Sequential dispatch.** Spawning N tasks in a plain `for` loop tends to let the last-scheduled task run
  last. If that task carries the "winning" value (e.g. the highest offset), the final state is correct by
  *scheduling order*, not by the lock — so the test passes even if the lock is broken.
- **A start-gate that isn't really a gate.** The fix for the above is to make all tasks wait on one signal,
  then release them together. But a `TaskCompletionSource` created *without*
  `TaskCreationOptions.RunContinuationsAsynchronously` runs its continuations **inline and synchronously on
  the thread that calls `SetResult()`**. So `gate.SetResult()` executes all the waiting tasks one after
  another on a single thread — again no real contention. (This is the flip side of the lost-wakeup reason
  for using `RunContinuationsAsynchronously` in production code, §earlier: there it prevents inline
  continuations from deadlocking under a lock; here their absence silently serializes a "concurrency" test.)

The one-question litmus for any concurrency test: **would it still pass if the lock under test were
removed?** If yes, it isn't testing the race. A genuine race needs (1) a gate with
`RunContinuationsAsynchronously`, (2) inputs shuffled so order can't save you, and (3) a bounded
`WhenAll(...).WaitAsync(timeout)` so a real deadlock fails fast instead of hanging (§FIX-008).

Two more fake-green shapes, not concurrency-specific:
- **Assertion that holds only via an internal detail** — e.g. relying on round-robin distributing messages
  exactly evenly; change the routing and the test silently breaks. Make the input deterministic instead.
- **Vacuous setup** — setup that never enters the code path under test (publishing to a topic before a guard
  that short-circuits first), or a setup step whose own result is discarded so its silent failure surfaces
  later as a confusing assertion. The test passes identically with the setup removed — which means the setup
  is proving nothing.

The meta-lesson: flaky and fake-green are *both* "the test is lying", just in opposite directions — flaky
lies by failing when the code is fine, fake-green lies by passing when the code is broken. Both are defects
in the test, not the code.

### When you resolve a value can fight with how you test it 🔒
A subtler lesson from FIX-013, worth keeping because it is not obvious. To make a relative read position
(LATEST = "from now") *atomic*, the natural move is to resolve it under the lock — but if that lock runs
inside a background reader task, resolution becomes **asynchronous**: the caller has no way to know when it
happened. A test that publishes right after subscribing then races the background resolution, and "fix" it
with a `Task.Delay` and you are back to a flaky test.

The resolution: resolve **at entry, on the caller's thread, before spawning the background work** — still
under the lock (so still atomic), but now **synchronous** (so the caller, and the test, can rely on "the
call returned ⇒ the value is pinned"). Atomicity and observability are not in conflict once resolution
happens at the synchronous boundary instead of deep inside an async hop. The general principle: **if a test
needs a `Task.Delay` to be reliable, the real problem is usually that something is resolved later (and less
observably) than it should be** — move the resolution earlier rather than papering over the timing.

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

## 11.8 CI/CD & quality gates

Built during the post-M2 infrastructure interval (DEC-017). The goal: move CI beyond "build + tests pass"
to **layered, visible, industry-standard quality and security gates**. The full pipeline design lives in
`docs/guides/ci-cd-and-quality-gates.md`; this section captures the *concepts and lessons* — several are
core ideas worth highlighting and are flagged **⭐ Key concept** below.

### The three independent PR gates ⭐ Key concept
A PR into `main` runs **three gates that are separate systems and do not share results** — each posts its
own check:
1. **build-and-test** (GitHub Actions `ci.yml`) — compile + unit/integration tests. Baseline correctness.
2. **SonarCloud Quality Gate** — overall quality: coverage, code smells, duplication, maintainability, some
   vulnerabilities. The *primary* quality gate.
3. **CodeQL** — deep security SAST (data-flow analysis for injection, unsafe deserialization, etc.); results
   in the GitHub Security tab.

The key point: **why both SonarCloud and CodeQL?** They don't overlap meaningfully — SonarCloud is
broad (overall quality), CodeQL is deep (security data-flow). Neither replaces the other. Analogy: a metal
detector (broad screen) plus an explosives dog (specialized) — independent screenings, both must pass.
CodeQL does **not** flow through SonarCloud's gate; each is wired into the branch ruleset separately.

### New-code gating — the "start low, raise gradually" mechanism ⭐ Key concept
A naive coverage gate ("whole codebase ≥ 80%") floods CI with red on day one and never passes. The better
design, which SonarCloud supports natively, is to gate **new code** (what *this PR* changed), not the whole
tree:
- "New code coverage ≥ X%" means *don't pay down old debt, but test what you write now.*
- The gate can be **on from the start** without legacy code blocking it, and overall coverage **rises
  naturally** as well-covered new code accumulates.
This is the actual mechanism behind "start the threshold low and raise it gradually" — you don't hand-tune a
global number, you let the new-code gate do the work. (A favorite follow-up: "how do you add a coverage gate
to a legacy project without halting the team?" → new-code gating.)

### Soft → hard rollout ⭐ Key concept
Turning on a *blocking* gate all at once risks paralysing work, so gates roll out in two stages:
1. **Soft** — the gate runs and its result shows on the PR, but it is **not** in the branch-protection
   ruleset; a failure does not block merge. Observe behaviour for a while.
2. **Hard** — once understood, add it as a **required status check** in the ruleset; from then a shortfall
   blocks merge.
This applies per gate (SonarCloud, CodeQL each go soft → hard). It's a low-risk way to introduce enforcement
without a "big bang" that surprises everyone with red.

### Coverage tooling: Coverlet, and why OpenCover not Cobertura ⭐ Key concept
- **Coverlet** (`coverlet.collector`) collects .NET coverage during `dotnet test --collect:"XPlat Code
  Coverage"`. It can emit different formats.
- **Critical detail:** the default format is **Cobertura**, but SonarCloud's .NET scanner reads
  **OpenCover**. Using the default silently produces **"coverage 0%"** in SonarCloud — the single most common
  first-integration failure. The fix is to force the format:
  `... --collect:"XPlat Code Coverage" -- DataCollectionRunSettings.DataCollectors.DataCollector.Configuration.Format=opencover`.
  (We hit this exact trap; once OpenCover was used, coverage jumped from 0% to ~91%.)
- **Exclusions** keep the % meaningful: generated proto code, `Program.cs` (host bootstrap), and the
  Observability/Security skeletons are excluded from the denominator — otherwise unwritten/auto-generated
  code drags the number down unfairly.

### Static analysis vs human review — complementary, not redundant ⭐ Key concept
A standout lesson (FIX-010): right after SonarCloud was introduced, it flagged an empty `catch (Exception)
{}` in the subscriber's per-partition reader that silently swallowed every exception — a faulted reader would
die invisibly. **M2's checkpoint reviews and the end-of-milestone zoom-out had both missed it**; a static
analyzer caught it on day one. The lesson: static analysis and human review see *different* things —
- **Human review / checkpoints** catch concurrency and semantic bugs a person can reason about (e.g. the
  lost-wakeup / LATEST-drop bug, FIX-009).
- **Static analysis** catches mechanical hazards a human skims past (an empty catch, an unhandled return).
Both are worth having; "a green test run is a starting point, not a conclusion" (§11.65) cuts both ways.

### Supply-chain hardening: pin actions to commit SHAs ⭐ Key concept
GitHub Actions referenced by a moving tag (`actions/checkout@v6`) are a **supply-chain risk**: a tag can be
repointed (compromised maintainer, hijacked repo) to a malicious commit, which CI would then run with access
to the repo's tokens and secrets. Pinning to an immutable **commit SHA** (`actions/checkout@<40-char-sha>  #
v6`) ensures only the exact, reviewed code runs. This is a standard hardening practice and a concrete,
credible security detail to mention. Trade-off: SHAs are opaque and need maintenance — which is what
Dependabot automates (below).

### Dependabot — and the secret it can't see ⭐ Key concept
- **Dependabot** is a GitHub bot (not a workflow): a `dependabot.yml` on the default branch makes it scan
  dependencies weekly and open **PRs** for updates (it never merges — a human reviews). We watch two
  ecosystems: **nuget** (.NET packages) and **github-actions** (keeps the SHA-pinned actions current).
- **The trap we hit:** Dependabot-opened PRs **do not receive the repository's normal Actions secrets** — by
  design, so a malicious dependency update can't exfiltrate them. So a CI step that needs `SONAR_TOKEN` fails
  on Dependabot PRs ("the format of the analysis property sonar.token= is invalid", because the token is
  empty). The fix: register the token *separately* as a **Dependabot secret** (Settings → Secrets →
  Dependabot), which is a different store from Actions secrets. (A clean answer to "why did SonarCloud
  fail only on Dependabot PRs?")

### What is *not* a gate — monitoring vs gating
Not every job blocks a merge. **Load/stress tests (`load.yml`) run nightly/manually, not per-PR**, because
they are slow and environment-sensitive — using flaky, runner-dependent timing as a merge gate would be the
performance-axis version of a flaky test (§11.65). They are *monitoring* (catching regressions over time),
not a gate. General rule: **fast + deterministic → per-PR gate; slow or noisy → periodic monitoring.**

### Concurrency testing has its own ladder 🔒
Static analysis and SonarCloud do **not** catch concurrency bugs. Those are addressed by a tiered approach:
checkpoint code review (humans reasoning about interleavings) + concurrency unit tests (e.g. 1000 concurrent
appends asserting unique/contiguous offsets) + load tests (indirect exposure). **Microsoft Coyote** —
systematic concurrency testing that deterministically explores schedules to find races/deadlocks — is the
tool for *direct* detection, reserved for **after M3** (when consumer-group/offset-commit concurrency
arrives). **Newman/Postman was rejected**: it's a REST/HTTP functional runner, but we're gRPC + protobuf and
need concurrency analysis, not API-functional assertions — a category mismatch, not just a tooling preference.

### Why this matters for the portfolio
The repo is public and doubles as a portfolio piece, so the gates are deliberately **visible**: README badges
(Quality Gate, Coverage, Bugs, Vulnerabilities, CI status), a SonarCloud dashboard, and PR checks. "I added a
coverage + quality + security gate set, gated new code so it didn't block legacy, rolled out soft→hard, and
pinned actions for supply-chain safety" is a compact, senior-sounding CI/CD story. (Decisions: 09 DEC-017 ·
FIX-010)

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

- Execution Plan: `docs/design/plan.md`
- Decision & Fix Log: `docs/decisions/decision-and-fix-log.md`
