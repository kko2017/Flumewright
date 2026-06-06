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
