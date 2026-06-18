# M2 — Partitioning + Append-Only Log + Offset-Based Consumption

This design note summarizes the outcomes and architectural boundaries of Milestone 2 (M2). The primary goal of M2 was to introduce topic partitioning, an append-only log model, and offset-based message consumption to Flumewright.

## What M2 Built

We transitioned the topic storage from a channel-based push model to a per-partition append-only log model (as defined in DEC-015). 

- **Per-Partition Logs:** A topic consists of a fixed number of partitions (configured to default to 4). Each partition holds an in-memory, ordered list of `StoredMessage` records and its own monotonic offset counter. Offsets represent a record's position in that partition's log, starting at 0 for the first append.
- **Routing System:**
  - **With Partition Key:** When a publisher provides a `partition_key`, the partition index is determined using a stable hash function (`PartitionRouter.ForKey(key, N)`) yielding `hash % N`. This guarantees that messages with the same key always go to the same partition, preserving ordering.
  - **Without Partition Key:** When no key is provided, messages are routed using round-robin distribution via a thread-safe atomic counter (`PartitionRouter.RoundRobin`). No ordering guarantee is promised across different keys or empty keys.
- **Consumption Model:**
  - Subscribers consume from a topic by reading all partitions.
  - The `ITopicStore` exposes a fanned-in reader via `SubscribeAsync(topic, startOffset, ct)`. This spawns independent partition reader tasks that stream records into a single merged channel.
  - An offset of `0` begins reading from the start of the log (retained read), while `-1` begins reading "from now" (delivering only newly appended messages).
  - The gRPC `Subscribe` RPC writes a `DeliverEnvelope` carrying the `partition` and `offset` for each message.
- **Concurrency & State Control:**
  - A per-partition `lock` guards the list of messages and ensures atomic, contiguous offset assignment during concurrent publishes.
  - A `TaskCompletionSource` notify-all pattern wakes waiting reader tasks upon new message appends. Waiting readers re-check for new messages under the partition lock, preventing lost wakeups or race conditions.

## Deliberately Deferred

Several capabilities are explicitly out of scope for M2 and are deferred to later milestones or phases:
- **Consumer Groups & Partition Assignment:** In M2, all subscribers receive all partitions. Dynamic partition assignment across group members is deferred to M3.
- **Offset Commit & Acknowledgements:** Tracking subscriber progress (commit/ack/DLQ) is deferred to M3.
- **Retention Policies:** Evicting messages from memory (by time or size) is deferred to Phase 2. Currently, the log grows unbounded in-memory for the lifetime of the process.
- **Replay / Seek API:** Arbitrary offset seeking over gRPC is deferred to Phase 2. M2 only supports "from start" (0) and "from now" (-1) via the internal store.
- **Streaming Publish:** Publish calls remain unary; streaming publish is deferred to M5.

## Key Decisions

- **DEC-015 (Log Model Transition):** Adopted a pull-based Kafka-style log store. This choice replaced the previous channel-based push model to prevent subscriber buffer overflows and support message retention.
- **FIX-009 (LATEST Semantics Bug):** Discovered during Checkpoint A that the channel-based store suffered from a bug where slower subscribers could receive messages out of order under LATEST semantics. This bug catalyzed the transition to the append-only log model.
