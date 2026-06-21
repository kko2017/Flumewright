# M3a — Consumer Groups & Offset Commit (static assignment)

> **Status:** design agreed, not yet implemented. First of three M3 sub-milestones.
> **Scope split:** M3a = static assignment + manual batch commit + resume (the at-least-once *happy path*).
> M3b = redelivery + DLQ (the *failure path*). M3c = rebalance (dynamic assignment). Built ON TOP OF M2.
>
> Concepts behind this doc (two bookmarks, manual batch commit, idempotency, the one-partition-one-member
> rule) are written up in [study-notes §8 — the log model's core](../learning/study-notes.md). 🔒

## Goal
Let the broker track, durably, how far a **consumer group** has processed each partition, so a consumer can
crash and resume without losing or skipping work — i.e. **at-least-once** delivery. M2 left the offset in the
subscriber's memory; M3a moves the *committed* offset into the broker, owned by the group.

## What M3a adds on top of M2
- A **consumer group** identity on subscribe (`group_id`).
- **Static partition assignment** — a consumer declares which partitions it will read. Non-overlap across
  members is the consumers' responsibility in M3a (the broker does not enforce it; that's M3c/rebalance).
- A **committed-offset store** in the broker, keyed by `(group, topic, partition)`.
- A **manual, batch commit** RPC — the consumer commits "processed up to offset N" after processing.
- **Resume from committed** — on subscribe, the broker streams from `committed + 1` (or a first-time
  default).

## Key design decisions
- **Commit timing:** after processing (→ at-least-once). Committing before processing would be at-most-once.
- **Commit mode:** **manual** (explicit RPC), not auto. Auto-commit tracks the *read* position and can lose
  in-flight work.
- **Commit granularity:** **batch** — the consumer commits once per processed batch, not per message.
  Trade-off: higher throughput, larger replay window on crash. Duplicates within that window are inherent to
  at-least-once and are absorbed by an **idempotent consumer** (the consumer's responsibility; the broker's
  contract is only at-least-once).
- **First-subscribe position (auto.offset.reset):** the consumer chooses; **default earliest** (0). A message
  bus should default to "process everything from the start" as the safe option; `latest` is opt-in.
- **Commit validation (broker side):** the broker validates a commit — the offset must be within the log's
  range, and a commit that moves **backwards** (below the current committed offset) is rejected/ignored.
  Guards correctness and concurrency safety.
- **One partition = one member (within a group):** assignment rule from study-notes §8. In M3a it is static;
  the broker trusts the consumers' declared partitions.

## Proto (sketch — to be finalized at implementation)
```protobuf
// Subscribe gains group + partitions + first-position policy
message SubscribeRequest {
  string topic = 1;
  string group_id = 2;            // NEW — which consumer group
  repeated int32 partitions = 3;  // NEW — static assignment: partitions this consumer reads
  OffsetReset reset = 4;          // NEW — earliest (default) | latest, used only when no committed offset
}
enum OffsetReset { EARLIEST = 0; LATEST = 1; }

// Manual batch commit
rpc CommitOffset(CommitRequest) returns (CommitAck);
message CommitRequest {
  string group_id = 1;
  string topic = 2;
  int32 partition = 3;
  int64 offset = 4;               // "processed up to here"
}
message CommitAck { bool ok = 1; string reason = 2; }  // reason set when a commit is rejected
```

## Broker data structures
- M2 log store unchanged (per-partition append-only logs + message offsets).
- **NEW:** committed-offset map — `(group, topic, partition) → committed offset`. Durable for the process
  lifetime in Phase 1 (disk persistence is Phase 2, consistent with M2's retention decision).

## Concurrency notes 🔒
- The committed-offset map is shared mutable state → guarded by a lock (or equivalent). Concurrent commits
  to the same `(group, partition)` (e.g. a reconnecting or duplicate member) must not interleave into a lost
  update. The backwards-commit rejection also protects ordering.
- This is exactly the surface the defense layers were built for (see
  [concurrency-strategy](concurrency-strategy.md)): CA1031 + threading analyzers at build, the reviewer
  sub-agent at checkpoints, concurrency tests, and Coyote.

## Out of scope for M3a (deferred)
- **Redelivery / DLQ** → M3b. (What happens when processing *fails*, not just when a consumer is slow.)
- **Rebalance / dynamic assignment** → M3c. (Members joining/leaving and the broker reassigning partitions.)
- The broker does **not** enforce partition non-overlap across members in M3a.

## Tentative step breakdown (to refine when writing the M3a instruction doc)
1. proto: `group_id` + `partitions` + `OffsetReset` on subscribe; `CommitOffset` RPC.
2. broker: committed-offset store (`(group,topic,partition) → offset`) with locking + backwards-commit
   rejection + range validation.
3. broker: resume-from-committed on subscribe (committed+1, or reset policy on first subscribe).
4. client SDK: group subscribe + manual batch commit call.
5. integration tests: commit + resume; crash-and-resume redelivers the uncommitted window; backwards commit
   rejected; first-subscribe earliest/latest.
