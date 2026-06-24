# M3b — Redelivery & Dead-Letter Queue (DLQ)

> **Status:** designed. Second of three M3 sub-milestones. Built ON TOP OF M3a.
> This note records the design decisions; the step-by-step instruction plan lives separately (it is a
> private working document, not in the repo).

## Where it fits

M3a delivered the at-least-once **happy path**: a consumer processes and commits, a crash resumes from the
committed offset. M3b adds the **failure path**: what happens when *processing a message fails* — not just
when the consumer is slow or has crashed. This completes at-least-once: delivery is retried, and a message
that can never be processed is quarantined rather than blocking its partition forever.

## The problem M3b solves

With M3a alone, a message a consumer cannot process (a bad payload, a downstream that keeps failing) sits at
the committed boundary: the consumer never commits past it, so it is redelivered forever and the partition
stalls behind it (head-of-line blocking). M3b gives that message somewhere to go, so the partition can
advance.

---

## Guiding principle — the broker stays a broker

The single most important framing for M3b: **the broker does only broker things; everything above that is
the user's decision, and we (the SDK) provide a thin, optional layer to make the common case easy.** This
mirrors Kafka exactly — the Kafka broker has **no** retry or dead-letter feature; retry/DLQ are entirely
client-side patterns built on top of plain topics + offset consumption. Our broker already provides that
substrate (append-only logs, offsets, subscribe, publish, commit — all from M1/M2/M3a), so M3b adds almost
nothing to the broker.

Three layers, and M3b builds the bottom two:

1. **Primitives (core SDK)** — `Publish`, `SubscribeGroup`, `Commit`. Already complete as of M3a. A user can
   build any retry/DLQ scheme directly on these.
2. **Thin helper (this milestone)** — an optional, policy-driven helper that implements the common
   non-blocking retry + DLQ pattern, so users don't re-write the boilerplate. Built *on top of* the
   primitives; the core SDK does not depend on it.
3. **Advanced (future / user-supplied)** — deeper behavior (multi-stage backoff, blocking retry) added by
   swapping the policy, either by a user or by us when a use case demands it. The thin helper is the
   foundation these extend.

## Decision A — non-blocking retry (with blocking left as an extension point)

When a message fails, there are two families of retry, and they trade off the same two properties —
**order** vs **progress** — which cannot both be kept once a failure occurs:

- **Blocking (in-place) retry** keeps order: the failed message is retried in place and later messages wait
  behind it. The partition makes no progress while it retries (head-of-line blocking).
- **Non-blocking retry** keeps progress: the failed message is moved aside (to a *retry topic*) and the
  consumer continues with later messages. Per-partition order is given up for the moved message.

**M3b chooses non-blocking as the default.** Rationale:
- It directly solves M3b's core problem — a poison message no longer blocks its partition. The partition
  advances immediately.
- It is the Kafka-ecosystem "fast path" and fits the project's throughput orientation.
- It reuses existing primitives entirely (publish to another topic + commit the original), so the broker is
  untouched and the M3a foundation is not disturbed.

**Order contract (explicit).** Per-partition order is preserved on the **normal path** (all successes commit
in order, exactly as M2/M3a). When a message **fails**, it is *isolated* to a retry/dlq topic and at that
moment leaves the original partition's order. Therefore a failed-then-retried message is **not** guaranteed
to preserve original order relative to its neighbours. This is a deliberate trade — order exchanged for
progress — not an accident. Users for whom strict order matters more than progress will use blocking retry
(future; see Deferred) or build an order-preserving pattern directly on the primitives.

**Blocking is a deliberate non-goal for now, kept as an extension point.** There is a real use case for it:
a *transient* downstream failure tends to fail the *next* messages on the same partition too, so retrying
one message non-blocking (letting later ones through) has little value — they will fail for the same reason,
and the retried message may finish out of order anyway. For that case, blocking is the better tool. M3b does
**not** implement blocking, because it requires the same delayed-redelivery machinery as multi-stage backoff
(see Decision on backoff), which is out of Phase-1 scope. The helper's policy interface is shaped so blocking
can be added later without restructuring.

## Decision B — responsibility split: SDK-side thin helper

- **Broker:** unchanged. Retry topics and the DLQ topic are *ordinary topics* it already serves. The broker
  does not know about attempts, retry limits, or DLQ semantics. (This is the Kafka model and matches the
  guiding principle.)
- **Core SDK:** essentially unchanged — the M3a primitives already suffice.
- **Thin helper (the actual M3b work):** an optional component (separate namespace, e.g.
  `Flumewright.Client.Resilience`) that *composes* the core primitives to implement non-blocking retry + DLQ.
  It depends on the core; the core does not depend on it. A user who doesn't want it references only the core.

## Mechanism (Phase-1 M3b)

The non-blocking flow for a single failed message:

1. Consumer reads offset *k* from the original topic.
2. Processing fails.
3. The helper classifies the failure (see error classification). If retryable and under the attempt limit,
   it **publishes the message to the retry topic** (`{topic}.retry`); if non-retryable or over the limit, it
   **publishes to the DLQ topic** (`{topic}.dlq`).
4. The helper **commits offset *k+1* on the original topic** — the message has been handed off, so the
   original partition advances and does **not** wait for the retry result.
5. The consumer continues with *k+1, k+2, …* on the original topic, unblocked.
6. A (separate) consumer of the retry topic reprocesses the moved message later.

**Attempt count and original metadata travel in headers, not broker state.** Each time a message is moved to
a retry topic, the helper increments an `x-attempts` header. Original location is preserved in
`x-original-topic`, `x-original-partition`, `x-original-offset` (and, if available, a failure reason) so it
survives all the way to the DLQ for inspection. This keeps the payload opaque (only headers are added,
consistent with ADR 0001), keeps the count *with the message* rather than in fragile consumer memory or
broker state (a global counter that resets on restart is an anti-pattern — a poison message would re-block
on every redeploy), and adds overhead only to messages that are actually retried. The redelivered message
gets a **new offset** on the retry topic; its original offset lives in the header — this is the essence of
non-blocking: the message leaves its original position for a new position on another topic.

**Why broker-side attempt tracking was rejected.** An alternative was a broker-side counter keyed by
`(group, topic, partition, offset)`, a sibling of M3a's committed-offset store. It was rejected because (a)
it is not the Kafka model — the broker would stop being a pure broker; (b) it adds broker memory and a
cleanup policy (delete counters below the committed offset) with its own concurrency hazards around the
commit/nack race; and (c) header-carried state is the standard Kafka approach and avoids all of that. The
broker stays stateless about retries.

## Error classification — transient vs poison

Not every failure should be retried. Two kinds:

- **Transient** (network blip, a downstream briefly down, rate limiting): worth retrying — it will likely
  recover. → retry topic.
- **Poison pill** (a permanently un-processable message: malformed payload, failed deserialization/
  validation, or a consumer-code bug): retrying is pure waste — it will fail identically every time. →
  straight to the DLQ, no retries.

Who "owns" a poison pill is usually the producer or the data itself (a malformed or business-invalid
message), or occasionally a consumer-code bug (which is fixed and then replayed). The point is that a poison
pill should be detected early and quarantined, not retried.

This classification is exactly the job of the policy's `shouldRetry` decision (below). The DLQ is **not a
trash can** — it is an inspectable, replayable quarantine: a failed message is preserved there (with its
header metadata) so it can be examined, the root cause fixed, and the message replayed (read from the DLQ,
fix/transform, publish back to the original topic) or idempotently reprocessed. Idempotency is a separate
consumer-side concern — it is the defense against the *duplication* that non-blocking introduces (below), not
a poison-pill remedy.

## Extension point — `RetryPolicy`

To be a real foundation (layer 3 extends it), the helper is **policy-driven**, not hard-coded. A
`RetryPolicy` decides, per failure/attempt:
- `maxAttempts` — how many retries before the DLQ.
- `shouldRetry(failure)` — transient (→ retry) vs poison (→ DLQ now). This is where users encode their
  domain's error taxonomy.
- destination + delay — which retry topic, and how long to wait before redelivery.

Phase-1 ships **one default policy**: a fixed attempt limit, a single retry topic (`{topic}.retry`), and
**no delay** (immediate retry). Because the policy returns *destination + delay*, the same interface can
express a single retry topic or a multi-stage chain (`retry.1` / `retry.2` / …) with increasing delays — so
multi-stage backoff and blocking retry are added later as new policies **without restructuring the helper**.
The structure is opened now; the delay/multi-stage/blocking implementations are deferred (see below).

## Why no delayed backoff in Phase 1

Implementing a *delay* before redelivery is the heavy part, and it is deliberately deferred. A naive "sleep
the consumer until the due time" does not work — the broker/coordinator would treat the consumer as dead and
reassign its partitions. Delayed redelivery instead needs a due-time mechanism (e.g. the retry-topic consumer
inspects a timestamp and pauses consumption of that topic-partition until due). That is a distinct scheduling
mechanism, out of Phase-1 scope. Phase 1 therefore uses **immediate** non-blocking retry: still a real
redelivery (move to retry topic, reprocess, DLQ after N attempts), just without timed backoff. Timed/
multi-stage backoff and blocking retry both build on this same delay mechanism and land together in Phase 2.

## Duplication trade-off (at-least-once, extended)

Step 3 (publish to retry topic) and step 4 (commit the original) are **two separate operations**. If the
consumer crashes between them — retry-publish done, original commit not — resume will read offset *k* again
and publish it to the retry topic a second time, so the message can appear twice. This is the same
at-least-once property M3a already has (at least once, possible duplicates); M3b does not change it.
Exactly-once would require binding the publish and the commit atomically (a transaction), which we do not
have. **Idempotency / de-duplication is therefore a consumer-side responsibility** (same as Kafka). The
design note records this explicitly so it is a conscious contract, not a surprise.

## Concurrency notes 🔒

M3b adds little broker-side shared state (the broker stays stateless about retries), so the concurrency
surface is smaller than M3a's. The points to watch:
- The helper's retry-publish-then-commit sequence is non-atomic by design (the duplication trade-off above);
  this is accepted, not fixed, in Phase 1.
- Header mutation (`x-attempts`) happens on a copy bound for a new topic, never on the original stored
  message — the stored log stays immutable (ADR 0001 / M2).
- The same defense layers apply to the helper as to the rest of the code (see
  [concurrency-strategy](concurrency-strategy.md)): checkpoint code review, integration tests over a real
  broker, and the FIX-008 / FIX-012 disciplines (bounded waits, no fake-green, no `Task.Delay`-as-sync).

## Testing approach

Retry/DLQ behavior is *our* code (the helper), so we test it — end to end over a **real broker** (Kestrel +
gRPC), the M3a `ConsumerGroupE2ETests` pattern. The test does **not** mock the broker; instead it injects a
processing handler whose success/failure it controls (e.g. a payload marked to fail), then asserts the
observable outcome through real topics: the failed message appears on `{topic}.retry` (or `{topic}.dlq` after
N attempts) with the correct `x-attempts` / original-metadata headers, and the original topic's committed
offset has advanced past it. "Mocking" here means controlling the handler's outcome, not faking the
transport.

## Relationship to deferred items (see decision-and-fix log → Deferred Items Ledger)

- **DEC-002 (`Acknowledge` RPC, deferred to M3): closed / superseded.** ack = offset commit (M3a
  `CommitOffset`); nack = the SDK pattern above (publish to retry/dlq topic), not a broker RPC — the
  Kafka-style broker has no ack/nack RPC. No `Acknowledge` RPC is added. (`Admin` RPC remains separately
  deferred.)
- **DEC-006 (dispose in-flight/redelivery timers, mapped to M3): deferred further to Phase 2.** Phase-1 M3b
  uses immediate retry with no timers, so there is nothing to dispose yet; the disposal concern arrives with
  the delayed-backoff scheduler in Phase 2.
- **Multi-stage / delayed backoff and blocking retry: Phase 2.** Both depend on the deferred delay mechanism;
  M3b opens the `RetryPolicy` structure for them but implements neither.

## Out of scope for M3b

- **Rebalance / dynamic assignment** → M3c. The broker still does not enforce partition non-overlap across
  group members.
- **Timed/multi-stage backoff, blocking retry, delayed-redelivery scheduling** → Phase 2 (above).
- **Exactly-once / transactional publish+commit** → not planned for Phase 1; idempotency is the consumer's
  responsibility.
