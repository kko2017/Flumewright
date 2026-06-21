# M3b — Redelivery & Dead-Letter Queue (DLQ)

> **Status:** outline only — not yet designed in detail. Second of three M3 sub-milestones.
> Detailed decisions are deferred until M3a is implemented; this doc captures the agreed shape so it isn't
> lost. Built ON TOP OF M3a.

## Where it fits
M3a delivers the at-least-once **happy path**: a consumer processes and commits, a crash resumes from the
committed offset. M3b adds the **failure path**: what happens when processing a message *fails* (not just
when the consumer is slow or crashed). This completes at-least-once — delivery is retried, and messages that
can never be processed are quarantined rather than blocking the partition forever.

## The problem M3b solves
With M3a alone, a message that a consumer cannot process (bad payload, a downstream that keeps failing) sits
at the committed boundary: the consumer never commits past it, so it is redelivered forever and the
partition stalls (head-of-line blocking). M3b gives that message somewhere to go.

## Rough shape (to be designed properly later)
- **Redelivery:** an uncommitted message is redelivered (this already falls out of M3a's resume). M3b makes
  it deliberate — track delivery attempts per message.
- **Retry limit:** after N failed attempts, stop retrying that message.
- **Dead-letter queue (DLQ):** the message that exhausted its retries is moved to a dead-letter destination
  (likely a separate topic / partition) so the main partition can advance. It can be inspected/replayed
  later out of band.
- **Negative acknowledgement (nack):** the consumer signals "I could not process this" explicitly, vs just
  not committing — to be decided whether M3b needs an explicit nack RPC or infers failure from non-commit +
  redelivery count.

## Open questions (decide at M3b design time)
- How is the attempt count tracked — per `(group, partition, offset)` in the broker, or carried with the
  message?
- Is the DLQ a normal topic the broker writes to, or a distinct mechanism?
- Does the consumer explicitly `nack`, or does the broker infer failure from elapsed time / redelivery
  count?
- Retry backoff — immediate, fixed delay, or exponential? (Likely keep simple in Phase 1.)
- Ordering: redelivering one failed message while later ones succeed breaks strict per-partition order —
  what's the contract?

## Concurrency notes 🔒
Attempt counts and DLQ writes are shared state touched under load — same defense layers apply (see
[concurrency-strategy](concurrency-strategy.md)). Detailed concurrency design comes with the M3b design.

## Out of scope
- Rebalance / dynamic assignment → M3c.

*This is a placeholder to preserve the agreed direction. The real design (data structures, proto, steps)
will be written when M3a is done and we start M3b.*
