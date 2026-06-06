# ADR 0002: At-least-once delivery with ack/nack

## Status
Accepted

## Context
The broker cannot know whether a delivered message was actually processed. Subscribers may crash,
fail, or disconnect. Exactly-once is very costly (distributed transactions + dedup).

## Decision
Adopt at-least-once delivery. Track in-flight messages until acked; redeliver on nack or ack timeout.
Repeatedly failing messages move to a Dead Letter Queue (DLQ).

## Consequences
- (+) No message loss; simpler than exactly-once.
- (-) Duplicates are possible -> consumer-side idempotency is the user's responsibility.
