# Flumewright Documentation

Design, learning, and process documents for Flumewright. (The project overview is the
[root README](../README.md).)

## Design
- [Execution Plan](design/plan.md) — scope, architecture, and roadmap (the top-level reference)
- [Concurrency Strategy](design/concurrency-strategy.md) — how concurrency correctness is defended in depth (five layers); the hazards, the tooling, and the bugs caught
- [M1 Design Note — gRPC Contract + Plaintext Broker](design/m1-grpc-contract.md) — the first end-to-end vertical slice
- [M2 Design Note — Partition Log Model](design/m2-partitioning.md) — per-partition append-only log, offset-based consumption
- [M3b Design Note — Redelivery & Dead-Letter Queue](design/m3b-redelivery-dlq.md) — non-blocking retry over a `{topic}.retry` topic, DLQ quarantine, all on plain topics
- [M3c Design Note — Rebalance / Dynamic Assignment](design/m3c-rebalance.md) — eager rebalancing, the group coordinator, generation fencing, the three-state machine

## Learning
- [Message Bus & PubSub Study Notes](learning/study-notes.md) — PubSub/message-bus concepts, the log model, test design

## Guides
- [Version Control & Validation Guide](guides/version-control-and-validation-guide.md) — branch, commit, and test-gate rules
- [CI/CD & Quality Gates](guides/ci-cd-and-quality-gates.md) — pipeline, the three PR gates, coverage strategy, concurrency testing

## Decisions
- [Decision & Fix Log](decisions/decision-and-fix-log.md) — running log of decisions and fixes
- [Architecture Decision Records (ADR)](decisions/) — point-in-time design decisions

## Collaboration
- [AI Collaboration & Cost Strategy](ai-collaboration.md) — how the project is built with an AI agent under human control