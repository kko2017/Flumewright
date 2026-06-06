# ADR 0001: The broker treats payloads as opaque bytes

## Status
Accepted

## Context
Publishers and subscribers each use their own `.proto`. If the broker had to understand every type,
it would need modification for each new message type, making it impossible to extend.

## Decision
The broker does not deserialize payloads; it handles them as opaque bytes plus headers.
Routing is performed solely via topic / partition key / headers.

## Consequences
- (+) The broker is agnostic to any client schema -> high extensibility.
- (-) Type-safety guarantees are lost -> mitigated by a Schema Registry (see ADR 0003, planned).
