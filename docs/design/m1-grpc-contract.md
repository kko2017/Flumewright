# M1 — gRPC Contract + Plaintext Broker + Single Pub→Sub

This design note summarizes the outcomes and architectural boundaries of Milestone 1 (M1). The primary goal of M1 was to build the thinnest vertical slice that proves a single message can pass end-to-end from a publisher, through the broker, to a subscriber over a plain gRPC connection.

## What M1 built

We established the fixed `fw.v1` protocol contract using a unary `Publish` RPC and a server-streaming `Subscribe` RPC. The broker host is implemented as a minimal ASP.NET Core Kestrel server running plaintext HTTP/2 (h2c) without TLS. 

At the core of the broker sits a pure, in-memory topic store that provides basic fan-out routing. It assigns monotonically increasing offsets to incoming messages and maintains isolated, per-subscriber channels using LATEST delivery semantics. We also provided a thin client SDK containing a basic `FlumewrightPublisher` and `FlumewrightSubscriber`, keeping the message payload completely opaque from end to end.

To guarantee that the plumbing works across network layers, we introduced a realistic integration test that hosts a real Kestrel port and verifies one message successfully traversing the publisher-to-subscriber path. Sample console applications were also wired up to manually demonstrate this behavior.

## Deliberately deferred

M1 is intentionally minimal. To preserve simplicity, several features are explicitly out of scope for the current design and must not be assumed present. Partitioning and partition keys are deferred to M2. Operational capabilities like consumer groups, message acknowledgements, in-flight message tracking, and dead-letter queues belong to M3, along with the Acknowledge and Admin RPCs. Transport layer security via mTLS and certificates will be introduced in M4. Advanced performance features such as streaming publish, request batching, memory backpressure, and the 100K throughput target are slated for M5. Finally, structured logging using Serilog and observable metrics are deferred to M6.

## Key decisions

Several architectural choices were made and recorded during this phase. They are documented in detail within the decision and fix log. DEC-001 formalized the choice to keep `Publish` as a unary call for simplicity in M1, while keeping the payload entirely opaque to the broker. DEC-005 established that dependencies should be injected via interfaces, and DEC-006 defined the disposal roadmap for managing client channels and unmanaged resources. 

For testing, DEC-007 opted for an in-process integration test that listens on a real Kestrel port rather than using an in-memory transport, ensuring the HTTP/2 networking stack is authentically exercised. This led to FIX-005, which recorded the shift from a standard WebApplicationFactory to directly launching the WebApplication via an IAsyncLifetime fixture due to hosting compatibility issues. Finally, DEC-008 confirmed that the .NET Http2UnencryptedSupport app context switch is unnecessary when communicating strictly over plaintext HTTP in .NET 8, leading to its removal from the samples (the switch was never in the SDK library).
