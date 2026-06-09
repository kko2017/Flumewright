# Flumewright — Decision & Fix Log

> A lightweight record of mid-development fixes and small design decisions — the things too small for
> a full ADR but too valuable to lose. Each entry: **Symptom → Cause → Fix → Future impact**.
> "Future impact" is the point: it tells a later milestone (or a later you) what to revisit, saving
> rediscovery cost. For large architectural decisions, use ADRs in `docs/decisions/` instead.
>
> Repo location (English version): `docs/decisions/decision-and-fix-log.md` (companion to the ADRs).
> The Korean version (`09-decision-and-fix-log.ko.md`) is a personal-reference draft, not committed.

---

## FIX-001 — Fan-out broken: a shared per-topic channel delivered to only one subscriber
- **Milestone/Step:** M1 / Step 2 → fixed in Step 2.1
- **Severity:** real bug (latent — passed tests with a single subscriber)
- **Symptom:** With 2+ subscribers on the same topic, a published message reached only ONE of them.
  Not observed initially because tests used a single subscriber.
- **Cause:** `InMemoryTopicStore` kept **one shared `Channel<StoredMessage>` per topic** and every
  subscriber read from it. A `Channel` is a single-consumer queue, so reads compete and each message
  is consumed once — the opposite of PubSub fan-out.
- **Fix:** Each subscriber now gets its **own private channel**; the topic holds a
  `ConcurrentDictionary<Guid, Channel<StoredMessage>>`. `PublishAsync` writes the same message to
  every subscriber's channel (offset assigned once at the topic level). `SubscribeAsync` registers a
  fresh channel and removes it in a `finally`. LATEST semantics now fall out naturally (a subscriber
  only sees messages published after it registered), so the old `offset > startOffset` hack was deleted.
- **Future impact:** M3 (consumer groups) will change delivery again — group members should split a
  topic's messages, not each get all of them. Revisit this fan-out model there; the per-subscriber
  channel becomes a per-group-member channel with assignment logic.

---

## FIX-002 — Publisher cancellation token misused on subscriber writes; blocking fan-out
- **Milestone/Step:** M1 / Step 2.1 → fixed in Step 2.2
- **Severity:** real bug (latent under unbounded channels) + design smell
- **Symptom:** A publisher's cancellation could interrupt delivery to subscribers; a slow/full
  subscriber channel could block the entire publish (one subscriber holding the others hostage).
- **Cause:** `PublishAsync` used `await sub.Writer.WriteAsync(message, ct)` inside the fan-out loop —
  (a) `ct` was the *publisher's* token, wrongly governing *subscriber* delivery; (b) awaiting per
  subscriber serializes the fan-out behind the slowest channel.
- **Fix:** Switched to non-blocking `sub.Writer.TryWrite(message)` (always succeeds on an unbounded
  channel), removed `ct` from the loop, and `PublishAsync` returns `new ValueTask<long>(offset)`
  directly. Also removes the latent risk of writing to a channel mid-removal (`TryWrite` returns
  false instead of throwing).
- **Future impact:** ⚠️ **M5** — when channels become **bounded + backpressure**, `TryWrite` can
  return `false` on a full channel. That path must be handled then (drop? block? signal backpressure
  upstream?). The intended M5 code comment for this was accidentally omitted; this entry is the record.

---

## DEC-001 — M1 Publish is unary (streaming deferred to M5)
- **Milestone/Step:** M1 / Step 1
- **Type:** scope decision
- **Decision:** The `Publish` RPC is **unary** in M1, even though the plan's §8 shows streaming and
  §3.4 says "no per-message unary".
- **Rationale:** The "no per-message unary" rule targets 100K throughput (M5). M1's goal is only to
  prove one message gets through; unary is simpler to build and debug. `Subscribe` stays server-
  streaming from the start (that's the PubSub essence).
- **Future impact:** M5 promotes `Publish` to client/bidi streaming with batching + backpressure.
  Record that change as an ADR or here when it happens.

---

## DEC-002 — Ack/Admin RPCs deferred to M3
- **Milestone/Step:** M1 / Step 1
- **Type:** scope decision
- **Decision:** The M1 proto contains only `Publish` + `Subscribe`. `Acknowledge` and `Admin` from
  plan §8 are omitted.
- **Rationale:** M1 is a thin vertical slice (one message through). Ack/nack belongs with at-least-once
  delivery (M3); Admin is low-priority control surface.
- **Future impact:** M3 adds `Acknowledge` (+ in-flight/DLQ) to the contract; Admin can come when topic
  management is needed. Adding RPCs is backward-compatible, so deferral is cheap.

---

## DEC-003 — `global.json` rollForward = latestFeature (not latestMinor)
- **Milestone/Step:** Phase 0 (doc alignment)
- **Type:** config decision / inconsistency fix
- **Decision:** Pin the SDK with `"rollForward": "latestFeature"`.
- **Rationale:** The scaffolding draft used `latestMinor`, which would accept SDKs beyond 8.x (up to
  the next major), weakening the ".NET 8 pinned" intent. `latestFeature` stays within 8.0.x feature bands.
- **Future impact:** When intentionally moving to .NET 9 (post-Phase 2), update this explicitly.

---

## DEC-004 — `.gitignore` must block certs/keys and logs
- **Milestone/Step:** Phase 0 (plan §7.1 / §4)
- **Type:** security/hygiene decision
- **Decision:** `.gitignore` blocks build output **plus** certs/keys (`*.pfx *.key *.pem *.crt *.cer
  *.p12 certs/`) and logs (`logs/ *.log`).
- **Rationale:** mTLS dev keys (M4) and log files (M6) must never be committed. The original drafts
  had weak cert patterns and no log entries; aligned across plan/03/04.
- **Future impact:** M4 certgen outputs to `certs/` (ignored); M6 logging writes to `logs/` (ignored).
  The store stays opaque, so no payload ever lands in logs by design.

---

## DEC-005 — Dependency Injection via the built-in container, depend on interfaces
- **Milestone/Step:** M1 / Step 3 (discussion)
- **Type:** architecture decision
- **Decision:** Use .NET's built-in `Microsoft.Extensions.DependencyInjection`. Services depend on
  **interfaces** (e.g. `MessageBusService` → `ITopicStore`), with the concrete type chosen at
  registration (`AddSingleton<ITopicStore, InMemoryTopicStore>()`).
- **Rationale:** Loose coupling enables swapping implementations without touching consumers — directly
  serves the "three independent axes" goal (persistence can change alone). Phase 2 will register a
  disk-backed `ITopicStore` by changing one line. Also improves testability (inject fakes).
- **Trade-offs / watch-outs:** DI errors surface at **runtime**, not compile time (cf. the Step 3
  singleton trap). Beware **captive dependencies** — a Singleton must not capture a Scoped service;
  as services grow (M3–M6), keep lifetimes consistent.
- **Future impact:** When adding services in M3+ (delivery manager, in-flight tracker), register them
  with deliberate lifetimes and avoid singleton→scoped capture.

## DEC-006 — Disposal roadmap (IDisposable / IAsyncDisposable)
- **Milestone/Step:** M1 / Step 3 (discussion) — applies M3, M4, Phase 2
- **Type:** resource-management decision
- **Decision:** Do NOT force Dispose where there's nothing unmanaged to release (M1 `InMemoryTopicStore`
  holds only managed channels). Introduce `IDisposable`/`IAsyncDisposable` exactly where disposable
  members or unmanaged handles appear.
- **Where it becomes required:**
  - **M1 (Step 4, client SDK)** — `GrpcChannel` is `IDisposable`; the Publisher/Subscriber hold one,
    so they implement `IDisposable` (simplified, sealed) and dispose the channel. This is the *earliest*
    real disposal case — earlier than originally estimated below.
    **✅ Realized as predicted in M1 Step 4** — both SDK classes are sealed, no finalizer, `Dispose() => _channel.Dispose()` (see 08 Step 4 verification).
  - **M3** — in-flight tracking, redelivery timers/schedulers → dispose timers/cancellation sources.
  - **M4** — `X509Certificate2` is `IDisposable`; secure connections → must dispose.
  - **Phase 2** — disk WAL: `FileStream`/segment file handles → disposal is mandatory.
- **Rationale:** GC handles managed memory but not unmanaged handles, and shouldn't be relied on for
  prompt release. The signals for needing Dispose: the class holds an `IDisposable` member, or grabs a
  native handle directly. M1 has neither.
- **Future impact:** Revisit at each milestone above. Also consider bounding `_topics` growth in M2+
  (a topic dictionary that only grows is a slow leak under many distinct topics).
- **Custom Dispose policy (our own pattern, when the time comes):** classes that own disposable
  members implement `IDisposable`/`IAsyncDisposable`; **registration with the DI container means the
  container calls Dispose automatically** at shutdown (Singleton) / request end (Scoped) — we author
  the Dispose, the container invokes it (no manual call site needed for DI-managed instances; use
  `using` for manually-created ones). Use the **simplified** form (no finalizer, `sealed` class,
  `Dispose()` releases members); reserve the **full** `Dispose(bool)+finalizer+GC.SuppressFinalize`
  pattern for the rare case of holding a **native handle directly**. Concrete target: Phase 2
  `DiskTopicStore` (WAL `FileStream`) as `IAsyncDisposable`, auto-disposed by the container.

## DEC-007 — M1 integration test is in-process (real-port Kestrel)
- **Milestone/Step:** M1 / Step 5
- **Type:** test-strategy decision
- **Decision:** The Step 5 e2e integration test starts the broker host **inside the test process on a real
  Kestrel port (h2c)**, and the SDK connects via `http://localhost:{port}` with a genuine gRPC channel.
  It does NOT spawn a separate OS process.
- **Rationale:** M1's completion bar is "one message through", not "prove physical OS-process separation"
  (07 §1). The process-separation *requirement* is already met by the architecture (standalone broker host +
  gRPC over the network), and even in-process the SDK traverses the real HTTP/2 stack. Spawning a separate
  process introduces port contention, startup waits, and teardown that make CI flaky — at odds with the
  "thin slice" philosophy.
- **Traps:** ① `WebApplicationFactory`'s default TestServer uses an in-memory handler that does not match
  `GrpcChannel.ForAddress` → force real-port Kestrel. ② LATEST semantics mean publishing before the
  subscription is established is flaky → absorb with a "re-publish + timeout wait" pattern.
- **Future impact:** True process-separation is covered by manual smoke (sample consoles) / the M5 load
  tests. M5 load scenarios may launch the broker as a genuinely separate process (revisit there).
- **⚠️ Implementation correction (Step 5 outcome → see FIX-005):** the proposed fix for trap ① —
  "subclass `WebApplicationFactory` and swap to `UseKestrel()` in `CreateHost`" — was incompatible with
  this project's `Program.cs` (a complete real-Kestrel host) and failed with `InvalidCastException`
  (KestrelServerImpl→TestServer). The final fix is to **drop `WebApplicationFactory` and have the test
  build/start a `WebApplication` directly** (`IAsyncLifetime`). The trap-② 4b switch was **confirmed
  unnecessary** (passed with it commented out). Details in FIX-005.

## FIX-003 — Subscriber channel not completed on unsubscribe
- **Milestone/Step:** M1 / Step 3 → fixed in Step 3.1
- **Severity:** hygiene (not a live leak in M1)
- **Symptom:** On unsubscribe, `SubscribeAsync`'s `finally` removed the subscriber from the dictionary
  but never called `Writer.Complete()`, leaving the channel reader/writer not fully finalized.
- **Cause:** cleanup did removal only.
- **Fix:** in `finally`, remove from the dictionary first (publishers stop targeting it), then call
  `channel.Writer.Complete()` so the reader terminates deterministically. A publisher `TryWrite` on a
  completed channel safely returns false.
- **Future impact:** none beyond clean teardown; aligns with the DEC-006 disposal habit before M3/M4.

## FIX-004 — Line-ending normalization (.gitattributes) missing
- **Milestone/Step:** M1 / Step 3.1 (observed) → to apply in M1 Step 6. (Recurred in M1 Step 4 — confirms still unapplied.)
- **Severity:** hygiene (harmless warning; not a code defect)
- **Symptom:** `git add` on a source file warns "LF will be replaced by CRLF the next time Git touches
  it" on Windows.
- **Cause:** `.editorconfig` sets `eol=lf` (repo standard is LF), but there is no `.gitattributes`, so
  Git applies its OS-default autocrlf behavior on Windows and warns about the LF↔CRLF mismatch.
- **Fix (deferred to Step 6):** add a root `.gitattributes` with `* text=auto eol=lf` (mark binaries
  like `*.pfx`, `*.png` as `binary`). This makes Git store text as LF regardless of OS, aligns with
  `.editorconfig`, and silences the warning. A Phase 0 omission, folded into the M1 docs/config commit.
- **Future impact:** none beyond consistency; ensures CI (Ubuntu/LF) and local (Windows) agree on line
  endings, avoiding noisy diffs.

## FIX-005 — Integration-test host: WebApplicationFactory incompatible with a real Kestrel host
- **Milestone/Step:** M1 / Step 5 (the actual resolution of DEC-007 trap ①)
- **Severity:** real blocker (the test would not run at all) — but a **test-strategy correction**, not a
  production-code defect
- **Symptom:** Following the 07 Step 5 skeleton (`BrokerAppFactory : WebApplicationFactory<Program>` with
  `UseKestrel()` in `CreateHost`) made the integration test fail through a 4-stage cascade:
  1. `UriFormatException: The URI is empty.` — `WebApplicationFactory` is lazy, so when `Address` was read
     `CreateHost` had not run yet → `Address` was still its initial `""` → `GrpcChannel.ForAddress("")` threw.
  2. `InvalidCastException: KestrelServerImpl → TestServer` — waking the factory via `.Server` to populate
     `Address` hit `EnsureServer()`, which unconditionally casts to `TestServer`.
  3. Same `InvalidCastException` — waking via `.Services` instead of `.Server` failed identically, because
     the `Services` getter also routes through `EnsureServer()`.
- **Cause (root):** this project's `Program.cs` is **already a complete real-Kestrel host**
  (`ConfigureKestrel` + `ListenAnyIP(5050, Http2)`). `WebApplicationFactory<Program>` runs `Program`,
  intercepts the host builder, then expects to **overlay its in-memory `TestServer`**. With a real Kestrel
  already in place, the `EnsureServer→ConfigureHostBuilder` `TestServer` cast is guaranteed to break — so
  **every** public path that materializes the factory (`Server`/`Services`/`CreateClient`) fails at the
  same spot. Swapping to `UseKestrel()` in `CreateHost` does not resolve this premise conflict.
- **Fix:** **abandon `WebApplicationFactory`** and have the fixture build/start a `WebApplication` directly
  (`BrokerAppFactory : IAsyncLifetime`). The test reconstructs the same bootstrap as `Program.cs`
  (`AddGrpc` + `AddSingleton<ITopicStore, InMemoryTopicStore>` + `MapGrpcService<MessageBusService>`),
  calls `await _app.StartAsync()`, then reads the real address from `IServerAddressesFeature`. DEC-007's
  intent (in-process, real Kestrel port, genuine gRPC channel) is fully preserved.
- **Two additional binding traps (cascading in the same Step 5):**
  4. `ArgumentException: 0.0.0.0 ... cannot be used as a target address` — `ListenAnyIP(0)` yielded
     `http://0.0.0.0:{port}`. Fine for the server to listen on, but invalid as a **gRPC client target**.
  5. `InvalidOperationException: Dynamic port binding is not supported when binding to localhost` — the
     workaround `ListenLocalhost(0)` is rejected by Kestrel (localhost resolves to both IPv4/IPv6, so a
     port-0 bind is ambiguous).
  → Final: **`options.Listen(IPAddress.Loopback, 0, l => l.Protocols = HttpProtocols.Http2)`**. Dynamic
    port is allowed and the address comes back as a clean `http://127.0.0.1:{port}` the client connects to
    directly. (A defensive `0.0.0.0`/`[::]`→`127.0.0.1` replacement was also kept; it never fires with Loopback.)
- **Side fix:** assertion typo `got.Paylaod` → `got.Payload`.
- **Future impact:** this in-process hosting-fixture pattern becomes the starting point for M3 (ack/nack
  e2e), M4 (mTLS — separate, extend Loopback + certs), and M5 (load scenarios may move to a separate
  process) integration tests. `Microsoft.AspNetCore.Mvc.Testing` is now effectively unused (removal
  candidate — decide at the zoom-out review). Future milestone instructions should base their Step-5
  guidance on this direct-startup pattern rather than `WebApplicationFactory`.

## DEC-008 — Http2UnencryptedSupport switch unnecessary for plaintext h2c (.NET 8)
- **Milestone/Step:** M1 / Step 5 (resolves the 4b decision deferred from Step 4)
- **Type:** environment/config confirmation
- **Decision:** `AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true)`
  is **unnecessary**. Confirmed by passing the integration test **with the switch line commented out**.
- **Rationale:** in .NET 8, `GrpcChannel.ForAddress("http://...")` supports plaintext HTTP/2 (h2c) by
  default, so no switch is needed. The switch "turned on without verifying" in Step 4 (08 Step 4 finding)
  had no basis.
- **Future impact:** remove this switch line from the sample `Program.cs` files
  (`SamplePublisher`/`SampleSubscriber`) too (folded into the Step 6 cleanup). Moot once M4 moves to TLS,
  since h2c disappears.
