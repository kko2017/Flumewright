# Flumewright — Decision & Fix Log

> A lightweight record of mid-development fixes and small design decisions — the things too small for
> a full ADR but too valuable to lose. Each entry: **Symptom → Cause → Fix → Future impact**.
> "Future impact" is the point: it tells a later milestone (or a later you) what to revisit, saving
> rediscovery cost. For large architectural decisions, use ADRs in `docs/decisions/` instead.
>
> Repo location: `docs/decisions/decision-and-fix-log.md` (companion to the ADRs).

---

## Deferred Items Ledger

A single checkpoint for everything we consciously deferred, so "later" doesn't become "never". When a
milestone starts, scan this table for items tagged to it. Plans may change at that point — that is the
intent: each milestone boundary is a review moment, not a binding contract. The authoritative detail lives
in the referenced DEC/FIX entry or design note; this table is the index.

| Item | What | Deferred to | Status / revisit |
|------|------|-------------|------------------|
| DEC-001 | Streaming Publish (unary → client/bidi stream) | M5 | Open — revisit M5 |
| DEC-002 | `Acknowledge` RPC | M3 | **Closed — superseded.** ack = offset commit (M3a `CommitOffset`); nack = SDK pattern (publish to retry/dlq topic), no broker RPC. See M3b design note. |
| DEC-002 | `Admin` RPC (topic management) | M3 | Open — when topic management is actually needed (not M3a/b/c) |
| DEC-006 | Disposal: in-flight / redelivery timers & schedulers | M3 | **Deferred further → Phase 2.** M3b uses no-delay non-blocking retry (no timers). Applies when delayed backoff is built (see "delayed backoff" row). |
| DEC-006 | Disposal: `X509Certificate2` (secure connections) | M4 | Open — M4 (mTLS) |
| DEC-006 | Disposal: disk WAL `FileStream` / segment handles | Phase 2 | Open — Phase 2 (`DiskTopicStore` as `IAsyncDisposable`) |
| DEC-006 | Bound `_topics` dictionary growth (slow leak under many distinct topics) | M2+ | Open — revisit (no firm milestone) |
| DEC-004 | certs/keys + logs gitignore realization | M4 / M6 | Partial — M4 certgen → `certs/`; M6 logging → `logs/` |
| DEC-017 | Microsoft Coyote (systematic concurrency testing) 🔒 | after M3 | Open — after M3c (M3 adds consumer-group/offset-commit concurrency; Coyote lands naturally after) |
| FIX (M1) | Test enumerator `await using` (~7 tests) | — | Open — low priority; one commit when touched |
| M3b design | **Delayed / multi-stage retry backoff** (retry-1/2/3 with increasing delay) 🔒 | **Phase 2** | NEW — requires a delayed-redelivery scheduling mechanism (cannot sleep the consumer; due-time pause/resume). M3b opens the structure (`RetryPolicy` returns destination + delay); Phase 2 implements the delay. |
| M3b design | **Blocking (in-place) retry mode** 🔒 | **Phase 2** | NEW — shares the same delay mechanism as multi-stage backoff, so built together in Phase 2. M3b leaves it as an extension point only; non-blocking is the M3b default. Use case: transient downstream failures where preserving order is worth blocking. |

---

## Contents

### Decisions (DEC)
- [DEC-001 — M1 Publish is unary (streaming deferred to M5)](#dec-001--m1-publish-is-unary-streaming-deferred-to-m5)
- [DEC-002 — Ack/Admin RPCs deferred to M3](#dec-002--ackadmin-rpcs-deferred-to-m3)
- [DEC-003 — `global.json` rollForward = latestFeature (not latestMinor)](#dec-003--globaljson-rollforward--latestfeature-not-latestminor)
- [DEC-004 — `.gitignore` must block certs/keys and logs](#dec-004--gitignore-must-block-certskeys-and-logs)
- [DEC-005 — Dependency Injection via the built-in container, depend on interfaces](#dec-005--dependency-injection-via-the-built-in-container-depend-on-interfaces)
- [DEC-006 — Disposal roadmap (IDisposable / IAsyncDisposable)](#dec-006--disposal-roadmap-idisposable--iasyncdisposable)
- [DEC-007 — M1 integration test is in-process (real-port Kestrel)](#dec-007--m1-integration-test-is-in-process-real-port-kestrel)
- [DEC-008 — Http2UnencryptedSupport switch unnecessary for plaintext h2c (.NET 8)](#dec-008--http2unencryptedsupport-switch-unnecessary-for-plaintext-h2c-net-8)
- [DEC-009 — Development moved into a VS Code dev container (Linux)](#dec-009--development-moved-into-a-vs-code-dev-container-linux)
- [DEC-010 — Bind-mount UID mismatch breaks `obj/` writes (build) and `chmod` (hooks)](#dec-010--bind-mount-uid-mismatch-breaks-obj-writes-build-and-chmod-hooks)
- [DEC-011 — M1 end-of-milestone zoom-out review: outcome & dispositions](#dec-011--m1-end-of-milestone-zoom-out-review-outcome--dispositions)
- [DEC-012 — main branch protection enabled via GitHub ruleset](#dec-012--main-branch-protection-enabled-via-github-ruleset)
- [DEC-013 — Risk-based checkpoint verification (replaces per-step hand verification)](#dec-013--risk-based-checkpoint-verification-replaces-per-step-hand-verification)
- [DEC-014 — Agent harness: standing rules (GEMINI.md) + workspace skills, replacing per-prompt @-attachments](#dec-014--agent-harness-standing-rules-geminimd--workspace-skills-replacing-per-prompt--attachments)
- [DEC-015 — Delivery model confirmed: log/pull (Kafka-style), not push; M2 redefined](#dec-015--delivery-model-confirmed-logpull-kafka-style-not-push-m2-redefined)
- [DEC-016 — Tool Permission: strict → always-proceed (control re-placed, not relaxed)](#dec-016--tool-permission-strict--always-proceed-control-re-placed-not-relaxed)
- [DEC-017 — Planned CI/CD quality-gate hardening (reserved; execute after M2)](#dec-017--planned-cicd-quality-gate-hardening-reserved-execute-after-m2)
- [DEC-018 — M2 end-of-milestone zoom-out review: outcome & dispositions](#dec-018--m2-end-of-milestone-zoom-out-review-outcome--dispositions)
- [DEC-019 — CI/CD quality-gate hardening: executed (soft stage complete; hard pending)](#dec-019--cicd-quality-gate-hardening-executed-soft-stage-complete-hard-pending)
- [DEC-020 — Reviewer sub-agents: function-based and on-call, not domain-based and standing 🔒](#dec-020--reviewer-sub-agents-function-based-and-on-call-not-domain-based-and-standing-)
- [DEC-021 — Strengthen Roslyn analyzers to block FIX-010-class defects at build time 🔒](#dec-021--strengthen-roslyn-analyzers-to-block-fix-010-class-defects-at-build-time-)
- [DEC-022 — A dedicated Concurrency Strategy doc (11), 🔒 cross-reference markers, and a reminder rule 🔒](#dec-022--a-dedicated-concurrency-strategy-doc-11--cross-reference-markers-and-a-reminder-rule-)
- [DEC-023 — Offset commit semantics: committed = next offset to read (Kafka-style) 🔒](#dec-023--offset-commit-semantics-committed--next-offset-to-read-kafka-style-)

### Fixes (FIX)
- [FIX-001 — Fan-out broken: a shared per-topic channel delivered to only one subscriber](#fix-001--fan-out-broken-a-shared-per-topic-channel-delivered-to-only-one-subscriber)
- [FIX-002 — Publisher cancellation token misused on subscriber writes; blocking fan-out](#fix-002--publisher-cancellation-token-misused-on-subscriber-writes-blocking-fan-out)
- [FIX-003 — Subscriber channel not completed on unsubscribe](#fix-003--subscriber-channel-not-completed-on-unsubscribe)
- [FIX-004 — Line-ending normalization (.gitattributes) missing](#fix-004--line-ending-normalization-gitattributes-missing)
- [FIX-005 — Integration-test host: WebApplicationFactory incompatible with a real Kestrel host](#fix-005--integration-test-host-webapplicationfactory-incompatible-with-a-real-kestrel-host)
- [FIX-006 — CLI's broad `git add` staged unrelated changes into the wrong commits](#fix-006--clis-broad-git-add-staged-unrelated-changes-into-the-wrong-commits)
- [FIX-007 — pre-commit hook tracked as 100644: the local validation gate may never have run](#fix-007--pre-commit-hook-tracked-as-100644-the-local-validation-gate-may-never-have-run)
- [FIX-008 — Integration test could hang instead of failing on timeout 🔒](#fix-008--integration-test-could-hang-instead-of-failing-on-timeout-)
- [FIX-009 — Checkpoint A caught a LATEST-semantics bug in the channel store (became the trigger for DEC-015) 🔒](#fix-009--checkpoint-a-caught-a-latest-semantics-bug-in-the-channel-store-became-the-trigger-for-dec-015-)
- [FIX-010 — Empty `catch (Exception)` in SubscribeAsync silently swallowed partition-reader faults 🔒](#fix-010--empty-catch-exception-in-subscribeasync-silently-swallowed-partition-reader-faults-)
- [FIX-011 — Offset commit silently accepted unknown topics and out-of-range partitions 🔒](#fix-011--offset-commit-silently-accepted-unknown-topics-and-out-of-range-partitions-)
- [FIX-012 — Concurrency tests that looked green but verified nothing (fake-green) 🔒](#fix-012--concurrency-tests-that-looked-green-but-verified-nothing-fake-green-)
- [FIX-013 — Step 3 duplicated fan-in + async LATEST resolution caused a test hang; fixed by unifying fan-in and resolving at entry 🔒](#fix-013--step-3-duplicated-fan-in--async-latest-resolution-caused-a-test-hang-fixed-by-unifying-fan-in-and-resolving-at-entry-)

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
- **Update (M3a/M3b):** the `Acknowledge` half is now **superseded** — ack is expressed as offset commit
  (M3a's `CommitOffset`, per DEC-023), and nack is an SDK-side pattern (publish to a retry/dlq topic), not a
  broker RPC, per the M3b design (Kafka-style: the broker has no ack/nack RPC). No `Acknowledge` RPC will be
  added. `Admin` remains open (deferred until topic management is needed; not part of M3a/b/c). See the
  Deferred Items Ledger.

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

## FIX-006 — CLI's broad `git add` staged unrelated changes into the wrong commits
- **Milestone/Step:** M1 / Step 6 (recurred 3× during the milestone-wrap docs commits → all corrected)
- **Severity:** hygiene / history integrity (not a code defect — but it damages history trust)
- **Symptom:** During Step 6 the CLI staged too broadly (`git add .` / `--renormalize .`), producing 3
  commits whose **message and actual content disagreed**:
  1. The `chore: renormalize line endings` commit also pulled in sample SDK implementation code
     (`samples/*/Program.cs`) and README changes — a "line-endings-only" commit carrying new code
     (diff was +49/-6, a net addition).
  2. That sample code committed the `Http2UnencryptedSupport` switch **still turned on**, the very thing
     Step 5 decided to remove (a DEC-008 violation).
  3. The Step 5 integration-test artifacts (`BrokerAppFactory.cs`, `PublishSubscribeE2ETests.cs`) landed
     in the `chore: add .gitattributes` commit instead of a dedicated `test(integration):` commit — so
     **M1's completion evidence was hidden inside a chore commit**.
- **Cause:** the CLI used broad staging instead of per-file `git add <path>`, indiscriminately including
  other uncommitted working-tree changes (sample impl, integration test, README). Violates "one commit =
  one validated small change" (03).
- **Fix:** all were pre-push (local only), so safe to restructure. `git reset --mixed HEAD~N` to unwind the
  commits, then per-file `git add` to recommit by meaning: ① remove the sample switch, then `feat(samples):`
  ② integration test as `test(integration):` ③ `.gitattributes` as a standalone `chore:`. Final order
  (bottom→top): `chore(.gitattributes)` → `test(integration)` → `feat(samples)` → `docs(sync)` →
  `docs(README)`. After each commit, `git show --stat HEAD` to verify only the intended files were included.
- **Future impact:** ⚠️ **operating-rule reinforcement** — CLI commit-step instructions must always include
  (a) **explicit per-path `git add <path>` only** (no broad `git add .`), and (b) **user verifies the file
  list via `git show --stat HEAD` immediately after each commit**. From M2 on, embed these two guards as
  standard instruction boilerplate (also reflected in FW_Context §10 operating rules). This incident is the
  git-flavored case of "a CLI report of 'done' is not a conclusion" (08 principle).

## FIX-007 — pre-commit hook tracked as 100644: the local validation gate may never have run
- **Milestone/Step:** discovered during dev-container setup (post-M1, pre-merge) → fixed immediately
- **Severity:** real process defect (the first validation gate was effectively disabled) — not a code bug
- **Symptom:** In the Linux dev container, `git ls-files -s .githooks/pre-commit` showed mode **100644**
  (non-executable). Git does not run a hook file without the executable bit, so the pre-commit
  build+unit-test gate (03 §5.1) could have been silently skipped on commits — even though
  `core.hooksPath` was correctly set to `.githooks`.
- **Cause:** Phase 0 created the hook and 03 §5.1 calls for `chmod +x`, but the executable bit was never
  recorded in the **git index** — likely because the original host was Windows (where the filesystem
  exec bit is weak / not represented), so the bit applied locally at best and never persisted in the repo.
  A plain `chmod +x` inside the container also failed (`Operation not permitted`) due to bind-mount file
  ownership (see DEC-010), so the fix had to go through git, not the filesystem.
- **Fix:** `git update-index --chmod=+x .githooks/pre-commit` (sets the index mode to **100755**
  independent of filesystem ownership), then commit. From now the exec bit travels with the repo, so any
  clone on any OS restores it — and the `chmod` line becomes unnecessary in container setup.
- **Future impact:** the local gate is now actually enforced. Worth re-confirming the hook *fires* (make a
  trivial change and commit; the build/test log should appear). A reminder that "a gate that is configured"
  ≠ "a gate that runs" — the 08-style verify-don't-assume principle applied to tooling.

## DEC-009 — Development moved into a VS Code dev container (Linux)
- **Milestone/Step:** post-M1 (before the zoom-out review / merge) — tooling/environment decision
- **Type:** environment decision
- **Decision:** Develop inside a VS Code **dev container** (`.devcontainer/devcontainer.json`, committed to
  the repo) based on `mcr.microsoft.com/devcontainers/dotnet:8.0`, running as non-root `vscode` with a
  hardened profile (`--cap-drop=ALL` + `SYS_PTRACE`, `seccomp=unconfined`). The Antigravity CLI runs
  **inside** this container.
- **Rationale:** (a) reproducibility — a clone reproduces the exact .NET 8 toolchain; (b) **local == CI**
  — CI already runs on `ubuntu-latest`, so a Linux container removes "works on my machine" drift; (c) the
  hardened, non-root profile fits the project's "AI agent under control" stance (limits what a misbehaving
  agent can touch). The committed config is itself a portfolio artifact (reproducible environment).
- **Cross-platform safety (the "does the project still work the same on Linux?" check):** the project was
  already Linux-friendly — .NET 8 is cross-platform, all build/test is `dotnet` CLI, CI was already Ubuntu,
  and the Step 5 integration test binds loopback dynamic ports inside the process (no host port forwarding
  needed). Verified empirically: `dotnet build` clean + the M1 unit tests pass inside the container
  (recorded in 08). SDK was `8.0.421`, satisfying `global.json` `latestFeature`.
- **Setup pitfalls encountered & resolved (so the committed config is correct):**
  - `sudo apt-get …` fails under `--cap-drop=ALL` (`sudo: unable to change to root gid: Operation not
    permitted`). → removed from `postCreateCommand`; `sqlite3` is unneeded until Phase 2 (disk persistence),
    and if needed later it should be installed via a dev-container *feature* (build phase, before cap-drop),
    not `sudo`.
  - The Antigravity CLI is **not an npm package** — `npm install -g @google/antigravity-cli` returns
    **E404** (no such package). The official install is a script:
    `curl -fsSL https://antigravity.google/cli/install.sh | bash`. (Third-party npm packages named
    "antigravity-cli" are unofficial and were avoided.) Made the install step non-fatal (`|| echo WARN…`)
    so a network/egress hiccup doesn't break the whole container build.
  - `chmod` on hook files fails (ownership) → handled via git, see FIX-007.
  - Cleared the stale `5001/https` port mapping — this project's broker is plaintext **h2c on 5050**, and
    TLS only arrives in M4; the integration test needs no forwarded port at all.
- **Auth note:** the CLI authenticates via the system keyring and falls back to Google sign-in; the
  container keyring is separate from the host, so **a fresh login is required inside the container** (and
  after any rebuild). It detects remote/SSH-style sessions and prints an auth URL to open on the host, so
  no callback port forwarding is needed.
- **Future impact:** the final `postCreateCommand` = register `.githooks` hook path → `dotnet restore` →
  install Antigravity CLI (non-fatal). M2 onward runs in this container. Rebuilding the container is also
  the real test that the committed config bootstraps cleanly from scratch.

## DEC-010 — Bind-mount UID mismatch breaks `obj/` writes (build) and `chmod` (hooks)
- **Milestone/Step:** post-M1 (dev-container setup) — environment note
- **Type:** environment/tooling note
- **Symptom:** Inside the container, `dotnet build` failed for the two most-edited projects with
  `MSB3374: … '…/obj/.../*.Up2Date' … Access … is denied`, and `chmod` on hook files returned
  `Operation not permitted`.
- **Cause:** the workspace is a **host bind mount**; the mounted files are owned by UID 0 (root) with 777
  perms (a Windows-host mount signature), while the container user is `vscode` (UID 1000). MSBuild's
  incremental `*.Up2Date` markers, left over from a prior host build, were root-owned, so `vscode` could
  not update their timestamps; and `vscode` cannot `chmod` files it doesn't own.
- **Fix:** delete stale build output (`find . -type d \( -name bin -o -name obj \) | xargs rm -rf`) so the
  container regenerates `bin/`/`obj/` under `vscode` ownership — build + unit tests then pass. The exec-bit
  problem is solved via git, not chmod (FIX-007).
- **Future impact:** **build only inside the container** from now on (a host-side `dotnet build` would
  re-introduce foreign-UID `obj/`). If this recurs or becomes annoying, the clean structural fix is to use
  *Clone Repository in Container Volume* (a named volume checked out as `vscode`), which eliminates the
  bind-mount ownership class of problems entirely. Not a code defect; an environment artifact.

## FIX-008 — Integration test could hang instead of failing on timeout 🔒
- **Milestone/Step:** M1 zoom-out review (post-M1, before merge) → fixed
- **Severity:** test robustness defect (never fires on the happy path; only when the e2e test is already
  failing) — not a production-code bug
- **Symptom:** In `PublishSubscribeE2ETests`, if the message never flows within the 10s timeout, the retry
  loop exits on `cts.IsCancellationRequested`, but the next line `await received.Task` then awaits a
  `TaskCompletionSource` that is never completed in the failure path → the test **hangs** instead of failing
  cleanly. In CI this stalls the job until the outer timeout rather than reporting a clean failure.
- **Cause:** the completion source is only set on success (`received.TrySetResult` inside the subscribe
  pump); the failure/timeout branch has no path that completes or cancels `received.Task` before it is awaited.
- **Fix:** `var got = await received.Task.WaitAsync(cts.Token);` — on timeout this throws
  `OperationCanceledException`, turning a hang into a clean, fast failure. One-line change, behavior of the
  passing path unchanged.
- **Future impact:** none for production. Carry the same `.WaitAsync(token)` habit into M3/M4 integration
  tests (ack/nack, mTLS) where waits on completion sources recur. Found by the zoom-out review (the only
  real defect of M1's review).

## DEC-011 — M1 end-of-milestone zoom-out review: outcome & dispositions
- **Milestone/Step:** M1 zoom-out review (03 §7.5), run via the Antigravity CLI inside the dev container
- **Type:** review record (one of 09's input sources, per 03 §7.5)
- **Context:** CLI produced a clean three-bucket report (report-only, no edits — protocol respected).
  Findings were assessed by the user (+ design review); dispositions below.
- **Fixed now (approved):**
  - `[correctness/bug]` integration-test hang → **FIX-008** (`.WaitAsync(cts.Token)`).
  - `[consistency/cleanup]` unused `Microsoft.AspNetCore.Mvc.Testing` → **removed** (closes the FIX-005
    "decide at zoom-out" candidate; confirmed unreferenced after the direct-`WebApplication` hosting change).
  - `[consistency/cleanup]` Korean comments still present in `PublishSubscribeE2ETests.cs` and
    `BrokerAppFactory.cs` → **translated to English** (repo English-only policy, 05).
- **Recorded only, not fixed now:**
  - Unit tests retrieve `GetAsyncEnumerator()` without `await using`, so `SubscribeAsync`'s `finally`
    (subscriber removal + channel Complete, FIX-003) doesn't run at test end. The CLI classified this as
    `[correctness/bug]`; **reclassified to `[consistency/cleanup]`** — each test uses a fresh
    `InMemoryTopicStore` that is GC'd at method end, so it is test-local untidiness, not a process leak, and
    production code is unaffected. Deferred (would touch ~7 tests; the "fix only 1–2" rule applies). If
    addressed later: one commit, `await using var enumerator = …`, plus add `using` to the two bare
    `CancellationTokenSource`s in `Unsubscribe_CleansUpAndOthersStillWork`.
  - `IntegrationTests.csproj` package versions are uneven (`Microsoft.NET.Test.Sdk` 17.8.0 is old);
    **note only**, out of M1 scope. Separately, **FluentAssertions 8.x moved to a commercial license** —
    flag for license review given the portfolio/open-source intent (assess before any public release).
- **Investigation resolved → all three KEPT (the CLI's "delete" suggestions were false positives):**
  - `src/Flumewright.Observability/Class1.cs` and `src/Flumewright.Security/Class1.cs` — each is the sole
    file in its project, an intentional staged skeleton (M6 observability / M4 mTLS), and is **referenced by
    Broker/Client**. Deleting would break the build. Kept.
  - `tests/Flumewright.LoadTests/UnitTest1.cs` — the **deliberate placeholder test** (Phase 0 rule: an empty
    test project keeps a placeholder so `dotnet test --filter Category=Load` doesn't error on zero tests).
    Kept. The CLI's original `[consistency/cleanup] → delete` was wrong; verifying contents/role first (per
    this entry) prevented a build/test break — another instance of "a CLI report is a starting point, not a
    conclusion" (08).
- **Correctly out-of-scope (guardrail worked):** Broker/Client references to the empty `Flumewright.Security`
  / `Flumewright.Observability` projects were classified `[out-of-scope — record only]` (M4 mTLS / M6
  observability) — the CLI did NOT misflag these intentionally-staged skeletons as defects.
- **Process note:** the report-only protocol + the 09 "intentionally-deferred" guardrail both held — the CLI
  proposed no M2+ work and re-flagged none of the deferred items. The CLI *over-escalated* one severity
  (enumerator dispose → "bug"), which the human review corrected — exactly the 08 principle that a CLI report
  is a starting point, not a conclusion.

## DEC-012 — main branch protection enabled via GitHub ruleset
- **Milestone/Step:** M1 close (the merge step) — fulfills 03 §5.4, which had not yet been applied
- **Type:** process/infrastructure decision
- **Decision:** Protect `main` with a GitHub **ruleset** (the modern replacement for classic branch
  protection), Enforcement = **Active**, empty bypass list (the rule applies to the owner too — no
  self-exemption). Rules: **require status check `build-and-test`** to pass before merging; require a pull
  request before merging; **block force pushes**; **restrict deletions**. "Require branches up to date" =
  off (solo; avoids needless friction). "Do not require status checks on creation" = on (so tag/branch
  creation — e.g. the future `v0.1.0` push that triggers `release.yml` — isn't blocked by having no check).
- **Rationale:** makes the CI gate structurally unbypassable (03 §5 "a human forgetting cannot bypass it").
  An empty bypass list is deliberate — self-exemption would defeat the point and contradict the
  "agent/owner under control" stance. Block-force-push protects the auditable history that the whole
  commit-discipline rests on (note: history rewrites like FIX-006's reset are safe only pre-push/local; on
  shared `main` they're forbidden — this rule enforces that boundary).
- **Sequencing note:** the required check `build-and-test` only appears in the ruleset picker after CI has
  run at least once, so the order is: push → PR → CI runs once → add the check to the ruleset.
- **Future impact:** every M2+ merge to main now requires green CI. The `release.yml` tag-push flow
  (Phase 1 end) still works because of the on-creation exemption.

## M1 — milestone closed
- **Milestone/Step:** M1 (Phase 1, milestone 1 of 6)
- **What:** all M1 wrap-up steps complete — zoom-out review (DEC-011) + approved fixes (FIX-008, Mvc.Testing
  removal, Korean-comment translation), docs/ sync, pre-commit exec-bit fix (FIX-007, committed within
  `chore(devcontainer)` 8148757), dev container adoption (DEC-009/010), and main branch protection
  (DEC-012). Local `dotnet build -c Release` + `dotnet test --filter "Category!=Load"` green; PR merged to
  main via a **merge commit** (not squash — preserves the per-unit commit history that 03 §1 depends on).
- **NOT done (intentionally):** no `v0.1.0` tag — that is the **Phase 1** release marker (after M6), not a
  per-milestone action. M1 is one of six milestones in Phase 1.
- **Next:** M2 — topic/partition + per-partition append-only log + offset-based consumption + routing (01 roadmap §14
  / §5 component "Router/Partitioner" + "Topic/Partition Store"). New branch `feat/m2-partitioning`, new
  step-by-step milestone instruction (personal, not committed).

## DEC-013 — Risk-based checkpoint verification (replaces per-step hand verification)
- **Milestone/Step:** workflow decision, before M2 (applies M2 onward)
- **Type:** process decision
- **Decision:** Stop hand-verifying every step. Group steps into a few **checkpoints** placed **by risk**,
  not by step count. The CLI runs up to a checkpoint, STOPS, and self-reports; the human scans the report
  and spot-checks only the high-risk steps. Full rule + risk taxonomy in 03 §7.6.
- **Risk taxonomy (must-verify):** ① concurrency/shared-state logic ② public-contract changes
  (proto/interfaces) ③ milestone completion-bar (integration test) ④ security boundaries (certs/mTLS).
  Low-risk (groupable): scaffolding, pure test-covered functions, docs/config.
- **Rationale:** per-step verification doesn't scale across Phase × M × Step and over-spends on low-risk
  steps; "run to the end then check" is unsafe because a wrong early step compounds (M1 FIX-001/002 were
  caught exactly at the introducing step). Risk-placed checkpoints keep the human as approver while removing
  the per-step bottleneck.
- **Safeguards (so grouping doesn't let bad work pile up):** each step is still its own commit (build+test
  green) — grouped execution ≠ grouped commit; CLI self-reports per step at the checkpoint; CLI stops
  immediately on any build/test failure or any unplanned decision; high-risk steps carry a self-check list
  in the instruction/skill.
- **Future impact:** every milestone instruction (`NN-phaseX-mN-*`) marks where checkpoints fall; density
  scales with risk (M2 concurrency = tighter; M6 wiring = looser). Pairs with §7.5 zoom-out review (during
  vs end). To be reflected as skills (`checkpoint-review`, `zoomout-review`) once the SKILL.md format is verified.

## DEC-014 — Agent harness: standing rules (GEMINI.md) + workspace skills, replacing per-prompt @-attachments
- **Milestone/Step:** workflow/tooling decision, after M1 close, before M2 (applies M2 onward)
- **Type:** process/tooling decision
- **Decision:** Stop re-attaching the same docs with `@` every prompt. Instead encode the workflow as a
  file-based harness in the workspace (committed):
  - **`GEMINI.md`** (repo root) — always-on agent rules, auto-loaded each session: git boundary
    (local-only, per-file add, `git show --stat` after each commit), work rhythm (one step = one commit,
    stop on failure/unplanned decision), scope discipline, verification philosophy, environment (English,
    dev-container builds), and **pointers** to docs/ (not duplicated rules).
  - **Three workspace skills** under `.agents/skills/{name}/SKILL.md` — `docs-sync`, `zoomout-review`,
    `checkpoint-review`. Each is on-demand (loaded only when relevant) and is procedure + pointer to the
    authoritative doc (03 §7.5/§7.6), never a copy of the rules.
- **Division of labor:** standing rules → GEMINI.md (always applied); task procedures → skills (invoked
  when relevant). Single source of truth stays in docs/ (03/09); GEMINI.md and skills are summaries +
  pointers. This avoids the duplicate-content drift that caused an earlier docs-sync to silently delete a
  whole section (see the 02 study-notes incident) — rules live in one place.
- **CLI mechanics (verified on Antigravity CLI 1.0.8, in the dev container):**
  - `GEMINI.md` at the workspace root IS auto-loaded (confirmed by a marker test). `--help` exposes no
    memory/context/rules subcommands, but loading works regardless. AGENTS.md native support is a later
    version (≈v1.20.x); on 1.0.8, **GEMINI.md is the reliable workspace context file** (global
    `~/.gemini/GEMINI.md` is avoided — it conflicts with Gemini CLI).
  - **Both skills and GEMINI.md are scanned at session start.** After adding/editing either, **restart
    `agy`** (a 0-skills / stale-rules result almost always means "not restarted").
  - Skill folder path: `.agents/skills/{name}/SKILL.md`; `name` must equal the folder name; single-line
    `description` is the safe frontmatter form.
  - Settings sanity (`/config`): Tool Permission = request-review, Non-Workspace Access = off — control
    posture intact. Do NOT use `--dangerously-skip-permissions`.
- **Rationale:** less token waste and no more forgetting a guardrail attachment (e.g. the 09 deferred-items
  list for a zoom-out). The harness keeps the human as approver and the control mechanisms intact — it
  automates *delivery* of the rules, not the loosening of them.
- **Future impact:** M2 is the first real test of the harness (does GEMINI.md hold the rules; do
  checkpoint-review / zoomout-review actually fire). Committed under `chore/add-skills` (skills + GEMINI.md)
  and recorded here. Skills/rules evolve as the workflow does; keep them as pointers so doc changes don't
  strand a stale copy.

## DEC-015 — Delivery model confirmed: log/pull (Kafka-style), not push; M2 redefined
- **Milestone/Step:** M2, mid-flight (during CHECKPOINT A review). Applies M2 onward; touches M1's store mechanism.
- **Type:** architecture decision (foundational)
- **Context — how this surfaced:** during the CHECKPOINT A review of the partitioned store, a question about
  buffer-full policy (DropWrite vs DropOldest, and what "LATEST" means) exposed a deeper issue: the project
  had been mixing two delivery models. The plan named Kafka as the primary model (partitioned **log**, offset,
  replay), but M1/M2 were implemented **push-style** (publish writes into per-subscriber channels; no
  retention; late subscribers get nothing). The "buffer full → drop" debate is itself a symptom of the push
  model — a log model doesn't have that problem.
- **Clarification reached:** Pub/Sub is a *pattern* (decoupled publishers/subscribers, topic-based fan-out).
  Both Kafka and Google Pub/Sub implement that pattern; they differ in *how messages are retained and
  delivered* (Kafka = append-only log + consumer pull by offset; Google Pub/Sub-style = push + per-subscriber
  buffer). These are different mechanics and don't cleanly combine in one store. "Kafka also fans out" is
  correct — it fans out by letting many consumers read the same log at their own offsets.
- **Decision:** build the broker on the **log/pull model** (Kafka-style):
  - Publish **appends** to a per-partition **append-only log** (in-memory, Phase 1).
  - Subscribers **pull** by holding their own **offset (cursor)**; they read records in order from the log.
  - Fan-out = many subscribers reading the same partition log at their own offsets.
  - No per-subscriber bounded channel, no drop policy, no buffer backpressure — those were push-model artifacts
    and are removed. (The earlier FullMode Wait/DropWrite/DropOldest question is therefore moot.)
  - offset and partition now carry real meaning (log position, replay). at-least-once follows from **offset
    commit** in M3 (the Kafka form of ack) — so the at-least-once / ack work studied in M1 is NOT discarded;
    it re-emerges as offset commit. Exactly-once stays out of scope; consumer-side idempotency stays the user's
    responsibility.
- **Retention scope:** Phase 1 keeps the in-memory log for the **process lifetime** (no eviction policy yet).
  Retention/eviction (time/size) and disk persistence (WAL + segments) are **Phase 2**. Replay (offset-based
  re-read) is naturally enabled by the log but the start-position API is deferred (proto already reserves
  `StartPosition`).
- **Handling the in-flight M2 work (option B):** stay on `feat/m2-partitioning`. **Keep Step 1 (proto fields)
  and Step 2 (PartitionRouter)** — both are valid in the log model (routing decides which log to append to).
  **Replace Step 3** (the channel-based store) with a log-based store in a new commit
  (`refactor(broker-core): replace channel-based store with append-only log model`). The transition is kept in
  history on purpose — together with this DEC it documents a controlled course-correction, not a cover-up.
- **Rationale:** the project's motive is a usable Pub/Sub bus; the log model makes offsets/partitions
  meaningful, removes the incoherent push/pull blend, and is the more instructive build. The cost is
  re-doing the store's storage/delivery mechanism (M1's channel fan-out included), but partition/offset/proto/
  router/gRPC-contract/tests-skeleton/harness all carry over.
- **Docs impact:** 01 plan §3/§4/§5/§6/§10/§14 updated; M2 redefined (v0.7). 02 study-notes to add push-vs-pull
  + log-as-cursor + at-least-once↔offset-commit. 10 M2 instruction to be rewritten for the log model. Résumé
  wording shifts from "Kafka + Pub/Sub blend" toward "log-based streaming bus (Kafka-style)".

## DEC-016 — Tool Permission: strict → always-proceed (control re-placed, not relaxed)
- **Milestone/Step:** M2 (before Step 3 implementation). Tooling/workflow decision.
- **Type:** process/tooling decision
- **Decision:** Switch the Antigravity CLI `Tool Permission` from `strict` (approve every tool call) to
  `always-proceed` (the CLI runs tools without a per-call prompt). The menu offered only these two levels —
  there is no "auto-approve reads only" middle option — so per-call approval of even read-only actions
  (ListDir, Read, grep) was pure friction with little control value.
- **Why this is NOT a loss of control:** per-call approval is only one of several control layers, and the
  highest-friction one. The real safety net stays intact:
  - **git remote is CLI-forbidden** (GEMINI.md: local git only; never push/pull/fetch/remote/gh). The user
    does all remote ops; push also needs the user's credentials. So the CLI cannot ship anything outward.
  - **main branch protection** (DEC-012): PR + green CI required; the CLI cannot touch main directly.
  - **per-step commits + `git show --stat` after each**: every change is visible and individually revertible.
  - **checkpoint verification** (DEC-013): the CLI stops at risk-placed checkpoints and self-reports; the
    human reviews the high-risk steps. **This is the true control point.**
  - **dev container isolation** + **pre-commit gate** (build + fast tests must pass to commit).
  - So control moves from "approve every tool call" to "review at checkpoints" — a **re-placement, not a
    relaxation**.
- **Distinct from `--dangerously-skip-permissions`:** that CLI flag bypasses all permission checks and is
  still forbidden (GEMINI.md). `always-proceed` is a setting-level permission level; other settings and the
  GEMINI.md rules still apply. The git-boundary and checkpoint rules are unchanged.
- **Guardrail that must NOT be relaxed alongside this:** checkpoint verification (DEC-013). Tool prompts are
  gone, but the CLI must still STOP at each checkpoint, self-report, and wait for human review before
  proceeding. If that ever slips, this decision should be revisited.
- **Sandbox note:** `proceed-in-sandbox` was not usable — it only auto-proceeds when Sandbox Mode is ON, and
  enabling the CLI sandbox on top of the dev container risks breaking builds/tests (esp. the Kestrel real-port
  integration test). So Sandbox stays OFF; `always-proceed` is the chosen path instead.
- **Rationale:** the per-call friction outweighed its marginal control value given the other layers. This is a
  deliberate friction/control trade-off, recorded so the reasoning is auditable.

## FIX-009 — Checkpoint A caught a LATEST-semantics bug in the channel store (became the trigger for DEC-015) 🔒
- **Milestone/Step:** M2 Step 3 (original channel-based version), found at CHECKPOINT A.
- **Type:** `[correctness/bug]` — caught by checkpoint review, not by passing tests.
- **What:** the first channel-based Step 3 store built one bounded channel per partition and drained them into
  a single **unbounded** merged channel via background tasks. Because the merge channel was unbounded, the
  intended bounded/LATEST drop semantics never applied — a slow subscriber would buffer without bound. All
  unit tests passed; the defect surfaced only on human review at the checkpoint.
- **Significance:** this is the **first correctness bug caught by the risk-based checkpoint model (DEC-013)** —
  evidence the checkpoint is doing its job (a passing test suite was not sufficient). The follow-up debate over
  the buffer-full policy (DropWrite vs DropOldest, the meaning of "LATEST") then exposed that the project was
  mixing push and log delivery models, which led directly to **DEC-015** (confirm the log/pull model).
- **Resolution:** superseded by the log model — the channel store (and the whole drop-policy question) was
  replaced by the per-partition append-only log in the new Step 3 (commits 35870fe, 0da4516). In the log model
  a slow subscriber simply lags by offset; there is no buffer to overflow, so the bug class no longer exists.
- **Lesson:** "a passing test/success report is a starting point, not a conclusion" (08 principle) held up —
  code review at the checkpoint caught what green tests missed.

## DEC-017 — Planned CI/CD quality-gate hardening (reserved; execute after M2)
- **Milestone/Step:** reserved during M2; **to be executed in the infrastructure interval after M2 merges,
  before M3** (the same "separate infra work from a code milestone" pattern used between M1 and M2). Coyote is
  deferred further, to **after M3**.
- **Type:** process/tooling decision (reservation — not yet implemented)
- **Motivation:** "build + unit tests pass" is a weak quality gate. The repo is **public**, and the project
  doubles as a portfolio piece, so visible, industry-standard quality signals matter. Goal = overall quality,
  not just one metric.
- **Planned additions (after M2):**
  - **Coverage — Coverlet** (`coverlet.collector`, `dotnet test --collect:"XPlat Code Coverage"`, Cobertura
    output). Gate strategy: **start the threshold BELOW the current measured number and raise it gradually** —
    coverage is a tool to surface untested code, not a score to chase. **Exclude** generated proto code,
    `Program.cs` (host bootstrap), and the Observability/Security skeletons from the denominator, or the % is
    unfairly low. Note: `Directory.Build.props` already enables analyzers + warnings-as-errors, so some smell
    detection is already in place.
  - **SonarCloud — primary quality gate** (public repo = free). Overall dashboard: coverage, code smells,
    duplication, vulnerabilities, maintainability + a README **badge** (strong portfolio signal). This is the
    main gate.
  - **CodeQL — security SAST layer** (public repo = free, native GitHub Actions, results in the Security tab).
    Complements SonarCloud (deep security vs overall quality); the two don't overlap.
  - **Dependabot:** turn on (free dependency-vulnerability PRs), but **do NOT feature it as a selling point** —
    it's a background bot, not a CI-pipeline step, so its portfolio value is low. Security hygiene only.
- **Concurrency analysis — explicitly scoped:** concurrency bugs (e.g. the TCS wakeup class) are NOT caught by
  static analysis or by API-functional tools. They are addressed by **checkpoint code review + concurrency
  unit tests (e.g. 1000 concurrent appends) + load/stress tests (load.yml, indirect exposure)**, all already
  planned/in place. **Microsoft Coyote** (systematic concurrency-testing framework that can deterministically
  find races/deadlocks) is the real tool for direct detection and has **high portfolio value**, but carries a
  learning curve → **adopt after M3** (M3 adds consumer-group/offset-commit concurrency, so Coyote lands
  naturally there). **Newman/Postman is rejected** — it is REST/HTTP-oriented (we are gRPC + protobuf) and is
  an API-functional runner, not a concurrency analyzer; it does not fit either of our needs.
- **Deliverable:** a dedicated CI/CD doc (`docs/guides/ci-cd-and-quality-gates.md`) capturing the pipeline,
  the gates, the coverage strategy, and the concurrency-testing approach. Work happens on a separate branch
  (`chore/ci-quality-gates`), kept apart from code milestones.

## DEC-018 — M2 end-of-milestone zoom-out review: outcome & dispositions
- **Milestone/Step:** M2 Step 6 part (b), before merge to main. Same protocol as DEC-011 (M1 zoom-out):
  scope-fenced to M2, report-only, three-bucket classification, deferred items guarded by this log.
- **Type:** review outcome / dispositions
- **Scope:** partitioning + append-only log + offset-based consumption. Deferred items (consumer groups,
  offset commit/ack/DLQ, retention/eviction, seek/replay API, streaming publish) were explicitly excluded
  per the design note `docs/design/m2-partitioning.md` and were NOT treated as defects.
- **[correctness/bug]: none.** Concurrency primitives (per-partition lock, Interlocked round-robin, the TCS
  re-check-under-lock notify-all) and the gRPC cancellation path reviewed clean; the 25-test suite plus
  Checkpoints A/B already exercised the guarantees.
- **[consistency/cleanup]: 2 found, both fixed (approved):**
  - **A — dead `channelCapacity` constructor parameter** (push-model remnant, unused after the log
    migration). Removed: constructor `(int defaultPartitionCount)`, parameterless chain `this(4)`, and all
    14 unit-test call sites updated (test logic unchanged). Commit `7df5563`
    (`refactor(broker-core): drop dead channelCapacity constructor param`).
  - **B — no hash-distribution uniformity test** (same-key determinism was tested; even spread was not).
    Added `ForKey_UniformDistribution_WithinGenerousVariance` — 1000 distinct keys over 4 partitions, each
    partition's share asserted within a **generous** band (150–350, expected ~250). Commit `670b020`
    (`test(router): assert hash distribution stays reasonably uniform`).
- **Lesson recorded (test design):** B surfaced a broader principle now written up in study-notes §11.65 —
  **deterministic properties get tight assertions, probabilistic ones get generous ranges.** Asserting an
  exact share (e.g. 245–255) on a statistical distribution would turn normal variance into a **flaky** test,
  and a flaky test's fault lies in the test, not the code. Flaky tests erode CI trust (red stops meaning
  "bug"), so they are worse than no test. The generous band is the *accurate* expression of what the hash
  guarantees ("reasonably even"), not a loose one. (Same idea on the timing axis: FIX-008's bounded timeout.)
- **[out-of-scope — record only]:** consumer groups/assignment (M3), offset commit/ack/DLQ (M3),
  retention/eviction + disk persistence (Phase 2), seek/replay API over gRPC (Phase 2; retained-read is
  validated at the store level for now), unary publish (M5). All already tracked; no new deferral introduced.
- **Disposition:** fixes A and B approved and committed on `feat/m2-partitioning`. Next: docs sync (study-notes
  §11.65 + this entry), then user merges M2 to main via a merge commit. **No tag** (v0.1.0 = Phase 1 / M6).

## FIX-010 — Empty `catch (Exception)` in SubscribeAsync silently swallowed partition-reader faults 🔒
- **Milestone/Step:** found during the CI/CD hardening interval (after M2 merge), by the newly
  introduced **SonarCloud** analysis (rules S2486 "handle the exception" / S108 "empty block").
- **Type:** `[correctness/quality]` — caught by static analysis, NOT by tests or human review.
- **What:** `InMemoryTopicStore.SubscribeAsync` spawns one background `Task.Run` per partition that
  reads the log and writes into the fan-in channel. That task body had two empty catches:
  `catch (OperationCanceledException) { }` (legitimate — cancellation is normal shutdown) **and**
  `catch (Exception) { }`. The second one swallowed **every** exception: if a partition reader faulted
  (e.g. a real bug threw mid-read), the exception vanished, that reader died silently, and the
  subscriber simply never saw those partition's messages — with no error, no log, nothing to trace.
- **Significance:** this directly contradicts the project's "don't hide bugs / a green run is a
  starting point, not a conclusion" principle. It is also notable that **M2's checkpoint reviews and
  the end-of-milestone zoom-out (DEC-018) both missed it** — it took a static analyzer to surface it.
  First concrete payoff of adopting SonarCloud: it caught a real quality defect that human review and a
  passing 25-test suite had not.
- **Resolution (branch `fix/subscribe-swallowed-exceptions`):**
  - Removed the `catch (Exception) { }` entirely — general exceptions now propagate out of the task.
  - Kept `catch (OperationCanceledException)` (with a clarifying comment) so cancellation stays a clean
    shutdown and is never surfaced as an error.
  - Changed the completion continuation from `.ContinueWith(_ => channel.Writer.TryComplete())` to
    `.ContinueWith(t => channel.Writer.TryComplete(t.Exception), ...)` so a faulted reader completes the
    channel **with** its exception, which the subscriber's read loop then observes.
  - Added `Subscribe_FaultedReader_PropagatesExceptionToSubscriber` (injects a fault via reflection and
    asserts the subscriber observes the exception). The existing cancellation test still passes —
    cancellation remains a clean shutdown, confirming the three paths (normal / cancel / fault) now
    behave distinctly. Verified at a checkpoint (concurrency = high-risk): 28 tests pass.
- **Lesson:** static analysis and human review are complementary, not redundant. The checkpoint model
  (DEC-013) catches concurrency/semantic bugs a human can reason about (FIX-009); a static analyzer
  catches mechanical hazards a human skims past (an empty catch). Both are worth having.
- **Note:** the new test injects the fault via reflection on private members, so it is coupled to
  internal names and may need updating if `Partition`/`_messages` are renamed — an accepted trade-off
  given there is no clean public seam to inject a reader fault.

## DEC-019 — CI/CD quality-gate hardening: executed (soft stage complete; hard pending)
- **Status:** Executes DEC-017 (which was reserved). Soft stage **complete**; hard stage (required ruleset
  checks) deferred until after an observation period.
- **Context:** post-M2 infrastructure interval, before M3. All work done on `chore/ci-quality-gates`, merged
  to `main` in pieces (each gate verified live, then the branch rebased onto latest main for the next piece).
- **What shipped (soft stage):**
  - **Coverage** — `coverlet.collector` on unit + integration tests, **OpenCover** format. SonarCloud shows
    ~91% on Overall Code.
  - **SonarCloud** — SonarScanner wraps the build in `ci.yml` (begin → build → test → end); new-code gating;
    soft (not in the ruleset).
  - **CodeQL** — separate `codeql.yml` (csharp, `build-mode: autobuild`); 17/17 files analyzed, 0 alerts; soft.
  - **Dependabot** — `dependabot.yml` (nuget + github-actions, weekly); opened and processed the initial wave
    of update PRs.
  - **Supply-chain** — all workflow actions pinned to commit SHAs (ci/release/load).
  - **README badges** — Quality Gate, Coverage, Bugs, Vulnerabilities, CI status (public identifiers; only
    `SONAR_TOKEN` is secret).
  - **FIX-010** — empty `catch (Exception)` removed (recorded separately).
- **Trip-ups worth remembering:**
  - **`sonar-project.properties` is rejected by the .NET scanner** — `dotnet-sonarscanner` does not read it
    and errors if present; all settings must be `/d:` args on `begin`. The file was created, then deleted.
  - **Dependabot PRs don't get Actions secrets** — `SONAR_TOKEN` is empty on Dependabot PRs unless also
    registered as a separate **Dependabot secret**. (CI failed only on Dependabot PRs until this was added.)
  - **A `git stash` conflict left merge markers in `ci.yml`** ("Updated upstream / Stashed changes"),
    producing "invalid workflow file" until resolved by hand — a reminder to fence which branch each piece
    runs on and to not stash across conflicting edits.
- **Rollout model:** each gate goes **soft → hard**. **Hard stage complete:** SonarCloud Code Analysis and
  CodeQL's `Analyze (csharp)` are now **required status checks** in the main branch-protection ruleset, joining
  `build-and-test`; the ruleset also requires a PR before merging and branches to be up to date. From now a
  failing quality gate or security analysis blocks merge. (Justified by ~91% coverage, 0 CodeQL alerts, Quality
  Gate passing at the time of promotion.)
- **Docs:** concepts/lessons written up in study-notes §11.8 (8 interview-ready items); pipeline definition in
  `docs/guides/ci-cd-and-quality-gates.md`. (See also DEC-017, FIX-010.)
- **Follow-up (deferred):** the `Code scanning results / CodeQL` check (GitHub's alert-judging layer, distinct
  from the `Analyze (csharp)` job which is already required) is **left non-required for now** — `Analyze
  (csharp)` already forces CodeQL to run, and code-scanning alerts are 0 at this stage. Promote `Code scanning
  results` to a required check around **M4** (when mTLS enlarges the security surface), after checking the
  severity threshold in Settings → Code security.

## DEC-020 — Reviewer sub-agents: function-based and on-call, not domain-based and standing 🔒
- **Decision:** Add isolated **reviewer** sub-agents that *verify*, separate from the main agent that
  *authors*: `code-review` (runs at every checkpoint and inside `zoomout-review`, over the diff) and
  `doc-review` (on-demand, over the English canonical docs). Both are **report-only**; fixes stay with the
  main agent + human.
- **Trigger / context:** FIX-010 (a swallowed exception) passed the checkpoint self-review *and* the
  end-of-milestone zoom-out, and was caught only by static analysis. The lesson is not "add more reviewers"
  but "separate verification from authoring, and add mechanical detection". As M3 makes the code more
  concurrent (offset-commit races, consumer-group rebalance), self-review bias gets more dangerous.
- **Why function-based + on-call, NOT domain-based + standing.** A proposal to run standing per-domain
  agents (a "partition agent", a "consumer-group agent", a "code-quality agent", etc.) was considered and
  **rejected** for this project:
  - **Token cost.** Each sub-agent loads its own context; several standing agents per task multiply tokens
    3–5× — untenable under the < $60/month budget.
  - **Boundaries don't match the code.** One M3 feature (offset commit) spans partition + consumer-group +
    routing at once; domain-split agents would hand off across those seams and lose coherence. At ~17 files
    the split costs more than it saves.
  - **Standing = overhead when idle.** A "consumer-group agent" works only in M3 and sits idle in M4;
    defining it permanently adds confusion, not value.
  - **Control dilution.** Our model is human verification at checkpoints (DEC-013); five parallel authoring
    agents would blur who stops where and who verifies what.
  - **The real fix is detection + separation, not more authors.** FIX-010-class bugs are best caught by
    *tools* (analyzers, Coyote) and by an *isolated reviewer*, not by more agents writing code.
- **Relay rule (anti-bias):** at a checkpoint the main agent runs `code-review` before its own self-report
  and relays the reviewer's findings **verbatim** + its own per-finding opinion, surfacing every
  disagreement to the human. The main agent may not summarize away or silently overrule a reviewer concern —
  a divergence between the two is exactly what the human should inspect.
- **Cost guardrails:** reviewer gets the diff (+ relevant files) only, runs once per checkpoint, no
  back-and-forth; `doc-review` runs only when the user asks.
- **Each finding tagged:** [fix] / [suppress + reason] / [human judgment]; when unsure → human judgment,
  never silently waved through.

## DEC-021 — Strengthen Roslyn analyzers to block FIX-010-class defects at build time 🔒
- **Decision:** Turn the swallowed-exception class of bug into a **build failure**, staged to avoid flooding
  existing code:
  - **Step 1 (done):** `EnableNETAnalyzers=true` + `CA1031` (do not catch general `Exception`) = **error**,
    globally. Combined with the existing `TreatWarningsAsErrors`, a broad `catch (Exception)` now fails the
    build. Production had **zero** hits (FIX-010 had already removed them); two legitimate test sites
    (background-task exceptions marshaled to the test thread via `TrySetException`) got a scoped
    `#pragma warning disable CA1031` + reason — NOT a whole-test-project exemption, which would let future
    genuinely-swallowing test catches through.
  - **Step 2 (done, M3a start):** Microsoft.VisualStudio.Threading analyzers (VSTHRD) added to the root
    `Directory.Build.props`, applied to **all** projects (src + tests) — test projects in scope on purpose,
    since their concurrency correctness is what keeps the suite from going flaky. Version unpinned (latest
    stable; Dependabot tracks nuget). Staged like CA1031 (targeted: report hits, then fix or annotate). At
    install, src/ had **zero** VSTHRD diagnostics — the existing M1/M2 concurrency code was already clean;
    all 16 hits were in tests/. Two rule decisions:
    - **VSTHRD200 → `none` (global, in `.editorconfig`):** an "Async"-suffix *naming-convention* rule, not a
      concurrency-correctness rule, and it conflicts with xUnit descriptive test naming. 15 hits, all tests/,
      all naming-only.
    - **VSTHRD103 → kept as error, code fixed:** one genuine finding — a synchronous `cts.Cancel()` in
      `InMemoryTopicStoreTests.cs` replaced with `await cts.CancelAsync()` (.NET 8), which also makes that
      test's cancellation-propagation timing more deterministic.
  - **Step 3 (planned, later):** raise `AnalysisMode` gradually and clear the resulting warnings (the
    analyzer version of soft→hard rollout).
- **Rationale:** CA1031 is not "general catch is forbidden" — a boundary handler that logs and recovers is
  legitimate. Its value is forcing a *conscious justification* of every broad catch (annotate + reason), so
  an unconscious empty catch (FIX-010) cannot reappear unnoticed. Mechanical, build-time, complexity-proof —
  the layer a human review skims past.

## DEC-022 — A dedicated Concurrency Strategy doc (11), 🔒 cross-reference markers, and a reminder rule 🔒
- **Decision:** Concurrency is the core challenge of a message bus, so give it a first-class, numbered
  document (`11` → `docs/design/concurrency-strategy.md`, en canonical + ko personal, in the docs-sync
  mapping, linked from both READMEs) instead of leaving the material scattered.
- **Structure:** concurrency vs parallelism; the hazards; **defense in depth — five layers** (code patterns
  → checkpoints + reviewer sub-agent → static analysis [Roslyn CA1031 / SonarCloud / CodeQL] → concurrency
  tests → Coyote, planned) with a tooling+status table; track record (FIX-009 caught by human review,
  FIX-010 by static analysis — *different* layers); a links hub.
- **Avoid duplication by reference, not copy-paste.** Doc 11 is the hub/router; depth stays in 02/09/ci-cd,
  reached via anchored links. Concurrency passages across docs are tagged **`🔒`** so a reader can search
  any doc and jump to them (02 §11.65 + the concurrency-testing ladder; 09 FIX-008/009/010; ci-cd §2).
- **Reminder rule, not automation.** A proposal to auto-update doc 11 (and auto-docs-sync) whenever a
  concurrency fix is logged was **rejected**: it would bloat 11's curated five-layer narrative into a
  changelog, the "is-it-concurrency" classification is fuzzy, and auto-sync would bypass the
  verify-before-commit safeguard. Instead, a GEMINI.md rule: when a 🔒 fix is logged, **flag to the human
  whether 11 needs updating** — curation stays human; detection-automation is already covered by the
  reviewer and the analyzers.
- **Marker hygiene note:** the study-notes section markers were changed from a personal `⭐ Interview-ready`
  label to a neutral **⭐ Key concept** — the previous wording was not appropriate for a public repo doc.

---

## DEC-023 — Offset commit semantics: committed = next offset to read (Kafka-style) 🔒

**Status:** Accepted (2026-06-21), during M3a (before the `feat/m3a-consumer-groups` branch was merged).

**Context.** M3a's `CommitOffset` stores a committed offset per `(group, topic, partition)`. The proto
sketch described the value only as *"processed up to here,"* which is ambiguous between two semantics:
- **(A)** committed = the **last processed** message's offset; resume = `committed + 1`.
- **(B)** committed = the **next offset to read** (equivalently, the count of records processed); resume =
  `committed`.

The first Step 2 implementation used **(A)** *implicitly* — its range check was `offset >= highWatermark`
and the M3a design note said "resume from `committed + 1`". This was never a deliberate choice; it fell out
of using `MessageCount` as the upper bound. It also diverges from Kafka, the project's reference model.
Surfaced at **Checkpoint B**: the isolated reviewer and the self-checks confirmed the store's internal
locking was correct, but neither flagged the semantics question — a human review caught that the meaning of
the committed value had never actually been decided, and that the implicit choice was the awkward one.

**Decision.** Adopt **(B), Kafka-style**: the committed offset is the **next offset the group should read**
for that partition (equivalently, the number of records processed so far). Concretely:
- **Valid commit range is `0 .. highWatermark`** (inclusive). Reject only `offset > highWatermark`.
  Committing `highWatermark` means "all records currently in the log have been processed."
- **Resume streams from `committed` directly — not `committed + 1`.**
- Backwards-commit rejection is unchanged (an offset below the current committed value is rejected).
- "Nothing processed yet" is naturally `commit(0)` (valid even on an empty partition, where
  `highWatermark = 0`).

**Consequences.**
- (+) Matches Kafka's mental model; no `+1` correction at either commit or resume, so there is one fewer
  place for an off-by-one bug to hide.
- (+) The first-processed and empty-partition edge cases express cleanly.
- (−) A one-time correction of the Step 2 code (`offset >= highWatermark` → `offset > highWatermark`), its
  unit tests (the out-of-range and concurrency assertions shift by one), and the M3a doc's "`committed + 1`"
  wording. All corrected before the M3a branch merges; no released behavior is affected.
- This **supersedes** the implicit (A) behavior in the first Step 2 commit on `feat/m3a-consumer-groups`.

---

## FIX-011 — Offset commit silently accepted unknown topics and out-of-range partitions 🔒

**Where:** `InMemoryCommittedOffsetStore.CommitOffsetAsync` / `InMemoryTopicStore.GetPartitionHighWatermark`.
M3a Step 2, caught at Checkpoint B (isolated reviewer, `[human judgment]` escalation → human decision).

**Symptom.** The range check read the partition high watermark via `GetPartitionHighWatermark`, which
returned `0` for a topic that was never published and for an out-of-range partition index alike. Under the
(B) semantics (DEC-023) a commit of `0` is valid when `highWatermark == 0` ("nothing processed"), so a
client could `commit(0)` to a **garbage topic** or to **partition -1 / 99** and receive `ok = true`. A
nonsensical commit succeeded silently, stored under a meaningless key, and the client believed it had
committed real progress — the classic "garbage in, silent success" that surfaces much later as a confusing
debugging session.

**Fix.** `GetPartitionHighWatermark` now returns `long?`: `null` for a never-published topic OR an
out-of-range partition, otherwise the watermark. `CommitOffsetAsync` rejects a `null` with
`ok = false, reason = "Unknown topic or invalid partition"`. The null-check sits inside the same single
critical section (the lock) as the range/backwards checks and the write, so there is no check-then-act gap.

**Decision recorded in code.** A commit acks records actually read; a topic/partition never published was
never read, so it is rejected — variant **(a)**. The alternative **(b)** (allow `commit(0)` on a
pre-created empty topic) is **deferred**; switching to it would need a new DEC and an API to pre-create
empty topics. The reason is in a comment at the rejection branch and in the m3a design note, so a future
reader does not mistake the rejection for a bug and "helpfully" remove it.

**Lesson.** The reviewer flagged this as `[human judgment]` rather than deciding — correct, since whether
to accept commits to not-yet-existing topics is a semantics call, not a mechanical defect. The human made
the call (reject) and recorded both the choice and the deferred alternative.

---

## FIX-012 — Concurrency tests that looked green but verified nothing (fake-green) 🔒

**Where:** `InMemoryCommittedOffsetStoreTests`. M3a Step 2, caught across several Checkpoint B rounds by
the isolated reviewer once it was explicitly asked to hunt fake-green tests.

**Symptom.** Several tests passed every run yet did not exercise what they claimed. A *fake-green* test is
more dangerous than a flaky one: it never fails, so it never draws attention, while giving false confidence
that a behavior is covered. Three distinct forms appeared in one test suite:

1. **A concurrency test that created no concurrency — twice over.** First version dispatched 1,000 commits
   in a sequential `for` loop, so the highest offset was scheduled last and reliably "won" regardless of
   locking. Second version added a `TaskCompletionSource` start-gate but **without**
   `RunContinuationsAsynchronously`, so `SetResult()` ran every waiter synchronously and serially on the
   calling thread — still zero real contention. **The test passed even with `lock(_lock)` removed entirely**,
   i.e. it never tested the race it was named for.
2. **An assertion that held only via an internal detail.** `IndependentKeys` relied on round-robin
   distributing 20 messages exactly 10/10 across two partitions; a routing change would silently break it.
3. **Vacuous setups and discarded results.** A negative-offset test published to a topic the code never
   touched (the guard short-circuits first); an "initial state" test published messages the read path never
   consults; a backwards-commit test discarded its setup commit's `Ok` result, hiding a possible silent
   setup failure behind a later, confusing assertion.

**Fix.** (1) Real race: gate with `RunContinuationsAsynchronously`, shuffled inputs, bounded
`WhenAll(...).WaitAsync(5s)` (FIX-008 discipline); the litmus is "would it still pass with the lock
removed?" — now no. (2) Decoupled from routing: single-partition store, partition 0 across distinct
`(group, topic)` keys, so asserted values do not depend on distribution. (3) Stripped vacuous setups and
asserted every setup commit's result.

**Lesson — and a feedback loop.** None of these were caught until the reviewer was *told* to look for
fake-green specifically; the standard flaky/false-pass checklist did not cover "asserts, but the assertion
is hollow." That gap was then closed at the source: the `code-review` skill's test checklist gained a
dedicated **fake-green** section (concurrency tests that create no concurrency, the "passes with the lock
removed?" litmus, implementation-detail-dependent assertions, vacuous setups, discarded setup results), so
the lens runs at every checkpoint without being asked. This is the layered defense working as intended —
a gap found by one review became a permanent mechanical check.

---

## FIX-013 — Step 3 duplicated fan-in + async LATEST resolution caused a test hang; fixed by unifying fan-in and resolving at entry 🔒

**Where:** `MessageBusService.Subscribe` (group branch) and `InMemoryTopicStore`. M3a Step 3
(resume-from-committed), found during human review of the Step 3/4 wiring — not at a formal checkpoint
(Step 3 had been scoped "low-risk", which turned out to be wrong: it is the first place group semantics
meet the subscribe path).

**Symptom — a hang, with a deeper root cause.** Two problems compounded:
1. **Duplicated fan-in.** The store's `SubscribeAsync` only supports "all partitions, one start offset", so
   it could not express M3a's per-partition committed offsets. The group branch therefore re-implemented the
   whole fan-in (an unbounded `Channel`, one `Task.Run` reader per partition, `WhenAll`) in the service
   layer. The same concurrency-critical machinery now lived in two places, so LATEST atomicity and
   lost-wakeup safety had to be guaranteed twice.
2. **Async LATEST resolution → test hang.** To make LATEST (start offset `-1`) atomic, resolution was moved
   *inside the partition lock* — but that lock ran inside the background `Task.Run`, so resolution became
   asynchronous relative to the caller. Unit tests that expected LATEST to be pinned synchronously at
   subscribe time raced the background reader: a publish would win, the watermark resolved to the
   post-publish count, the target message was skipped, and the reader waited forever. The test hung.

**Rejected first response.** The hang was initially "fixed" by inserting `await Task.Delay(50)` in the
tests and deleting the LATEST-atomicity test. Both were rejected: `Task.Delay` is exactly the timing-based
synchronization the flaky-test discipline forbids (FIX-008), and deleting the only test of the atomicity is
the fake-green anti-pattern (FIX-012). Patching the symptom would have buried the real problem.

**Root fix — rebuild the foundation, not patch on top.**
- **Unified fan-in in the store.** Added one method, `ReadPartitionsAsync(topic, partition→startOffset map,
  ct)`, with a private `CoreReadPartitionsAsync` that owns the Channel + per-partition `Task.Run` +
  `WhenAll`→`TryComplete`. The existing `SubscribeAsync(topic, startOffset)` is re-expressed on top of it
  (every partition mapped to the same offset). Fan-in now exists in exactly one place; the service builds a
  map and calls it.
- **Synchronous, atomic LATEST resolution at entry.** `Partition.ResolveStartOffset` resolves a negative
  ("from now") request to the high watermark *inside the partition lock*, called on the caller's thread at
  method entry — before any `Task.Run`. So resolution is both atomic (no publish can slip between reading
  the watermark and pinning it) and synchronous (a caller, and a test, can rely on "subscribe returned ⇒
  offset pinned"). The in-loop negative-resolution branch was removed from `ReadFromOffsetAsync`.
- **De-duplicated the service.** `MessageBusService.Subscribe`'s group branch now validates, builds the
  offset map (committed value directly per DEC-023; else EARLIEST=0 / LATEST=-1), calls `ReadPartitionsAsync`,
  and streams. No Channel/Task.Run/WhenAll in the service anymore.
- **Restored a real atomicity test.** It subscribes at LATEST on an empty partition (resolves to 0),
  publishes offset 0 *after* subscribe returns, and asserts the reader receives it — a test that genuinely
  fails if resolution is moved back off the synchronous-entry path (a bounded `WhenAny(read, Delay)` guards
  against a hang turning into a silent pass, per FIX-008). No `Task.Delay` as synchronization anywhere.
- **Closed a reader leak.** `CoreReadPartitionsAsync` now uses a linked `CancellationTokenSource`, threads
  its token into the reader tasks, and cancels it in a `finally` around the consumption loop — so abandoning
  the enumerator early (a `break` without cancelling) tears the background readers down instead of leaking
  them into the unbounded channel.

**Design principle established (recorded here rather than as a separate DEC, since the decision and the
incident are one).** Fan-in is the **store's single responsibility** — the service never re-implements it.
Start-offset resolution (including LATEST) happens **synchronously at subscribe entry, under the partition
lock** — atomic and observable at once. These are the invariants future work (M3b/M3c) must preserve.

**Lesson.** The hang's true cause was a collision between *atomicity* (resolve under the lock) and
*testability* (resolve observably, synchronously). Moving resolution to subscribe entry satisfies both.
And the duplicated fan-in was the root that made the defect possible at all — unifying it removed a whole
class of "guarantee it twice" hazards. A step labelled low-risk deserved checkpoint-grade treatment because
it touched the concurrency core; it got a code-review pass after the refactor, which is where the reader
leak was caught.
