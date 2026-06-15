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

## FIX-008 — Integration test could hang instead of failing on timeout
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
- **Deferred to investigation (not deleted):**
  - Reported leftover `UnitTest1.cs` (LoadTests) and `Class1.cs` (Observability, Security). The CLI gave no
    full paths and may have inferred them from common templates. **Do not delete blindly:** the Phase 0
    instruction keeps a *placeholder passing test* so empty test projects keep the pre-commit hook working —
    a "leftover" `UnitTest1.cs` may be that placeholder. Verify each file's path/contents/role first, then
    decide. Empty skeleton `Class1.cs` are likely safe to delete but confirm they're truly unreferenced.
- **Correctly out-of-scope (guardrail worked):** Broker/Client references to the empty `Flumewright.Security`
  / `Flumewright.Observability` projects were classified `[out-of-scope — record only]` (M4 mTLS / M6
  observability) — the CLI did NOT misflag these intentionally-staged skeletons as defects.
- **Process note:** the report-only protocol + the 09 "intentionally-deferred" guardrail both held — the CLI
  proposed no M2+ work and re-flagged none of the deferred items. The CLI *over-escalated* one severity
  (enumerator dispose → "bug"), which the human review corrected — exactly the 08 principle that a CLI report
  is a starting point, not a conclusion.
