# Concurrency Strategy

> **Why this document exists.** Flumewright is a message bus: many publishers append concurrently, many
> subscribers read concurrently, and each partition is served by its own background reader. Concurrency is
> not a side concern here — it *is* the hard part. A single race, lost wakeup, or swallowed exception can
> silently drop a message, break ordering, or hang a subscriber with no error to trace. This document is the
> single place that explains how the project treats concurrency: the hazards, the layered defenses, and the
> bugs those defenses have actually caught.
>
> Concurrency correctness here is defended in **depth** — five independent layers — rather than relying on a
> single review to catch everything. The sections below lay out those layers and the bugs they have caught.

> **Finding the concurrency parts of other docs quickly.** Concurrency material is spread across several
> documents by design (concepts in the study notes, incidents in the decision log, CI integration in the
> CI/CD guide). To jump straight to it, search any document for the lock marker **`🔒`** — every
> concurrency-relevant passage is tagged with it. Section links below also point at the exact anchors.

---

## 1. Why concurrency is the core challenge

**Concurrency vs parallelism — not the same thing.** *Concurrency* is about *structure*: dealing with many
things at once — interleaving multiple tasks that are all in progress, whether or not they literally run at
the same instant. *Parallelism* is about *execution*: actually running multiple things simultaneously on
multiple cores. Concurrency is the harder, more general problem — you can have concurrency on a single core
(tasks interleaved by the scheduler) with no parallelism at all, and a correctly concurrent design stays
correct whether it ends up running in parallel or not. A message bus is fundamentally a **concurrency**
problem: the challenge is coordinating many interleaved publishers, subscribers, and background readers
correctly, regardless of how many cores happen to run them. (Parallelism is then a throughput bonus — more
cores process the interleaved work faster — but it is not where the correctness difficulty lives.)

A message bus is concurrent by definition:

- **Concurrent publish** — many publishers append to the same topic at the same time. Each append must get a
  unique, contiguous offset; two appends must never collide on one.
- **Concurrent subscribe** — many subscribers read the same log independently, each at its own offset
  (cursor). One slow or cancelled subscriber must not affect the others.
- **Per-partition background readers** — a subscription fans in across partitions via one background task
  per partition. A fault or cancellation in one of those tasks must be handled correctly, not lost.
- **Shared mutable state** — the append-only log, the offset counter, and the wakeup signal for waiting
  readers are all touched from multiple threads.

When this goes wrong it does so *quietly*: a dropped message looks like "the subscriber just didn't get
it", a lost wakeup looks like "it hangs sometimes", a swallowed exception looks like nothing at all. That
silence is the danger — and the reason a single layer of review is not enough.

---

## 2. The hazards we guard against 🔒

| Hazard | What it looks like here |
|--------|-------------------------|
| **Race on offset/counter** | Two concurrent appends read-then-write the same offset → duplicate or skipped offset, broken ordering. |
| **Lost wakeup (TCS)** | A reader waits on a `TaskCompletionSource`; a publish signals just before the wait registers → the reader sleeps forever despite data being present. |
| **Swallowed exception** | A background reader faults, the exception is caught and discarded → the reader dies invisibly and the subscriber simply never sees that partition's messages (this is exactly [FIX-010](../decisions/decision-and-fix-log.md#fix-010--empty-catch-exception-in-subscribeasync-silently-swallowed-partition-reader-faults-)). |
| **Cancellation mishandled** | A cancel is treated as an error (or an error as a cancel) → either spurious failures or silent hangs. |
| **Deadlock** | Holding a lock across an `await`, or lock-ordering inversions → threads wait on each other forever. |
| **Flaky test** | A test that synchronizes on timing (`Sleep`/`Delay`) or asserts a tight value on a probabilistic result → passes and fails without code changes, destroying trust in CI (worse than no test). |
| **Fake-green test** | A test that passes *every* time but does not verify what it claims — e.g. a "concurrency" test that creates no real contention (sequential dispatch, or a TCS gate without `RunContinuationsAsynchronously`), so it would pass even with the lock removed (this is exactly [FIX-012](../decisions/decision-and-fix-log.md#fix-012--concurrency-tests-that-looked-green-but-verified-nothing-fake-green-)). More dangerous than flaky: it never draws attention. |
| **Non-atomic cross-operation boundary** *(accepted, not a bug)* | Two separate operations that cannot be made atomic — e.g. the M3b helper's *publish-to-retry-then-commit-original* — leave a crash window between them: resume re-runs the first, so the message can appear twice. This is **at-least-once duplication**, an accepted contract, not a defect (see below). |
| **Membership-table races** *(M3c)* | The group coordinator's membership table is shared mutable state touched by joins, leaves, the session-timeout sweeper, and (later) the commit-path generation check. Concurrent membership changes that are not serialized can tear the table or lose updates; a sweeper deciding "dead" while a heartbeat arrives can double-evict or revive a member. Guarded by the coordinator's own lock + check-then-act on every membership change. |
| **Non-monotonic generation** *(M3c)* | The coordinator's generation token must increase by exactly one per membership change, atomically with that change. If the bump is not in the same critical section as the change (a race on the counter), two rebalances can collide on one generation or skip one — and the generation fence (which rejects stale commits) silently stops protecting. Generation monotonicity-under-contention is asserted by a real-contention test (start-gate, would fail with the lock removed). |
| **Rebalance-phase boundary race** *(M3c)* | The rebalance is a three-state machine (Stable → PreparingRebalance → CompletingRebalance → Stable), not two states. A join must be classified against the state *atomically inside the coordinator lock*: admitted to the current generation only while PreparingRebalance, otherwise it triggers a fresh rebalance. Collapsing this to one "rebalancing" state opens an orphan window — a member that joins after membership closed but before the leader distributes the assignment is silently included in a generation the leader computed without it (assigned nothing, believes it is healthy). Guarded by the Preparing→Completing transition; asserted by "every member that returns for generation N is in the leader's generation-N snapshot." |
| **Lock-free read across components** *(M3c, pattern not hazard)* | The commit fence must check the current generation *inside the offset-store lock*, but must NOT take the coordinator lock there (offsetStore→coordinator nesting is a deadlock-ordering hazard once a reverse path appears). Resolution: the coordinator exposes its generation via a lock-free read (`Volatile.Read` over a `ConcurrentDictionary`), so the offset store reads it inside its own lock without acquiring a second lock. The fence stays atomic with the offset update; no cross-component lock is held. This is the safe pattern for "validate against another component's state without nesting locks." |
| **Interleaving-dependent assertion** *(M3c, test hazard)* | A concurrency test where the race has several valid final states (depending on the interleaving) must not assert one fixed outcome (false failure on a valid schedule) nor assert nothing meaningful (fake-green). Both are failures of the same kind. The correct shape — surfaced when Coyote explored schedules a luck-based test never reached — is to **observe which interleaving actually occurred, branch on it, and assert the exact invariant for that branch** (e.g. sweeper-won → evicted + generation bumped; heartbeat-won → kept + generation unchanged). This is the twin of the fake-green hazard: too-weak hides bugs, too-strong invents failures. A corollary (M3c zoom-out, [DEC-026](../decisions/decision-and-fix-log.md#dec-026--m3c-zoom-out-record-only-dispositions-intended-not-defects-)): when the *code itself* already guarantees an invariant (e.g. the three-state machine prevents orphans, [FIX-017](../decisions/decision-and-fix-log.md#fix-017--other-m3c-checkpoint-fixes-orphan-window-cross-component-lock-interleaving-dependent-assertions-)), the test assertion for that invariant may be deliberately left loose rather than tightened into an over-assertion that would flake under a valid interleaving — a loose-but-explained assertion can be the chosen design, not a gap. |
| **Coyote that never actually runs** *(M3c, tooling hazard)* | Coyote can only control Tasks in a **rewritten** assembly. If the test assembly holding the test's own `Task.Run`/`Task.WhenAll` is not in `coyote.json`, those Tasks run as native ThreadPool tasks invisible to the engine; Coyote finds zero controlled operations, declares deadlock, and explores **0 iterations** — yet still reports "0 bugs." This is the extreme fake-green: a layer that silently does nothing reports the same result as one that proved correctness. Guard: the test assembly must be in the rewrite set, and the check is **"explored ~N iterations, 0 bugs"** (verify `engine.TestReport`'s iteration count), never "0 bugs" alone. (See [FIX-016](../decisions/decision-and-fix-log.md#fix-016--the-coyote-layer-was-never-actually-running-checkpoint-ds-0-bugs--100-iterations-was-a-false-positive-), [DEC-025](../decisions/decision-and-fix-log.md#dec-025--coyote-is-a-dedicated-assembly-its-coverage-is-a-documented-roadmap-not-a-label-).) |

These are the failure modes the layers below are designed to catch. The last row is different in kind from
the others: it is not a hazard to *eliminate* but a trade-off to *accept consciously*. Where two operations
cannot be bound atomically (no distributed transaction), the project does **not** pretend otherwise — it
chooses at-least-once with possible duplicates and pushes the de-duplication responsibility to the consumer
(idempotency). The concurrency discipline here is the opposite of the others: instead of adding a lock to
make something atomic, you **document the non-atomic boundary as a contract** so no one later "fixes" it with
a fake transaction. M3b's `publish → commit` is exactly this (the duplication trade-off is recorded in the
[m3b design note](m3b-redelivery-dlq.md) and [FIX-014](../decisions/decision-and-fix-log.md#fix-014--retryingconsumer-committed-the-processed-offset-instead-of-the-next-offset-broke-resume-surfaced-by-m3b-integration-tests-)'s neighbourhood); exactly-once is explicitly not
planned.

---

## 3. Defense in depth — five layers 🔒

No single technique catches all concurrency bugs; each layer sees what the others miss. The point is that a
hazard has to pass through **all five** to reach `main`.

| Layer | What it does | Tooling | Status |
|-------|-------------|---------|--------|
| 1 | Code patterns (prevent at write time) | source discipline (locks, TCS, cancellation) | in place |
| 2 | Human checkpoints + isolated AI reviewer | risk-based checkpoints ([DEC-013](../decisions/decision-and-fix-log.md#dec-013--risk-based-checkpoint-verification-replaces-per-step-hand-verification)), `code-review` skill (Gemini sub-agent) | in place |
| 3 | Static analysis (mechanical, build/CI) | **Roslyn analyzers** (CA1031, **VSTHRD threading**), **SonarCloud**, **CodeQL** | in place |
| 4 | Concurrency tests (behavioral) | **xUnit** concurrency tests, flaky-test discipline | in place |
| 5 | Systematic concurrency exploration | **Microsoft Coyote** | active (M3c): coordinator + offset store |


### Layer 1 — Code patterns (prevention at write time) *— in place*
Disciplined patterns in the source itself:
- **Atomic offset assignment** under a lock (or `Interlocked`), so concurrent appends never collide.
- **Lost-wakeup-safe TCS**: re-check the condition under the lock before waiting, `RunContinuationsAsynchronously`, and complete the TCS outside the lock.
- **Lock scope correct**: shared state is only touched inside its lock; `await` happens outside it (no lock held across await).
- **Start-offset resolution is synchronous and atomic at entry**: resolving a relative start position (LATEST → "from now") reads the high watermark *under the partition lock, on the caller's thread, before any background reader is spawned* — so no publish can slip between reading the watermark and pinning it (atomic), and the resolved offset is observable the moment subscribe returns (synchronous). Resolving it later, inside the background reader, was the cause of [FIX-013](../decisions/decision-and-fix-log.md#fix-013--step-3-duplicated-fan-in--async-latest-resolution-caused-a-test-hang-fixed-by-unifying-fan-in-and-resolving-at-entry-).
- **Fan-in lives in one place**: the partition fan-in (Channel + per-partition reader + completion) is the store's single responsibility; callers (the service layer) never re-implement it. One implementation means the lost-wakeup and atomicity guarantees are made once, not duplicated (the duplication was the root of [FIX-013](../decisions/decision-and-fix-log.md#fix-013--step-3-duplicated-fan-in--async-latest-resolution-caused-a-test-hang-fixed-by-unifying-fan-in-and-resolving-at-entry-)).
- **Cancellation is normal shutdown**: `OperationCanceledException` is caught and treated as a clean stop; other exceptions are *not* swallowed but propagated to the subscriber (via channel completion with the exception).
- **Shared-lifetime tasks are all awaited**: when two long-running tasks share a lifetime (e.g. the dual-subscription retry helper), awaiting only the one that finished (`WhenAny`) is not enough — the survivor is awaited too (cancellation swallowed, other faults propagated), or its exceptions vanish. This is [FIX-013](../decisions/decision-and-fix-log.md#fix-013--step-3-duplicated-fan-in--async-latest-resolution-caused-a-test-hang-fixed-by-unifying-fan-in-and-resolving-at-entry-)'s reader-leak rule applied one layer up ([FIX-015](../decisions/decision-and-fix-log.md#fix-015--unawaited-tasks-in-the-dual-subscription-helper-and-in-the-redelivery-tests-swallowed-exception--fake-green-risk-)).

### Layer 2 — Human checkpoints + an isolated reviewer sub-agent *— in place*
Tooling: **risk-based checkpoints ([DEC-013](../decisions/decision-and-fix-log.md#dec-013--risk-based-checkpoint-verification-replaces-per-step-hand-verification))** + the **`code-review` skill** (an isolated Gemini sub-agent).
- **Checkpoints**: concurrency/shared-state steps are high-risk and stop for human verification, with explicit self-checks (atomic increments? isolated per-partition state? lost-wakeup-safe?).
- **Reviewer sub-agent**: a separate, isolated agent inspects the diff with fresh eyes against a concurrency/exception/flaky-test checklist, tagging each finding fix / suppress / human-judgment. The author of code is biased toward its own work; an isolated reviewer is not.

### Layer 3 — Static analysis (mechanical, at build/CI time) *— in place*
Four independent analyzers already run:
- **Roslyn analyzer — CA1031 = error** *(in place)*: a broad `catch (Exception)` fails the build, so the swallowed-exception class of defect cannot return to production code (tests annotate the two legitimate marshaling sites). This is the build-time lock on [FIX-010](../decisions/decision-and-fix-log.md#fix-010--empty-catch-exception-in-subscribeasync-silently-swallowed-partition-reader-faults-).
- **SonarCloud** *(in place)*: the quality gate flags empty catches, code smells, and some vulnerabilities on every PR — this is the tool that first surfaced [FIX-010](../decisions/decision-and-fix-log.md#fix-010--empty-catch-exception-in-subscribeasync-silently-swallowed-partition-reader-faults-).
- **CodeQL** *(in place)*: security SAST (data-flow). More security than concurrency, but the same "mechanical detection" layer.
- **Roslyn threading analyzers (VSTHRD)** *(in place)*: unawaited `Task` / `async void` / synchronous blocking on async flagged at build, catching fire-and-forget mistakes that are a common source of concurrency bugs. Applied to src + tests ([DEC-021](../decisions/decision-and-fix-log.md#dec-021--strengthen-roslyn-analyzers-to-block-fix-010-class-defects-at-build-time-) stage 2); the first install was clean in src/ and caught one synchronous-cancel issue in tests (VSTHRD103). The *safety* rules (VSTHRD003 foreign-task await, VSTHRD103 sync-blocking, VSTHRD110 unobserved task, …) stay active build-wide under warnings-as-errors; only **VSTHRD200** — a pure *naming-style* rule ("async methods end in `Async`"), not a concurrency check — is suppressed for test projects, since test methods use descriptive names. That split (silence a naming rule, never a safety rule) is part of the analyzer tiering in [DEC-029](../decisions/decision-and-fix-log.md#dec-029--shift-left-analyzer-tiering-mechanical-rules-hard-gate-locally-semantic-rules-are-sonarcloud--human-review-never-build-gated-).

### Layer 4 — Concurrency tests (behavioral) *— in place*
Tooling: **xUnit** concurrency tests + flaky-test discipline.
- **Concurrency unit tests**: e.g. 1,000 concurrent appends asserting offsets are unique and contiguous — exercising lock integrity and atomic state.
- **Flaky-test discipline**: deterministic properties get tight assertions; probabilistic ones get generous ranges; every async wait has a bounded timeout so a failure fails fast instead of hanging ([FIX-008](../decisions/decision-and-fix-log.md#fix-008--integration-test-could-hang-instead-of-failing-on-timeout-)). A flaky test is treated as a defect, not noise.

### Layer 5 — Systematic concurrency testing (Microsoft Coyote) *— active (M3c)*
Tooling: **Microsoft Coyote**.
- Coyote rewrites and controls task scheduling to **deterministically explore interleavings**, reproducing races and deadlocks that ordinary tests hit only by luck. It was reserved for the point where consumer-group assignment and offset-commit concurrency make the schedule space large enough to need it — that point is **M3c (rebalance)**, which adds the group coordinator: membership, the session-timeout sweeper, generation fencing, and handover, all racing in-flight processing.
- **Active coverage (M3c):** the **group coordinator** (join/leave/sweep/fence interleavings — Step 8) and the **committed-offset store** (commit serialization + the generation-fence-vs-commit race — Step 8.5). Both explore ~100 schedules per test with outcome-branched assertions.
- **How rewriting works, and its danger.** Coyote *binary-rewrites* an assembly to hook every Task and scheduling primitive, replacing the runtime's scheduler with its own so it can drive the interleavings. That makes the rewritten binary a **modified, test-only artifact** — it must never run in production. And because rewriting affects **every** Task in the assembly, Coyote tests live in a **dedicated assembly** (`Flumewright.ConcurrencyTests`, the only test assembly in `coyote.json`); ordinary xUnit tests stay in a non-rewritten assembly, or Coyote's scheduler would hijack their Tasks. **Coyote only controls Tasks in a rewritten assembly** — if the test assembly itself is not rewritten, the test's own Tasks are invisible, the engine explores 0 iterations, and still reports "0 bugs" (the [FIX-016](../decisions/decision-and-fix-log.md#fix-016--the-coyote-layer-was-never-actually-running-checkpoint-ds-0-bugs--100-iterations-was-a-false-positive-) trap). So a Coyote result is trusted only when `engine.TestReport` shows it **actually explored ~N iterations**, not merely "0 bugs."
- **Scope.** Coyote controls **in-process** Task scheduling, so it targets components directly, not the gRPC/Kestrel host — the integration tests (over real gRPC) remain a separate layer. Crucially, Coyote has **no background services and no real clock**, so a property whose resolution depends on an *external actor over wall-clock time* — e.g. the session-timeout **sweeper** evicting a vanished member so the group recovers — is **out of Coyote's scope** and belongs to the integration layer; forcing such a liveness scenario into Coyote yields a hang that is a category error, not a bug ([FIX-021](../decisions/decision-and-fix-log.md#fix-021--a-ci-only-e2e-failure-that-looked-like-a-rebalance-defect-but-was-a-flaky-test-against-correct-product-behaviour-), [DEC-027](../decisions/decision-and-fix-log.md#dec-027--tool-boundary-interleaving-races--coyote-sweeper--wall-clock-liveness--integration-tests-)). **Coverage roadmap:** coordinator + offset store are covered now (M3c); extending Coyote to the **topic store** and any remaining in-process concurrency core is explicit **follow-up work** (post-M3), recorded so this layer's coverage is a documented promise, not a label. (See [DEC-025](../decisions/decision-and-fix-log.md#dec-025--coyote-is-a-dedicated-assembly-its-coverage-is-a-documented-roadmap-not-a-label-).)
- This is the answer to "as the code grows, these bugs get harder to find by reading": a machine explores the schedules a human or an ordinary test never will — but only if it is actually wired to run.

---

## 4. Track record — bugs these layers actually caught 🔒

Concurrency defense is not theoretical here; the layers have already paid off:

- **[FIX-009](../decisions/decision-and-fix-log.md#fix-009--checkpoint-a-caught-a-latest-semantics-bug-in-the-channel-store-became-the-trigger-for-dec-015-) — lost-wakeup / LATEST-drop bug.** The first channel-based store drained bounded per-partition
  channels into an unbounded merged channel, so the intended drop semantics never applied. Caught by a
  **human checkpoint review** (Layer 2), and it became the trigger for switching to the log/pull model
  ([DEC-015](../decisions/decision-and-fix-log.md#dec-015--delivery-model-confirmed-logpull-kafka-style-not-push-m2-redefined)). A bug a static analyzer could never have reasoned about.
- **[FIX-010](../decisions/decision-and-fix-log.md#fix-010--empty-catch-exception-in-subscribeasync-silently-swallowed-partition-reader-faults-) — swallowed exception in the partition reader.** An empty `catch (Exception)` discarded every
  fault from the background reader. **Both** the checkpoint review and the end-of-milestone zoom-out missed
  it; **static analysis (SonarCloud / CA1031)** — Layer 3 — caught it on day one. A bug a human reads past.
- **[FIX-011](../decisions/decision-and-fix-log.md#fix-011--offset-commit-silently-accepted-unknown-topics-and-out-of-range-partitions-) — offset commit silently accepted unknown topics / out-of-range partitions.** The watermark
  read returned `0` for both "empty" and "does not exist", so a commit to a garbage topic or partition `-1`
  returned `ok=true`. Caught by the **isolated reviewer** (Layer 2), which correctly escalated it as a
  *human-judgment* semantics call rather than deciding itself.
- **[FIX-012](../decisions/decision-and-fix-log.md#fix-012--concurrency-tests-that-looked-green-but-verified-nothing-fake-green-) — fake-green concurrency tests.** Tests that passed every run while creating no real contention
  (sequential dispatch; a TCS gate without `RunContinuationsAsynchronously`) — they would have passed with
  the lock removed. Caught by the **isolated reviewer** (Layer 2) *only after it was asked to hunt
  fake-green specifically*; the gap was then closed permanently by adding a fake-green section to the
  `code-review` checklist, so the lens now runs unprompted at every checkpoint. A review-found gap became a
  mechanical check — defense in depth improving itself.

- **[FIX-013](../decisions/decision-and-fix-log.md#fix-013--step-3-duplicated-fan-in--async-latest-resolution-caused-a-test-hang-fixed-by-unifying-fan-in-and-resolving-at-entry-) — duplicated fan-in + async LATEST resolution → test hang.** The group-subscribe path
  re-implemented the store's fan-in (because the store couldn't express per-partition offsets), and making
  LATEST atomic pushed its resolution into the background reader, so it became async and a test hung. Caught
  by **human review** (Layer 2) of the wiring; the first attempt to patch it (a `Task.Delay` and deleting the
  atomicity test) was rejected as [FIX-008](../decisions/decision-and-fix-log.md#fix-008--integration-test-could-hang-instead-of-failing-on-timeout-)/FIX-012 violations, and the foundation was rebuilt instead (single
  fan-in in the store; LATEST resolved synchronously-at-entry under the lock). A post-refactor **code-review**
  (Layer 2) then caught a reader leak. A bug whose real cause was a duplicated-implementation smell, not a
  single wrong line.

- **[FIX-014](../decisions/decision-and-fix-log.md#fix-014--retryingconsumer-committed-the-processed-offset-instead-of-the-next-offset-broke-resume-surfaced-by-m3b-integration-tests-) — off-by-one offset commit in the retry helper.** `RetryingConsumer` committed `msg.Offset`
  instead of `msg.Offset + 1`, so a resumed consumer re-read the message it had just processed and the
  partition never advanced ([DEC-023](../decisions/decision-and-fix-log.md#dec-023--offset-commit-semantics-committed--next-offset-to-read-kafka-style-): committed = the *next* offset to read). It passed Checkpoint A because
  Step 2 had no integration test yet — the defect is invisible in a diff and only shows as behaviour over a
  real broker. Caught by the **behavioural integration test** (Layer 4) in Step 4, not by the diff-level
  **code-review** (Layer 2) that signed off Step 2. The mirror image of [FIX-010](../decisions/decision-and-fix-log.md#fix-010--empty-catch-exception-in-subscribeasync-silently-swallowed-partition-reader-faults-): there mechanical analysis
  caught what a human skimmed; here a behavioural test caught what a diff review structurally cannot reason
  about.
- **[FIX-015](../decisions/decision-and-fix-log.md#fix-015--unawaited-tasks-in-the-dual-subscription-helper-and-in-the-redelivery-tests-swallowed-exception--fake-green-risk-) — unawaited survivor task (helper) + unawaited pumps (tests).** `ConsumeWithRetriesAsync` ran
  two subscriptions and, after `Task.WhenAny`, awaited only the finished one — the still-running survivor's
  faults were never observed (the "half-working" swallowed-exception shape, [FIX-013](../decisions/decision-and-fix-log.md#fix-013--step-3-duplicated-fan-in--async-latest-resolution-caused-a-test-hang-fixed-by-unifying-fan-in-and-resolving-at-entry-) reproduced one layer up).
  The same shape appeared in the tests, whose background pumps were never awaited (a pump failure could not
  fail the test — fake-green, [FIX-012](../decisions/decision-and-fix-log.md#fix-012--concurrency-tests-that-looked-green-but-verified-nothing-fake-green-)). Both were caught in a single pass by the **isolated reviewer**
  (Layer 2) at Checkpoint B; the fix awaits both tasks (cancellation swallowed, other faults propagated) and
  awaits every pump. A `Task.Delay(Infinite)`-as-sync trick found in the same review was removed, not
  suppressed ([FIX-008](../decisions/decision-and-fix-log.md#fix-008--integration-test-could-hang-instead-of-failing-on-timeout-)). The `code-review` checklist's fire-and-forget item now explicitly covers `WhenAny`
  survivors and background test pumps.

- **[FIX-016](../decisions/decision-and-fix-log.md#fix-016--the-coyote-layer-was-never-actually-running-checkpoint-ds-0-bugs--100-iterations-was-a-false-positive-) — the Coyote layer was never actually running (Checkpoint D false-positive).** `coyote.json`
  rewrote only the production assembly, not the test assembly holding the tests' own `Task.Run`/`Task.WhenAll`,
  so Coyote controlled zero of the tests' Tasks: it found no controlled operations at the first `await`,
  declared deadlock, explored **0 iterations**, and still reported "0 bugs." Checkpoint D had been signed off
  on that false-positive. Caught not by a layer but by **reading the engine's behaviour** during the next step
  (8.5) — the tell was a 0-iteration immediate termination. The fix rewrites the test assembly too and verifies
  the **explored iteration count**, not just the bug count. Once it actually ran, Coyote immediately surfaced a
  real test-level deadlock — Layer 5 finally doing its job. A defense layer that silently does nothing is worse
  than none, because it reports the same "0 bugs" as a layer that proved correctness.

- **[FIX-021](../decisions/decision-and-fix-log.md#fix-021--a-ci-only-e2e-failure-that-looked-like-a-rebalance-defect-but-was-a-flaky-test-against-correct-product-behaviour-) — a CI-only e2e timeout that was a flaky test, not a rebalance defect (and the tool-boundary lesson).**
  A five-stage composite integration test (`MembershipLifecycle`) timed out on CI but passed locally. It looked
  like a coordinator bug — a cancelled member lingering as a partition-holding "ghost," even deadlocking the group
  if it was the leader — and three reviewers (own analysis, the agent's pass, an isolated `code-review` sub-agent)
  agreed, matching the real Kafka bugs KAFKA-9752 / KAFKA-7610. They were all wrong about the *product*. A
  confirmation diagnosis of the sweeper showed the coordinator is **correct**: a vanished member — including a
  ghost leader — is always evicted on the session timeout, and that eviction's `StartRebalance` cancels the
  survivors' `SyncTcs` to unblock them (self-heal by design). The CI failure was a **test artifact**: the test
  left ungracefully (heartbeat re-joined in the gap before the consumer task was cancelled), modelling an
  ungraceful disconnect that intentionally costs one ~10s session-timeout penalty — ×5 stages on a slow runner,
  over budget. **The decisive tool-boundary lesson:** the fix attempt went off the rails trying to reproduce the
  *leader-deadlock* in Coyote, which (correctly) reported an unresolvable hang — because Coyote has no sweeper and
  no clock, and leader-vanish recovery is a liveness property resolved by an external actor (the sweeper) over
  wall-clock time, **outside Coyote's interleaving-exploration scope**. That belongs in an integration test
  (Layer 4), never Coyote (Layer 5) — codified as [DEC-027](../decisions/decision-and-fix-log.md#dec-027--tool-boundary-interleaving-races--coyote-sweeper--wall-clock-liveness--integration-tests-). The product was left untouched; the two
  structurally-fragile composite e2e tests were removed (to be rebuilt stage-isolated in M4), and Coyote's CI
  step was hardened to be build-decoupled and iteration-guarded ([FIX-019](../decisions/decision-and-fix-log.md#fix-019--coyote-was-coupled-to-every-build-coyote-not-found-broke-ci-decoupled-into-a-dedicated-manifest-pinned-step-) / [FIX-020](../decisions/decision-and-fix-log.md#fix-020--a-ci-iteration-guard-so-a-future-regression-cant-silently-drop-coyote-to-one-iteration-)) so its "did it really run?"
  guarantee is now machine-enforced. The episode is the sharpest illustration of Layer 4 vs Layer 5: a symptom
  surfaced by an e2e (Layer 4) must not be chased into Coyote (Layer 5) when its resolution depends on time and an
  external actor.

The lesson that shapes this whole document: **these bugs were caught by different layers, and no single
layer would have caught them all.** Human reasoning catches semantic/concurrency bugs a person can think
through ([FIX-009](../decisions/decision-and-fix-log.md#fix-009--checkpoint-a-caught-a-latest-semantics-bug-in-the-channel-store-became-the-trigger-for-dec-015-), [FIX-013](../decisions/decision-and-fix-log.md#fix-013--step-3-duplicated-fan-in--async-latest-resolution-caused-a-test-hang-fixed-by-unifying-fan-in-and-resolving-at-entry-)); mechanical analysis catches the empty-catch a person skims over ([FIX-010](../decisions/decision-and-fix-log.md#fix-010--empty-catch-exception-in-subscribeasync-silently-swallowed-partition-reader-faults-)); the
isolated reviewer catches both a semantics call it knows to escalate ([FIX-011](../decisions/decision-and-fix-log.md#fix-011--offset-commit-silently-accepted-unknown-topics-and-out-of-range-partitions-)) and a hollow test the author
is blind to ([FIX-012](../decisions/decision-and-fix-log.md#fix-012--concurrency-tests-that-looked-green-but-verified-nothing-fake-green-)); and a behavioural integration test catches an off-by-one a diff review cannot see
([FIX-014](../decisions/decision-and-fix-log.md#fix-014--retryingconsumer-committed-the-processed-offset-instead-of-the-next-offset-broke-resume-surfaced-by-m3b-integration-tests-)), while the isolated reviewer catches the swallowed-exception shape that the author reproduced one
layer up ([FIX-015](../decisions/decision-and-fix-log.md#fix-015--unawaited-tasks-in-the-dual-subscription-helper-and-in-the-redelivery-tests-swallowed-exception--fake-green-risk-)). And a layer must be verified to be *running at all* — [FIX-016](../decisions/decision-and-fix-log.md#fix-016--the-coyote-layer-was-never-actually-running-checkpoint-ds-0-bugs--100-iterations-was-a-false-positive-) is the reminder that "0 bugs"
from a tool that never executed is the most dangerous green of all. When a review finds a gap in its own
checklist, that gap becomes a permanent check ([FIX-012](../decisions/decision-and-fix-log.md#fix-012--concurrency-tests-that-looked-green-but-verified-nothing-fake-green-) → the fake-green checklist item; [FIX-015](../decisions/decision-and-fix-log.md#fix-015--unawaited-tasks-in-the-dual-subscription-helper-and-in-the-redelivery-tests-swallowed-exception--fake-green-risk-) → `WhenAny`
survivors and test pumps under the fire-and-forget item; [FIX-016](../decisions/decision-and-fix-log.md#fix-016--the-coyote-layer-was-never-actually-running-checkpoint-ds-0-bugs--100-iterations-was-a-false-positive-) → verify Coyote's explored-iteration count).
And a layer must be used *within its boundary*: [FIX-021](../decisions/decision-and-fix-log.md#fix-021--a-ci-only-e2e-failure-that-looked-like-a-rebalance-defect-but-was-a-flaky-test-against-correct-product-behaviour-) is the reminder that a liveness property resolved by an
external actor over wall-clock time (the sweeper) belongs to the integration layer, not Coyote — forcing it into
Coyote's clockless, sweeper-less world produces a hang that is a category error, not a bug ([DEC-027](../decisions/decision-and-fix-log.md#dec-027--tool-boundary-interleaving-races--coyote-sweeper--wall-clock-liveness--integration-tests-)).
Neither layer alone is enough — which is why there are five, and why they feed each other.

---

## 5. Where the concurrency details live (anchored links)

This document is the hub; the depth lives in the documents below. Search any of them for **`🔒`** to jump to
the concurrency passages.

- **Concepts & theory** — study notes:
  - TCS / lost wakeup, lock scope, atomic offsets → [study-notes](../learning/study-notes.md)
  - flaky tests, deterministic vs probabilistic assertions → [study-notes §11.65](../learning/study-notes.md#1165-test-design-deterministic-vs-probabilistic-assertions-and-flaky-tests-)
  - static analysis vs human review (why both) → [study-notes §11.8](../learning/study-notes.md#118-cicd--quality-gates)
- **Incidents** — the bugs and their fixes → [FIX-008](../decisions/decision-and-fix-log.md#fix-008--integration-test-could-hang-instead-of-failing-on-timeout-), [FIX-009](../decisions/decision-and-fix-log.md#fix-009--checkpoint-a-caught-a-latest-semantics-bug-in-the-channel-store-became-the-trigger-for-dec-015-), [FIX-010](../decisions/decision-and-fix-log.md#fix-010--empty-catch-exception-in-subscribeasync-silently-swallowed-partition-reader-faults-)
- **CI integration** — how the analyzer/Coyote gates run in the pipeline → [CI/CD & quality gates](../guides/ci-cd-and-quality-gates.md)
- **Design** — the log/pull model that the concurrency design rests on → [M2 partition log model](m2-partitioning.md), [plan](plan.md)
- **Decisions** — [DEC-013](../decisions/decision-and-fix-log.md#dec-013--risk-based-checkpoint-verification-replaces-per-step-hand-verification) (checkpoints), [DEC-015](../decisions/decision-and-fix-log.md#dec-015--delivery-model-confirmed-logpull-kafka-style-not-push-m2-redefined) (log model), and the reviewer sub-agent → [decision-and-fix-log](../decisions/decision-and-fix-log.md)

---

*Layer 5 (Coyote) is active as of M3c — the rebalance milestone whose coordinator concurrency is what it was
reserved for; it now covers the group coordinator and the committed-offset store, with the topic store and
remaining in-process core recorded as follow-up ([DEC-025](../decisions/decision-and-fix-log.md#dec-025--coyote-is-a-dedicated-assembly-its-coverage-is-a-documented-roadmap-not-a-label-)). Layer 3 is fully in place (threading analyzers
landed in [DEC-021](../decisions/decision-and-fix-log.md#dec-021--strengthen-roslyn-analyzers-to-block-fix-010-class-defects-at-build-time-) stage 2). The strategy itself is in force now.*
