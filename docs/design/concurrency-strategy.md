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
| **Swallowed exception** | A background reader faults, the exception is caught and discarded → the reader dies invisibly and the subscriber simply never sees that partition's messages (this is exactly FIX-010). |
| **Cancellation mishandled** | A cancel is treated as an error (or an error as a cancel) → either spurious failures or silent hangs. |
| **Deadlock** | Holding a lock across an `await`, or lock-ordering inversions → threads wait on each other forever. |
| **Flaky test** | A test that synchronizes on timing (`Sleep`/`Delay`) or asserts a tight value on a probabilistic result → passes and fails without code changes, destroying trust in CI (worse than no test). |

These are the failure modes the layers below are designed to catch.

---

## 3. Defense in depth — five layers 🔒

No single technique catches all concurrency bugs; each layer sees what the others miss. The point is that a
hazard has to pass through **all five** to reach `main`.

| Layer | What it does | Tooling | Status |
|-------|-------------|---------|--------|
| 1 | Code patterns (prevent at write time) | source discipline (locks, TCS, cancellation) | in place |
| 2 | Human checkpoints + isolated AI reviewer | risk-based checkpoints (DEC-013), `code-review` skill (Gemini sub-agent) | in place |
| 3 | Static analysis (mechanical, build/CI) | **Roslyn analyzers** (CA1031, **VSTHRD threading**), **SonarCloud**, **CodeQL** | in place |
| 4 | Concurrency tests (behavioral) | **xUnit** concurrency tests, flaky-test discipline | in place |
| 5 | Systematic concurrency exploration | **Microsoft Coyote** | planned (after M3) |


### Layer 1 — Code patterns (prevention at write time) *— in place*
Disciplined patterns in the source itself:
- **Atomic offset assignment** under a lock (or `Interlocked`), so concurrent appends never collide.
- **Lost-wakeup-safe TCS**: re-check the condition under the lock before waiting, `RunContinuationsAsynchronously`, and complete the TCS outside the lock.
- **Lock scope correct**: shared state is only touched inside its lock; `await` happens outside it (no lock held across await).
- **Cancellation is normal shutdown**: `OperationCanceledException` is caught and treated as a clean stop; other exceptions are *not* swallowed but propagated to the subscriber (via channel completion with the exception).

### Layer 2 — Human checkpoints + an isolated reviewer sub-agent *— in place*
Tooling: **risk-based checkpoints (DEC-013)** + the **`code-review` skill** (an isolated Gemini sub-agent).
- **Checkpoints**: concurrency/shared-state steps are high-risk and stop for human verification, with explicit self-checks (atomic increments? isolated per-partition state? lost-wakeup-safe?).
- **Reviewer sub-agent**: a separate, isolated agent inspects the diff with fresh eyes against a concurrency/exception/flaky-test checklist, tagging each finding fix / suppress / human-judgment. The author of code is biased toward its own work; an isolated reviewer is not.

### Layer 3 — Static analysis (mechanical, at build/CI time) *— in place*
Four independent analyzers already run:
- **Roslyn analyzer — CA1031 = error** *(in place)*: a broad `catch (Exception)` fails the build, so the swallowed-exception class of defect cannot return to production code (tests annotate the two legitimate marshaling sites). This is the build-time lock on FIX-010.
- **SonarCloud** *(in place)*: the quality gate flags empty catches, code smells, and some vulnerabilities on every PR — this is the tool that first surfaced FIX-010.
- **CodeQL** *(in place)*: security SAST (data-flow). More security than concurrency, but the same "mechanical detection" layer.
- **Roslyn threading analyzers (VSTHRD)** *(in place)*: unawaited `Task` / `async void` / synchronous blocking on async flagged at build, catching fire-and-forget mistakes that are a common source of concurrency bugs. Applied to src + tests (DEC-021 stage 2); the first install was clean in src/ and caught one synchronous-cancel issue in tests (VSTHRD103).

### Layer 4 — Concurrency tests (behavioral) *— in place*
Tooling: **xUnit** concurrency tests + flaky-test discipline.
- **Concurrency unit tests**: e.g. 1,000 concurrent appends asserting offsets are unique and contiguous — exercising lock integrity and atomic state.
- **Flaky-test discipline**: deterministic properties get tight assertions; probabilistic ones get generous ranges; every async wait has a bounded timeout so a failure fails fast instead of hanging (FIX-008). A flaky test is treated as a defect, not noise.

### Layer 5 — Systematic concurrency testing (Microsoft Coyote) *— planned, after M3*
Tooling: **Microsoft Coyote**.
- Coyote rewrites and controls task scheduling to **deterministically explore interleavings**, reproducing races and deadlocks that ordinary tests hit only by luck. Reserved for after M3, when consumer-group assignment and offset-commit concurrency make the schedule space large enough to need it. This is the answer to "as the code grows, these bugs get harder to find by reading": a machine explores the schedules a human or an ordinary test never will.

---

## 4. Track record — bugs these layers actually caught 🔒

Concurrency defense is not theoretical here; the layers have already paid off:

- **FIX-009 — lost-wakeup / LATEST-drop bug.** The first channel-based store drained bounded per-partition
  channels into an unbounded merged channel, so the intended drop semantics never applied. Caught by a
  **human checkpoint review** (Layer 2), and it became the trigger for switching to the log/pull model
  (DEC-015). A bug a static analyzer could never have reasoned about.
- **FIX-010 — swallowed exception in the partition reader.** An empty `catch (Exception)` discarded every
  fault from the background reader. **Both** the checkpoint review and the end-of-milestone zoom-out missed
  it; **static analysis (SonarCloud / CA1031)** — Layer 3 — caught it on day one. A bug a human reads past.

The lesson that shapes this whole document: **FIX-009 and FIX-010 were caught by different layers.** Human
reasoning catches semantic/concurrency bugs a person can think through; mechanical analysis catches the
empty-catch a person skims over. Neither layer alone is enough — which is why there are five.

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
- **Decisions** — DEC-013 (checkpoints), DEC-015 (log model), and the reviewer sub-agent → [decision-and-fix-log](../decisions/decision-and-fix-log.md)

---

*Layer 5 (Coyote) is still planned — marked above — and will be filled in after M3. Layer 3 is now fully in
place (threading analyzers landed in DEC-021 stage 2). The strategy itself is in force now.*
