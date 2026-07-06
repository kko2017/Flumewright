# AI Collaboration & Cost Strategy

> How Flumewright was built: a human-controlled, agent-assisted workflow with deliberate cost
> management. This document explains the collaboration model, the control mechanisms that keep the
> AI from over-reaching, the verification discipline, and how the whole thing was kept under a small
> monthly budget. Repo location: `docs/ai-collaboration.md`.

---

## 1. Why this document exists

Flumewright is a distributed message bus (C# / .NET 8 / gRPC, mTLS, at-least-once delivery). It was
built **with AI agents**, but the goal was never "let the AI write it." The goal was to demonstrate a
repeatable way to build a non-trivial, well-verified system **quickly and cheaply** while the human
stays firmly in control of architecture, verification, and every decision.

The interesting part of this project is not only the code — it is the **operating system around the
code**: how work is split, how the AI is constrained, how output is verified, and how cost is managed.

---

## 2. Collaboration model — who does what

| Role | Responsibilities |
|------|------------------|
| **Human (owner)** | Owns architecture & scope. Makes every design decision. Reviews code by hand. Triggers milestones. Performs all remote git (push/merge). Approves or rejects every AI proposal. |
| **Claude (planning/design)** | Drafts execution plans and step-by-step instructions. Designs the contract/data structures. Reviews code for correctness & design. Maintains the document system. Catches design issues before they reach the CLI. |
| **Gemini CLI (Antigravity, implementation)** | Executes the scoped instructions: scaffolding, writing code, running tests, local commits. Stops at defined boundaries and reports. Never pushes to remote. |

The human is the integration point. Neither AI talks to the repo's remote; neither AI decides scope.
The AIs are powerful tools operated within explicit boundaries — **assisted, not autonomous.**

---

## 3. Control mechanisms — keeping the agent on a leash

Agentic AI tends to over-reach: it adds features, refactors beyond scope, and "helpfully" implements
things you deferred. The following mechanisms were designed to prevent that.

- **Git boundary.** The CLI may only run *local* git (branch/add/commit). All remote operations
  (push/pull/fetch/remote) are performed by the human. The CLI stops at "Ready to push" and waits.
- **Small units, risk-based checkpoints.** Work is decomposed into steps; each step is its own
  built-and-tested commit. Rather than hand-verifying every step (which doesn't scale), steps are grouped
  into a few **checkpoints placed by risk** — the CLI runs to a checkpoint, stops, and self-reports; the
  human scans the report and spot-checks the high-risk steps (concurrency/shared-state, public-contract
  changes, completion-bar tests, security boundaries). The CLI stops immediately on any failure or
  unplanned decision. This keeps every change reviewable while removing the per-step bottleneck.
- **Explicit scope fences.** Instructions list what is *deliberately deferred* ("no partitions — that's
  M2; no mTLS — that's M4") and say "do not add them." This stops the agent from pulling future work in.
- **End-of-milestone zoom-out review.** At each milestone the CLI reviews the whole codebase — but with
  a *bounded* question (correctness/consistency/leaks within scope only) and **report-only** output,
  classified into [correctness/bug] / [consistency/cleanup] / [out-of-scope]. The human approves before
  any fix. The decision log (which lists intentionally-deferred items) is attached as a guardrail so the
  agent doesn't re-flag them.
- **Standing rules over per-prompt reminders.** The CLI's durable rules live in a `GEMINI.md` the agent
  reads every session; every instruction opens by pointing at it. This survives session resets (the CLI
  has no memory across sessions) and keeps the git boundary, per-file staging, and stop rule in force
  without re-stating them each time.
- **Discipline holds through the finish, not just the start.** The most dangerous moment is the wrap-up:
  once a milestone "looks done", both the agent and the reviewer relax — completion inertia. A late slice
  is treated with the same checkpoint rigor as the first (this caught an EKU security gap in mTLS at the
  *last* code checkpoint, after the CLI and a review sub-agent had both passed it). "A completion signal
  does not suspend the rules" is written into the standing rules for exactly this reason.

---

## 4. Verification discipline — a passing test is not a conclusion

An AI report of "all tests pass" is a *starting point*, not proof. The CLI makes tests pass quickly;
that does not mean the design is right. So every step was verified **by hand** against the code.

This caught real defects the AI reported as successes:

- **FIX-001 — broken fan-out.** The store used one shared channel per topic; with 2+ subscribers a
  message reached only one. Tests passed because they used a single subscriber. Caught by reading the
  code, not the report.
- **FIX-002 — cancellation-token misuse / blocking fan-out.** Publish applied the *publisher's* token
  to *subscriber* writes and awaited per subscriber. Caught in code review, fixed with non-blocking
  `TryWrite`.

Later milestones produced stronger instances of the same discipline:

- **FIX-021 — a flaky test nearly misdiagnosed as a product bug.** A CI-only e2e failure looked like a
  rebalance defect; investigating (rather than "fixing" the product) proved the product self-heals and the
  *test* was timing-fragile. Diagnose before fixing: a red CI is a symptom, not a diagnosis.
- **FIX-023 — an EKU security gap caught by an independent code read.** mTLS certificate validation checked
  the chain but not the certificate's *purpose* (EKU), so a CA-signed server cert would have passed as a
  client cert. Neither the CLI nor the review sub-agent flagged it; the orchestrator's own read of the code
  at the checkpoint did. Three independent layers, and only the third caught it — the case for reading the
  code, not the report.
- **A flaky test surfaced by a dependency bump.** A test-runner upgrade exposed a latent timing-dependent
  unit test that had been passing by luck; the product was correct. Confirmed flaky (re-run passed), then
  quarantined for a test-only fix rather than a rushed change.

These are recorded in the decision-and-fix log with cause and future impact. The lesson encoded into
the workflow: **read the code, verify the tests actually assert, never trust the summary alone** — and when
a flaky test has a history, raise the verification bar (the mTLS reject tests were run 20×, not once).

---

## 5. Cost strategy — deliberate, not accidental

Total tooling budget: **under $60/month** (Claude subscription + Gemini Pro). The point isn't that it
was cheap — it's that spend was *allocated on purpose*.

- **Task-difficulty routing.** Trivial edits (e.g. a one-line channel fix) are done by hand, not sent
  to the CLI. Only non-trivial implementation is delegated. This conserves the agent's quota for where
  it adds the most value.
- **Two AIs, by strength.** Claude for planning, design, document maintenance, and code review;
  Gemini CLI for large-context codebase work and execution. Each is used where it's strongest rather
  than forcing one tool to do everything.
- **Quota-aware pacing.** When a quota limit is hit, small work proceeds by hand and planning/document
  work continues, so progress never fully stalls and momentum is preserved.
- **Context handoff to cut re-reads.** A living handoff note (`FW_Context`) lets a fresh AI session
  recover state from a few attached files instead of re-deriving everything — fewer wasted tokens per
  session.

---

## 6. The document system

A small set of documents makes the workflow repeatable and auditable:

| Document | Role |
|----------|------|
| `docs/design/plan.md` | Scope, architecture, milestone roadmap — the single source of truth |
| `docs/learning/study-notes.md` | Concept notes (PubSub, delivery guarantees, DI/Dispose, …) |
| `docs/guides/version-control-and-validation-guide.md` | Commit/validation rules + the milestone-review protocol |
| `docs/decisions/` (ADRs) | Major architectural decisions |
| `docs/decisions/decision-and-fix-log.md` | Lightweight log of mid-build fixes & smaller decisions (symptom → cause → fix → future impact) |
| *(personal, not committed)* handoff note + step instructions + verification log | Drive day-to-day execution and recovery |

All repository text is English; Korean drafts are kept outside the repo for personal reference.

**Designed for a memory-less reader.** A subtle but load-bearing principle: the primary readers of these
docs are AI sessions that reset between conversations — the orchestrator and the CLI both start each session
knowing nothing. What suffices for a human (a table of contents, a heading) is not enough for a reader with
no memory to trigger: a heading is a recall cue, and an AI has no recollection to cue. So the doc system
leans on devices that serve the memory-less reader — stable **ID schemes** (DEC/FIX numbers) for precise
cross-reference, a **one-line index** of every decision (so "what was DEC-031?" is answered by scanning, not
by opening each entry), **fixed entry formats** (symptom → cause → fix → future) so partial reads work, and
**self-contained instructions** that re-brief context every time. The general lesson: **when a document's
readers include AI, document design changes — build for the reader with no memory.**

---

## 7. What this demonstrates

- **AI as a tool under control**, not a replacement for engineering judgment.
- A **repeatable, auditable process**: scoped delegation → build → hand-verification → milestone review.
- **Architecture ownership**: every decision is the human's, recorded and defensible.
- **Cost discipline**: deliberate routing of work across tools within a small budget.
- A system designed for **low defect risk and high extensibility** — verified incrementally, with
  deferred work explicitly tracked rather than forgotten.
