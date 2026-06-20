---
name: code-review
description: At a checkpoint (and at end-of-milestone zoom-out), have an ISOLATED reviewer sub-agent inspect the accumulated diff against a concurrency/exception/flaky-test checklist, and report findings (with a fix / suppress / human-judgment recommendation each) — report only, never edit. Use when the CLI reaches a checkpoint, or when the user asks for a code review of a diff. The reviewer is a fresh, narrow set of eyes; the main agent relays its findings verbatim plus its own opinion.
---

# code-review

A separate, **isolated reviewer sub-agent** inspects a diff with fresh eyes against a fixed checklist.
The author of code (the main agent) is biased toward its own work — that bias is exactly how FIX-010
(a swallowed-exception bug) passed checkpoint and zoom-out review. An isolated reviewer with a narrow
mandate and an explicit checklist catches mechanical/concurrency hazards a self-review skims past.

> This complements, does not replace, the other layers: `checkpoint-review` (the main agent's own
> structured self-report), static analysis (CA1031 etc. — caught at build), and Coyote (systematic
> concurrency, M3+). See study-notes §11.8 and the concurrency-strategy doc for the full defense-in-depth.

## When to run
- **At each checkpoint**, automatically: after the main agent finishes the steps up to a checkpoint and
  BEFORE it writes its `checkpoint-review` self-report. The reviewer's findings are folded into that report.
- **At end-of-milestone**, called by `zoomout-review` over the milestone's full diff.
- The user may also invoke it manually on any diff.
- NOT per commit (too noisy, too small to see cross-step concurrency issues).

## How it runs (cost-controlled)
- The main agent spawns a reviewer **sub-agent** with an **isolated context** (fresh eyes; same model is
  fine — the value is isolation + a narrow mandate + the checklist, not a different model).
- Pass the reviewer **only the diff since the last checkpoint plus the directly-relevant files** — NOT the
  whole codebase, NOT the full conversation. This keeps tokens bounded.
- **One pass, report only. No back-and-forth dialogue.** The reviewer returns its findings and is done.
- The reviewer **must not** edit, stage, build, or run git. Report only.

## Checklist — production code
- **Concurrency:** unawaited `Task` / fire-and-forget; `async void`; shared state touched outside its lock;
  race on offsets/counters (two threads updating the same value); TCS lost-wakeup (no re-check under lock,
  missing `RunContinuationsAsynchronously`); cancellation token not propagated.
- **Exceptions:** swallowed exception (empty `catch`); broad `catch (Exception)` with no logging AND no
  recovery (note: CA1031 already blocks this at build — flag any that slipped through a suppression);
  background-task exception not surfaced to its awaiter.
- **Consistency:** a public-contract change (proto / public API) not reflected in docs or tests; new code
  with no test covering it.

## Checklist — test code (flaky-test prevention)
Test code is concurrency-heavy here (background `Task.Run`, real Kestrel ports, real gRPC, cancellation +
timeouts), so it gets its own checks — a flaky or false-passing test is worse than no test (§11.65).
- **Flaky:** timing-based synchronization (`Task.Delay` / `Thread.Sleep` used to "wait" instead of a real
  signal/TCS); an unbounded wait with no timeout (could hang — see FIX-008); a tight assertion on a
  probabilistic result (deterministic vs probabilistic confusion, §11.65).
- **False pass:** a background/async result that is never asserted on the main thread; an exception caught
  and dropped so an assertion failure is hidden; a background failure that never propagates to the test
  (the opposite-good pattern is marshaling via `TrySetException` — see PublishSubscribeE2ETests).

## Recommendation per finding (three levels)
For EVERY finding, attach exactly one recommendation:
- **[fix]** — a clear defect (swallow, race, hidden assertion failure, hang). State the smallest fix in words.
- **[suppress + reason]** — the construct is flagged but legitimate (e.g. a background-task exception
  marshaled to the test thread via `TrySetException`; a top-level boundary handler that logs and recovers).
  Propose the exact suppression + the one-line reason comment to put on it. Use ONLY when legitimacy is
  clear.
- **[human judgment]** — ambiguous; do NOT decide it is fine. Escalate to the human. When unsure between
  fix and suppress, choose this — never silently wave something through (that is how swallowed bugs return).

## How the main agent relays it (no filtering)
The main agent MUST NOT summarize away or quietly drop the reviewer's findings. In its checkpoint report:
1. **Main self-report** (from `checkpoint-review`).
2. **Reviewer sub-agent report — verbatim.** All findings + their [fix]/[suppress]/[human judgment] tags.
3. **Main agent's opinion**, finding by finding: agree / disagree + why. **Disagreements are stated
   explicitly and surfaced to the human** — a divergence between the two agents is precisely what the human
   should look at. The main agent may not overrule a reviewer concern into silence.

## After the report
- STOP. The human reads both voices and decides. Approved fixes/suppressions: explicit per-file `git add`,
  one logical commit each, `git show --stat HEAD` after each.
- `[fix]` items that were real defects become FIX entries in the decision-and-fix log.

## Hard rules (inherited)
- Report only — the reviewer never edits, builds, or runs git.
- Local git only; the user does all remote operations.
- A "looks clean" from either agent is a starting point, not a conclusion — the human approves.
