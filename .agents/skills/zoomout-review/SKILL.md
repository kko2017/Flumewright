---
name: zoomout-review
description: Run the end-of-milestone zoom-out code review (report-only, three-bucket classification, scope-fenced to the current milestone). Use when a milestone's last step is committed and the user asks for a zoom-out review or end-of-milestone review before merge. Report only — never edit code.
---

# zoomout-review

Whole-codebase review at the **end of a milestone, before merge**. Per-step verification has a narrow view
and misses consistency/duplication/leak issues that only emerge after several steps stack up. This review
fills that gap — but a bounded scope is mandatory, or it becomes an entry point for needless refactors and
scope creep.

> Full rules: `docs/guides/version-control-and-validation-guide.md` §7.5. Read it before reviewing.
> This skill is the procedure; that section is the authority.

## When to run
- The milestone's last feature step is committed (e.g. the integration test that defines "the milestone works").
- After the checkpoint reviews (§7.6), before the user merges to main.
- NOT per step. NOT mid-milestone.

## Inputs to read first (the guardrail matters)
Read these before reviewing:
- `docs/decisions/decision-and-fix-log.md` — **the guardrail.** Its intentionally-deferred items are NOT
  defects; do not flag them.
- `docs/design/plan.md` — scope and roadmap (what belongs to this milestone vs later).
- `docs/guides/version-control-and-validation-guide.md` — the review protocol (§7.5).
- The handoff note (FW_Context) if available — current state.

Do NOT use the verification log (`08-*`) — it is the user's personal check record, intentionally excluded.

## Hard constraints
1. **REPORT ONLY.** Do not edit, create, delete, or stage any file. Do not run git, build, or tests. Output
   is a written report and nothing else. The user decides what (if anything) gets fixed.
2. **Scope fence.** Only correctness, consistency, and resource-leak issues **within the current milestone's
   scope.** No new features, no future-milestone (M+1) proposals, no out-of-scope refactors.
3. **Intentionally-deferred items are not defects.** Anything the decision-and-fix log records as
   deliberately deferred (e.g. unbounded vs bounded tradeoffs, unary vs streaming, missing RPCs that belong
   to a later milestone) is EXPECTED and correct — do not flag it. If you think one is a problem, you have
   misread the scope; leave it out.

## Output — classify EVERY finding into exactly one bucket
- `[correctness/bug]` — a real defect to fix now, within scope. Give file/location, one-line description,
  why it matters, and the smallest fix in words (not applied).
- `[consistency/cleanup]` — naming/pattern alignment; the user decides whether to fix.
- `[out-of-scope — record only]` — future-milestone related; do not propose a code change, just note it.

If a bucket is empty, say so explicitly. A clean result is valid — do NOT invent findings to fill buckets.

## After the report
- STOP. Wait for the user to choose what to fix (usually 1–2 `[correctness/bug]` items).
- `[correctness/bug]` fixes become FIX entries in the decision-and-fix log; `[out-of-scope]` items become
  notes/DEC there. This review is one of that log's input sources.
- When fixing approved items: explicit per-file `git add`, one logical commit each, `git show --stat HEAD`
  after each. Then the user merges (merge commit, not squash). No tag unless it is the Phase end.

## Hard rules (inherited — see the VC guide)
- Local git only; the user does all remote operations.
- Build/test inside the dev container only.
- A CLI "passed" or "looks clean" is a starting point, not a conclusion — the human approves.
