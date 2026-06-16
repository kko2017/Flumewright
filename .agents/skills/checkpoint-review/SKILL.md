---
name: checkpoint-review
description: At a milestone checkpoint, stop and self-report per step, and run high-risk self-checks before the human verifies. Use when the CLI reaches a checkpoint marked in a milestone instruction, or when the user asks for a checkpoint report. Report and stop — do not flow past a checkpoint.
---

# checkpoint-review

Milestones group steps into a few **checkpoints placed by risk** (not by step count) instead of
hand-verifying every step. At each checkpoint the CLI stops and self-reports so the human can scan rather
than re-read every diff, then spot-check the high-risk steps.

> Full rules + risk taxonomy: `docs/guides/version-control-and-validation-guide.md` §7.6 (decision: §09
> DEC-013). Read §7.6 before reporting. This skill is the procedure; that section is the authority.

## When to run
- The CLI has reached a checkpoint marked in the milestone instruction (`NN-phaseX-mN-*`, e.g. "CHECKPOINT A").
- Each step up to the checkpoint is already its own commit that built and passed fast unit tests.
- Do NOT flow past the checkpoint — stop here and report.

## Behavior up to a checkpoint
- Run the steps up to the marked checkpoint, **each as its own commit** (grouped execution must NOT become a
  grouped commit — per-step commits keep `git log` traceable and individually revertible).
- **Stop immediately** — before reaching the checkpoint — if a build/test fails OR a decision is required
  that the plan does not cover. Do not push forward on a guess. Report the blocker and wait.

## Self-report (at the checkpoint)
For each step since the last checkpoint, report concisely:
- **What** the step did (file(s) touched, the change in one line).
- **Decisions / assumptions** made (anything not spelled out in the instruction).
- **Commit** id + message (so the human can `git show` any step).

Then list which steps in this group are **high-risk** (see taxonomy) and run their self-checks below.

## Risk taxonomy (high-risk → must be human-verified)
1. **Concurrency / shared-state** — channels, offsets, locks, parallel loops.
2. **Public-contract changes** — proto, public interfaces, SDK surface.
3. **Milestone completion-bar** — the integration/e2e test defining "the milestone works".
4. **Security boundaries** — certs, mTLS, auth.

Low-risk (no self-check needed): scaffolding, pure test-covered functions, docs/config.

## High-risk self-checks (run and report results, before the human looks)
Pick the checks that apply to the steps in this group. Examples by category:
- **Concurrency/state:** Are counters/offsets independent where they should be (not accidentally sharing one
  shared counter)? Are increments atomic (`Interlocked`)? On a bounded-channel `TryWrite` returning false,
  is accounting still correct and nothing thrown/lost? Are per-consumer/per-partition structures isolated?
- **Public contract:** Were existing fields/tags left unchanged (no renumbering / no breaking renames)? Is
  the change additive? Does the surface still match what callers expect?
- **Completion-bar test:** Does it actually assert the milestone's guarantee (not a hollow test)? Bounded
  timeout / `WaitAsync` so it fails fast instead of hanging?
- **Security:** No secrets committed? Boundaries enforced, not just present?

Report each check as pass / concern (with location). A concern is for the human to judge — do not auto-fix.

## After the report
- STOP. The human scans the report and spot-checks the high-risk steps, then approves or requests fixes.
- Approved fixes: explicit per-file `git add`, one logical commit each, `git show --stat HEAD` after each.
- Then proceed to the next checkpoint (repeat), and at the milestone end run the `zoomout-review` skill.

## Hard rules (inherited — see the VC guide)
- Local git only; the user does all remote operations.
- Per-file `git add` only; `git show --stat` after every commit; per-step commits, never a grouped blob.
- Build/test inside the dev container only.
- A CLI self-report is a starting point, not a conclusion — the human approves.
