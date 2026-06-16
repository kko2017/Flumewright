# Flumewright — Agent Rules (always-on)

You are working on Flumewright (distributed message bus, C#/.NET 8/gRPC). These rules apply to **every**
turn. They are a summary + pointers — the authoritative detail lives in the repo docs; read them when a
task needs the specifics. Do not restate or duplicate those docs here.

## Git boundary (non-negotiable)
- **Local git only**: init/add/commit/branch/checkout/merge/tag/status/log/diff/config.
- **NEVER** run push / pull / fetch / remote / gh. All remote operations are done by the user.
- **Stage per file with explicit paths**: `git add <path>`. NEVER `git add .` or `git add -A`.
- After **every** commit, run `git show --stat HEAD` and show the output — confirm only intended files changed.
- Never rewrite shared history. `reset`/`amend` only on un-pushed local branches.

## Work rhythm
- **One step = one commit** that builds and passes fast unit tests (the pre-commit hook enforces this).
- Grouped execution (running several steps to a checkpoint) must NOT become a grouped commit — keep per-step commits.
- **Stop immediately** and report if a build/test fails OR a decision is needed that the plan does not cover. Never push forward on a guess.
- Do **one step (or one checkpoint group) at a time**; do not start the next until told.
- Conventional Commits format (`feat`/`fix`/`test`/`refactor`/`perf`/`docs`/`chore`).

## Scope discipline
- Build only what the current milestone's instruction asks. Do **not** add features, future-milestone work, or out-of-scope refactors.
- Items recorded as **intentionally deferred** in the decision-and-fix log are NOT defects and must not be "helpfully" added or flagged. The decision log is the guardrail.

## Verification philosophy
- A passing test or a "looks clean" report is a **starting point, not a conclusion**. The human is the approver.
- For any doc/file copy or sync, verify the content actually landed (markers + line counts) before committing — do not trust that a copy "ran".

## Environment
- All repository text is **English** (code, comments, commit messages, docs). Korean stays in personal drafts only (never committed).
- Build/test **inside the dev container** only (host builds pollute `obj/` with foreign UIDs).
- Do not use `--dangerously-skip-permissions`. Permission review stays on.

## Where the detail lives (read when needed)
- `docs/design/plan.md` — scope, architecture, milestone roadmap.
- `docs/guides/version-control-and-validation-guide.md` — commit rules, validation gates, §7.5 zoom-out review, §7.6 risk-based checkpoints.
- `docs/decisions/decision-and-fix-log.md` — decisions + fixes; the **intentionally-deferred guardrail list**.

## Skills (use the right one; restart `agy` after adding/editing a skill)
- `checkpoint-review` — at a milestone checkpoint: self-report per step + high-risk self-checks, then stop.
- `zoomout-review` — end-of-milestone whole-codebase review (report-only, 3 buckets, scope-fenced).
- `docs-sync` — sync changed root `NN-*.en.md` into `docs/` paths with verification, then commit.

## Never
- No secrets in the repo (certs/keys: `*.pfx`/`*.key`/`*.pem`/...). Generate locally, commit only generators.
- No malicious code, no weakening of the permission/validation gates.
