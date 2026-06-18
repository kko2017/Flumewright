# Flumewright — Version Control & Validation Workflow Guide

> Purpose: enforce "don't build everything at once; validate and commit each small working unit."
> Use this document directly as the reference ruleset when working with the Antigravity CLI.
> Environment: GitHub + GitHub Actions / main + milestone-branch strategy / .NET 8 (LTS)

---

## 1. Core Principle

> **One commit = one validated small change.**
> The code at the moment of commit must always be in a "builds and passes tests" state.

Why:
- If something breaks, you can trace which commit broke it (`git bisect`).
- Easy to revert (just that one commit).
- Eliminates the pain of reviewing/debugging huge change blobs.

### Three Levels of Granularity

| Level | Unit | Frequency | Git representation |
|-------|------|-----------|--------------------|
| Commit | Smallest working change + test | Several times a day | commit |
| Milestone (M1–M6) | Feature unit | A few days | branch → merge |
| Phase (1, 2) | Release unit | Weeks–months | tag (v0.x.0) |

---

## 2. Branch Strategy (Simple Model)

```
main  ──●──●──────●──────────●────  (always green, tag v0.x.0)
         \        /          /
          ●──●──●  (M2)     /        ← milestone branch
                   \       /
                    ●──●──●  (M3)
```

Rules:
- **Keep main always green (tests passing).** Never push broken code directly.
- Branch per milestone: `feat/m2-partitioning`, `feat/m3-delivery`, etc.
- Stack small commits inside the milestone branch.
- Merge into main on milestone completion + CI pass (PR recommended, useful for records even solo).
- Tag on Phase completion: `git tag v0.1.0`.

### Branch Naming
| Prefix | Use | Example |
|--------|-----|---------|
| `feat/` | New feature (milestone) | `feat/m4-mtls` |
| `fix/` | Bug fix | `fix/ack-timeout-leak` |
| `refactor/` | Behavior-preserving improvement | `refactor/router-cleanup` |
| `test/` | Tests only | `test/load-burst-100k` |
| `chore/` | Build/config/tooling | `chore/ci-cache` |

---

## 3. Commit Message Rules (Conventional Commits)

Format:
```
<type>(<scope>): <description>

[body — why the change (optional)]
```

Types:
| Type | Meaning |
|------|---------|
| `feat` | New feature |
| `fix` | Bug fix |
| `test` | Add/modify tests |
| `refactor` | Behavior-preserving code change |
| `perf` | Performance improvement |
| `docs` | Documentation |
| `chore` | Build/config/dependencies |

Examples:
```
feat(broker): implement partition hash routing
fix(delivery): fix missed redelivery after ack timeout
test(router): add same-key -> same-partition guarantee test
perf(channel): reuse message buffers via ArrayPool
chore(ci): add NuGet cache
```

Effect: a readable history, plus the ability to auto-generate CHANGELOG and versions later.

---

## 4. The "Feature -> Test -> Commit" Loop (TDD-friendly)

Cycle through this for each small task:

```
1. Decide one small feature (e.g., "Partitioner: key hash -> partition number")
2. Write the test first (or alongside)
3. Implement
4. Run dotnet test locally -> confirm pass
5. git add + commit (Conventional Commit)
6. Move to the next small feature
```

When a milestone is done:
```
7. Confirm whole-branch CI pass
8. PR / merge to main
9. (If Phase end) tag
```

**Example CLI instruction:**
> "First task of M2: implement PartitionRouter, but first write the xUnit tests
> (same key -> same partition, distribution uniformity); after implementing, if `dotnet test`
> passes, commit as `feat(router): consistent hash partition routing`. One feature per commit."

---

## 5. Validation Gates (Automation)

Force validation in two layers so a human forgetting cannot bypass it.

### 5.1 Local first gate — pre-commit hook
On commit, run build + fast unit tests. Block the commit on failure.

`.githooks/pre-commit` (needs execute permission: `chmod +x`):
```bash
#!/usr/bin/env bash
set -e
echo "> pre-commit: build + fast unit tests"
dotnet build --configuration Debug --nologo
# Fast unit tests only (excludes load/integration)
dotnet test tests/Flumewright.UnitTests --no-build --nologo \
  --filter "Category!=Load&Category!=Integration"
echo "OK pre-commit passed"
```

Register the hook (to version-control hooks in the repo):
```bash
git config core.hooksPath .githooks
```

> If unit tests get slow, commits become painful. Keep **only fast unit tests** in pre-commit;
> run heavy integration/load tests in CI.

### 5.2 Remote second gate — GitHub Actions CI
Full validation on push / PR. Cannot merge to main without passing (branch protection).

`.github/workflows/ci.yml`:
```yaml
name: CI

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

permissions:
  contents: read
  checks: write
  pull-requests: write

jobs:
  build-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6

      - name: Setup .NET
        uses: actions/setup-dotnet@v5
        with:
          dotnet-version: '8.0.x'

      - name: Cache NuGet packages
        uses: actions/cache@v4
        with:
          path: ~/.nuget/packages
          key: ${{ runner.os }}-nuget-${{ hashFiles('**/*.csproj') }}
          restore-keys: ${{ runner.os }}-nuget-

      - name: Restore
        run: dotnet restore

      - name: Build
        run: dotnet build --no-restore --configuration Release

      - name: Unit & Integration tests
        run: >
          dotnet test --no-build --configuration Release
          --filter "Category!=Load"
          --logger "trx;LogFileName=test-results.trx"
          --results-directory TestResults

      - name: Publish test results
        uses: dorny/test-reporter@v1
        if: always()
        with:
          name: 'Test Results'
          path: 'TestResults/*.trx'
          reporter: dotnet-trx
```

> Exclude load/stress tests (`Category=Load`) from the default CI flow and run them in a separate
> workflow (manual `workflow_dispatch` or nightly schedule). Running 100K load on every CI run is
> slow and flaky.

### 5.3 Dedicated load-test workflow (optional)
`.github/workflows/load.yml` (manual + nightly):
```yaml
name: Load Tests
on:
  workflow_dispatch:        # manual run
  schedule:
    - cron: '0 3 * * *'     # daily 03:00 (UTC)
jobs:
  load:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - uses: actions/setup-dotnet@v5
        with:
          dotnet-version: '8.0.x'
      - run: dotnet test tests/Flumewright.LoadTests --filter "Category=Load" --configuration Release
```

### 5.4 Branch protection rules (GitHub settings)
For the main branch:
- "Require status checks to pass before merging" -> require CI (build-and-test).
- "Require branches to be up to date before merging" recommended.
- (Even solo) merge via PR to keep records.

---

## 6. Splitting Gates by Test Category

Tag tests with categories to run different sets at each gate.

```csharp
[Fact]
[Trait("Category", "Unit")]
public void Router_SameKey_GoesToSamePartition() { ... }

[Fact]
[Trait("Category", "Integration")]
public async Task EndToEnd_PublishToSubscribe_OverGrpc() { ... }

[Fact]
[Trait("Category", "Load")]
public async Task Burst_100k_NoMessageLoss() { ... }
```

| Gate | Runs | When |
|------|------|------|
| pre-commit (local) | Unit | on commit |
| CI (push/PR) | Unit + Integration | before merge to main |
| Load (manual/nightly) | Load | separate |

---

## 7. Per-Milestone Commit Plan (Example Flow)

Each milestone decomposes into several small commits. M2 example:

```
feat/m2-partitioning branch
├─ test(router): add partition routing test
├─ feat(router): implement consistent hash partition routing
├─ test(channel): bounded channel enqueue/dequeue test
├─ feat(channel): introduce per-partition bounded channel
├─ feat(broker): topic->partition mapping + multi-threaded consume loop
├─ test(broker): multi-threaded concurrent consume consistency test
└─ docs: add M2 design notes
        │
        ▼  CI pass
   merge to main → (at Phase 1 end) tag v0.1.0
```

Principle: **each line (commit) is itself in a build/test-passing state.**

---

## 7.5 End-of-Milestone Review (zoom-out code review)

When a milestone (M1, M2…) finishes, have the CLI zoom out and review the whole codebase **before**
merging to main. Per-step verification (done by the human) has a narrow view and can miss the
consistency / duplication / leak issues that emerge only after several steps stack up. This review
fills that gap. **But without a bounded scope it becomes an entry point for needless refactors and
feature creep**, so enforce the rules below.

### Rules
1. **Timing:** end of milestone only (NOT per step). After the milestone's last-step commit, before merge.
2. **Narrow the question:** "any improvements?" (open) is banned. **Only correctness, consistency, and
   resource-leak issues, within the current milestone's scope.** Exclude new features, future-milestone
   (M2+) proposals, and out-of-scope refactors. In particular, **intentionally deferred choices**
   (e.g. unbounded channels, unary publish) are NOT defects — do not flag them.
3. **Report only, no edits:** the CLI classifies findings and **reports only**. It must not touch code
   before the user approves.
4. **Force classification** into three buckets:
   - `[correctness/bug]` — a real defect to fix now
   - `[consistency/cleanup]` — naming/pattern alignment (decide whether to fix)
   - `[out-of-scope — record only]` — future-milestone related → don't edit code, log it in 09

### Flow
```
last-step commit of the milestone done
      │
      ▼
[CLI] zoom-out review → report only, in 3 buckets (no edits)
      │
      ▼
[user review] per bucket: fix now / defer to 09 / ignore
      │
      ▼
[CLI] edit only approved items → commit  (usually 1–2)
      │
      ▼
   merge to main → (if Phase end) tag
```

> Of the findings: `[correctness/bug]` fixes become FIX entries in the 09 decision-and-fix log;
> `[out-of-scope]` items become DEC/notes. This review is one of 09's input sources.

### Running the review
Use the **`zoomout-review` skill**, which encodes this procedure: it reads the decision-and-fix log (the
"intentionally deferred" list is the guardrail that prevents wrongly flagging deferred items as bugs), keeps
the scope fenced to the current milestone, classifies findings as [correctness/bug] / [consistency/cleanup] /
[out-of-scope — record only], and **reports only** — no edits until the user approves. (The verification log
is intentionally NOT used — it is the user's personal check record.)

### Milestone wrap-up sequence (the review is one step in this flow)
A milestone is **triggered by the user** (not automatic — the user decides where the milestone ends)
and closed in this order:
1. Last feature step committed (e.g. M1 = Step 5 integration test passing).
2. **docs/ sync + design note** (M1 = Step 6): re-copy (overwrite) the changed English documents into
   their repo `docs/` paths to refresh them (use the `docs-sync` skill), update the milestone design note
   (`docs/design/mN-*.md`) and the README "Quick Start", and `docs:` commit.
3. **7.5 zoom-out review** (instruction above) → fix only 1–2 [correctness/bug] items after approval.
4. Reflect outcomes into 08 (verification) and 09 (fix/decision).
5. Confirm CI green → **user** merges to main → (if Phase end) tag.

> So docs/ sync is not "every time" but **bundled into milestone wrap-up (step 2)** — cleaner history,
> and Step 6 is already the docs step, so it absorbs naturally.

---

## 7.6 Risk-Based Checkpoint Verification (instead of per-step)

Verifying **every** step by hand does not scale across Phase × Milestone × Step, and most of that effort
is spent on low-risk steps. But "let the AI run and check at the end" is unsafe — a wrong early step
compounds (M1's FIX-001/002 were both caught at the step that introduced them; if buried under three later
steps they'd have been 4× costlier to undo). So group steps into a few **checkpoints**, but place the
boundaries by **risk**, not by step count.

### Classify each step by risk
**MUST be human-verified at a checkpoint (high-risk):**
1. **Concurrency / shared-state logic** — channels, offsets, locks, parallel loops. (Every M1 fix lived here.)
2. **Public contract changes** — proto, public interfaces (`ITopicStore`, SDK surface). A wrong contract makes every later step build on a bad foundation.
3. **Milestone completion-bar steps** — the integration/e2e test that defines "the milestone works".
4. **Security boundaries** — certificates, mTLS, auth. One mistake here is catastrophic and easy to miss.

**May be grouped and flowed through (low-risk):**
- Scaffolding, file moves, configuration.
- Pure, deterministic functions well covered by unit tests (e.g. a hash router).
- docs / comments.

### How checkpoints work
- The CLI runs the steps **up to a checkpoint**, then STOPS and reports. A checkpoint is placed **right after a high-risk step**, not at an arbitrary step count.
- **Each step is still its own commit** that builds + passes fast tests (§1 unchanged). Grouped *execution* must not become a grouped *commit* — per-step commits keep `git log` traceable and individually revertible at the checkpoint.
- At each checkpoint the CLI **self-reports**: for each step, what it did and any decision/assumption made. The human scans this rather than re-reading every diff, then spot-checks the high-risk steps against the code.
- The CLI **stops immediately** (does not flow on) if a build/test fails OR it must make a decision not covered by the plan. No pushing three steps forward on a guess.
- High-risk steps carry a **self-check list** in the instruction/skill (e.g. for a partition store: "is the offset per-partition independent? is the counter `Interlocked`? does the `TryWrite`-false path keep offset accounting intact?"), so the CLI filters obvious defects before the human looks.

### Checkpoint density varies by milestone
A concurrency-heavy milestone (e.g. M2 partitioning/parallel consume) deserves tighter checkpoints; a
low-risk one (e.g. M6 observability wiring) can be grouped more loosely. The milestone instruction
(`NN-phaseX-mN-*`) marks **where the checkpoints fall** in its step map.

### Flow (per milestone)
```
[CLI] run steps up to checkpoint A (each step = its own commit, build+test green)
      │
      ▼
[CLI] checkpoint A self-report (per-step: what + decisions)
      │
      ▼
[human] scan report + spot-check high-risk steps → approve / fix
      │
      ▼  (repeat for checkpoint B, …)
      │
[CLI] zoom-out review (§7.5) → human review
      │
   wrap-up (docs sync, 08/09) → CI green → user merges to main → (Phase end) tag
```

> This sits alongside §7.5: checkpoints catch issues *during* the milestone (risk-targeted), the zoom-out
> review catches cross-step issues *at the end* (whole-codebase). Both keep the human as the approver while
> removing the per-step bottleneck.

---

## 8. .gitignore Basics (.NET)

```gitignore
# Build
bin/
obj/
[Bb]in/
[Oo]bj/

# Test results
TestResults/
*.trx
coverage*.xml

# IDE / OS
.vs/
.idea/
*.user
.DS_Store

# Certificates & keys (never commit — key leakage risk; see plan 7.1)
*.pfx
*.key
*.pem
*.crt
*.cer
*.p12
certs/

# Logs (never commit; see plan §4 logging)
logs/
*.log

# Local config
appsettings.Development.json
*.local.json
```

> **Security note:** never commit mTLS dev certificates/keys. Generate them locally via the certgen
> tool and keep only the generation script in the repo.

> **Dev container note:** `.devcontainer/devcontainer.json` (and any `post-create.sh`) **is committed** —
> it defines the build/test environment, not a personal local setting. Keep it tracked. The hook exec bit
> is carried in git (`100755` via `git update-index --chmod=+x`), so dev-container setup needs no `chmod`.

---

## 9. At-a-glance — The Flow You Follow

```
[Start milestone] git checkout -b feat/mN-...
      │
      ▼
[Small feature loop]  write test → implement → dotnet test passes
      │                              │
      │          pre-commit hook auto-validates build+unit tests
      ▼
   git commit  (feat/fix/test...: description)
      │
      ▼  (repeat)
      │
[Milestone done] git push → PR
      │
      ▼  GitHub Actions CI (Unit+Integration) must pass to merge
      │
   merge to main
      │
      ▼  (at Phase completion)
   git tag v0.x.0 → push --tags
```

Since each step requires passing validation to advance, it becomes structurally impossible for
unvalidated code to accumulate or enter main.

---

## 10. Related Documents

- Execution Plan: `docs/design/plan.md`
- Decision & Fix Log: `docs/decisions/decision-and-fix-log.md`
- AI Collaboration: `docs/ai-collaboration.md`
