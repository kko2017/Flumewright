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

- Execution Plan: `01-execution-plan.en.md`
- Phase 0 Scaffolding Instructions: `04-phase0-scaffolding.en.md`
- CLI Master Instruction & CI/CD: `05-cli-master-instruction.en.md`
