---
name: docs-sync
description: Sync updated English canonical docs (root NN-*.en.md) into their repo docs/ paths with content verification, then commit. Use at milestone wrap-up or whenever the root canonical docs changed and docs/ must catch up. Detects which files actually drifted and syncs only those.
---

# docs-sync

Keep the repo's `docs/` tree in sync with the English canonical source files at the workspace root.
The root `NN-*.en.md` files are the editing source; `docs/` holds the committed, path-renamed copies.
This skill copies **only the files that actually changed**, verifies the content really landed, and
commits with per-file staging.

> Why this skill exists: docs sync has silently shipped stale content before (a source file was an older
> version, so the copy faithfully overwrote good docs with old content). A "success" report is not proof —
> always verify the markers and line counts actually match before committing.

## Mapping (root source → repo path)

Only English canonical files are committed. Korean drafts, the verification log (08), FW_Context, and
instruction docs (04/05/07/NN-phaseX-*) are personal and NEVER committed.

| Root source | docs/ path |
|-------------|-----------|
| 01-execution-plan.en.md | docs/design/plan.md |
| 11-concurrency-strategy.en.md | docs/design/concurrency-strategy.md |
| 12-m3a-consumer-groups.en.md | docs/design/m3a-consumer-groups.md |
| 13-m3b-redelivery-dlq.en.md | docs/design/m3b-redelivery-dlq.md |
| 14-m3c-rebalance.en.md | docs/design/m3c-rebalance.md |
| 02-study-notes.en.md | docs/learning/study-notes.md |
| 03-version-control-guide.en.md | docs/guides/version-control-and-validation-guide.md |
| 09-decision-and-fix-log.en.md | docs/decisions/decision-and-fix-log.md |
| ai-collaboration.en.md | docs/ai-collaboration.md |
| 06-README.en.md | README.md (repo root — the only target outside docs/) |

(Milestone design notes like `docs/design/mN-*.md` are written directly in `docs/`, not synced from a root
source — leave them alone unless explicitly editing them.)

## Procedure

### 1. Detect drift (do not copy blindly)
For each mapped pair, compare the root source to its docs/ copy and list only the ones that differ:
```bash
for pair in \
  "01-execution-plan.en.md:docs/design/plan.md" \
  "11-concurrency-strategy.en.md:docs/design/concurrency-strategy.md" \
  "12-m3a-consumer-groups.en.md:docs/design/m3a-consumer-groups.md" \
  "13-m3b-redelivery-dlq.en.md:docs/design/m3b-redelivery-dlq.md" \
  "14-m3c-rebalance.en.md:docs/design/m3c-rebalance.md" \
  "02-study-notes.en.md:docs/learning/study-notes.md" \
  "03-version-control-guide.en.md:docs/guides/version-control-and-validation-guide.md" \
  "09-decision-and-fix-log.en.md:docs/decisions/decision-and-fix-log.md" \
  "ai-collaboration.en.md:docs/ai-collaboration.md" \
  "06-README.en.md:README.md"; do
  src="${pair%%:*}"; dst="${pair##*:}"
  if [ -f "$src" ] && ! diff -q "$src" "$dst" >/dev/null 2>&1; then
    echo "DRIFT: $src -> $dst"
  fi
done
```
Report the drift list. If nothing drifted, STOP — nothing to sync.

### 2. Copy only the drifted files
Overwrite each drifted `docs/` path with the full content of its root source (exact copy).

### 3. Verify the content actually landed (mandatory — before committing)
For each file copied, confirm with BOTH a line-count match and a content marker:
```bash
# line counts must match exactly for each synced pair
wc -l <root-source> <docs-path>
# a distinctive marker that should now be present in the docs/ copy
grep -c "<a phrase unique to the latest change>" <docs-path>
```
Pick markers from the actual change (e.g. a new section heading, a new DEC/FIX id, a new term).
**If any wc -l pair does not match, or any marker grep returns 0 → STOP and report. Do NOT commit.**
A copy that "ran" but left stale content is the exact failure this skill guards against.

### 3.5 If the file has a Table of Contents, verify its anchors (02 and 09)
Some docs carry a linked Table of Contents whose entries jump to section headings (currently
`02-study-notes` and `09-decision-and-fix-log`). A TOC link only works if its anchor exactly matches the
heading's GitHub-generated id. Before committing such a file, check **both**:

1. **No broken anchors** — every TOC link `[...](#anchor)` resolves to a real heading in the same file.
2. **No missing entries** — if this sync added a new section / DEC / FIX, the TOC has a matching entry.

**GitHub heading-id (anchor slug) rule** — this is where it goes wrong, so follow it exactly:
- lowercase; spaces → `-`;
- characters like em-dash `—`, `&`, `/`, `+`, `:`, backtick `` ` ``, `.`, `(` `)` are **removed, and a
  hyphen is left in their place — adjacent hyphens are NOT collapsed**, so they become **double hyphens**;
- Hangul/CJK characters are kept as-is.
- Examples (real ones from this repo):
  - `## FIX-001 — Fan-out broken: ...` → `#fix-001--fan-out-broken-...` (em-dash → `--`, `:` dropped)
  - ``## DEC-003 — `global.json` rollForward = latestFeature (not latestMinor)`` → `#dec-003--globaljson-rollforward--latestfeature-not-latestminor`
- If unsure, open the file on GitHub and copy the heading's 🔗 link to read the real id.

A quick way to check all anchors resolve (run on the docs/ copy):
```bash
# extract TOC anchors and heading slugs, list any anchor with no matching heading
python3 - "$DOCS_PATH" <<'PY'
import re,sys
c=open(sys.argv[1]).read()
def slug(h):
    s=h.strip().lower();o=[]
    for ch in s:
        if ch.isalnum() or ch=='-':o.append(ch)
        elif ch==' ':o.append('-')
        elif '\uac00'<=ch<='\ud7a3' or '\u4e00'<=ch<='\u9fff':o.append(ch)
    return ''.join(o)
heads={slug(l[3:]) for l in c.splitlines() if l.startswith('## ') or l.startswith('### ')}
anchors=re.findall(r'\]\(#([^)]+)\)',c)
bad=[a for a in anchors if a not in heads]
print("BROKEN:",bad) if bad else print("all TOC anchors resolve")
PY
```
**If any anchor is broken, or a new section has no TOC entry → STOP and report. Do NOT commit.**

### 4. Commit (only if every check passed)
- Stage ONLY the synced target paths with explicit per-file `git add <path>`. NEVER `git add .` / `-A`.
  (Targets are the `docs/` paths above, plus the root `README.md` for the 06 mapping.)
- Single commit, Conventional Commits:
  `docs(sync): <short summary of what changed and why>`
- After committing, run `git show --stat HEAD` and show the output. It MUST list exactly the synced
  target files and nothing else (no code, no unrelated docs).
- The pre-commit hook (build + fast unit tests) must pass. If it fails, STOP and report — do not work around it.

## Hard rules (inherited from the project — see docs/guides/version-control-and-validation-guide.md)
- Local git only — never push/pull/fetch/remote/gh. The user does all remote operations.
- Per-file `git add` only; verify with `git show --stat` after every commit.
- Do NOT touch: any `.ko.md`, `08-*` (verification log), `FW_Context.md`, instruction docs
  (`04`/`05`/`07`/`NN-phaseX-mN-*`), ADRs (`docs/decisions/0001*`, `0002*`), or any code/`.csproj` file.
- Build/test inside the dev container only.

## Notes
- This is docs-only. It must never modify source code or project files.
- Run at milestone wrap-up (03 §7.5 step 2) or when the user says the root docs changed.
- The mapping and the verify-before-commit step are the whole point — keep both even if it feels redundant.
