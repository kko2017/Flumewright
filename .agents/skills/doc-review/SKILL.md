---
name: doc-review
description: On-demand review of the English canonical docs for duplication, cross-doc contradiction (drift), structural awkwardness, and broken cross-references — report only, never edit. Run ONLY when the user explicitly asks for a doc review (e.g. at a milestone wrap-up or when the docs feel messy). NOT automatic, NOT per checkpoint.
---

# doc-review

The English root docs (`NN-*.en.md`, README, `ai-collaboration.en.md`) grow incrementally across many
sessions. Over time that produces **duplication** (the same point in two docs), **drift** (a fact updated in
one place but not another), **structural awkwardness** (a section that reads oddly or sits in the wrong
doc), and **stale cross-references** (a link or "see §X" that no longer resolves). This skill is a fresh,
isolated pass that surfaces those — so the human can decide what to fix.

> On-demand only. Unlike `code-review` (which runs at every checkpoint), doc-review runs **only when the user
> asks**. Docs are not high-frequency or high-risk; reviewing them every checkpoint would waste tokens on
> files that did not change. Good moments to ask: a milestone wrap-up, or when the docs feel messy.

## Scope
- Default target: the **English canonical docs** — the root `NN-*.en.md`, `06-README.en.md`,
  `ai-collaboration.en.md`. The user may narrow it ("just 02 and 09", "only the READMEs", "check 11 against
  02 for overlap").
- Korean `.ko.md` drafts are personal and out of scope unless the user explicitly asks.
- Read the docs in scope (and only those) — do not pull in code or the whole repo.

## What to look for
- **Duplication** — the same explanation/fact stated in full in more than one doc. Flag it and suggest which
  doc should own it, with the others pointing via a link (the project prefers reference over copy-paste).
- **Drift / contradiction** — the same fact stated differently across docs (e.g. a status, a number, a
  decision id, a model name). Flag the conflicting statements and their locations; the human resolves which
  is correct.
- **Structural awkwardness** — a section that is hard to follow, out of logical order, or living in the
  wrong document (e.g. deep concurrency detail in a doc whose job is overview). Suggest a clearer structure
  or a better home.
- **Stale cross-references** — a link, "see §X", or doc/section name that no longer matches the target
  (renamed heading, moved file, changed section number, a `🔒`-anchor that drifted).
- **Tone/voice inconsistency** *(light touch)* — only flag clear mismatches (e.g. a personal-sounding label
  in a public doc); do not rewrite the author's voice.

## What NOT to do
- **Do not edit anything.** Report only. Documentation carries the author's voice; fixes are the human's.
- Do not invent problems to look busy — if a doc is clean, say so.
- Do not flag intentionally-deferred items or the personal/never-committed files as defects.

## Report format
Group findings by type, each with file + location + a one-line suggestion:
```
## Doc review — <scope>
### Duplication
- [02 §11.8 ↔ ci-cd §1] both fully explain new-code gating → suggest ci-cd owns it, 02 links to it
### Drift / contradiction
- [README status ↔ FW context] README says "M2 done", other says "M2 in progress" → human resolves
### Structure
- [11 §3] table and prose repeat the same status tags → consider dropping one
### Stale cross-references
- [11 §5] link to study-notes §11.7 — no such section (renumbered?) 
### Clean
- 09, ai-collaboration: no issues found
```
Then STOP. The human decides what to act on; approved edits are made normally (and synced via `docs-sync`
if they touch canonical docs).

## Hard rules (inherited)
- Report only — never edit, never run git.
- English canonical docs are the subject; never touch `.ko.md` unless explicitly asked.
- A "looks clean" is a finding, not a guarantee — the human is the approver.
