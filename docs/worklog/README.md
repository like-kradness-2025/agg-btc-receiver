# Worklog Operating Rules

This directory is the shared scratchpad / progress log for ongoing design and implementation work.

## Purpose

Use this folder to avoid drift during long-running work. Put short durable notes here while working:
- current understanding
- decision rationale
- blockers
- assumptions
- next steps
- subagent findings summary
- verification notes

This is **not** the final spec location. Final contracts and plans still belong in `docs/` proper.

## Rules

1. Keep notes short and concrete.
2. Prefer append-only entries with timestamps.
3. Record facts, decisions, and open questions — not long freeform thinking.
4. If a note becomes normative, promote it into a proper contract/spec doc under `docs/`.
5. Subagent work must leave a short summary note here before or after results are integrated.

## File layout

- `current-focus.md` — current active task, latest known state, immediate next step
- `decision-log.md` — durable decisions with rationale
- `open-questions.md` — unresolved questions that can block next contracts
- `subagent-notes.md` — short summaries from delegated work
- `verification-notes.md` — spot checks, test notes, evidence pointers
- `templates/entry-template.md` — copy template for new notes if needed

## Update expectation

During normal work and subagent-assisted work, update these notes when:
- a decision is made
- a contract is closed
- a blocker is found
- a subagent returns something worth preserving
- the active focus changes

## Current project usage

For the burst feature track, use this folder as the running memory between:
- `docs/burst-feature-contract-plan.md`
- each contract doc written under `docs/`
- implementation work that follows later
