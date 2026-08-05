# Worklog: TFP Phase C1 Pure 30s Rollup (2026-07-12)

## State

- C0 read-only reconnaissance: complete.
- C1: starting after C0 HOLD resolution.
- Kanban card: `t_80ccbe34` (C0), implementation card to be created.

## Evidence from C0

- Existing `lib/burst-reducer/rollup.mjs` is untracked and unconnected.
- Existing prototype accepts partial windows, uses array-position windows, emits premature 5min rows, and lacks P2 field/operator/quality contracts.
- No rollup-specific tests exist.

## C1 decision

Use a pure 30s module plus independent tests only. Do not wire pipeline, persistence, checkpoint, manifest, or 5min.

## Gate

Focused tests → full npm test → static checks → parent scope verification → independent review >=95 → commit/push.
