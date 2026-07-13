# P3-C3 Plan: 5min Pipeline Wiring (draft)

- Date: 2026-07-13
- Prerequisite: P3-C2 committer + consumer committed, independently reviewed ≥95.
- Scope: wire `aggregate5min` → `Rollup5minCommitter` into `pipeline.mjs` after durable 30s commit.

## What changes in pipeline.mjs

- After `commitRollupAfter1s()` call, check if 5 consecutive 30s windows for the same market are committed → call `commitRollup5min()`.
- `commitRollup5min()` reads ten committed 30s rows, calls `aggregate5min`, calls 5min committer.
- Recovery: after 30s reconcile, check for complete ten-row windows → re-aggregate missing 5min rows.
- No change to 1s commit path, no change to 5min/Receiver/raw/cron/Gateway.

## What does NOT change

- `pipeline.mjs` 5min/Receiver/raw/cron/Gateway paths
- `rollup-5min.mjs` pure module
- `rollup-5min-committer.mjs`
- `consumer-5min.mjs`
- Schema constants
- Any test or doc outside allowlist

## Excluded

- Cross-market join
- 1s/30s manifest or checkpoint mutation
- Dashboard/backtest wiring

## Sequence

1. C2 committer+consumer committed + reviewed ≥95
2. Create P3-C3 kanban card
3. coder: wire pipeline.mjs + focused test + static checks
4. parent: focused/full/static verification
5. reviewer: independent review ≥95
6. commit + push + kanban complete
