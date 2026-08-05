# PDD Plan: P5 Retention / Cleanup

- Date: 2026-07-13
- Branch: `v2`
- Base: P4 commit (after P4 book activation)
- Mode: PDD / delegate_task / independent 95-point gate

## Scope

Implement safe cleanup/retention scripts for derived data. Retention targets:
1. Raw trades (once consumed and committed to features_1s/30s/5min)
2. Old features_1s shards (beyond retention window)
3. Old features_30s shards (beyond retention window)
4. Old features_5min shards (beyond retention window)

## Design principles

1. Cleanup is **manual/dry-run first** — never delete without explicit confirmation
2. **Dependency-aware**: don't delete data that's still needed for downstream processing
3. **Quarantine-preserve**: quarantined data is NOT deleted
4. **Rollback-capable**: deleted paths logged for potential restoration
5. **Retention window**: configurable in days, default TBD

## Files to create

1. `scripts/cleanup.mjs` — orchestration entrypoint
2. `scripts/cleanup-derived.mjs` — features_1s/30s/5min cleanup
3. `scripts/cleanup-raw.mjs` — raw trades cleanup

## TDD fixtures

- dry-run cleanup fixture (no actual deletion)
- keep-last-N-days fixture
- dependency-aware: preserve data younger than N days
- quarantine-preserve: quarantine files survive cleanup

## Verification

- `node scripts/cleanup.mjs --help`
- `node scripts/cleanup.mjs --dry-run --days=7`
- `node --test test/cleanup/*.test.mjs`
- `npm test`
- Independent review >=95

## Out of scope

- Automatic scheduled cleanup (cron job)
- Compression format migration (Parquet etc.)
- Cross-market universe management
