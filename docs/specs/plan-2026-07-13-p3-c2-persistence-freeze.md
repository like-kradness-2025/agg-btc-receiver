# P3-C2 Plan: 5min Persistence Contract Freeze

- Date: 2026-07-13
- Base: P3-C1 commit `3cf2fdc`, review 98/100 PASS.
- Status: **APPROVED** — multi-perspective analysis complete, 8 contract items accepted.

## Facts

- `features_5min` producer exists only as a pure market-local transform.
- Existing 30s committer is a reference pattern, not a direct reusable committer.
- No 5min producer caller, committer, manifest/checkpoint, recovery, or consumer exists.

## Multi-perspective findings (6 views: forward, recovery, adversarial, consumer, isolation, performance)

### 3 unaddressed risks

1. **Orphan staging file** — crash after stage write, before intent manifest. No manifest record → recovery cannot detect. If source trades changed between crash and restart, silent output overwrite with no hash anchor. **Fix:** recovery-time `.staging/` scan + orphan cleanup.

2. **Missing intent hash verification** — crash before intent manifest leaves no hash anchor. Recovery cannot verify renamed output against manifest. **Fix:** external reconciliation from source 30s manifest (like existing `reconcileCommitted1s()` pattern), not dependent on surviving intent record.

3. **Hash conflict / duplicate window** — same window with different source hash cannot be quarantined. Duplicate output path for same `(market, window_start_ms)` has no detection. **Fix:** explicit quarantine contract for hash mismatch, source contradiction, and duplicate key.

### Consumer gap

No 5min consumer exists. Must implement: committed-only manifest reader, row validation vs schema, range query, diagnostic status, hash integrity check, index rebuild from files. This is **not** the same as dashboard/backtest wiring.

### Performance baseline

- 105 atomic writes per market per 5min window
- 1,575 writes/5min at 15-market concurrency
- Existing `writeFileDurable` + atomic rename pattern is sufficient

## Proposed contract terms (incorporating all findings)

### 1. Dataset / path / row identity

- Dataset: `features_5min`
- Row grain: **1 market × 1つの5分 window**
- Primary identity: `(market, window_start_ms)`
- Output path: `<derivedRoot>/features_5min/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl`
- Staging path: `<derivedRoot>/features_5min/<market>/<YYYY-MM-DD>/.staging/<runId>/<HH-MM-SS>.jsonl`

### 2. Idempotency / collision policy

- Key: `burst_features_5min_v1:<market>:<window_start_ms>:<source_window_hash>`
- Same key + same output hash: idempotent success
- Same window + same source hash + output missing: rebuild output, repair checkpoint
- Same window + different source hash: **quarantine** — no silent overwrite
- `runId` is collision avoidance only, not identity

### 3. Dedicated manifest namespace

- Path: `<derivedRoot>/manifests/features_5min/<market>.json`
- Schema includes: `schema_version`, `namespace`, `source_layer`, `market`, `processed_windows` keyed by idempotency key, each with `window_start_ms/end_ms`, `source_window_count`, `source_window_hash`, `output_path`, `output_row_hash`, `checkpoint_generation`, `status` (intent|committed|blocked|quarantined)
- `status=committed` only visible to consumer
- `intent` requires output existence + hash match to promote
- `blocked` = transient not-yet-arrived
- `quarantined` = corruption, hash conflict, source contradiction

### 4. Dedicated checkpoint namespace

- Path: `<derivedRoot>/manifests/checkpoints/features_5min/<market>.json`
- Payload: `schema_version`, `namespace`, `source_layer`, `market`, `last_committed_window_start_ms`, `finalized_through_ms`, `generation`, `output_path`, `output_row_hash`, `updated_at`
- Checkpoint is **restart hint** not authority
- Committed truth = manifest record + output existence + output hash
- Checkpoint missing/stale → reconstruct from manifest

### 5. Finalized-through / EOF authority

- 5min row commits only when: ten input rows exist, all `features_30s`, coverage complete, `has_missing_input=false`, all rows `finalized=true`
- `finalized_through_ms` = consecutive committed finalized 5min windows' exclusive end
- EOF **never inferred** from missing next file, wall clock, or stopped checkpoint
- EOF requires explicit upstream authority: `eof=true` + `eof_through_ms`
- Without explicit EOF, last window stays blocked/pending

### 6. Recovery / reconcile / quarantine

Commit order:
1. Validate ten-row window
2. Staging: durable JSONL write
3. Manifest: `intent` record with source hash + staged hash
4. Atomic rename staging → final path
5. Verify renamed output hash matches staged hash
6. Checkpoint: atomic write with generation increment
7. Manifest: promote `intent` → `committed`

Recovery rules:
- Orphan staging scan at startup: clean files not referenced by manifest intent
- Source-referenced reconciliation (like `reconcileCommitted30s`): scan committed 30s manifest for complete windows → re-aggregate missing 5min rows
- `intent` + correct output hash → `committed` promote
- `committed` + output missing → rebuild if source valid, else quarantine
- Output hash mismatch, source hash conflict, quality contradiction → quarantine
- 1s/30s bytes/manifest/checkpoint: **never modified** by 5min recovery

### 7. Empty / missing / not-yet-arrived

- **empty-valid**: ten rows all `arrived-empty-valid`, all features zero. `has_empty_input=true`, finalized OK → committed as normal row
- **not-yet-arrived**: no output row generated. Manifest `blocked` allowed but consumer-invisible. Retry on source arrival
- **missing / verified-missing**: no zero-fill. Not committed. After explicit EOF authority, if still missing → `quarantined`
- Empty-valid and missing **never** conflated

### 8. Consumer / index contract

- Consumer reads `manifests/features_5min/<market>.json` as index
- Only `status=committed` rows are visible
- Per record validation: output exists, output hash match, `(market, window_start_ms)` unique, `_quality.source_layer=features_30s`, source count=10, coverage complete, `has_missing_input=false`, `finalized=true`
- Query key: `(market, from_ms, to_ms)`, results in `window_start_ms` ascending
- `blocked` / `quarantined` / missing → diagnostic status only, no data row
- Dashboard/backtest wiring is out of C2 scope

### 9. State isolation

- 5min reads only: 30s as **input rows**, own `features_5min` config/schema constants
- 5min writes only: own `features_5min/` output, `manifests/features_5min/`, `checkpoints/features_5min/`
- **Never** reads or writes 1s/30s manifest/checkpoint/temp/output
- 1s/30s schema/committer/recovery unchanged
- Consumer scope uses only 5min namespace

## Explicitly excluded

- 1s/30s state, manifest, checkpoint reuse or mutation
- Legacy cross-market field promotion
- Cross-market aggregation
- Receiver / raw ingestion
- Cron / Gateway
- Dashboard / backtest wiring
- Missing input zero-fill
- EOF inference without upstream authority
- Committed row overwrite

## Required approvals

1. [ ] The path, idempotency key, and `burst_features_5min_v1` schema version
2. [ ] Hash conflict → quarantine policy (no silent overwrite)
3. [ ] `finalized_through_ms` as consecutive committed finalized windows' exclusive end
4. [ ] Upstream `eof/eof_through_ms` authority required (inference forbidden)
5. [ ] `intent/committed/blocked/quarantined` manifest status model
6. [ ] Empty-valid committed, missing/not-yet-arrived not committed
7. [ ] Consumer reads manifest committed-only; dashboard wiring out of scope
8. [ ] Orphan staging cleanup, source-referenced reconciliation, hash verification
