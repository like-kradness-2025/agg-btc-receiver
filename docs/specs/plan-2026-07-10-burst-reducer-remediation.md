# Burst Reducer Safety Remediation Plan (PDD)

## Goal
市場ごとに古いraw blockから順次処理し、再開・中断・cron実行時にも重複・逆順・巨大state・同時書込みを発生させない。現Phaseのtrade-only #1-#12だけを対象とし、book/RVZは実装しない。

## Non-goals
- Receiver変更
- book/RVZ/#13-#22の計算
- raw input削除
- 30s/5min集計
- 全面再設計や並列化

## P0-1: single writer
Files: `scripts/cron-reduce-burst-v1.sh`, `scripts/backfill-all-markets-serial.sh`, new/approved lock helper under `scripts/`.

- lock scope = output root + market.
- lock acquisition failure means skip, not concurrent execution.
- backfill and cron must use the same lock protocol.
- 5-minute test uses one market only and cron remains paused.
- Verification: two simultaneous invocations; exactly one enters reducer; no manifest/checkpoint temp collision.

## P0-2: authoritative cursor
Files: `lib/burst-reducer/pipeline.mjs`, `lib/burst-reducer/pending-block-manager.mjs`, CLI.

- Initial run: select the oldest available raw block for the market.
- Resume run: checkpoint is authoritative. The next candidate must be strictly after the committed/pending cursor according to the one-block-lag contract.
- CLI `from` is only a lower bound for initial discovery; it must never move the cursor backwards after checkpoint restore.
- Validate contiguous 30s order and quarantine gaps rather than feeding old/out-of-order blocks.
- Verify restart after N blocks produces byte-identical committed rows and no duplicate manifest record.

## P0-3: intent reconcile
Files: `lib/burst-reducer/manifest-manager.mjs`, `lib/burst-reducer/output-committer.mjs`.

Before processing a market:
- detect intent entries and `.tmp`/staged artifacts;
- compare committed manifest, final shard, checkpoint and input hash;
- complete the commit if all durable pieces exist, otherwise remove only orphan temp/staged artifacts and retry the same cursor;
- never advance checkpoint based only on intent.

Verification: inject interruption after each commit step; restart and assert one committed record, one shard, monotonic generation.

## P1-1: minimal checkpoint state
Files: `lib/burst-reducer/burst-state-codec.mjs`, `lib/burst-reducer/pipeline.mjs`, tests.

- Persist only state required to continue an open cross-block burst and the pending block identity.
- Do not persist all historical closed bursts, their prints, or duplicated `open_burst_before_N1` snapshots.
- Do not remove in-memory history until feature correctness is proven; persisted-state reduction must have boundary-crossing golden tests.
- The exact retained state must be derived from `BurstBuilder` semantics and the feature's required finalization window, not guessed.
- Version the checkpoint schema and reject incompatible old checkpoints with an explicit migration/reset path; never silently reinterpret a 500MB checkpoint.

Verification: checkpoint size remains bounded over a synthetic long run; restart at burst boundary matches uninterrupted output; RSS does not grow linearly with block count.

## P1-2: bounded feature lookup
Files: `lib/burst-reducer/feature-computer-1s.mjs`, relevant tests.

- Replace full closed-burst scans only after a correctness oracle exists.
- Use time-indexed/bounded lookup for the 1s window.
- Preserve half-open block semantics, cross-block overlap, zero rows, null/zero P1 values.

## P2: manifest write cost
Files: `manifest-manager.mjs`, `output-committer.mjs`.

- Keep atomic commit contract.
- Avoid unnecessary whole-manifest parse/rewrite where safe, or partition by market/run while retaining recovery metadata.
- This is after P0/P1 and not a prerequisite for the first controlled test if checkpoint is bounded.

## Test sequence
1. Existing unit/golden suite.
2. New cursor/restart/reconcile/lock tests.
3. Static checks: no old cursor path; no unbounded checkpoint fields; no `require`; output root contract.
4. Adversarial review >=95/100.
5. Controlled 5-minute test: one market, cron paused, dedicated output root, lock enabled.

## Adversarial review additions / one-strike failures
- A missing raw/aux block must not be treated as an arrived empty block. Zero-volume is valid only when a valid file exists and parses as an empty/zero record set; absent or partial files stop before commit.
- Recovery must verify hashes/content, not file existence alone. A committed manifest with missing/mismatched final shard, checkpoint, or input hash is quarantine/blocking, never silently repaired.
- Crash after final-shard rename and before checkpoint/manifest completion must be replayed in a test; recovery must produce exactly one composite-key commit.
- EOF may be used only when the input inventory/watermark proves the block is complete. Directory enumeration alone is not proof that a live block will not arrive later.
- Removing `closedBursts` from persistence is prohibited until boundary, single-print, max-duration, restart, and byte-identical golden oracles pass. If those oracles fail, retain a bounded summary rather than silently restoring an empty history.

## P0-4: Finalized input horizon / EOF authority

### Problem closed by this contract
Directory enumeration ending is not EOF. A live input may simply not have produced the next 30s block yet. The reducer must distinguish an arrived valid empty block, a not-yet-arrived block, and a verified missing/corrupt block.

### Authoritative source
- **Frozen/backfill input:** an explicit input inventory manifest created before the run is the authority. It lists every expected trade and auxiliary block, its byte size and raw-byte SHA-256. The reducer must not finalize beyond the inventory horizon.
- **Live input:** an explicit `--finalized-through <ISO>` watermark supplied by the caller is the authority. No implicit wall-clock or directory-enumeration EOF is allowed. The watermark is exclusive and must be aligned to the 30s block boundary.
- If neither frozen inventory nor `--finalized-through` is present, the run is live/unfinalized: it may commit only blocks proven by a complete next block; it must retain the pending block and exit 0 with `blocked_reason=not-yet-arrived` rather than EOF-flush it.

### Required state transitions
- `arrived-valid`: expected file exists, is complete/parseable, and its raw-byte hash (when inventory is present) matches. It may be fed.
- `arrived-empty-valid`: expected file exists and validates as an empty/zero-volume block. It is a valid watermark proof; it is not E007.
- `not-yet-arrived`: next expected block start is at or after the live finalized watermark, or the frozen inventory does not yet declare it. Keep `pending_block`, do not quarantine, exit 0 with a structured blocked reason.
- `verified-missing`: next expected block start is before the authoritative finalized horizon/inventory, but the file is absent, partial, invalid, or hash-mismatched. Do not finalize; write quarantine and stop the market.
- `eof-finalizable`: only the frozen inventory explicitly ends at the pending boundary, or `--finalized-through` proves the pending block and its required next-boundary proof complete. Only this state may call `flushAll()` and set `pending_block=null`.

### Concrete contract
- `--finalized-through` is required for live EOF behavior and is rejected if not 30s-aligned.
- Range exhaustion alone never calls EOF flush.
- No proof means `pending_block` remains persisted and the process exits successfully with `processed=0` for that finalization step.
- `gap` handling is split into `not-yet-arrived` vs `verified-missing`; only the latter creates quarantine.
- The horizon decision and reason are emitted as structured stderr JSON and recorded in the run report.

### Tests required before implementation passes review
Create/extend tests under `test/burst-reducer/`:
- raw next block absent + no watermark => pending retained, no EOF, no quarantine;
- explicit valid empty next block => previous pending finalizes;
- absent next block at/after `--finalized-through` => blocked, not quarantine;
- absent/invalid next block before `--finalized-through` => verified-missing quarantine;
- frozen inventory ending at boundary => EOF finalizes exactly one pending block;
- live-like directory scan exhaustion alone => never EOF;
- blocked pending followed by later arrival => restart is byte-identical and no duplicate composite key;
- `--finalized-through` misaligned => reject before processing.

### Five-minute test rule
The controlled 5-minute live test must run without EOF flush unless an explicit aligned `--finalized-through` is supplied. It observes pending/cursor progression only; it must not infer completion from directory listing.

### Rollback/stop conditions
- Any EOF flush without an explicit authority proof is a P0 failure.
- Any not-yet-arrived block quarantined is a P0 failure.
- Any verified-missing block silently skipped is a P0 failure.
- Any final pending lost after a blocked restart is a P0 failure.

- process remains alive and advances cursor monotonically;
- processed count increases continuously or is explicitly blocked by a verified missing input;
- no duplicate composite block keys;
- no E007/E020/E031 or uncaught exception;
- RSS does not show linear growth; checkpoint and open-burst sizes remain bounded;
- CPU/IO do not starve Receiver; receiver health remains normal;
- checkpoint size remains bounded and `.tmp`/intent residue is absent after clean completion;
- output row count/schema/hash checks pass against the oracle;
- stop test leaves a resumable state and second run does not duplicate output;
- controlled kill after each commit boundary, followed by restart, passes the same invariants.

## Adversarial review scope / penalty rule

The adversarial review evaluates implementation substance only:

- data loss, silent corruption, duplicate commit, cursor regression/skip;
- crash recovery and intent/checkpoint/output consistency;
- EOF/finalized-horizon correctness;
- same-market concurrent writer safety;
- checkpoint/state growth and abnormal resource load;
- required output schema and existing contract regressions.

Out-of-scope findings are not blockers or score deductions: naming, comments, dead code, cosmetic refactors, hypothetical API generalization, or improvements outside the fixed P0/P1 contract. A reviewer must tie every deduction to a reproducible failure mode, violated contract, or measured risk. Unsupported/speculative concerns receive no penalty.


1. **Live EOF proof:** `--finalized-through` is exclusive, 30s-aligned, and a pending block may be EOF-flushed only when the pending trade block and required auxiliary lookback blocks are within the finalized horizon and have validated successfully. Without it, no EOF flush.
2. **Frozen inventory minimum schema:** `{market, block_start_ms, block_kind, path, byte_size, sha256, empty_valid}`; trade blocks and required auxiliary blocks are explicit records. Inventory horizon is authoritative.
3. **Live hash source of truth:** each committed record stores raw trade hash, auxiliary hashes, final shard hash, and checkpoint hash/generation. Recovery compares these exact fields; existence alone never completes a commit.
4. **Lock primitive:** same-host `flock -x -n` on `<output_root>/locks/<market>.lock`; kernel releases the advisory lock on process death. No mkdir stale-lock protocol is used. Lock contention exits 0 with structured skip output.
5. **Boundedness ceilings for controlled test:** checkpoint <= 64 KiB, serialized pending/open-burst state <= 256 KiB each, and RSS must not grow linearly over the 5-minute window. Any ceiling violation stops the test. These are safety ceilings, not correctness substitutes.
6. **Blocked exit contract:** not-yet-arrived returns exit code 0 and one structured stderr record containing `processed:0`, `blocked_reason:"not-yet-arrived"`, `market`, `cursor_ms`, and `expected_block_start_ms`.
7. **Verified-missing quarantine contract:** write `quarantine/<market>/<block_start_ms>.json` containing reason, horizon, expected path, observed file metadata, input/aux hashes when available, and retry disposition; do not advance the cursor.
8. **Controlled 5-minute test:** a separately titled acceptance section must check cursor monotonicity, no duplicate composite keys, hash/schema validity, bounded state, Receiver health, no E007/E020/E031, and controlled kill→restart byte-identical recovery. Live test supplies no EOF flush unless it explicitly supplies the aligned watermark.

- Keep raw input untouched.
- Stop reducer and pause cron.
- Preserve failed output root for forensics.
- Revert only the remediation commit; do not delete evidence or reuse untrusted checkpoints.
