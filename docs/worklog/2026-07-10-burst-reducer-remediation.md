# PDD Worklog: burst reducer safety/performance remediation (2026-07-10)

## Status
INTAKE/PLAN. Conversion is stopped. No implementation change from this request has been applied.

## User goal
PDDで修正案を作成し、敵対的レビューループで95点以上に合格した場合だけ修正・検証する。合格後に5分テストを行い、異常負荷・進捗停止・エラーがなければ終了する。

## Observed state
- burst-reducer-cron job `266c0a8804a6` is paused.
- Known running reducer children were stopped by explicit PID; a follow-up process probe returned no `reduce-burst`, `backfill-all-markets`, or `cron-reduce-burst` process.
- Existing output root: `data/derived/burst_features_v1/`.
- Observed manifest examples: `binance_perp.json` 603,506 bytes / 597 processed blocks / 2 intent records; `binance_spot.json` 530,621 bytes / 525 blocks / 2 intents; `bybit_spot.json` 649,170 bytes / 646 blocks / 1 intent.
- Observed checkpoint examples: `binance_perp.json` 533,623,819 bytes; `binance_spot.json` 536,296,776 bytes; `bybit_perp.json` 536,672,479 bytes. All observed checkpoints had a pending block.
- `burst-state-codec.mjs` serializes all `_closedBursts`, including `same_price_runs` and `prints`, and restores all of them. This is the direct cause of unbounded checkpoint growth and repeated deep clone/JSON work.
- `pipeline.mjs` scans CLI `[fromMs,toMs)` blocks, restores checkpoint state, and then feeds the scanned candidates. A resume cursor must be made authoritative so an older CLI range cannot feed blocks older than the restored state.
- Output commit rewrites manifest/checkpoint per block. There is no verified market-level single-writer lock in the current scripts.
- The prior run demonstrated cron/backfill overlap and high load: reducer CPU around 110%, RSS in the multi-GB range.
- Current implemented feature scope is trade-only #1-#12. Book/RVZ/#13-#22 are not implemented; #13/#14-#22 remain P1 contract placeholders.

## Decision/status
- Do not resume conversion during PLAN/REVIEW.
- Do not delete raw input.
- Do not claim the current reducer is safe to resume.
- Proposed implementation must be reviewed adversarially and score >=95 before execution.

## Candidate remediation scope
P0: single writer per market, paused cron during backfill, authoritative resume cursor, intent reconciliation.
P1: minimal checkpoint state; remove historical closed-burst prints from persisted state; bounded retention only if the feature contract proves the required lookback.
P2: reduce repeated scans and full manifest rewrites.
Out of scope: book/RVZ implementation, receiver changes, raw data deletion, full phase redesign.

## Verification gates
1. Unit/regression tests for cursor, restart, intent recovery, boundary burst, EOF, idempotency.
2. Static contract checks against design/spec.
3. Adversarial review score >=95.
4. Only then a 5-minute controlled test with one market, lock enabled, cron paused, bounded output root, CPU/RSS/IO/backlog/error sampling.
5. Stop immediately on abnormal RSS growth, duplicate/cursor regression, E007/E020/E031, output corruption, or no progress.
