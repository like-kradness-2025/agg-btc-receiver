# Worklog: TFP Phase B Book Contract

## Goal
Prepare and execute a gated Phase B that wires the P0-0 book contract into TFP production without mixing Phase C rollup or breaking trade-only contracts.

## Current state
- Phase A Gate A: 95/100 PASS.
- HEAD: `a750ddd` on `v2`, pushed to `origin/v2`.
- Full tests at Phase A completion: 555/555 PASS.
- Phase C `lib/burst-reducer/rollup.mjs` remains untracked and out of scope.

## PDD loop

Each sub-phase follows:

`delegate_task researcher/designer → parent evidence check → delegate_task coder → parent tests/probes → delegate_task reviewer → FIX/re-review until >=95`

`profileSession` is not used.

## Safety constraints

- Receiver remains raw-only.
- No production raw/output root execution.
- No cron/Gateway/restart.
- No commit until the current sub-phase gate passes.
- Do not overwrite #13/#14 placeholders.

## B0 evidence

- P0-0正本を確認: 現行depth eventはexchange-specific shallow shape（`type/bids/asks/ts/seq`）で、canonical envelope未達。
- canonical必須項目を固定: `schema_version=book_updates_v1`, `market`, `type`, `event_ts_ms`, `seq`, `prev_seq`, `source`, `bids`, `asks`。
- 既存FullBookのonline sequence検査はTFP raw replayの共通gap/quarantine契約の代替ではない。
- Phase C rollup、B2 state machine、pipeline join、#13/#14変更はB1非対象。

## Current phase

B0完了。B1 adapterを通常`delegate_task` coderが実装中。親側の実ファイル再読・focused tests・全体テスト・独立95点レビューが未実施のため、B1は未完了扱い。
