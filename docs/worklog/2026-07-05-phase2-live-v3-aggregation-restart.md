# Phase2 live_v3 aggregation restart plan

## 背景

receiver は raw 保存専用へ寄せ、30秒 raw rotation は `data/live_v3` に常駐済み。

いま必要なのは、receiver ではなく後段で次を作ること。

- `data/1s_features/<date>/<market>.jsonl`
- `data/30s_book/<date>/<market>.jsonl`

過去には `scripts/aggregate-1s.mjs` や phase2a batch-first spec 検討があったが、現行 `live_v3` 30秒 raw layout へ接続された後段集約は未完成。

## SDD 方針

一括実装しない。契約単位で閉じる。

1. live_v3 raw schema contract
2. 30s_book reconstruction contract
3. 1s feature replay adapter contract
4. aggregation invocation / marker / cleanup contract
5. cross-doc review gate
6. bridge plan
7. 実装

各 contract は Codex review 95点以上で次へ進む。

## 現在の authoritative input

```text
data/live_v3/trades/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
data/live_v3/book_updates/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
data/live_v3/snapshots/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
data/live_v3/liquidations/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
```

`.open` は書き込み中なので後段は読まない。finalized `.jsonl` のみ読む。

## 既存資産

- `lib/feature-accumulator.mjs`: 1s feature / burst feature の既存部品
- `scripts/aggregate-1s.mjs`: git 上の旧実装原型。現ワークツリーでは削除状態。旧 raw_hot flat layout 用。
- `docs/book-coverage-tiers.md`: 30s_book の解釈に必要な coverage tier
- `docs/raw-receiver-separation-plan.md`: .processing / .processed / deletion buffer の基本方針

## 最初の課題

`docs/phase2-live-v3-raw-schema-contract.md` を作成し、実 raw JSONL に基づいて以下を固定する。

- trade schema
- book_updates schema
- snapshots schema
- liquidations schema
- timestamp normalization
- market / exchange / side normalization
- seq / prevSeq nullability
- decimal parsing
- invalid row policy

## 完了条件

- raw schema contract 作成
- 実 `data/live_v3` サンプルで schema と一致確認
- Codex review >= 95
- todo `raw-schema-contract` completed
