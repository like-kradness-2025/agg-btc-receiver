# 過去資料

`legacy-2026-07-21/`は、DuckDB単一保存へ切り替える前の設計・計画・handoff・検証資料です。

これらは履歴と判断根拠の保全用で、現行の実装方針や運用手順ではありません。現行仕様は`docs/current/`だけを参照します。

旧資料に含まれる主な前提は、現在は採用していません。

- JSONLの時間・日付分割
- nightly Parquet変換
- Receiver常駐のTFP / Book Snapshot / OrderHeatmap
- downstream feature outputをReceiverの正本として扱うこと
