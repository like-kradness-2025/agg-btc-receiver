# agg-btc-receiver 現行方針

このディレクトリが、2026-07-21時点の正本ドキュメントです。

## 方針

- Receiverの責務は取引所から受け取ったraw market eventの保存だけ。
- raw保存先はmarket別のSQLiteファイル。
- WorkerはDBへ直接接続せず、Receiver本体の書き込みキューへ送る。
- PerpのOIはREST観測値を`open_interest` raw streamとしてmarket別SQLiteへ保存する。
- 時間単位のディレクトリ・JSONL・Parquet archiveは新規保存経路では使わない。
- rawは90日保持し、期限超過行をDB内から削除する。
- TFP、Book Snapshot、OrderHeatmapなどの後工程はReceiverの常駐処理に含めない。

## 3-Tier Live Pipeline（本番）

```text
Receiver SQLite                          (agg-btc-receiver)
   │ raw_batches (market別 .sqlite)
   │ read-only polling
   ▼
agg-btc-downstream-live                  (agg-btc-downstream)
   │ → book_qty_1usd_30s (30s板状態)
   │ → features_1s (1s特徴量)
   │ → footprint_1usd_1s (1s約定集約)
   │
   │ agg_btc_tool.py render
   ▼
agg-btc-orderheatmap-publisher           (agg-btc-downstream)
   → chart_snapshot_heatmap()
   → Discord webhook (PNG, 15分周期)
```

詳細は [canonical-pipeline.md](canonical-pipeline.md) および
ルートの [docs/canonical-pipeline.md](../canonical-pipeline.md) を参照。

## 文書

- [receiver-storage.md](receiver-storage.md) — 保存方式と処理経路
- [data-contract.md](data-contract.md) — SQLiteテーブルとraw envelope
- [canonical-pipeline.md](canonical-pipeline.md) — 3-tier正本経路
- [legacy-artifacts.md](legacy-artifacts.md) — 非本番コード一覧
- [operations.md](operations.md) — systemd、保持、確認手順

過去のJSONL/Parquet、TFP、特徴量、検証レポートは`docs/archive/`に移動した履歴資料であり、現行仕様ではありません。
