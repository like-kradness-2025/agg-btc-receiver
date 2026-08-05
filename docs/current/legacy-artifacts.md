# 非本番コード一覧（Legacy / Non-production）

以下のファイルは **本番パイプラインでは使用されていません**。
本番パイプラインは Receiver SQLite → agg-btc-downstream-live → agg-btc-orderheatmap-publisher の3層構成です（[canonical-pipeline.md](canonical-pipeline.md) 参照）。

これらのファイルの不在・変更・データ欠如を本番障害と誤判定しないでください。

## Receiver 内 Legacy Materializer / Writer

| ファイル | 経路 | 状態 |
|---|---|---|
| `lib/book-snapshot-materializer.mjs` | Book Snapshot v2 (JSONL) | 本番未使用 |
| `lib/orderheatmap-materializer.mjs` | OrderHeatmap (JSONL) | 本番未使用 |
| `lib/snapshot-writer.mjs` | Seed snapshot (JSONL) | 本番未使用 |
| `lib/raw-v4-writer.mjs` | v4 hourly JSONL segment | 本番未使用 |
| `lib/downstream/raw-v4-segment-reader.mjs` | v4 segment byte-offset reader | 本番未使用 |
| `lib/downstream/raw-v4-block-source.mjs` | v4 → 30s block source | 本番未使用 |
| `lib/downstream/incremental-cursor.mjs` | 汎用append-only JSONL cursor | 本番未使用 |
| `lib/raw-db-writer.mjs` | DuckDB raw_batches | 移行完了（SQLite が正本） |
| `lib/raw-rotation-writer.mjs` | 30s window JSONL rotation | 本番未使用（v4 writer時代の残余） |
| `lib/burst-reducer/trade-flow-features.mjs` | Phase 0 raw-trade OrderFlow features | 本番未使用（burst-reducer pipeline経由） |
| `lib/market-registry.mjs` | 板数量のBTC正規化メタデータ | 本番使用（book-updates-adapter経由） |

## Receiver 内 Legacy Scripts

| ファイル | 用途 | 状態 |
|---|---|---|
| `scripts/materialize-orderheatmap.mjs` | JSONL-based orderheatmap生成 | 本番未使用 |
| `scripts/materialize-book-snapshots.mjs` | Book snapshot v2 一括生成 | 本番未使用 |
| `scripts/run-book-snapshots-live.sh` | Live book snapshot timer wrapper | 本番未使用 |
| `scripts/run-tfp-live.sh` | TFP incremental converter | 本番未使用 |
| `scripts/archive-raw-v4.mjs` | v4 JSONL → Parquet archive | 本番未使用 |
| `scripts/migrate-duckdb-to-sqlite.mjs` | 一括移行（one-shot） | 実行済み、再実行不要 |
| `scripts/finish-slot3.sh` | 過去burst_agg補助 | 本番未使用 |
| `scripts/book_replay_gen.py` | Book replay生成 | 本番未使用 |
| `scripts/recover-orderheatmap-cursor.mjs` | OrderHeatmap cursor復旧 | 本番未使用 |
| `scripts/recover-tfp-manifest.mjs` | TFP manifest復旧 | 本番未使用 |
| `scripts/recover-v4-book-cursor.mjs` | v4 book cursor復旧 | 本番未使用 |
| `scripts/repair-orderheatmap-depth-limit.mjs` | OrderHeatmap depth制限修復 | 本番未使用 |
| `scripts/with-maintenance-lock.sh` | メンテナンスロック共通ラッパー | 本番未使用 |

## Legacy systemd Units

| ファイル | 用途 | 状態 |
|---|---|---|
| `systemd/legacy/agg-btc-receiver-archive.service` | Old archive | 無効 |
| `systemd/legacy/agg-btc-receiver-archive.timer` | Old archive timer | 無効 |
| `systemd/agg-btc-receiver-maintenance-book-snapshots.service` | Book snapshot maintenance | 本番未使用 |
| `systemd/agg-btc-receiver-maintenance-cleanup-raw.service` | Raw data cleanup | 本番未使用 |
| `systemd/agg-btc-receiver-maintenance-tfp.service` | TFP maintenance | 本番未使用 |

## 旧データディレクトリ

| パス | 内容 | 取扱い |
|---|---|---|
| `data/derived/burst_features_v1/` | 旧JSONL derived features | 生存していても障害ではない |
| `data/live_v4/` | 旧v4 raw segments | 生存していても障害ではない |
| `data/agg-btc-receiver.duckdb` | 移行元DuckDB | ロールバック用に保全中 |

## 監視上の注意

- `data/derived/` の有無は監視対象外。空でも Receiver 本番に影響なし。
- `data/live_v4/` の欠如 or 空 → 無視。Receiver は SQLite のみ使用。
- `lib/downstream/raw-v4-*` の不在 → 正常。削除されても本番障害ではない。
- systemd `legacy/` 以下の timer の停止 → 正常。本番運用では使用しない。
