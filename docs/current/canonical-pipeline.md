# Canonical Live Pipeline（正本パイプライン）

本ドキュメントはReceiver視点の3-tier live pipeline定義です。
全容はルート [docs/canonical-pipeline.md](../canonical-pipeline.md) を参照。

## 3-Tier 構成

| 層 | リポジトリ | systemd service | データ |
|---|---|---|---|
| **Receiver** (raw保存) | `agg-btc-receiver` | `agg-btc-receiver.service` | market別 SQLite (`data/sqlite/<market>.sqlite`) |
| **Downstream** (live materializer) | `agg-btc-downstream` | `agg-btc-downstream-live.service` | stage別 market別 SQLite (`runtime/output-final/<stage>/<market>.sqlite`) |
| **Publisher** (Discord送信) | `agg-btc-downstream` | `agg-btc-orderheatmap-publisher.service` | Python → PNG → Discord webhook |

### 各層のReceiverから見た契約

**C1 — Receiver SQLite → Downstream Reader**
- データソース: `data/sqlite/<market>.sqlite`
- テーブル: `raw_batches`
- カラム定義: `docs/current/data-contract.md` 参照
- Downstream は `file:` URI + `mode=ro` で接続 (read-only)
- ファイル不在 または 空DB の場合、downstream は market をスキップ（障害ではない）

**データフロー制約**
- Receiverは raw SQLite のみを生成。Parquet / DuckDB / JSONL 出力は行わない。
- 後工程（TFP・book snapshot・orderheatmap）は Receiver の管轄外。
- Downstream は Receiver のデータを常に read-only で参照する。
