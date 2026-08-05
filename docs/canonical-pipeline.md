# 正本パイプライン — Canonical Pipeline

本ドキュメントはagg-btc 3-tier live pipelineの正本経路を定義し、
legacy codeとの混同を防止するための唯一の参照点とする。

## 3-Tier Live Pipeline（本番）

```
Receiver SQLite                              (agg-btc-receiver)
   │
   │   read-only (RawSqliteReader)
   ▼
Downstream Live Materializer                 (agg-btc-downstream)
   │   bin/run-live.mjs
   │   → book_qty_1usd_30s/*.sqlite
   │   → features_1s/*.sqlite
   │   → footprint_1usd_1s/*.sqlite
   │
   │   agg_btc_tool.py  (render)
   │   bin/publish_orderheatmap.py  (publisher)
   ▼
OrderHeatmap Publisher → Discord Webhook    (agg-btc-downstream)
      bin/publish_orderheatmap.py
      → render via chart_snapshot_heatmap()
      → POST multipart PNG to Discord
```

### 各層の責務

| 層 | ディレクトリ | systemd service | データ形式 |
|---|---|---|---|
| **Receiver** (raw) | `/home/weed420/Tool/agg-btc-receiver` | `agg-btc-receiver.service` | market別 SQLite (`data/sqlite/<market>.sqlite`) |
| **Downstream** (live materializer) | `/home/weed420/Tool/agg-btc-downstream` | `agg-btc-downstream-live.service` | stage別 market別 SQLite (`runtime/output-final/<stage>/<market>.sqlite`) |
| **Publisher** (Discord送信) | `/home/weed420/Tool/agg-btc-downstream` | `agg-btc-orderheatmap-publisher.service` | Python → PNG → Discord webhook |

### データフロー

1. **Receiver** は WebSocket/REST を受信し、market別 raw SQLite へ書き込む。
   - テーブル: `raw_batches` (timestamp, payload JSON)
   - Parquet / DuckDB / JSONL 出力は行わない。

2. **Downstream Live Materializer** は Receiver の SQLite を **read-only** で polling し、
   3つの derived stage を生成:
   - `book_qty_1usd_30s`: 30秒ごとの板状態、mid ± $10,000、$1 bucket
   - `features_1s`: 1秒ごとの best/OHLCV/depth
   - `footprint_1usd_1s`: 1秒ごとの約定を $1 price bucket 集約

3. **Publisher** は `agg_btc_tool.py` を呼び出して downstream output から
   heatmap PNG をレンダリングし、Discord webhook へ POST する。
   - 15分周期、8時間窓、9 markets
   - レンダリング前の `renderer_commit` チェックあり

### 本番設定

| 項目 | 値 |
|---|---|
| 有効 market | coinbase_spot, binance_spot, binance_spot_usdc, binance_perp, binance_perp_btcusdc, bitfinex_spot, bitmex_perp, bitstamp_spot, kraken_spot |
| book_qty interval | 30,000 ms |
| features interval | 1,000 ms |
| publisher 周期 | 15分 |
| publisher 窓 | 8時間 |
| publisher timeout | 720秒 (12分) / cycle |
| publisher 最大連続全滅 | 3回 |

---

## 非本番 Path（Legacy）

### Path B: Receiver 内 Legacy Materializer
- ファイル: `scripts/materialize-orderheatmap.mjs`
- 入力: `data/derived/burst_features_v1/book_snapshots_v2/` (JSONL)
- 出力: `data/derived/burst_features_v1/orderheatmap_1s/` (JSONL)
- **状態: 本番未使用**。live pipeline は downstream の derived SQLite を直接参照する。
- この script が出力する JSONL は `chart_snapshot_heatmap()` の `load_snapshot_heatmap()` で
  読み取り可能だが、live pipeline の `agg_btc_tool.py` は経由しない。
- `ORDERHEATMAP_ROOT` 環境変数で参照されることはあるが、publisher が実際に使用するのは
  `chart_snapshot_heatmap` 関数のみ（DuckDB 経由の `load_ring_data` は不使用）。

### Path C: agg_orderheatmap.py (Imported by Live)
- ファイル: `scripts/agg_orderheatmap.py`
- **Live pipeline では `chart_snapshot_heatmap()` 関数のみ import される。**
- このファイル内の `load_ring_data()` (DuckDB → Parquet) および
  `load_snapshot_heatmap()` (JSONL → dict) は live publisher では呼ばれない。
- main() はスクリプト単体実行用の diagnostic entry point。

### 旧 v4 Path
- 関連ファイル: `scripts/archive-raw-v4.mjs`, `lib/raw-v4-writer.mjs`,
  `lib/downstream/raw-v4-segment-reader.mjs`, `lib/downstream/raw-v4-block-source.mjs`
- **本番未使用**。Receiver は raw SQLite のみ使用。v4 writer は archive-once-only の
  バックフィル用。
- これらのファイルが存在しなくても本番障害ではない。

---

## 契約 (Contract) 一覧

### C1: Receiver SQLite → Downstream Reader
- データソース: `data/sqlite/<market>.sqlite`
- テーブル: `raw_batches`
- カラム: `id INTEGER PRIMARY KEY, ts INTEGER NOT NULL, payload BLOB NOT NULL, batch_seq INTEGER`
- Downstream は `file:` URI + `mode=ro` で接続 (read-only)
- ファイル不在 または 空DB の場合、downstream は market をスキップ（障害ではない）

### C2: Downstream Output → Publisher Renderer
- データソース: `runtime/output-final/<stage>/<market>.sqlite`
- Stage: `book_qty_1usd_30s`, `features_1s`, `footprint_1usd_1s`
- Publisher は `agg_btc_tool.py` 経由で `render_orderheatmap()` → `chart_snapshot_heatmap()`
- Renderer は `--derived-dir` で downstream output を指定
- `quality_valid=0` (pending_watermark) の行は heatmap から除外される（正常動作）

### C3: Publisher → Discord Webhook
- Discord multipart/form-data POST
- 1 market = 1 channel = 1 webhook
- Webhook 検証: GET /api/webhooks/{id}/{token} → channel_id 照合
- リトライ: 429 (Retry-After), 5xx (exponential backoff), 最大2回再試行
- 全 market 3 consecutive failures → abort (exit 1)
- 全 market 0 sent → consecutive counter increment → 3回で abort

---

## 監視・障害判定ルール

1. **旧 v4 パス不在 → 障害にしない**
   - `data/derived/burst_features_v1/` が空でも無視。
   - `lib/downstream/raw-v4-*` が存在しなくても正常。

2. **Publisher 停止/failed=0 → 必ず検出する**
   - systemd の active 状態を確認。
   - journal から最新 cycle の sent/failed を取得。
   - consecutive_total_failures >= 3 または sent=0 が続く場合はアラート。

3. **Source timestamp は publisher 出力の生成時刻**
   - publisher は各成功送信の market, ファイルサイズ, 生成時刻を JSON で出力。
   - 監視はこの JSON を証拠として利用可能。
