# raw_batches データ契約（SQLite）

## テーブル

```sql
CREATE TABLE raw_batches (
  batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
  schema TEXT,
  market TEXT,
  stream TEXT,
  first_event_ts_ms INTEGER,
  last_event_ts_ms INTEGER,
  first_recv_ts_ms INTEGER,
  last_recv_ts_ms INTEGER,
  row_count INTEGER,
  raw_gzip BLOB,
  raw_bytes INTEGER,
  written_at_ms INTEGER
);
```

`raw_gzip`を展開すると、受信したenvelope全体のJSONが1行1件で並びます。DB上ではpayloadを別列に複製しません。

## 列の意味

| 列 | 意味 |
|---|---|
| `batch_id` | DB内のバッチID。取引所sequenceの代替ではない |
| `schema` | 新規SQLite保存は`raw_v6_sqlite`。移行済み旧行は`raw_v5_duckdb`を保持 |
| `market` | `binance_perp`等の市場識別子 |
| `stream` | `trades`、`book_updates`、`liquidations`、`snapshots`、`open_interest` |
| `first_event_ts_ms` / `last_event_ts_ms` | バッチ内のイベント時刻の範囲 |
| `first_recv_ts_ms` / `last_recv_ts_ms` | バッチ内のReceiver受信時刻の範囲。保持期限は`last_recv_ts_ms`基準 |
| `row_count` | gzip内のJSON envelope件数 |
| `raw_gzip` | envelope全体を改変せず改行区切りでgzip圧縮したBLOB |
| `raw_bytes` | 圧縮前JSON Linesのバイト数 |
| `written_at_ms` | DB書き込み時刻 |

Receiverはpayloadの特徴量化、正規化、集計を行いません。保持期限の削除はバッチ単位なので、同じバッチ内の期限境界は最大10秒または16,384件分の粒度になります。

`open_interest`のpayloadには、OI本体に加えて取得時刻、取引所側の`source_ts`、mark price、funding rate、単位変換後の`oi_btc` / `oi_usd`、取得状態を含めます。OI未取得時もエラー状態の観測として保存します。

## 基本確認SQL

Receiver停止中、または専用のquery経路から実行します。

```sql
SELECT stream, market, count(*) AS rows
FROM raw_batches
GROUP BY stream, market
ORDER BY stream, market;
```

raw件数を確認する場合は`count(*)`ではなく`sum(row_count)`を使います。

```sql
SELECT stream, market, sum(row_count) AS raw_rows, sum(raw_bytes) AS raw_bytes
FROM raw_batches
GROUP BY stream, market
ORDER BY stream, market;
```
