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

## 時刻の区別（receive time / event time / write time）

Issue #12以降、envelope内の時刻は3種類に明確に区別されます（`raw_gzip`に収まるJSON Linesの各envelope列）。

| フィールド | 意味 | 代替値の禁止 |
|---|---|---|
| `recv_ts_ms` | **receive time**: socket message境界で確定したwall-clock受信時刻。buffer/replay後も不変 | connector/workerは`Date.now()`で再採番しない。欠落は`null`（unknown） |
| `recv_mono_ns` | プロセスmonotonic時刻(ナノ秒)。同一コネクション内で厳密単調増加 | 欠落時は`null` |
| `receive_seq` | コネクション世代内の単調増加frame counter。新コネクションで1から再開 | — |
| `connection_id` | socket世代の識別子(`market:pid:seq`) | 未接続時は`null` |
| `source_event_ts_ms` | **event time**: 取引所が付与したevent発生時刻(パース済みms)。不正/欠落は`null` | `Date.now()`等のローカル時刻を代用しない |
| `source_event_time_known` | source時刻が既知かどうか(`true`/`false`) | — |
| `event_ts_ms` | envelopeのイベント時刻。source既知ならsource時刻、ローカル合成ならその処理時刻 | — |
| `written_at_ms` / batchの`first/last_recv_ts_ms` | **write time**: DB書き込み時刻 | — |

- source timestampを持たないfeed（Coinbase Advanced Trade l2、Kraken/Gemini/Bitfinex/BitMEX book等）は`ts`にローカル処理時刻を持ちますが、`source_event_ts_ms=null`・`source_event_time_known=false`・`event_time_source`(`local`/`book_local`/`rest_snapshot`等)で「source時刻不明」を明示します。
- RESTスナップショット/定期bookスナップショットも同様に`source_event_ts_ms=null`とし、`receive_seq=null`（socket frameではないため）で区別します。
- `recv_ts_ms >= source_event_ts_ms`は必須条件にしません（clock差を許容）。

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
