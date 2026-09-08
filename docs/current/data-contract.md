# raw データ契約（SQLite）

本ドキュメントはmarket別SQLite（`data/sqlite/<market>.sqlite`）内の2系統を定義する。

1. **`raw_batches`**（schema `raw_v6_sqlite` / 移行済み `raw_v5_duckdb`）— 従来のnormalized互換層。event-time sort / late-event UPDATE mergeを含む可変batch。
2. **`canonical_frames`**（schema `raw_v7_canonical`）— Issue #10/#11のcanonical raw。**immutable append-only / arrival-order**のsource frame log。

## 1. raw_batches（従来契約・変更なし）

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

## 2. canonical_frames（Issue #10/#11 新契約）

```sql
CREATE TABLE canonical_frames (
  frame_id INTEGER PRIMARY KEY AUTOINCREMENT,
  schema TEXT NOT NULL,            -- 'raw_v7_canonical'
  market TEXT NOT NULL,
  stream TEXT NOT NULL,            -- trades / book_updates / liquidations ...
  channel TEXT,                    -- 取引所stream識別子（例 'btcusdt@trade'）
  connection_id TEXT,              -- socket世代識別子（#12）
  receive_seq INTEGER,             -- コネクション内単調増加frame counter（#12）
  recv_ts_ms INTEGER NOT NULL,     -- socket境界で確定した受信時刻（#12）
  recv_mono_ns INTEGER,
  source_event_ts_ms INTEGER,      -- 保存時に解釈しないため原則 null（metadata）
  source_event_time_known INTEGER,
  event_ts_ms INTEGER,             -- 検索用metadata。並び順を決めない
  ingest_seq TEXT,
  writer_session_id TEXT,
  frame_json TEXT NOT NULL,        -- envelope JSON 1行（下記shape）
  frame_sha256 TEXT NOT NULL,      -- frame_jsonのSHA-256（immutability検証用）
  written_at_ms INTEGER NOT NULL,
  UNIQUE (connection_id, receive_seq)
);
```

### 不変条件（Issue #10 / #11 Done条件のstorage側）

1. **追記のみ** — writerはINSERTしか実行しない。commit済行は後続appendで変更されない（UPDATE/merge/sort/dedupe-rewriteは本経路に存在しない）。`frame_sha256`で改変検出できる。
2. **arrival-order** — `frame_id`（AUTOINCREMENT）が物理到着順。`event_ts_ms`はmetadataであり並び順を決めない。`ORDER BY frame_id`で受信順全走査できる。
3. **重複防止はappend-safe** — 同一 `(connection_id, receive_seq)` が既存なら**skip**（INSERT constraint violationを期待動作として数える）。既存行のUPDATEはしない。`receive_seq = NULL`（REST等の非socket観測）はSQLite UNIQUEのNULLセマンティクスにより常に追記される。
4. **fail-closed** — market単位の `BEGIN IMMEDIATE` トランザクション内で全行INSERT。想定外エラーはROLLBACKしバッチ全体を失敗扱い（部分書き込みなし）。呼び出し側（main process）は`reportRawDbFailure`でfail-closedにする。
5. **dual-path** — 旧 `raw_batches` は従来どおり可変として維持（downstream互換）。両テーブルは同一market DB内でschema分離され、相互に書き込まない。

### frame_json のenvelope shape（additive互換）

`frame_json`は既存raw parse（`raw_batches.raw_gzip`の各行）とadditive互換なJSON 1行。

```json
{
  "schema": "raw_v7_canonical", "market": "binance_perp", "stream": "trades",
  "channel": "btcusdt@trade",
  "connection_id": "binance_perp:123:1", "receive_seq": 42,
  "recv_ts_ms": 1750000000000, "recv_mono_ns": 123456,
  "source_event_ts_ms": null, "source_event_time_known": null,
  "event_ts_ms": null, "ingest_seq": "17", "writer_session_id": "sqlite:1:2",
  "frame": "{\"stream\":\"btcusdt@trade\",\"data\":{\"e\":\"trade\",...}}",
  "written_at_ms": 1750000000100
}
```

- `frame` が**parser前の完全なsource frame text**（socket受信bytesをそのまま文字列化）。canonical raw経路はこのtextを解釈しない（qty/side/ts換算ゼロ = Issue #10のNon-goal順守）。
- 既存raw parseの必須キー（`schema/market/stream/recv_ts_ms/connection_id/receive_seq`等）を維持し、`frame`・`channel`は追加キー。

### 補足

- 有効化: `raw_storage=sqlite` 時に、`_canonicalFrameMeta()` を実装したconnector（現状 Binance系6 market）のsocket messageをparser前で捕捉し `canonicalFrames` IPC → `RawSqliteWriter.appendCanonical()` へ流す。
- 保持: `pruneExpired()` が `recv_ts_ms` 基準で90日経過行を削除（運用TTL。writerの書き込み経路にDELETE/UPDATEはない）。
- 実データ移行手順: `docs/current/canonical-raw-migration.md` 参照。なお旧 `raw_batches` はnormalized eventでありsource frameを含まないため、**過去分のcanonical化は不可能**（新経路は有効化時点から蓄積）。
