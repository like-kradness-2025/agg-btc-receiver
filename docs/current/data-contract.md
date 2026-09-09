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

1. **追記のみ** — writerはINSERTしか実行しない。commit済行は後続appendで変更されない（UPDATE/merge/sort/dedupe-rewriteは本経路に存在しない）。**自動TTL削除も存在しない**（後述: `pruneExpired()`の対象はlegacy `raw_batches` のみ）。`frame_sha256`で改変検出できる。
2. **arrival-order** — `frame_id`（AUTOINCREMENT）が物理到着順。`event_ts_ms`はmetadataであり並び順を決めない。`ORDER BY frame_id`で受信順全走査できる。
3. **重複防止はappend-safe** — 同一 `(connection_id, receive_seq)` が既存なら**skip**（INSERT constraint violationを期待動作として数える）。既存行のUPDATEはしない。`receive_seq = NULL`（REST等の非socket観測）はSQLite UNIQUEのNULLセマンティクスにより常に追記される。
4. **fail-closed** — market単位の `BEGIN IMMEDIATE` トランザクション内で全行INSERT。想定外エラーはROLLBACKしバッチ全体を失敗扱い（部分書き込みなし）。呼び出し側（main process）は`reportRawDbFailure`でfail-closedにする。
5. **dual-path** — 旧 `raw_batches` は従来どおり可変として維持（downstream互換）。両テーブルは同一market DB内でschema分離され、相互に書き込まない。
6. **TTL対象外（無期限保持）** — `canonical_frames` は `pruneExpired()`（6時間毎・`orderflow_monitor.mjs`）の自動削除対象**ではない**。削除はlegacy `raw_batches` 系のみ。canonicalの削除はoperatorの明示操作のみ（手動DELETE / archive job）で、Receiverプロセスはcanonical履歴を自動破棄しない。容量増大は承知の上で、Issue #9/#10/#11 の「保存済みsource rawから全履歴を再計算できる」前提を守るための設計。

### frame_json のenvelope shape（canonical専用・読み取りは新規adapterが必要）

`frame_json`はcanonical専用のJSON 1行（下記shape）。**既存downstreamのraw parser（例: `agg-btc-downstream` の `parseRawLine`）はこのenvelopeを読めない** — payload本体が`frame`値に閉じているため既存parserの期待する構造とは異なり、渡すとnull扱いで読み捨てられる。canonical行を読むには**新規adapter**（envelopeから`frame`値を取り出し既存parserへ渡す変換層）が**将来のdownstream作業**として必要。これに対し legacy `raw_batches`（dual-path）は従来どおり既存parserで読み書きできる。

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

- `frame` が**parser前の完全なsource frame text**（socket受信bytesをそのまま文字列化）。canonical raw経路はこのtextを解釈しない（qty/side/ts換算ゼロ = Issue #10のNon-goal順守）。byte-exact保存のため、text内の改行はrejectしない（外側の`JSON.stringify()`がエスケープし、`frame_json`は常に1行JSONとして格納される）。
- キー名は既存envelopeと一部重複するが**additive互換ではない**（上記のとおり既存parserは読めない）。canonical行の消費は新規adapterの責務であり、legacy `raw_batches` の互換性は影響を受けない。

### 補足

- 有効化: `raw_storage=sqlite` 時に、`_canonicalFrameMeta()` / `_canonicalUnparsedFrameMeta()` を実装したconnector（現状 Binance系6 market）のsocket messageをparser前で捕捉し `canonicalFrames` IPC → `RawSqliteWriter.appendCanonical()` へ流す。
- **保持（TTL対象外）**: `canonical_frames` は自動TTL削除の対象外。`pruneExpired()`（起動時+6時間毎・`orderflow_monitor.mjs`）は legacy `raw_batches` のみを `last_recv_ts_ms` 基準（既定90日）で削除し、canonical行には一切触れない。canonicalは無期限に単調増加する（容量増大は許容する設計。Issue #9/#10/#11 の「保存済みsource rawから全履歴を再計算できる」前提を維持）。削除が必要な場合はoperatorの明示操作（手動DELETE / archive job）のみ。なお行DELETEは、削除した `(connection_id, receive_seq)` の再送に対してappend-safe重複ガードが効かなくなる副作用を持つため、削除より退避（コピー + cut-over）を推奨する。archive方針・実装は将来の運用作業として別途定める。
- **分類 `parse_failed`（reserved stream）**: socket境界で `JSON.parse` に失敗したframeも、**parse試行より先に**byte-exact textを捕捉して保存する（Issue #10の「parser前source frame保存」を充足。`lib/base-connector.mjs` のmessage handlerはparse成功/失敗に関わらず先にcanonical envelopeをemitする）。失敗frameは内容を解釈できないためstream分類は不可能で、予約値 `stream='parse_failed'`・`channel=null` で記録される（`CANONICAL_UNPARSED_STREAM`定数）。`frame_json.frame` に失敗したsource textそのものを保持し、`receive_seq`/`recv_ts_ms` 等のingress metadataは他frameと同じ。既存downstream parserはこの行も読まない（新規adapterで `parse_failed` 行を除外/別扱いするかはdownstream側の設計判断）。対象connector（Binance系）のみ有効で、opt-inしないconnectorのparse失敗frameは従来どおりerror eventのみ。**空文字`''`のframe**（空socket message → parse失敗）も同様に `frame: ""` としてbyte-exact保存される（Astra再監査P2修正。拒否されるのは`frame_text`が**欠落/非文字列**の場合のみ。空文字と欠落を区別する）。
- 実データ移行手順: `docs/current/canonical-raw-migration.md` 参照。なお旧 `raw_batches` はnormalized eventでありsource frameを含まないため、**過去分のcanonical化は不可能**（新経路は有効化時点から蓄積）。
