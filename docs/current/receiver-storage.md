# Receiver保存仕様

## 1. 保存先

本番DBはmarket別のSQLiteファイルです。

```text
data/sqlite/<market>.sqlite
```

実行時の運用ルートは次です。

```text
data/live_sqlite/health.jsonl   # 接続状態などの運用ログ
data/live_sqlite/locks/output-root.lock
data/sqlite/<market>.sqlite     # market rawの正本
```

market rawを日付・時刻・market別のファイルへ分割しません。`event_ts_ms`と`recv_ts_ms`は検索・保持期限判定のため、DBの列として保持します。

## 2. 書き込み経路

```text
Exchange WebSocket
    ↓
Worker thread
    ↓ IPC rawEvent
Receiver main process
    ↓ 最大10秒、または16,384件でflush
market別SQLite WAL raw_batches（gzipしたraw JSON Lines）
```

SQLiteの書き込み接続はReceiver本体だけが所有します。WorkerごとにDB接続を作らず、marketごとに1ファイルへまとめます。WALにより同一ホストの別Toolはread-onlyで参照できます。

## 3. 保存対象

`raw_batches.stream`には次の値を使います。

- `trades`
- `book_updates`
- `liquidations`
- `snapshots` — connectorが受信したsnapshot event
- `open_interest` — perp marketのREST OI観測値（30秒間隔）

Receiver内部のbook syncは受信継続性のために必要ですが、特徴量、1秒集計、order heatmap、canonical full snapshotは生成しません。OIは既存のDerivativesHelperで取得した観測値をraw eventとして保存します。

## 4. 保持

- 保持期間：90日
- 起動時に期限超過行を削除
- 稼働中は6時間ごとに削除
- 削除後に`CHECKPOINT`を実行
- 判定基準はバッチの`last_recv_ts_ms`
- 1バッチ内のrawは同じmarket・streamで、最大16,384件または最大10秒分

削除処理はrawの書き込みキューの後ろに直列化されます。

## 5. 既存データ

切替前のDuckDB raw_batchesは市場別SQLiteへ検証付きで移行済みです。

```text
data/agg-btc-receiver.duckdb       # ロールバック用に保持
data/sqlite/<market>.sqlite        # 現行の正本
```

旧JSONL/ParquetやderivedはReceiverの保存対象外です。DuckDBからの再移行は、既存SQLiteを上書きしないone-shot importerで行います。

## 6. 制約

SQLiteはWALモードで運用します。板可視化などの外部Toolは、対象marketのSQLiteをread-onlyで直接参照できます。長時間のread transactionは避け、DBと同じホスト上で参照します。
