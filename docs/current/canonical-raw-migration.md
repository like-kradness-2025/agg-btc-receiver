# Canonical raw (raw_v7_canonical) 実データ移行手順

対象: Issue #10 / #11。本PRは**コード+手順書のみ**で、実DBへの適用・移行コマンドの実行は行わない。

## 前提

- `canonical_frames` テーブルは `RawSqliteWriter` のopen時（`configureDatabase`）に各market SQLiteへ自動作成される。
- 旧 `raw_batches`（raw_v6_sqlite）はnormalized eventであり、取引所source frameを含まない。したがって**過去分のcanonical化（backfill）は構造的に不可能**。canonical rawは有効化時点からの蓄積となる。これはIssue #10の「parser誤りを後から直しても過去rawから再計算できない」問題に対する前方修正であり、過去データの再解釈はraw-v4等のarchive由来でのみ可能（別Issue Scope）。

## 有効化手順（本番）

1. レポジトリ更新・再起動で `raw_storage=sqlite` の各market DBに `canonical_frames` が作成される。
2. 稼働確認:
   ```sql
   -- 各marketで受信frameが追記され始めていること
   SELECT count(*) AS frames, min(recv_ts_ms), max(recv_ts_ms)
   FROM canonical_frames;
   ```
3. arrival-order検証（frame_id == 受信順）:
   ```sql
   SELECT frame_id, receive_seq, recv_ts_ms, stream, channel
   FROM canonical_frames ORDER BY frame_id DESC LIMIT 20;
   ```
4. immutabilityスポット検証: `frame_sha256` と `sha256(frame_json)` の突合（運用開始から数時間後に再実行し、commit済行のhash不変を確認）。
   ```sql
   SELECT count(*) FROM canonical_frames
   WHERE frame_sha256 != lower(hex(sha256(frame_json)));
   -- → 0 を期待（SQLite sha256() は3.45+ / JSON1不要）
   ```
5. 重複skip確認: `(connection_id, receive_seq)` の重複が無いこと。
   ```sql
   SELECT count(*) FROM (
     SELECT connection_id, receive_seq FROM canonical_frames
     WHERE receive_seq IS NOT NULL
     GROUP BY connection_id, receive_seq HAVING count(*) > 1
   );
   -- → 0 を期待
   ```

## ロールバック

- 新テーブルは既存 `raw_batches` と独立のため、旧バージョンへ戻してもdownstream互換層（raw_batches）は影響を受けない。`canonical_frames` はDROPしても良いが、追記済みデータは失われる（不可逆）。

## 想定外エラー時の挙動

- `appendCanonical` 失敗はmain processの `flushCanonicalDbQueue` → `reportRawDbFailure` でfail-closed（プロセス停止）。再起動後に未flush分は失われる（既存rawバッチ方針と同一）。
