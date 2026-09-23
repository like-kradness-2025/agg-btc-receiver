# Receiver 修正マップ (fix-map)

対象: `/home/weed420/Tool/agg-btc-receiver`（systemd: `agg-btc-receiver.service`）
目的: **修正時のトークン節約**。まずこの1枚を読み、「症状→最初に読む」で指定されたファイルだけを開く。
リポジトリ全体を grep しない。記載は実測/実装で確認済みの事実のみ（未確認は明記）。

既存docs: `docs/current/README.md`（正本の入口）, `docs/current/receiver-storage.md`（保存仕様）,
`docs/current/data-contract.md`（データ形式の細部・必要な時だけ）, `docs/canonical-pipeline.md`。

---

## 1. 全体像

```
systemd (agg-btc-receiver.service)
  └─ node orderflow_monitor.mjs … main。受信した生frameを market別SQLite へ書く
       ├─ worker_threads ×9 (lib/orderflow-worker.mjs)
       │    └─ connectors (lib/*-connector.mjs ← lib/base-connector.mjs)
       │         取引所WS → 正規化 → 生frame(frame_text) を IPC で main へ
       └─ data/sqlite/<market>.sqlite … 現行の正本（テーブル: raw_batches ほか）
    別process: scripts/receiver-auto-restart-watchdog.py（health鮮度で再起動）
               ~/Tool/btc-net-monitor（別repo・5分heartbeat, logs/anomalies.jsonl）
```

- **現行の保存経路** = main の `lib/raw-sqlite-writer.mjs` が per-market SQLite へ書く一本のみ。
  **保持(retention)と削除も同じファイル**（`retentionDays` / prune）。
- **worker 側にも writer がある**（`lib/orderflow-worker.mjs:166-173` で `RawV4Writer`/`RawRotationWriter`/
  `SnapshotWriter` を選択、呼び出しは span `worker.writeBatch`）。**main の SQLite 正本ライターとは別物**。
  live の SQLite 設定でこの writer が実DBへ書いているかは**未確認**（触る前に確認する）。
- 時間分割・Parquet archive・TFP・Book Snapshot・OrderHeatmap は**現行の保存経路に含めない**
  （`SPEC.md`）。`lib/raw-rotation-writer.mjs`・`scripts/cleanup-raw.mjs`・`scripts/archive-raw-v4.mjs` は
  **旧経路/アーカイブ用**。容量問題の参照先にしない（現行は `raw-sqlite-writer.mjs`）。
- 観測（span/異常記録）: `lib/stall-probe.mjs`（main/worker 共通）, `lib/ipc-flush.mjs`（IPC送信＋span）,
  `lib/health-monitor.mjs`（health行の書込ポリシー）。

## 2. 症状 → 最初に読むファイル / テスト

| 症状 | 実装 | テスト |
|---|---|---|
| 処理が止まる・遅い | `lib/stall-probe.mjs`, `lib/raw-sqlite-writer.mjs`(append) | `test/stall-probe.test.mjs`, `test/raw-sqlite-writer-slow-append.test.mjs` |
| append内訳が知りたい | `lib/raw-sqlite-writer.mjs` の `AppendTrace`（`begin`区間名 `writer.*`） | 同上 |
| 重複行 | `_dropFreshRedeliveries` / `_rememberFreshIdentities` / `_sweepFreshDedupe` | `test/raw-sqlite-fresh-dedupe.test.mjs` |
| 遅着イベントの取り込み | `_backfillLateGroup`（`INDEXED BY raw_batches_stream_last_event_idx`） | `test/raw-sqlite-late-candidates.test.mjs` |
| IPC送信の不具合 | `lib/ipc-flush.mjs`（+ worker の `flushRawIpc`/`flushCanonicalIpc`） | `test/ipc-flush.test.mjs` |
| 欠落・無音・再接続 | `lib/base-connector.mjs`, `lib/orderflow-worker.mjs` | `test/base-connector.test.mjs`, `test/connector-parser.test.mjs`, `test/additional-markets-connector.test.mjs` |
| health/監視 | `lib/health-monitor.mjs` + `scripts/receiver-auto-restart-watchdog.py` | `test/health-monitor.test.mjs` |
| market状態/完了判定 | `lib/market-status.mjs`, main の readiness 出力 | `test/market-status-file.test.mjs`, `test/market-status-tracker.test.mjs` |
| 保持・prune | `lib/raw-sqlite-writer.mjs`(`retentionDays`) | `test/raw-sqlite-prune-resilience.test.mjs`, `test/raw-sqlite-writer.test.mjs` |

集中テスト: `node --test test/<file>.test.mjs` / 全件: `npm test`（`node --test 'test/**/*.test.mjs'`）

## 3. 壊してはいけない不変条件

1. **raw は正本**。過去の重複行も消さない（修正は今後のみ）。
2. **順序**は (market, stream, 受信順)。main が SQLite 正本の唯一のライター。
3. **同一性**: `source_id` が無い行は重複除去しない。Kraken v1 の `source_id` は trade_id ではないので
   「正当な双子」を残す。fresh-dedupe の窓キーは (market, stream, source_id, event_ts)。
4. **窓の意味**: 失効は `at < now - window`（境界は残す）、上限超過は挿入順の古い方から追い出す。
5. **観測は本処理を壊さない**（観測側の例外を書き込み/送信の失敗として扱わない）。
6. **記録は異常時のみ**。ただし health の **30秒生存行は必須**（watchdog が90秒鮮度で再起動判断）。
7. **順序を保証できないなら flush を分割しない**（無音検知の遅延・順序破壊を招く）。

## 4. 観測データの読み方（`data/live_sqlite/`）

- `stall-events.jsonl` … 異常のみ。`kind`: `slow_append` / `stall` / `recovered` / `worker_stall`。
  - `spans`: `{name, dur_ms, details}`。`writer.*` = append内訳、`worker.ipc_send_*` = IPC送信、
    `worker.writeBatch` = worker側writer。
  - `cpu_ms`（process.cpuUsage 差）, `loop_ticks`（50ms間隔カウンタの更新回数）, `gc_count`/`gc_ms`,
    `gc_supported`（**false なら `gc_ms=0` は「GCなし」の証明にならない**）, `heap_delta_mb`。
  - **`loop_ticks=0`** は「その計測区間内にカウンタ更新が観測されなかった」ことを示す（カウンタの
    説明ではない）。**区間長と併せて見る**（区間が秒単位で 0 なら、ほぼ連続ブロックと判断できる）。
  - `spans` 合計と wall の差を「未計測時間」と決めつけない（計測点の外側がある）。
- `health.jsonl` … 異常時は毎秒／状態変化・静かな市場(60秒)は即／正常時は30秒の生存行のみ。
- `../market-status.json` … `process_ready` / `data_complete` / markets（`state: running` 等）。監視が参照。
- `~/Tool/btc-net-monitor/logs/anomalies.jsonl` … `receiver:not_running=<market>:reconnecting` など。

## 5. 変更〜本番反映〜確認（この順で。確認はここだけに集約）

1. 変更は `~/Tool/agg-btc-receiver`（本番の実行ツリー。branch `feat/canonical-raw-off`）で行う。
   別worktreeで作業した場合は、**本番ツリーへ適用**する:
   `git -C ~/Tool/agg-btc-receiver merge --ff-only <sha>`（不可なら `cherry-pick <sha>`）。
   本番ツリーのブランチは**切り替えない**。
2. 全テスト: `npm test`（fail 0 が条件）。集中確認は §2 の表のファイル指定で。
3. commit → `git branch -f fix/<topic> HEAD && git push origin fix/<topic>`（公式記録は `master`）。
4. `systemctl --user restart agg-btc-receiver`。
5. **確認（4点すべてを見るまで「反映した」と言わない）**:
   1. **変更が本番ツリーに入っているか**: `git -C ~/Tool/agg-btc-receiver log --oneline -1`
      （対象コミット、またはその子孫であること。稼働確認だけでは反映の証明にならない）
   2. `systemctl --user is-active agg-btc-receiver` が `active`
   3. `systemctl --user show agg-btc-receiver -p NRestarts --value` が増えていない
   4. 稼働の健全性（**絶対パスで**）:
      `python3 -c "import json;d=json.load(open('/home/weed420/Tool/agg-btc-receiver/data/market-status.json'));print(d['process_ready'],d['data_complete'])"`
      が `True True`（相対パスだとworktree側の別ファイルを読んでしまう）
6. 失敗時の戻し: `git -C ~/Tool/agg-btc-receiver revert <sha>` → restart → 5 の確認。

## 6. 実際に踏んだ罠

1. **`StallProbe.begin()` は `{ end() }` を返す**（コールバックではない）。戻り値を関数として呼ぶと
   `end is not a function` で**起動失敗ループ**（2026-09-23、約85秒停止）。`lib/ipc-flush.mjs` が正しい形。
2. **この事例では**集中テストが `flushRawIpc` を通らなかったため、不具合が**本番で初めて顕在化**した
   （「未テスト経路は必ず本番で初めて動く」と一般化はしない）。送信/書き込み経路を変えたら
   実経路のテストを足す。
3. **全suite一斉実行は負荷依存のflakeがある**（`test/burst-reducer/tfp-lock-integration.test.mjs` は
   分離実行で pass、並列の全suiteで落ちることがある）。まず分離実行で切り分ける。
4. **ベンチは対象分岐を実際に通す**。fresh-dedupe の窓を「上限ちょうど」まで埋めたベンチは削除が
   1件も走らず、「窓は安い」という誤結論を出した。
5. 個別事例: 遅着候補SELECTが索引選択で全履歴走査になった（本番DBコピーで 12.7秒→0.1ms、
   `INDEXED BY` で固定し結果同一を確認）。窓の sweep は **Map 反復中の delete** が高コストだった
   （実測833ms、削除対象を集めてから削除する形に変更）。
6. **`systemctl restart` 後は待って §5-5 を確認**。起動失敗ループ時は `NRestarts` が増える。

## 7. 変更後チェックリスト

- [ ] 変更した経路を実際に実行するテストがある（無ければ追加）
- [ ] `npm test` が fail 0
- [ ] 不変条件 1〜7 を壊していない
- [ ] §5-5 の3点を確認した
