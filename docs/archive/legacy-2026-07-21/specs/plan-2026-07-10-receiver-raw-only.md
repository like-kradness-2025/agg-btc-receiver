# Receiver Raw-Only Implementation Plan

> **実装反映（2026-07-19）**
>
> 本計画のraw-only境界は現行実装へ反映済み。現在のworkerには派生writer用の
> `doFlushCycle()`は存在しないが、raw writerのactive `.open`を保護するための
> 定期flush timerは存在する。以下の「flush timerを削除」は旧派生処理用timerを
> 指し、raw 3 writerのflushを削除する意味ではない。

> **For Hermes:** PDDでタスク単位に specialist へ委譲し、各タスク後に親が実ファイルを再読する。実装レビューは本質のみ、95点未満は次へ進めない。

**Goal:** Receiverを「取引所イベントの受信・標準化・raw 30秒ブロック保存・受信品質監視」だけに限定し、1秒集計・ローカル板snapshot・REST補助収集を別責務へ移す。

**Architecture:** Receiverの正本出力を `trades/`、`book_updates/`、`liquidations/` に限定する。`health.jsonl` は運用監視として残す。`agg_trades/`、`snapshots/`、`book_snapshots/` はReceiver内で生成しない。依存破壊を避けるため、最初にburst reducer #12をraw trades直計算へ移行し、REST補助収集は別entrypointへ分離してからReceiverから外す。

**Tech Stack:** Node.js ESM、worker_threads、RawRotationWriter、node:test、JSONL 30秒絶対時刻ブロック。

---

## 0. 固定する責務境界

### Receiverに残す

| 出力/処理 | 判定 | 根拠 |
|---|---|---|
| `trades/<market>/<date>/<HH-MM-SS>.jsonl` | 保持 | connectorがemitした標準化済み個別trade |
| `book_updates/<market>/<date>/<HH-MM-SS>.jsonl` | 保持 | connectorがemitした標準化済みsnapshot/updateイベント。板再生の正本 |
| `liquidations/<market>/<date>/<HH-MM-SS>.jsonl` | 保持 | connectorがemitした標準化済み清算イベント |
| `health.jsonl` | 保持 | state/stats/鮮度を監視する運用データ。市場派生特徴量ではない |
| connector内のbook sync/checksum/reconnect | 保持 | 正しい受信を成立させる品質管理 |
| worker IPC `ready/stateChange/stats/liquidation` | 保持 | 稼働管理とhealth更新に必要 |

「raw」は取引所wire payloadの無加工保存ではなく、**connectorが標準化してemitした受信イベント**を意味する。既存schemaは変更しない。

### Receiverから外す

| 現在の出力/処理 | 判定 | 移管先 |
|---|---|---|
| `TradeAggregator` | Receiverから削除 | downstream raw-trade reducer |
| `agg_trades/` | Receiverから削除 | 必要ならraw tradesから後段生成。ただしburst #12は中間物なしで直計算 |
| `snapshots/` | Receiverから削除 | 必要なら`book_updates` replayから後段生成 |
| `book_snapshots/` | Receiverから削除 | 同上。現在は`snapshots`と同一local snapshotの重複保存 |
| `book.toSnapshot(now)`の周期実行 | Receiverから削除 | downstream replay/feature pipeline |
| `DerivativesHelper` | main Receiverから分離 | 別entrypointのaux collector |
| `MarketDataCollector` (`ohlcv/ticker/lsratio/takervol/premium`) | main Receiverから分離 | 別entrypointのaux collector |

既存の派生ファイルは削除しない。切替後に新規生成を止めるだけとする。

## 1. 現在の経路と責務違反

### raw経路

```text
exchange connector
  ├─ trade       → orderflow-worker → RawRotationWriter(trades)
  ├─ depth       → orderflow-worker → RawRotationWriter(book_updates)
  └─ liquidation → orderflow-worker → RawRotationWriter(liquidations)
```

### Receiver内に混入していた派生経路（migration前）

```text
trade → TradeAggregator → 1秒OHLCV → RawRotationWriter(agg_trades)
book state → book.toSnapshot(now) → RawRotationWriter(snapshots)
                                  └→ RawRotationWriter(book_snapshots)
main thread → REST polling → derivatives/ohlcv/ticker/lsratio/takervol/premium
```

### migration前の重要な依存

当時の`lib/burst-reducer/pipeline.mjs`は`agg_trades`を読み、特徴量 #12の分母を作っていた。現在はraw trades直計算へ移行済みであり、receiverから`agg_trades`を生成する必要はない。

## 2. 非対象

- connector parser、subscription、checksum、book sync方式の変更
- raw `trades/book_updates/liquidations` schemaの変更
- 既存raw/派生ファイルの削除・移動・再書換え
- burst特徴量#12の数学定義変更
- Receiverのmarket構成変更
- systemd/Hermes/gateway restartの即時実行
- dashboardの見た目変更

## 3. 不変条件

1. Receiverの新規市場データ出力は`trades`、`book_updates`、`liquidations`だけ。
2. すべて絶対UTC 30秒ブロック、半開区間 `[N, N+30s)`。
3. raw schema、timestamp、market名、late-event/watermark契約を維持。
4. market/kind単位のRawRotationWriter直列queueを維持。
5. recovery完了前にconnectorからのwriteを開始しない。
6. shutdownは3種類のraw writerを`finalize()`してからworkerを終了。
7. `health.jsonl`のstate/stats更新を維持。
8. `book_updates`内の`type=snapshot/update`を板再生の正本にする。
9. downstream #12はraw tradesのみで従来と同じ値を生成し、欠損はE007 fail-closed。
10. Receiver純化後、production codeから`agg_trades/snapshots/book_snapshots` writer生成が0件。

## 4. 実装順序

### Task 0: 基準線を固定する

**Objective:** 大きな既存working treeを壊さず、今回の変更前挙動を記録する。

**変更:** 文書のみ。

**確認:**

```bash
git status --short
git diff -- lib/orderflow-worker.mjs orderflow_monitor.mjs \
  lib/burst-reducer scripts dashboard.mjs config.v3.json
git show HEAD:orderflow_monitor.mjs
git ls-files lib/orderflow-worker.mjs
node --test
```

**停止条件:** 既存テストにFAILがあれば、今回由来か既存FAILかを記録するまで実装しない。`lib/orderflow-worker.mjs`は現状untrackedなので、HEADに存在する前提で差分判定しない。

### Task 1: #12をraw trades直計算へ移す（Receiverをまだ変更しない）

**Objective:** `agg_trades`を止めてもburst reducerが成立するようにする。

**Files:**
- Create: `lib/burst-reducer/raw-trades-notional-reader.mjs`
- Create: `test/burst-reducer/raw-trades-notional-reader.test.mjs`
- Modify: `lib/burst-reducer/pipeline.mjs`
- Modify: `lib/burst-reducer/feature-computer-1s.mjs`（E007文言のみ必要なら変更）
- Modify: `test/burst-reducer/pipeline.test.mjs`
- Modify: `test/burst-reducer/horizon.test.mjs`
- Modify: `test/burst-reducer/cursor-restart.test.mjs`
- Remove after migration: `lib/burst-reducer/agg-trades-reader.mjs`

**数学契約:** 各`secondTs`について、raw tradeの `price * qty` を `[secondTs-30000, secondTs)` で合計し、30秒traded notionalを返す。対象second 30個のlookup keyを必ず作る。空だが入力blockが存在する区間は0、必要block自体の欠損・malformed行・非有限price/qtyはE007。

**TDD:** 
1. 境界trade（開始含む、終了除外）のfixtureを書く。
2. rawから計算したoracleと従来agg fixtureの値が一致するテストを書く。
3. 前日跨ぎ、2ブロックlookback、valid-empty、missing、malformedをREDにする。
4. readerを最小実装。
5. pipeline importとcoverage validationをrawへ切替。
6. testsをGREENにする。

**正のprobe:** `pipeline.mjs`がraw trade pathを読む。

**負のprobe:** production `lib/burst-reducer/`に`agg_trades` path/importが残らない。

### Task 2: workerのpure raw contractテストを先に追加

**Objective:** 派生writer削除前に、残す挙動と消す挙動をテストで固定する。

**Files:**
- Create: `test/orderflow-worker-raw-only.test.mjs`
- Modify if needed: `lib/orderflow-worker.mjs`（test seamのみ、production挙動はまだ変えない）

**RED assertions:**
- tradeイベントは`trades`へ1回保存。
- depthイベントは`book_updates`へ1回保存。
- liquidationイベントは`liquidations`へ1回保存。
- `agg_trades/snapshots/book_snapshots` writerは生成されない。
- 1秒待機しても派生ファイルは生成されない。
- `stats/stateChange/ready` IPCは維持。
- shutdown時にraw 3 writerだけがfinalizeされる。

### Task 3: startup recoveryをconnect前へ移す

**Objective:** 再起動直後の受信と`.open` recoveryの競合をなくす。

**Files:**
- Modify: `lib/orderflow-worker.mjs`
- Modify: `orderflow_monitor.mjs`
- Modify: `test/orderflow-worker-raw-only.test.mjs`
- Modify: `test/orderflow-monitor.test.mjs`
- Existing verification: `test/raw-rotation-writer.test.mjs`

**設計:** `startConnector()`を少なくとも次の2段階へ分ける。

```text
prepareMarket(market)
  → book/connector/raw writers生成
  → handlers登録
  → raw writers startupRecovery完了

connectMarket(market)
  → connector.start/connect
```

全marketのrecoveryを終えてから接続を開始する。recovery失敗時はそのworkerをreadyにせずfail-closed。受信開始後にstartupRecoveryを呼ばない。

main側もfail-closedに揃える。`orderflow_monitor.mjs`のready待機が60秒を超え、`readyWorkers.size !== workers.size`なら、そのまま処理を続けてはならない。起動済みworkerへshutdownを送り、`healthMonitor`やaux serviceを開始せず、non-zeroで終了する。worker error/exitがready前に発生した場合もtimeoutまで待たず同じfatal startup pathへ入る。

**TDD oracle:**
- mock writerの`startupRecovery`完了時刻がmock connectorの`start/connect`呼出より必ず前。
- 1 workerがrecovery errorまたはready前exitした場合、mainはworkerを停止してnon-zero終了する。
- 全worker ready前に`healthMonitor.start()`と、分離前のaux service startが呼ばれない。

### Task 4: workerから派生処理を削除

**Objective:** workerを受信＋raw保存だけにする。

**File:** `lib/orderflow-worker.mjs`

**Remove:**
- `TradeAggregator` import/instances
- `aggregators`
- `snapshotRotationWriters`
- `tradeWriters` (`agg_trades`)
- `bookWriters` (`book_snapshots`)
- `bookSnapshotMs` / `lastBookSnapshot`
- `aggregator.addTrade()`
- `doFlushCycle()`と派生writer用flush timer
- derived writerのstartupRecovery/checkStale/finalize
- shutdown時の派生writer用`flushNow()`

**Keep:**
- books（connector同期・depth処理に必要）
- connector handlers
- raw 3 writer maps
- stale timer（raw 3 writerのみ）
- stats timer
- graceful shutdown

**Verification:** Task 2/3 testsをGREENにする。`TradeAggregator`参照のproduction grepが0件になる。

### Task 5: main entrypointから補助REST収集を分離

**Objective:** `orderflow_monitor.mjs`をworker orchestration＋healthだけにする。ただし補助データ収集機能自体は消さない。

**Files:**
- Modify: `orderflow_monitor.mjs`
- Create: `aux_data_collector.mjs`（名称は実装時に固定）
- Modify: `package.json`
- Create: `systemd/agg-btc-aux-collector.service`
- Modify/Create tests: `test/orderflow-monitor.test.mjs`, `test/aux-data-collector.test.mjs`
- Reuse unchanged: `lib/derivatives-helper.mjs`, `lib/market-data-collector.mjs`

**Receiverから削除:** imports、instance生成、market登録、start/close。

**別entrypointへ移す:** 同一config/output/market選択を使うDerivativesHelper/MarketDataCollector lifecycle。

**運用起動契約:** 機能をReceiverから外すだけで停止させない。

1. `package.json`に`aux`と`smoke:aux`を追加する。
2. repo管理の`systemd/agg-btc-aux-collector.service`を作る。WorkingDirectory、ExecStart、Restart、ログ出力を明示する。
3. 本番切替前にservice fileを`~/.config/systemd/user/`へ配置する手順を提示するが、`daemon-reload/enable/start`はユーザー承認後に実行する。
4. cutoverは「aux serviceを先に起動して出力更新を確認 → Receiverから内蔵auxを外した版へ切替」の順にし、無収集時間を作らない。同一outputへの二重writer時間はsmokeで短時間に限定し、本番では新Receiver起動直前に旧内蔵auxを停止して専用serviceを開始する手順を秒単位で固定する。
5. aux serviceだけ独立停止・rollback可能にする。

**Smoke command:**

```bash
npm run smoke:aux -- --config config.v3.json --seconds 5 \
  --markets binance_spot,binance_perp --output /tmp/agg-btc-aux-smoke
```

期待値は対象市場に応じた`derivatives/ohlcv/ticker/lsratio/takervol/premium`のうち設定済み系列が更新されること。イベントがない系列は必須にせず、起動ログと少なくとも1つの実ファイル最終JSON行を確認する。

**Contract:** `orderflow_monitor.mjs` production importsに`DerivativesHelper|MarketDataCollector`が0件。aux collector unit/smokeが独立して動き、専用serviceから起動可能。Receiver切替後もaux最新timestampが停止していない。

### Task 6: scripts/config/dashboard/docsを実装へ追随

**Files:**
- Modify: `config.v3.json`
- Modify: `scripts/cleanup-raw.mjs`
- Modify: `scripts/burst-agg.mjs`
- Modify: `dashboard.mjs`
- Update relevant docs/help.

**Changes:**
- Receiverで不要になる`tick.feature_ms`、`tick.book_snapshot_ms`を削除またはaux/downstream configへ移動。
- cleanupのraw種類を`trades/book_updates/liquidations`契約へ合わせる。ただしliquidation削除条件を特徴量生成済みだけで代用しない。安全条件がなければcleanup対象外のままにする。
- `snapshots`をrawと呼ぶ説明・削除対象から外す。
- dashboardの`snapshots` file countを削除。
- `scripts/burst-agg.mjs`のprocessed cleanupから`snapshots`を外す。

**禁止:** 過去ディレクトリを実際に削除するmigrationは作らない。

### Task 7: 不要コードの最終整理

**Files:**
- Remove only after all production references are zero: `lib/trade-aggregator.mjs`
- Remove/migrate: `test/trade-aggregator.test.mjs`

`classifyTradeNotional`に残存consumerがあれば、責務が明確な別moduleへ移してテストを維持する。参照が残る場合はファイル削除を保留し、Receiverからの非参照だけを合格条件にする。

### Task 8: deterministic verification

```bash
node --check orderflow_monitor.mjs
node --check lib/orderflow-worker.mjs
node --check aux_data_collector.mjs
node --test test/orderflow-worker-raw-only.test.mjs
node --test test/raw-rotation-writer.test.mjs
node --test test/burst-reducer/*.test.mjs
node --test

git diff --check
```

**Static probes:**

```bash
# Receiver productionに派生writerがない
rg 'TradeAggregator|agg_trades|book_snapshots|snapshotRotationWriters|doFlushCycle' \
  orderflow_monitor.mjs lib/orderflow-worker.mjs
# expected: 0

# Receiver entrypointにREST補助収集がない
rg 'DerivativesHelper|MarketDataCollector' orderflow_monitor.mjs
# expected: 0

# aux collectorは実際に単独起動できる
node --check aux_data_collector.mjs
npm run smoke:aux -- --config config.v3.json --seconds 5 \
  --markets binance_spot,binance_perp --output /tmp/agg-btc-aux-smoke
# expected: RC=0、設定済みaux系列の少なくとも1ファイルが存在し、最終JSON行のtsがsmoke時間内

# repo管理serviceの起動契約
systemd-analyze --user verify systemd/agg-btc-aux-collector.service
rg 'aux_data_collector.mjs' package.json systemd/agg-btc-aux-collector.service
# expected: package script + ExecStartの両方に存在

# raw保存経路は残る
rg "'trades'|'book_updates'|'liquidations'" lib/orderflow-worker.mjs
# expected: all 3

# burst reducer productionにagg_trades依存がない
rg 'agg_trades|agg-trades-reader' lib/burst-reducer
# expected: 0（migration説明コメントを除く）
```

## 5. 敵対的レビュー95点ゲート

採点対象は実装の本質だけ。

1. raw trade/depth/liquidationのデータ損失・重複。
2. recovery-before-connectとshutdown finalize。
3. raw 30秒block/schema/watermark契約の破壊。
4. Receiver内の派生処理残留。
5. #12の数学値・境界・欠損時fail-closedの回帰。
6. health/stats/stateChangeの停止。
7. aux collector分離による補助データ停止の無告知。
8. restart/rollback不能。
9. 5分試験でのCPU/RSS/reconnect storm/ファイル鮮度異常。

命名、コメント、dead code、cosmetic refactor、今回の責務外の改善案は減点しない。95点未満なら実装・計画を修正して再レビューする。

## 6. Restart前ゲートとライブ試験

### restart前に親が提示する証拠

- 全test結果。
- static probes。
- raw-only隔離smokeの実ファイル名・行数・最終JSON行。
- `agg_trades/snapshots/book_snapshots`が新規生成されない証拠。
- #12 raw計算のgolden比較。
- aux collectorの実運用起動契約（`package.json`、repo管理service、実行コマンド）。
- aux独立smokeのRC、生成ファイル、最終JSON行、最新timestamp。
- cutover手順と、Receiver分離後もaux timestampが停止していない証拠。
- rollback対象commit/diffと手順。

**Receiver/systemd restartはユーザーの明示承認後のみ。** Hermes/gateway restartは不要。

### 5分試験

まず一時output rootで1〜2 marketを5分実行する。

合格条件:
- `trades`と`book_updates`の30秒ファイルが連続生成。
- 各最新ファイルを直接読み、JSON schema・market・timestampが正しい。
- liquidationはイベントがなければファイル非生成を正常扱い。
- `health.jsonl`が毎秒更新、stats/stateが進む。
- `agg_trades/snapshots/book_snapshots`は0件。
- rawを入力したburst reducer #12がE007なしで処理。
- worker crash、unhandled rejection、reconnect stormなし。
- CPU/RSSが基準線より異常増加しない。

本番切替後も5分監視し、全marketのraw最新時刻とhealthを直接確認する。

## 7. Rollback

1. Receiver停止/再起動は承認制。
2. 問題発生時は今回のcode差分だけを直前安定版へ戻す。
3. rawファイルは削除・書換えしない。
4. aux collectorは独立停止可能にする。
5. downstreamはraw入力版を維持できるため、`agg_trades`再有効化をrollback必須条件にしない。
6. rollback後、raw 3種類とhealthの鮮度を再確認する。

## 8. 完了条件

- Receiverの市場データ出力がraw 3種類だけ。
- health維持。
- REST補助収集は別entrypoint。
- burst #12はraw trades直計算。
- snapshots系は`book_updates` replayへ一本化。
- 全test PASS。
- 本質レビュー95点以上。
- 承認後の5分ライブ試験PASS。
- rawデータの欠損・schema回帰なし。
