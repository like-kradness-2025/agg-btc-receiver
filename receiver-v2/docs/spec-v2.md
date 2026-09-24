# Receiver v2 仕様（C案: 骨格は新規・中身は資産を移植）

- 状態: **確定候補 / Astra 監査待ち**
- 前提: **現行を分割するのではなく、完全新規の構造（`receiver-v2/`）を作る** ✓
  **ただし検証済みの資産（venue差の知識・canonical raw 形式・下流契約・テスト）は移植して使う** ✓
- 対象外: **Downstream（別サービス）** ✗ — 下流は**読む側**であり、その契約を v2 が守る ✓
- 実装の場所: `~/worktrees/agg-receiver-v2`（branch `feat/receiver-split`・基点 `origin/master` ✓）

## 1. 構造（3プロセス＋共通の監視）
| プロセス | 責務 | 所有者（決定権） |
|---|---|---|
| **ingest（受信）** | WS接続・購読・venue差の吸収・keep-alive・再接続 | 接続世代・購読状態・受信メタ（`recv_ts_ms`/`recv_mono_ns`/`connection_id`/`receive_seq`）|
| **organize（整理）** | raw の耐久化・重複排除・並び順の確定・watermark・遅着・backfill | 耐久化境界・ACK・再開 cursor・欠測の確定 |
| **book（板更新）** | 板の維持・同期境界の証明・diff 適用 | 同期境界・世代判定・fail-closed 判定 |
| **supervisor（共通）** | 起動・監視・停止・readiness 判定 | プロセスの生存・停止順 |

- **役割の独立性**: 板が止まっても受信・整理は止まらない ✓（整理は板の状態を参照しない ✓）
- **停止順**: 受信 → 整理 → 板 ✓（受信を先に止めて末尾を確定 ✓ 整理が全ACK確認後に正常終了マーカー ✓）
- **移行スイッチは作らない** ✗（`RECEIVER_SPLIT_MODE` のような段階切替は不要 ✓ — v2 は独立実装 ✓）

## 2. 契約（プロセス間）
- **envelope**（実装済 ✓ `src/envelope.mjs`）: `recv_ts_ms`（socket境界・不変 ✓）／`recv_mono_ns`（接続内の順序 ✓）／`(connection_id, receive_seq)`＝**再送を no-op にする鍵** ✓／`raw` は生バイトが正本 ✓
- **IPC**: UNIX domain socket・**uint32 長プレフィクスのフレーム**（実装済 ✓）／バッチ 512件 or 100ms ✓／ACK は**非同期・連続耐久範囲の上限のみ** ✓（穴があればそこで止める ✓）
- **spool**: market 別の追記専用セグメント（64MB ✓）／`O_APPEND`＋1秒 fsync ✓／古い順に流す ✓／上限＝空き5% or 2GB ✓
- **耐久化（テーブル3つ ✓）**: `pending_boundary`（watermark と同一tx ✓）／`received_tail`（1秒or1000件ごとの下限 ✓）／`run_marker`（起動時に旧世代を無効化 ✓ 正常終了時のみ現世代を complete ✓）
- **バックプレッシャ**: メモリキュー → spool → **受信停止＋欠測記録** → 記録不能なら**非ゼロ exit** ✓
- **不変条件（守る順）**: ①世代・購読順序・欠落/二重適用なし・**旧世代の同期結果は拒否**・**fail-closed** ②受信メタは不変・**耐久化前に ACK/再開位置を進めない**・再送は冪等・満杯でも黙って捨てない ✓

## 3. 移す／捨てる線引き表（**この仕様の核**）
| 現行 | 行数 | v2 での扱い | 理由 |
|---|---:|---|---|
| `lib/base-connector.mjs` | 1042 | **分割して移植**: 接続・購読・venue差 → `src/ingest/` ✓／板同期・世代・fail-closed → `src/book/`（新規実装 ✓） | 停滞の原因＝**この同居** ✗ を構造で切る |
| `lib/*-connector.mjs`（15本 ✓） | — | **移植** → `src/ingest/venues/` ✓ | venue差は**実測で潰した経験の塊** ✓ 作り直すと穴を踏む ✗ |
| `lib/subscribe-tracker.mjs`・`pong-watch.mjs`・`reconnect-backoff.mjs`・`sync-retry.mjs` | — | **移植** ✓（受信側の制御 ✓） | 実測で閾値を確定済（15秒 ✓ 60秒安定 ✓）✓ |
| `lib/orderflow-worker.mjs` | 825 | **廃止** ✗ → 役割を3プロセスへ分配 ✓ | 1プロセスに受信・整理・板が同居していた本体 ✗ |
| `orderflow_monitor.mjs` | 1159 | **分割**: 監視・起動・停止・readiness → `src/supervisor/` ✓／他は各プロセスへ ✓ | 監視の意味（readiness ✓）は維持 ✓ |
| `lib/raw-sqlite-writer.mjs` | 1306 | **移植** → `src/organize/` ✓ | **形式 `raw_v6_sqlite` が下流契約** ✓（変えると下流が壊れる ✗）|
| `lib/raw-rotation-writer.mjs` | 1102 | **移植** ✓ | ローテーションの実績 ✓ |
| `lib/raw-db-pending.mjs`・`buffered-writer.mjs` | — | **移植** ✓ | pending の意味・落ちない書き込みの実績 ✓ |
| `lib/book-*.mjs`（state-machine / full-book / snapshot-writer / materializer ✓） | — | **移植** → `src/book/` ✓ | 板の実装は検証済 ✓ |
| `lib/stall-probe.mjs` | 313 | **移植＋拡張** ✓ | 全worker＋mainの lag 同時記録・span を追加 ✓（Step 0 の残り ✓）|
| `lib/health-monitor.mjs`・`market-status.mjs` | 319 | **移植** ✓ | **異常時のみ＋30秒生存行** ✓／readiness キーの意味を維持 ✓ |
| `lib/downstream/*` | — | **触らない** ✗ | 別サービス・読む側 ✓ |
| mono 構成の暫定スイッチ・枝葉 ✗ | — | **捨てる** ✗ | v2 では不要 ✓ |

## 4. 下流互換＝**受け入れ条件**（ここが満たせないと切替できない ✓）
- `raw_v6_sqlite` のスキーマ・フィールドを**同一に保つ** ✓（`recv_ts_ms`/`recv_mono_ns`/`connection_id`/`receive_seq`・重複排除の意味 ✓）→ **下流は無変更で読めること** ✓
- `market-status.json` の readiness キー（`process_ready`・`data_complete`・`ts_ms` ✓）を維持 ✓
- 監視系の出力（`health.jsonl`・`stall-events.jsonl` ✓）の**形を維持** ✓（書き込み頻度は「異常時のみ＋30秒」✓）
- systemd ユニット名の互換: 切替は**ユニット差し替え**で行えること ✓

## 5. 併走と切替（カットオーバー）
- **併走**: v2 は**別の出力先**に書く ✓（v1 のデータを壊さない ✓）
- **比較指標**: 停滞件数／欠落・重複0／append 遅延 p99／CPU・RSS／`data_complete` の一致 ✓
- **切替条件**: **24時間の併走で停滞0件・欠落0・遅延が v1 以下** ✓ → **明示指示で切替** ✓
- **ロールバック**: ユニットを v1 に戻すだけ ✓（v1 のデータは無傷 ✓）

## 6. 実装順（1つずつ・テスト付き ✓）
1. **`src/ipc.mjs`**（framed transport・有界キュー・バックプレッシャ ✓）— envelope は実装済 ✓
2. **`src/spool.mjs`**（market別セグメント・古い順の再送 ✓）
3. **`src/durability.mjs`**（`pending_boundary`/`received_tail`/`run_marker` ✓・再開契約 ✓）
4. **`src/book/`**（世代・同期境界・fail-closed ✓）+ `src/ingest/`（venue差の移植 ✓）
5. **`src/organize/`**（raw writer の移植＋watermark・遅着 ✓）
6. **`src/supervisor/`**（起動・監視・readiness ✓）
7. **下流互換テスト**（v1 と同一形式で読めること ✓）→ 併走 → 切替

## 7. 未確定（実装中に確定させる ✓）
- venue 仕様の未確認項目（Kraken のサーバー切断条件・Binance perp の保守通知・Hyperliquid の snapshot ack ✓）
- spool の実運用値（セグメント長・上限 ✓）と fsync 間隔の最終値
- 併走時の**データ量**（raw が倍になる期間のディスク見積り ✓）
