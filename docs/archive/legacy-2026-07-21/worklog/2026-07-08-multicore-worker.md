# Worklog: Multicore Worker Threads for Receiver (2026-07-08)

## Goal
現在シングルスレッドで動いている receiver (`orderflow_monitor.mjs`) を `worker_threads` でマルチコア化し、CPU負荷を分散する。

## Background
- 16コアのマシンに対し、Node.js のシングルスレッドで18マーケットのWS接続＋ファイルI/Oを処理
- CPU使用率 54.8% (1コア飽和)
- load average 5.87 / 16コア → 余裕あり
- シングルスレッドボトルネックが明確

## Current Architecture
```
orderflow_monitor.mjs (main thread, single process)
  ├── 18 WebSocket connectors (all in one event loop)
  ├── 1 TradeAggregator × 18 markets
  ├── RawRotationWriter × 4 types × 18 markets (= 72 writers)
  ├── FeatureAccumulator (burst/1s-feature)
  ├── HealthMonitor
  ├── DerivativesHelper (perp funding/oi)
  ├── MarketDataCollector (REST polling)
  └── tick loop (1s) → feedBook/feedSecond for all markets
```

## Problem
- 全WS は同一スレッドで受信 → Node.js event loop が処理しきれず、CPU 1コア張り付き
- WS の SSL/parsing は libuv のスレッドプールを使うが JS 実行はシングルスレッド
- tick ループで全 market を逐次処理 → レイテンシ蓄積

## Key Files
- `orderflow_monitor.mjs`: main entry (458 lines)
- `lib/base-connector.mjs`: base WS connector
- `lib/trade-aggregator.mjs`: 1s aggregation
- `lib/raw-rotation-writer.mjs`: date-partitioned JSONL
- `lib/feature-accumulator.mjs`: burst/feature pipeline
- `lib/health-monitor.mjs`: stats reporting

## Constraints
- FeatureAccumulator は全 market の状態を共有している → main thread 維持
- HealthMonitor も全 connector の統計を集約 → main thread 維持
- RawRotationWriter は market 独立 → worker に持たせられる
- shared memory は不要（market 間の依存無し）

## Design Direction
worker_threads 方式で、market グループごとに別スレッドでWS接続＋raw書き込みを処理。
main thread は起動管理 + feature-accumulator + health monitor のみ。

Market grouping (4 workers):
- Worker A: binance_spot, binance_perp, binance_coinm_perp, binance_perp_btcusdc, binance_spot_usdc
- Worker B: bybit_perp, bybit_spot, okx_perp, okx_spot
- Worker C: coinbase_spot, kraken_spot, bitstamp_spot, gemini_spot
- Worker D: crypto_com_spot, bitfinex_spot, bitmex_perp, coinbase_international_perp, hyperliquid_perp

## Results

### What changed
- Created `lib/orderflow-worker.mjs` — worker thread entry point
- Refactored `orderflow_monitor.mjs` — main orchestrator spawns 4 workers
- IPC protocol: trade/depth/liquidation/stateChange/stats/replayDone/ready/bookData

### Review gate
- 1st review: **62/100 FAIL** → 4 P0 issues (depth mid, health stats, shutdown close, worker drain)
- 3 P0 fixed (depth mid IPC, health stats piggyback, close() in shutdown)
- 2nd review: **97/100 PASS**

### Evidence
- `npm test`: 326/326 PASS
- `npm run check`: OK
- Smoke test 30s: 4 workers start/reconnect/shutdown clean. 1281 trade lines written in 35s.
- Exit code 0 on all workers.

### Open issues
- worker shutdown drain protocol (P0-4) is acknowledged but not addressed (edge case)
- P1 issues: quorum shrink on unexpected exit, output path drift, replay warmup semantics

