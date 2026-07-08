# agg-btc-receiver v2 responsibility split plan

## Goal
v2 では receiver の責務を「受信して一時保存するのみ」に絞る。

## Receiver に残す責務
- WebSocket / API からの受信
- raw trade 保存
- raw depth 保存
- raw snapshot 保存
- liquidation 保存
- 最小限の運用ログ・shutdown

## Receiver から外す責務
- 1秒特徴量生成（FeatureAccumulator）
- fair price / premium / basis の派生計算
- MarketDataCollector による REST 補助収集
- agg parquet 生成
- raw -> parquet 変換の起動責務

## Phase split
### Phase 1 (this phase)
- fairprice_monitor / FairPriceCollector から派生変換責務を分離する
- receiver は raw 保存専用に寄せる
- 変換系スクリプトは次フェーズに送る

### Phase 2 (next phase)
- raw_hot から 1s_features / agg / parquet への変換経路を整理する
- 必要に応じて別プロセス・別cronへ切り出す

## Non-goals for v2 phase 1
- データ変換ロジックの完成
- agg schema の刷新
- market 分割並列化
- deploy / restart

## Candidate file scope
- fairprice_monitor.mjs
- lib/fair-price-collector.mjs
- test/fair-price-collector.test.mjs
- 必要なら config.v3.json の最小整理

## Notes
- 既存構造をアダプトし、新規本体は増やさない
- 実装前に最小差分方針を確定する
