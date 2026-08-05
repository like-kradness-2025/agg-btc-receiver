# Worklog: Burst Feature Specification (2026-07-09)

## Goal
後段集約パイプラインで生成する BTC burst 特徴量の仕様書を22項目・3層時間スケールで作り込み、保存する。

## 決定済み事項
- 22項目を MVP (14) / 研究 (7) / 監視 (1) に分類
- 時間スケール: 1s raw → 30s 集約統計 → 5min cross-market の3層
- sounding-board レビュー済み（Phase A/B/C 合格）

## 入力データ
Receiver の30秒ブロック:
- `trades/<market>/<date>/<HH-MM-SS>.jsonl` — raw trades
- `agg_trades/<market>/<date>/<HH-MM-SS>.jsonl` — 1s aggregated OHLCV
- `book_updates/<market>/<date>/<HH-MM-SS>.jsonl` — book depth diff
- `snapshots/<market>/<date>/<HH-MM-SS>.jsonl` — full book snapshot
- `book_snapshots/<market>/<date>/<HH-MM-SS>.jsonl` — book snapshot（agg版）

## 特徴量22項目

### MVP (14)
1. burst_count_1s
2. total_burst_notional_1s
3. max_burst_notional_1s
4. max_burst_prints_1s
5. max_burst_duration_ms_1s
6. same_price_burst_count_1s
7. multilevel_burst_count_1s
8. buy_burst_notional_1s
9. sell_burst_notional_1s
10. burst_imbalance_ratio_1s
11. largest_burst_share_notional_1s
12. burst_notional_vs_30s_traded_notional
13. burst_notional_vs_top_depth
14. burst_mid_move_bps_1s

### 研究 (7)
15. same_price_burst_max_len_1s
16. same_price_burst_notional_1s
17. multilevel_burst_max_span_ticks_1s
18. multilevel_burst_max_span_bps_1s
19. multilevel_burst_notional_1s
20. same_price_absorption_ratio_1s
21. burst_delta_notional_1s

### 監視 (1)
22. outlier_trade_flag_1s

## 注意点（codex レビューより）
- burst_mid_move_bps は event-time anchor 必須
- 正規化（vs_30s / vs_depth）がないと cross-market 比較不能
- quality flag は後回しにすると分析汚染
- ticks より bps を優先（cross-market 比較のため）
