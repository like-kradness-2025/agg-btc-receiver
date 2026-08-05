# ADR-009: BTC Burst Feature Specification v1

## Status
Accepted (2026-07-09)

## Context
Receiver から FeatureAccumulator（1s特徴量生成）を削除した。後段集約パイプラインを新設するにあたり、burst 特徴量セットの設計が必要。

sounding-board 2回のレビューを経て仕様書が完成。

## Decision
22特徴量を MVP(14) / 研究(7) / 監視(1) に分類し、3層時間スケール（1s raw → 30s 集約統計 → 5min cross-market）で実装する。

## Alternatives Considered
- 旧14項目のみ: 却下 — cross-market 比較不能、正規化不足
- 22項目一気実装: 却下 — MVP優先が安定
- book 特徴量を burst に含める: 却下 — burst は trade-only、book は context のみ

## Consequences
- 良: 22項目で burst 分析の全軸をカバー
- 良: MVP 14項目のみ先行実装可能
- 悪: 30s/5min 層の実装は別途
- リスク: burst_mid_move_bps の event-time anchor 実装に注意

## Verification
sounding-board レビュー: 54/100 → 90/100 PASS

## References
- docs/specs/specify-2026-07-09-burst-features.md
- docs/worklog/2026-07-09-burst-feature-spec.md
