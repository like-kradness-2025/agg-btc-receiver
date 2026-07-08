# Receiver集計分離 SDD計画

## 目的
受信側から集計処理を外し、生データ保存に寄せる。

## 今回の範囲
- `orderflow_monitor.mjs` から 1秒/30秒集計処理を外す
- 既存の raw / aggregated trade / book update / snapshot 出力は維持する
- ローカル check とテストで回帰確認する

## 非対象
- 後段集計側の新設
- 30秒区切りローテーション実装
- 削除処理

## フェーズ
1. 現状確認と変更点の最終凍結
2. 実装
3. レビュー（95点ゲート）
4. 修正
5. 検証

## 成功条件
- `FeatureComputer` / `FeatureAccumulator` が receiver から外れている
- raw系出力のコードは残る
- `npm run check` が通る
- 関連テストが通る
- 変更内容と検証結果が worklog に残る
