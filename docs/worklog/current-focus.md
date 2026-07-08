# Current Focus

- Track: 受信側から集計処理を外す
- Governing context:
  - `docs/raw-receiver-separation-plan.md`
  - `docs/remove-aggregation-from-receiver-plan.md`
  - `docs/worklog/2026-07-04-receiver-aggregation-separation-plan.md`
- Verified completed work:
  - `orderflow_monitor.mjs` から `FeatureComputer` を削除
  - receiver での feature 計算と `features.jsonl` 出力を削除
  - raw trade / aggregated trade / book update / periodic book snapshot / liquidation / health / derivatives / market data 出力は維持
  - コメント上の stale な出力一覧も修正済み
- Current operational decision:
  - 受信側は生データ保存に寄せる
  - 1秒集計と30秒板まとめは後段集計へ移す
  - 集計は30秒区切りを第一候補にする
  - 並列化は market ごとに分け、同じ market 内は時間順に処理する
  - 生データ削除にはロックと待ち時間を入れる
- Next concrete step:
  - `FeatureAccumulator` の live 配線を別入口へ移す設計に進む
