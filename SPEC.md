# agg-btc-receiver 現行仕様

現行の正本は[docs/current/README.md](docs/current/README.md)です。

Receiverは、取引所から受信したraw market eventとperp OI観測値をmarket別SQLiteへ圧縮保存します。時間分割、Parquet archive、TFP、Book Snapshot、OrderHeatmapはReceiverの現行保存経路に含めません。

過去のJSONL/Parquet前提の仕様書は`docs/archive/`に履歴資料として保管しています。

## 修正するとき

まず `docs/current/fix-map.md`（症状→読むファイルのルーティング、不変条件、反映手順、罠）を読む。
リポジトリ全体を grep しない。
