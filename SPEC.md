# agg-btc-receiver 現行仕様

現行の正本は[docs/current/README.md](docs/current/README.md)です。

Receiverは、取引所から受信したraw market eventとperp OI観測値をmarket別SQLiteへ圧縮保存します。時間分割、Parquet archive、TFP、Book Snapshot、OrderHeatmapはReceiverの現行保存経路に含めません。

過去のJSONL/Parquet前提の仕様書は`docs/archive/`に履歴資料として保管しています。
