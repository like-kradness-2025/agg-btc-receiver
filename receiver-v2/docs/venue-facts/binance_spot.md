# binance_spot

## 接続URL（public market data）

- 基本URL: `wss://stream.binance.com:9443` または `wss://stream.binance.com:443`。
- raw stream: `/ws/<streamName>`。combined stream: `/stream?streams=<streamName1>/<streamName2>`。combined の受信データは `{"stream":"<streamName>","data":<rawPayload>}` で包まれる。stream 名中の銘柄記号は小文字。
- market data 専用URLとして `wss://data-stream.binance.vision` も利用できる。こちらに User Data Stream はない。
- 根拠: `/tmp/venue-docs/binance_spot.txt`「General WSS information」38–50行。公式: [WebSocket Streams for Binance](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#general-wss-information)。

## 購読メッセージ（正確な JSON／フィールド・複数チャンネル可否・購読上限）

WebSocket 接続後に送る例（複数 stream を1メッセージで指定可能）:

```json
{"method":"SUBSCRIBE","params":["btcusdt@aggTrade","btcusdt@depth"],"id":1}
```

`method` は `SUBSCRIBE`、`params` は購読する stream 名の配列、`id` は応答と対応づける識別子。資料上の許容形式は符号付き64-bit整数、最大36文字の英数字文字列、または `null`。1接続あたり最大1024 stream。接続 URL の combined 形式でも複数 stream を指定できる。

根拠: 同「General WSS information」39–43行、「WebSocket Limits」56–63行、「Live Subscribing/Unsubscribing to streams」「Subscribe to a stream」92–117行。公式: [WebSocket Streams for Binance](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#live-subscribingunsubscribing-to-streams)。

## 購読の応答（ack の形・失敗時の形・「既に購読済み」等の状態コード）

成功 ack:

```json
{"result":null,"id":1}
```

失敗時の一般例は `{"code":<number>,"msg":"<message>","id":<id>}`。資料にあるコードは、`0` unknown property、`1` invalid value type、`2` invalid request（未知 method、余分な引数、ID/field 不正など）、`3` invalid JSON。エラー表の各行で `id` が省略される例もあるため、すべての失敗応答が同じ形とは断定できない。

「既に購読済み」の場合の特別な ack、エラー、状態コードは資料に記載なし (**UNKNOWN**)。

根拠: 同「Live Subscribing/Unsubscribing to streams」95–117行、「Error Messages」195–207行。公式: [WebSocket Streams for Binance](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#error-messages)。

## keep-alive（こちらから送るもの: 形式と推奨間隔）

アプリケーションレベルのクライアント発 keep-alive メッセージや推奨間隔は記載なし (**UNKNOWN**)。サーバー ping frame への応答として、ping の payload を複製した pong frame を速やかに返す必要がある。自発的 pong frame は許可されるが切断防止にはならず、送る場合の payload は空が推奨されている。

根拠: 同「General WSS information」46–49行。公式: [WebSocket Streams for Binance](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#general-wss-information)。

## サーバー側 ping/pong（送られてくるか・間隔・返答期限）

サーバーは WebSocket ping frame を20秒ごとに送信する。受信側は payload をそのまま pong frame にコピーして速やかに返す。サーバーが1分以内に pong を受信しなければ接続を切断する。

根拠: 同「General WSS information」46–49行。公式: [WebSocket Streams for Binance](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#general-wss-information)。

## 無音/切断の判断材料（サーバーが切る条件・こちらが切るべき条件）

- サーバーが切断する条件として明記されているもの: 接続の有効期間は24時間で、その時点で切断される。ping に対して1分以内に pong が届かない場合も切断される。受信するクライアントメッセージが毎秒5件を超えた場合も切断される。
- クライアント側では、24時間到達前に接続を張り替える計画を立てる。serverShutdown 通知を受けた場合も新接続を速やかに確立する。
- 市場データが無音のときにサーバーが切るまでの追加タイムアウト、または無音と判定すべき時間は資料に記載なし (**UNKNOWN**)。20秒ごとの ping は接続の生存確認情報だが、市場イベントが無音になる条件・閾値は示されていない。

根拠: 同「General WSS information」44–49行、「WebSocket Limits」56–63行、「Server Shutdown」65–90行。公式: [WebSocket Streams for Binance](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#general-wss-information)。

## 計画停止・再接続の通知（コード/イベント名・意味・再購読の要否）

イベント名は `serverShutdown`。サーバーが停止しようとしていることを通知し、その後接続が切断される。raw payload は `{"e":"serverShutdown","E":1770123456789}`、combined payload は `{"stream":"!serverShutdown","data":{"e":"serverShutdown","E":1770123456789}}`（`E` はイベント時刻）。資料は新しい接続を速やかに確立するよう指示している。切断後は新しい WebSocket 接続を作り、必要な stream を再度購読する。

根拠: 同「General WSS information」45行、「Server Shutdown」65–90行。公式: [WebSocket Streams for Binance](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#server-shutdown)。

## 更新の連番・欠落回復（sequence/prev/checksum/event_id・snapshot 取得手順）

Spot diff depth の `depthUpdate` にはイベント範囲 `U`（最初の update ID）と `u`（最後の update ID）がある。資料に `pu`、checksum、全 stream 共通の event ID は記載されていない (**UNKNOWN**)。

板の初期化・回復手順:

1. `wss://stream.binance.com:9443/ws/<symbol>@depth` を開き、差分イベントを一時バッファする。
2. REST snapshot `https://api.binance.com/api/v3/depth?symbol=<SYMBOL>&limit=5000` を取得する。
3. snapshot の `lastUpdateId` がバッファ先頭イベントの `U` より小さければ snapshot を再取得する。バッファ内の `u <= lastUpdateId` のイベントを捨て、`lastUpdateId` が `[U,u]` の範囲に入る最初のイベントを探す。
4. snapshot をローカル板に設定して該当差分から適用する。以後、`u` がローカル update ID より小さいイベントは無視する。`U > localUpdateId + 1` なら欠落として板を破棄し、最初から同期し直す。差分 quantity が0ならその価格レベルを削除する。
5. snapshot は各側最大5000価格レベル。snapshot 範囲外で更新されていない価格レベルの数量は把握できない。

根拠: 同「Diff. Depth Stream」599–627行、「How to manage a local order book correctly」629–652行。公式: [WebSocket Streams for Binance](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly)。

## レート制限・接続数制限

- 接続あたり受信メッセージ上限は毎秒5件。ここで数えるものは PING frame、PONG frame、JSON 制御メッセージ（subscribe/unsubscribe 等）。超過時は切断され、繰り返し切断される IP は ban される可能性がある。
- 1接続で購読できる stream は最大1024。
- IPあたり接続試行は5分ごとに最大300回。

根拠: 同「WebSocket Limits」56–63行。公式: [WebSocket Streams for Binance](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#websocket-limits)。

## 不明点（UNKNOWN と理由）

- 重複購読時に返る応答・状態コード: 購読 ack とエラー一覧に重複購読専用の挙動がない（同「Subscribe to a stream」101–117行、「Error Messages」195–207行）。
- クライアント発 keep-alive の要否と推奨間隔: サーバー ping への pong 要件と unsolicited pong の注意のみで、別のクライアント heartbeat は定義されていない（同「General WSS information」46–49行）。
- 市場イベントが無音の場合の切断閾値: 資料は接続の24時間制限、ping/pong deadline、メッセージ頻度超過を示すが、市場データ無音のタイムアウトを示していない（同44–49行、56–63行）。
- 全ストリーム共通の event ID や欠落回復方法: depth には `U`/`u` と REST snapshot 手順があるが、共通 event ID/checksum は定義されていない（同「Diff. Depth Stream」599–627行、「How to manage a local order book correctly」629–652行）。
