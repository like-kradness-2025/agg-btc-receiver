# binance_futures

## 接続URL（public market data）

- 高頻度 public market data の入口は `wss://fstream.binance.com/public`。
- raw: `wss://fstream.binance.com/public/ws/<streamName>`。例: `wss://fstream.binance.com/public/ws/btcusdt@depth`。
- combined: `wss://fstream.binance.com/public/stream?streams=<streamName1>/<streamName2>`。例: `wss://fstream.binance.com/public/stream?streams=btcusdt@depth/ethusdt@depth`。
- 全 stream が `/public` 対象とは限らない。markPrice、kline、ticker 等は `/market` に分類される。ここでは `/public` 対象だけを扱う。

出典: `/tmp/venue-docs/binance_futures.raw`（ページ見出し “Public - Futures (USDⓈ-M) WebSocket Market Streams”）；[Websocket Market Streams — Connect](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)（“The connection method for Websocket”）；[Base URL Split & Migration](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice)（“What’s New”, “Examples”, “Endpoint & Stream Mapping”）。

## 購読メッセージ（正確な JSON／フィールド・複数チャンネル可否・購読上限）

public endpoint に接続後、複数 stream を一度に指定する例:

```json
{
  "method": "SUBSCRIBE",
  "params": ["btcusdt@depth", "ethusdt@depth"],
  "id": 1
}
```

`method` は `SUBSCRIBE`、`params` は購読 stream 名の配列、`id` は往復メッセージを対応付ける unsigned integer。stream 名の symbol は小文字。1 接続あたり最大 1024 streams。public depth stream 名の例は `<symbol>@depth`（`@500ms` / `@100ms` も対応）。

出典: `/tmp/venue-docs/binance_futures.raw`（“Diff. Book Depth Streams”）；[Live Subscribing/Unsubscribing to streams](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams)（“Subscribe to a stream”）；[Websocket Market Streams — Connect](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)（接続・stream 上限）。

## 購読の応答（ack の形・失敗時の形・「既に購読済み」等の状態コード）

- 成功 ack: `{"result":null,"id":1}`。`id` は要求に対応する。
- 購読失敗時の一般的な JSON 形、および同じ stream を重複購読した場合の専用コード・動作は **UNKNOWN**。購読ページの記載は成功例のみで、エラー表は `SET_PROPERTY` / `GET_PROPERTY` の要求エラーを列挙しているが、重複購読を定義していない。
- `LIST_SUBSCRIPTIONS` で現接続の購読一覧を照会できる（要求 `{"method":"LIST_SUBSCRIPTIONS","id":3}`、応答例 `{"result":["btcusdt@depth"],"id":3}`）。

出典: [Live Subscribing/Unsubscribing to streams](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams)（“Subscribe to a stream”, “Listing Subscriptions”, “Error Messages”；2026-09-25 確認）。

## keep-alive（こちらから送るもの: 形式と推奨間隔）

JSON keep-alive 要求やクライアント定期送信の推奨間隔は **UNKNOWN**（public market stream の接続説明に記載なし）。WebSocket control frame の ping を受けたら対応する pong frame を返す。サーバー ping がない間の unsolicited pong も許可され、接続維持目的なら 15 分より短い間隔で送れる。特定の推奨定期間隔は明記されていない。

出典: [Websocket Market Streams — Connect](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)（ping/pong の説明）。

## サーバー側 ping/pong（送られてくるか・間隔・返答期限）

サーバーは WebSocket ping frame を 3 分ごとに送る。サーバーが 10 分以内に pong frame を受信しなければ切断する。pong は ping に対応する control frame として返す。

出典: [Websocket Market Streams — Connect](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)（ping/pong と disconnect 条件）。

## 無音/切断の判断材料（サーバーが切る条件・こちらが切るべき条件）

- サーバー側で明記された切断条件: 1 接続は 24 時間で期限となり切断される。ping に対する pong を 10 分以内に受信しない場合も切断される。受信 message rate が 1 秒あたり 10 件を超えた接続は切断され、繰り返す IP は ban される。
- クライアント側: pong 待ち期限を超えた場合は接続を健全とみなさず再接続する。depth 差分を処理中に `pu` が直前イベントの `u` と一致しなければ、現 order book を破棄し snapshot から作り直す。
- 市場データが一定時間来ないこと自体を切断条件とする時間値は **UNKNOWN**。stream ごとの無更新許容時間は接続説明に定義されていない。

出典: [Websocket Market Streams — Connect](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)（接続寿命・ping/pong・rate limit）；[How to manage a local order book correctly](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)（“While listening to the stream”）。

## 計画停止・再接続の通知（コード/イベント名・意味・再購読の要否）

計画停止を事前通知する event 名・code・意味は **UNKNOWN**（確認した USDⓈ-M public market streams の接続／購読ページに通知仕様がない）。24 時間接続寿命による切断は明記されているが、切断 frame の code/reason は **UNKNOWN**。再接続後は新しい接続として必要 stream を再購読すること。購読一覧は接続上で照会する仕様で、切断をまたぐ購読保持の記載はない。

出典: [Websocket Market Streams — Connect](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)（24-hour lifetime）；[Live Subscribing/Unsubscribing to streams](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams)（購読・照会要求）；計画通知については同ページ群の該当節に記載なし。

## 更新の連番・欠落回復（sequence/prev/checksum/event_id・snapshot 取得手順）

Diff depth payload は `U`（イベント内最初の update ID）、`u`（最後の update ID）、`pu`（直前イベントの `u`）、`b` / `a`（bid/ask の価格水準更新）を持つ。初期同期手順:

1. WebSocket 差分を先に購読し、受信イベントを buffer する。
2. REST snapshot `https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000` を取得する。
3. `u < lastUpdateId` の buffer event を捨てる。
4. 最初に適用する event は `U <= lastUpdateId <= u` を満たすもの。
5. 続く各 event は `pu == 直前 event の u` を満たすこと。不一致なら step 2 から再初期化。
6. level の quantity は絶対値。0 はその価格 level を削除。

checksum / event_id はこの depth 仕様に記載なし。出典: `/tmp/venue-docs/binance_futures.raw`（“Diff. Book Depth Streams” の payload schema）；[How to manage a local order book correctly](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)（手順 1–9）。

## レート制限・接続数制限

1 接続あたり incoming messages は最大 10 件/秒、購読できる stream は最大 1024 個。message rate 超過時は接続切断、繰り返す切断は IP ban の可能性がある。接続試行数の上限は確認した USDⓈ-M 接続ページに記載がなく **UNKNOWN**。

出典: [Websocket Market Streams — Connect](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect)（WebSocket limits）。

## 不明点（UNKNOWN と理由）

- 購読エラー応答の包括的な形と重複購読時の扱い: 購読ページは成功応答例のみで、重複購読のコード／動作を規定しない。
- 定期クライアント keep-alive の推奨間隔: control-frame ping/pong の仕様はあるが、定期 client ping の推奨値はない。
- 市場データ無音の切断閾値: 接続レベルの ping/pong と 24 時間寿命は規定されるが、stream 別無更新 timeout はない。
- 計画停止通知の event/code、切断 frame の code/reason: 確認した public market streams ページに記載なし。
- 接続試行数制限: USDⓈ-M public 接続ページに記載なし。別 product や Spot の制限値はこの venue に流用していない。

出典: 上記の Binance USDⓈ-M WebSocket Market Streams 各ページ（各項目に付記、2026-09-25 確認）；補助資料 `/tmp/venue-docs/binance_futures.raw`（公式ページ見出し “Public - Futures (USDⓈ-M) WebSocket Market Streams”）。
