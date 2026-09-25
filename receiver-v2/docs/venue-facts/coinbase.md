# coinbase

対象は Coinbase Advanced Trade の spot public market data WebSocket（依頼で指定された `wss://advanced-trade-ws.coinbase.com`）。根拠は取得済み公式本文 `/tmp/venue-docs/coinbase.raw` と、下記 Coinbase 公式ページ／AsyncAPI 定義。Global Derivatives 用ホストや private/user stream は対象外。

## 接続URL（public market data）

`wss://advanced-trade-ws.coinbase.com`。JWT は public channel に必須ではない。公式 endpoints は Spot および US Derivatives の public market data 用と説明する。

出典: `coinbase.raw`「Advanced Trade WebSocket Overview」; [WebSocket Endpoints](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints)「Public URL / Notes」; [AsyncAPI JSON](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json) `servers.production`。

## 購読メッセージ（正確な JSON／フィールド・複数チャンネル可否・購読上限）

Unauthenticated public `level2` の例:

```json
{
  "type": "subscribe",
  "product_ids": ["BTC-USD"],
  "channel": "level2"
}
```

`type` は `subscribe` または `unsubscribe`、`channel` は一つの channel 名、`product_ids` は対象製品 ID の配列。public channel ごとに別の JSON message を送る。複数製品は一つの `product_ids` 配列で指定できる。`heartbeats` 購読は次の形で `product_ids` 不要:

```json
{"type":"subscribe","channel":"heartbeats"}
```

AsyncAPI schema は market-data の `product_ids` 配列に最大要素数を指定していない。数値の購読上限は UNKNOWN。

出典: `coinbase.raw`「Sending Messages without API Keys > Subscribing」および「Subscribing」; [Advanced Trade WebSocket Overview](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview)「Subscribing / Sending Messages without API Keys」; [AsyncAPI JSON](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json) `components.messages.SubscribeLevel2`, `SubscribeHeartbeats`。

## 購読の応答（ack の形・失敗時の形・「既に購読済み」等の状態コード）

購読成功 ack の JSON、購読失敗の JSON／状態コード、および重複購読時の応答は UNKNOWN。確認した Overview と AsyncAPI 定義はこれらの応答形を定義していない。公式 Overview は unsubscribe に対して `subscriptions` message を受け取ると記すが、その JSON schema は示していない。

出典: `coinbase.raw`「Unsubscribing」; [Advanced Trade WebSocket Overview](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview)「Unsubscribing」; [AsyncAPI JSON](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json)（subscribe request と server stream の定義。subscribe ack/error 定義なし）。

## keep-alive（こちらから送るもの: 形式と推奨間隔）

別途クライアント ping を送る形式・間隔は公式記載なし。無音での切断を避ける目的では、接続後に `{"type":"subscribe","channel":"heartbeats"}` を一度購読する。サーバーからの heartbeat は1秒ごとで、購読中の各 connection を開いたままにする用途。クライアントが heartbeat に pong を返す手順は定義されていない。

出典: `coinbase.raw`「Sending Messages without API Keys > Subscribing」; [WebSocket Endpoints](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints)「Public > Endpoints / Notes」; [AsyncAPI JSON](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json) `channels.heartbeats` および `components.messages.SubscribeHeartbeats`。

## サーバー側 ping/pong（送られてくるか・間隔・返答期限）

サーバーは WebSocket control-frame ping としてではなく、JSON `heartbeats` channel message を毎秒送る（`channel:"heartbeats"`、`events[].heartbeat_counter` 等）。この JSON heartbeat への pong 応答期限は記載なし。WebSocket protocol ping frame の有無・周期・返答期限も UNKNOWN。公式文中の “server pings” は AsyncAPI では heartbeat message と定義されており、control frame と同一視できない。

出典: `coinbase.raw`「Advanced Trade WebSocket Overview」; [WebSocket Endpoints](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints)「heartbeats」; [AsyncAPI JSON](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json) `channels.heartbeats`, `components.schemas.HeartbeatEnvelope`。

## 無音/切断の判断材料（サーバーが切る条件・こちらが切るべき条件）

- 最初の `subscribe` を WebSocket 接続後5秒以内に送らないと切断される。
- 多くの channel は更新がない状態が60–90秒続くと閉じる。`heartbeats` 購読は接続を開いたまま保つ方法として案内されている。
- クライアント側で heartbeat が途絶えた場合に切断と判定する期限は UNKNOWN。heartbeat は毎秒送信とされるが、許容欠落数／期限は資料にない。

出典: `coinbase.raw`「Subscribing」; [Advanced Trade WebSocket Overview](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview)「Subscribing」; [WebSocket Endpoints](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints)「Public > Notes」; [AsyncAPI JSON](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json) `info.description`, `channels.heartbeats`。

## 計画停止・再接続の通知（コード/イベント名・意味・再購読の要否）

public market-data stream の計画停止を表すイベント名／コードや事前通知は確認できず UNKNOWN。切断後に同じ接続の購読が保持されるかも記載なし。feed を受けるには `subscribe` message が必要と公式 Overview にあるため、再接続後に必要な channel を再度 subscribe する。

出典: `coinbase.raw`「Subscribing」; [Advanced Trade WebSocket Overview](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview)「Subscribing」; [WebSocket Endpoints](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints)（public channel 一覧に計画停止通知なし）。

## 更新の連番・欠落回復（sequence/prev/checksum/event_id・snapshot 取得手順）

現行 AsyncAPI 定義の共通 envelope は `sequence_num`（整数）を持ち、説明は「connection 単位」で欠落／順序外れの検出に使うとしている。heartbeat event 内の `heartbeat_counter` も heartbeat 欠落検知用。対して WebSocket Overview の「Sequence Numbers」は sequence が製品ごとに増加すると説明している。この連番スコープは公式資料間で食い違うため UNKNOWN とし、製品単位／接続単位のいずれかに決め打ちしない。

`level2` はすべての更新の配送を保証する channel。受信 envelope の channel 名は `l2_data` で、event `type` は `snapshot` または `update`。price level の `new_quantity` は差分量でなく更新後の数量で、`0` はその level の削除を意味する。sequence gap 後に snapshot を取り直す具体的な REST 手順、`prev_sequence`、checksum、共通 `event_id` は公式資料に記載なし (**UNKNOWN**)。

出典: `coinbase.raw`「Sequence Numbers」; [Advanced Trade WebSocket Overview](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview)「Sequence Numbers」; [Level 2 order book](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/websocket/level2); [AsyncAPI JSON](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json) `components.schemas.EnvelopeBase`, `HeartbeatEnvelope`, `L2Envelope`。

## レート制限・接続数制限

public market-data の接続数、接続試行頻度、購読 message 頻度、1接続あたり channel／product 数のハード上限は UNKNOWN。現行公式 [WebSocket Rate Limits](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-rate-limits) はページ名のみ取得でき、制限値本文を確認できなかった。AsyncAPI 定義にも上限値はない。購読未送信の5秒切断は「レート制限」ではなく接続条件として上記に記載。

出典: [WebSocket Rate Limits](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-rate-limits); [AsyncAPI JSON](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json)（購読 schema に数値上限なし）。

## 不明点（UNKNOWN と理由）

- 購読成功 ack の形、subscribe 失敗 JSON／状態コード、重複購読時の挙動: Overview と AsyncAPI に応答 schema がない。unsubscribe の `subscriptions` response も形の定義なし。
- WebSocket protocol ping frame の有無と pong 期限、JSON heartbeat 欠落時の timeout: 公式定義は1秒ごとの heartbeat message までで、期限を示していない。
- 計画停止・再接続イベント／コード: public endpoints と AsyncAPI に該当仕様を確認できない。
- sequence のスコープ: Overview は製品単位、AsyncAPI は接続単位と異なる説明。
- gap 発生後の snapshot 再取得手順、checksum、共通 event ID: 対象資料に仕様なし。`level2` は配送保証を明記するが、回復プロトコルの説明はない。
- 数値レート制限、同時接続上限、channel／product 購読数上限: Rate Limits ページ本文を取得できず、AsyncAPI にも記載なし。

出典: `coinbase.raw`「Unsubscribing / Sequence Numbers」; [Advanced Trade WebSocket Overview](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-overview); [AsyncAPI JSON](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/advanced-trade-asyncapi.json); [WebSocket Rate Limits](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-rate-limits)。
