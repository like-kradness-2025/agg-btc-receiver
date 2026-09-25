# kraken

対象は Kraken Spot WebSocket v2 の public market data。以下は提供された公式本文と現行公式ドキュメントを照合した結果。

## 接続URL（public market data）

`wss://ws.kraken.com/v2`。Primary の v2 public data URL として公式ガイドに記載。認証なしの public 市場データ用。

出典: [Spot WebSocket Introduction](https://docs.kraken.com/exchange/guides/websockets/introduction)「Connection Details」; [Kraken docs index](https://docs.kraken.com/llms.txt)「Spot WebSocket v2」; `/tmp/venue-docs/kraken.txt`「WSS」67–69行。

## 購読メッセージ（正確な JSON／フィールド・複数チャンネル可否・購読上限）

例: ticker の BTC/USD を購読する JSON:

```json
{"method":"subscribe","params":{"channel":"ticker","symbol":["BTC/USD"]}}
```

`method` は `subscribe`、`params.channel` は `ticker`、`params.symbol` は必須の通貨ペア配列。任意フィールドは `event_trigger` (`bbo` または `trades`、既定 `trades`)、`snapshot` (boolean、既定 `true`)、`req_id` (integer)。同じ形式で `channel` を `book` または `trade` にして各チャンネルを購読できる。1メッセージの `channel` は1つ。複数の銘柄を `symbol` 配列に指定できる。複数チャンネルは同一接続に個別の subscribe リクエストを送る例が公式再接続ガイドにある。銘柄数・購読数の数値上限は **UNKNOWN**（参照した v2 公開チャンネル仕様に記載なし）。

出典: `/tmp/venue-docs/kraken.txt`「Subscribe Request」19–29行および「ticker」87–112行; [Ticker](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ticker)「Subscribe」; [Book (Level 2)](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/book)「Subscribe」; [Trades](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade)「Subscribe」; [Reconnection and resilience](https://docs.kraken.com/exchange/guides/websockets/reconnection)「Re-subscribing after reconnect」.

## 購読の応答（ack の形・失敗時の形・「既に購読済み」等の状態コード）

成功 ack は `method:"subscribe"` と `result` object を持ち、`result` に `channel`、`symbol`、要求に応じ `snapshot`、さらに `success:true`、`time_in`、`time_out` が含まれる。要求に `req_id` があれば応答にも返る。失敗時は `success:false` と `error` string。チャンネル仕様ページはこれらのフィールドを定義するが、固定の状態コードやエラー文字列一覧は示さない。重複/既に購読済みの応答形・コードも **UNKNOWN**（仕様に記載なし）。

出典: [Ticker](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ticker)「Subscribe Response」; [Spot WebSocket Introduction](https://docs.kraken.com/exchange/guides/websockets/introduction)「Responses」; `/tmp/venue-docs/kraken.txt`「Subscribe Request」19–29行、「req_id」110–112行。

## keep-alive（こちらから送るもの: 形式と推奨間隔）

通常の market data 購読では、こちらから定期送信する keep-alive は不要。購読中は他のチャンネル更新がない間に `{"channel":"heartbeat"}` が約1秒ごとにサーバーから届く。必要なら application-level ping `{"method":"ping"}` をクライアントから送れるが、公式 v2 資料に推奨送信間隔は **UNKNOWN**（間隔の指定なし）。

出典: [Heartbeat](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/heartbeat)「Update」; [Ping](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ping)「Request」; [Spot WebSocket Introduction](https://docs.kraken.com/exchange/guides/websockets/introduction)「Connection times out」.

## サーバー側 ping/pong（送られてくるか・間隔・返答期限）

サーバー開始の定期 ping と、それに対するクライアント pong の要件は **UNKNOWN**（参照した v2 資料に記載なし）。アプリケーションレベルでは、サーバーから `heartbeat` チャンネル通知が他の更新がない時に約1秒ごとに送られ、クライアントが `{"method":"ping"}` を送るとサーバーは `method:"pong"` を含む応答を返す。pong 返答期限は **UNKNOWN**。

出典: [Heartbeat](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/heartbeat)「Update」; [Ping](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/ping)「Response」.

## 無音/切断の判断材料（サーバーが切る条件・こちらが切るべき条件）

- v2 overview は、約1分間接続が inactive ならサーバーが切断し、ping 等のリクエストで接続を保てるとする。market data 停止だけで inactive と判断しないこと。heartbeat を含め何のメッセージも約5秒受信しなければ切断・再接続する、という例示のクライアント側判断基準が公式再接続ガイドにある。
- 切断がネットワーク障害・保守・load balancer 再利用等で起きることはある。接続 close/error も再接続材料とする。
- idle の厳密な定義、close code、特定チャンネル無音のサーバー切断条件は **UNKNOWN**。

出典: [Spot WebSocket Introduction](https://docs.kraken.com/exchange/guides/websockets/introduction)「Connection times out」; [Reconnection and resilience](https://docs.kraken.com/exchange/guides/websockets/reconnection)「Detecting disconnects」「Connection lifecycle」.

## 計画停止・再接続の通知（コード/イベント名・意味・再購読の要否）

`status` チャンネルの `update` で engine 状態 (`online`, `cancel_only`, `maintenance`, `post_only`) を通知。`upcoming_maintenance` と `emergency` 配列が有効な deployment では、保守/障害の `event_id`、開始予定・phase・対象 service・推奨対応なども含まれる。イベント/コードによる切断予告の固定名称は **UNKNOWN**（資料に記載なし）。接続が切れた後は subscribe を再送する必要があり、Kraken は購読を自動復元しない。保守/再起動にともなう購読再開では snapshot から状態を再構築する。

出典: [Status](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/status)「Update」; [Reconnection and resilience](https://docs.kraken.com/exchange/guides/websockets/reconnection)「Re-subscribing after reconnect」「State reconciliation after reconnect」.

## 更新の連番・欠落回復（sequence/prev/checksum/event_id・snapshot 取得手順）

- `trade` の `trade_id` は integer で、公式仕様は book ごとの sequence number と説明する。ただし `prev` / cursor や欠落後の再送取得手順は **UNKNOWN**（公開仕様に記載なし）。
- `book` は snapshot に `bids` / `asks` と CRC32 `checksum` があり、update の checksum は各更新後に板を検証するためのもの。明示的な sequence/prev sequence は仕様にない。checksum 不一致時は unsubscribe/subscribe して新しい snapshot を取り直す。再接続時は古い板を破棄し、新 snapshot から再構築する。
- `ticker` は `timestamp` を含むが timestamp は一意 ID とみなせず、欠落検出の sequence/event_id 仕様は **UNKNOWN**.

出典: [Trades](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/trade)「Snapshot / Update」; [Book (Level 2)](https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/book)「Snapshot」「Update」; [Book checksum (WebSocket v2)](https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2)「Maintaining the book」; [Reconnection and resilience](https://docs.kraken.com/exchange/guides/websockets/reconnection)「Order book」; [Spot WebSocket Introduction](https://docs.kraken.com/exchange/guides/websockets/introduction)「General Considerations」.

## レート制限・接続数制限

- IP あたり接続/再接続試行は rolling 10分で約150回まで。超えるとその IP は10分間 ban。通常/保守後の再接続方針は同 introduction の「Recommended reconnection behaviour」を参照。
- 同時接続数の数値上限、market-data subscribe request の頻度上限、1接続あたりの購読数上限は **UNKNOWN**（確認した現行 v2 公式資料に数値記載なし）。

出典: [Spot WebSocket Introduction](https://docs.kraken.com/exchange/guides/websockets/introduction)「General Considerations (v1 and v2 connections)」「Recommended reconnection behaviour」.

## 不明点（UNKNOWN と理由）

- 接続あたり/全体の同時接続数上限: **UNKNOWN** — 現行 v2 資料に数値なし。
- 一度の subscribe に指定できる銘柄数、および接続あたり購読数: **UNKNOWN** — v2 チャンネル仕様に数値上限なし。
- 既購読時専用の応答/コード、エラー文字列・状態コード一覧: **UNKNOWN** — subscribe ack schema はあるが状態コード表はない。
- サーバー開始 ping/pong、ping 応答期限、クライアント ping 推奨間隔: **UNKNOWN** — v2 の heartbeat/ping 資料は周期または timeout を定めていない。
- ticker/trade の欠落後 replay/snapshot cursor 手順、book の sequence/prev: **UNKNOWN** — 公開 v2 channel schema に記載なし。

調査に使用した資料: `/tmp/venue-docs/kraken.txt`（ticker ページ本文）および本文中に引用した Kraken Developers 公式 v2 documentation pages。
