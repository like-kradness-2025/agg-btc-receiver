# hyperliquid

## 接続URL（public market data）

- Mainnet public WebSocket URL: `wss://api.hyperliquid.xyz/ws`。公式接続例にある通り、接続後 JSON 購読を送る。
- 出典: `/tmp/venue-docs/hyperliquid.txt`:10–18, 23–29行; [Websocket](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)「WebSocket URLs」「Connecting」。

## 購読メッセージ（正確な JSON／フィールド・複数チャンネル可否・購読上限）

購読形式:

```json
{"method":"subscribe","subscription":{"type":"trades","coin":"SOL"}}
```

`method` は `subscribe`、`subscription` 内にチャンネルの `type` と必要な値を指定する。公開市場データの公式例・定義:

- `allMids`: `{"type":"allMids"}`。任意の `dex` を指定可能。
- `candle`: `{"type":"candle","coin":"<coin_symbol>","interval":"<candle_interval>"}`。interval は `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `8h`, `12h`, `1d`, `3d`, `1w`, `1M`。
- `l2Book`: `{"type":"l2Book","coin":"<coin_symbol>"}`。任意フィールド `nSigFigs` (int), `mantissa` (int), `fast` (boolean)。`fast` は5段、slow は20段と記載。
- `trades`: `{"type":"trades","coin":"<coin_symbol>"}`。
- `bbo`: `{"type":"bbo","coin":"<coin>"}`。
- `activeAssetCtx`: `{"type":"activeAssetCtx","coin":"<coin_symbol>"}`。
- `allDexsAssetCtxs`: `{"type":"allDexsAssetCtxs"}`。

複数購読は、接続中にそれぞれの `subscription` を持つメッセージを送る形。解除は `{"method":"unsubscribe","subscription":{...}}` で元の購読オブジェクトを指定し、解除は他の有効な購読に影響しない。上限は全接続合計 1000 subscriptions。

- 出典: [Subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)「Subscription messages」行2–22, 38–48, 75–106, 「Unsubscribing from WebSocket feeds」行591–602; `/tmp/venue-docs/hyperliquid.txt`:27–29行; [Rate limits and user limits](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)行12–17。

## 購読の応答（ack の形・失敗時の形・「既に購読済み」等の状態コード）

成功 ack の形式（元の購読オブジェクトを `data` に返す）:

```json
{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"SOL"}}}
```

成功時は続いて `channel` が対象 subscription type（例: `trades`）のデータが届く。失敗時の ack 形式、エラーコード、重複購読済みの場合の応答/状態コードは **UNKNOWN**。公式購読資料は成功応答だけを説明し、失敗または重複時の定義を載せていない。

- 出典: `/tmp/venue-docs/hyperliquid.txt`:27–31行; [Websocket](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)「Connecting」行11–18; [Subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)「Data formats」行106–108。

## keep-alive（こちらから送るもの: 形式と推奨間隔）

購読チャンネルから60秒以内にメッセージが来ない場合、クライアントから次を送って接続を維持する:

```json
{"method":"ping"}
```

**推奨送信間隔は UNKNOWN**。公式資料は、60秒間サーバーからメッセージが来ない接続は切断されることと、ping の形式を示すが、ping の具体的な間隔は規定していない。運用上の送信間隔を公式推奨値として扱える根拠はない。

- 出典: [Timeouts and heartbeats](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats)「Timeouts and heartbeats」行1–9。

## サーバー側 ping/pong（送られてくるか・間隔・返答期限）

- WebSocket アプリケーションメッセージとして、サーバーからの定期 `ping` が送られるとの記載はない (**UNKNOWN: 送信有無・間隔**)。
- クライアントが `{"method":"ping"}` を送ると、サーバーは `{"channel":"pong"}` を返す。
- クライアント ping に対する応答期限は **UNKNOWN**。資料は deadline を定めていない。
- 出典: [Timeouts and heartbeats](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats)行5–14。

## 無音/切断の判断材料（サーバーが切る条件・こちらが切るべき条件）

- サーバーは過去60秒にその接続へメッセージを送っていない場合、その接続を閉じる。無通信が60秒に達する前に keep-alive `ping` を送る。
- API サーバー側の切断は予告なく定期的に起こり得るため、自動クライアントは切断を処理して再接続するよう公式資料が指示している。サーバーが市場データ無音以外に切断する個別条件・close code は **UNKNOWN**。
- 出典: [Timeouts and heartbeats](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats)行5–9; `/tmp/venue-docs/hyperliquid.txt`:31行; [Websocket](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)「Connecting」行18。

## 計画停止・再接続の通知（コード/イベント名・意味・再購読の要否）

計画停止の事前通知イベント名・コード・意味はいずれも **UNKNOWN**。公式資料は予告なしの定期的切断があり得ると述べるが、停止通知プロトコルを記載していない。再接続後は購読を改めて行う（接続後に購読メッセージを送る手順）。未受信データは再接続時の snapshot ack に含まれると説明され、必要なら対応する `info` request で補完できる。

- 出典: `/tmp/venue-docs/hyperliquid.txt`:18, 31行; [Websocket](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)「Connecting」行7–18; [Subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)「Subscription messages」行5–16。

## 更新の連番・欠落回復（sequence/prev/checksum/event_id・snapshot 取得手順）

- 公開資料に市場データ共通の sequence / prev sequence / checksum は定義されていない (**UNKNOWN**)。
- `trades` の trade `tid` は買い手/売り手 order ID の50-bit hash。資料は globally unique な識別に `(block_time, coin, tid)` を使うと説明するが、全チャンネル共通 event ID ではない。
- `l2Book` は `WsBook {coin, levels, time}` の snapshot feed で、直近 push から0.5秒以上経過した block ごとに送出と記載。snapshot を再取得する場合の HTTP `info` request は `{"type":"l2Book","coin":"<coin>"}`。candles の履歴再取得は `{"type":"candleSnapshot","req":{"coin":"<coin>","interval":"<interval>","startTime":<epoch_ms>,"endTime":<epoch_ms>}}`。公式 docs は再接続時 snapshot ack または該当 info request による missed data 回復を案内するが、stream ごとの厳密な差分 replay/cursor 手順は **UNKNOWN**。
- 出典: [Subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)行138–156, 177–188; [Info endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)「L2 book snapshot」「Candle snapshot」; [Websocket](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)「Connecting」行18。

## レート制限・接続数制限

IP あたりの上限:

- 最大10 WebSocket接続。
- 1分あたり新規接続最大30。
- WebSocket購読最大1000。
- user-specific WebSocket 購読全体で unique user 最大10。
- 全 WebSocket 接続合計で Hyperliquid へ送るメッセージ最大2000/分。

接続単位か IP 単位かなど資料に上記以外の窓・burst 詳細はありません (**UNKNOWN**)。

- 出典: [Rate limits and user limits](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)行1–17。

## 不明点（UNKNOWN と理由）

- 購読失敗 ack、失敗コード、重複購読時の状態コード: 公式購読ページは成功応答形式のみ記載し、これらのケースを定義していない（[Subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)「Data formats」）。
- クライアント heartbeat の推奨間隔と ping への応答期限: heartbeat 形式と60秒無通信切断は示すが、どちらの時間値も示していない（[Timeouts and heartbeats](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats)）。
- サーバー起点 ping の有無・間隔、切断 close code / 追加条件: heartbeat ページはクライアント発 ping とサーバー pong のみ記載（同上）。
- 計画停止の通知コード/イベント: 公開 WebSocket 資料に記載なし（[Websocket](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)「Connecting」）。
- 市場 stream の sequence/prev/checksum、および stream 別の完全な replay 手順: 型定義に共通連番等がなく、再接続時 snapshot または info request で missed data を回復するとの概説まで（[Subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions), [Websocket](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)）。
