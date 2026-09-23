# 配信WSの仕様メモ（一次資料ベース）

目的: 取引所WSの「サーバー側が何をするか / クライアントが何を満たすべきか」を一次資料で押さえ、
無音・突然切断による raw 欠落（2026-09-23 実測: 24hで再接続56回、30〜38秒の欠落、復元なし）の
切り分けと対策の根拠にする。

取得日: 2026-09-23。**取得できたものは原文の文言を引用**し、取得できなかったものは「未確認」と明記する
（推測で埋めない）。URL はすべて公式ドキュメント。

---

## 1. 検証済み（原文を取得できたもの）

### Binance Spot
出典: 公式ドキュメント（binance-spot-api-docs / web-socket-streams）
- 「A single connection to stream.binance.com is only valid for **24 hours**; expect to be disconnected at the 24 hour mark.」
- 「The WebSocket server will send a `ping frame` **every 20 seconds**.」
- 「If the WebSocket server does not receive a `pong frame` back from the connection **within a minute** the connection will be disconnected.」
- 「Unsolicited `pong frames` are allowed, but will not prevent disconnection.」
- 「WebSocket connections have a limit of **5 incoming messages per second**. A connection that goes beyond the limit will be disconnected; IPs that are repeatedly disconnected may be banned.」
- 「A single connection can listen to a maximum of **1024 streams**.」

### OKX
出典: 公式ドキュメント（docs-v5）
- 「The connection will break automatically if the subscription is not established or **data has not been pushed for more than 30 seconds**.」
  → **サーバー側が30秒で切る**。こちらの30秒無音検知と同じ桁で、検知の遅さだけが原因とは言えない。
- Connection limit: **3 requests per second (based on IP)**。
- 「60 seconds prior to the upgrade of the WebSocket service, the notification message will be sent to users
  indicating that the connection will soon be disconnected.」→ 計画切替の予告がある。

### Bybit
出典: 公式ドキュメント（v5/ws/connect）
- 「In general, if there is **no "ping-pong" and no stream data** sent from server end, the connection will be
  cut off **after 10 minutes**.」
- `max_active_time` は **30s〜600s**（private stream / order entry 向け）。
- ハートビートは **アプリ層メッセージ** `{"op":"ping"}`（「How to Send the Heartbeat Packet」）。
- 「Due to network complexity, your may get disconnected at any time.」「Reconnect as soon as possible if disconnected」

### Bitstamp
出典: 公式ドキュメント（websocket v2）
- 単一接続で **1024 channel** まで。**1025件目の購読で接続が silent close** される。
- 応答性は **`bts:heartbeat`** で判定する（「you should do so by websocket heartbeat」）。
- auth トークンは **60秒**有効（期限切れは 4009）。
- diff と REST snapshot の**シーケンス突合**が明記されている。再接続時の欠落対策は
  `live_orders_[market]` の `event_id` / `pre_event_id` を使う方式が案内されている。

### Bitfinex
出典: 公式ドキュメント（ws-general）
- サーバーは **15秒ごとに heartbeat メッセージ**を送る。
- クライアントから `ping` を送れる（サーバーが `pong` を返す）。
- サーバーから **「Stop/Restart Websocket Server (please reconnect)」** の info が来る
  → **取引所側が定期再起動しており、クライアントは再接続が前提**（＝突然切断は正常動作）。

### Hyperliquid
出典: 公式ドキュメント（for-developers/api/websocket）
- 「all automated users should handle disconnects from the server side and gracefully reconnect.」
- 「Disconnection from API servers may happen **periodically and without announcement**.」
- 「**Missed data during the reconnect will be present in the snapshot ack on reconnect.**」
  → **再接続時に欠けたデータは snapshot ack で回復できる**（＝回復手段が文書化されている）。

### Kraken（WS v2）
出典: 公式APIリファレンス（spot-websocket-v2/ping）
- 「Clients can ping the server to verify connection is alive and the server will respond with a `pong`.
  This is an **application level ping**, distinct from the protocol-level ping in the WebSockets standard.」
- サーバー側のアイドル切断条件・接続寿命は、取得できたページからは**特定できず（未確認）**。

## 2. 未確認（公式ページがJS描画等で本文を取得できず、断定しない）

- **Binance USDⓈ-M Futures** の ping 間隔・pong 猶予・24時間制限・受信メッセージ制限
- **Coinbase Exchange** の heartbeat 仕様・切断条件・購読上限
- **Kraken** のサーバー側アイドル切断条件・接続上限

→ 必要になった時点で、ブラウザ（CDP）経由か公式の別経路で取り直す。

## 3. こちらの実装との突合（点検結果）

| 項目 | 仕様 | 実装 | 判定 |
|---|---|---|---|
| Binance の購読 | 受信 5 msg/s 制限 | **combined stream（URLパラメータ）で購読フレームを送らない** | 違反なし |
| 無音検知 | OKXは30秒で切る／Bybitは10分 | `STALE_MSG_THRESHOLD_MS = 30000` を**全venue共通**で使用 | 一律（venue別でない） |
| protocol ping | Binanceは20秒ごとに ping を送ってくる | `ws` の autoPong が応答（自前の5秒pingも追加済み・観測用） | 満たす見込み |
| Bitfinex の再起動 info | 受信して再接続が前提 | `lib/bitfinex-connector.mjs:92` で `info` を**明示的に無視**（`subscribed` のみ処理） | **未対応（確認済み）** |
| Hyperliquid の欠落回復 | snapshot ack に欠落が入る | `lib/hyperliquid-connector.mjs:52` で subscription ack を**設計上無視**（`restUrl: ''`、l2Book を full replace で受ける前提） | **設計判断（要確認）** |
| Bitstamp の購読上限 | 1024/接続 | 購読数は少数（trades/book/depth） | 問題なし |
| 板の再接続後回復 | — | REST snapshot 取得→検証→ring buffer 再同期（`base-connector.mjs:316-324`, `resyncCount`） | 実装あり |

## 4. この精査で変わった理解

1. **「30秒無音」はこちらの検知条件だけの問題ではない**。OKX は仕様として30秒で切る。
   一律30秒閾値は「venue が切る前に自分で切り直す」用途にはほぼ無力で、venue別の設計が必要。
2. **突然切断（1006）は異常ではなく、文書化された運用**（Bitfinex は明示、Hyperliquid は
   「予告なく周期的に切れる」）。こちらのソケット死ではない（実測: `pings=1194 pongs=1194 lastPong=2991ms`）。
3. **回復手段が文書化されている venue がある**（Hyperliquid の snapshot ack、Bitstamp の
   event_id/pre_event_id、Bitfinex の再購読+snapshot）。点検の結果、**板（depth）の回復は
   REST snapshot 経由で実装済み**（`base-connector.mjs:316-324`）だが、
   **Bitfinex の「再起動します」info を無視している**（`bitfinex-connector.mjs:92`）ことは確認できた
   ＝ 取引所が予告してくれているのに、それを使っていない。

## 5. 次にやること（この順）

1. **Bitfinex の info メッセージ対応**（実装確定・最小）: `Stop/Restart Websocket Server (please reconnect)` を
   受けたら、黙って切られる前に自分から再接続する。予告を使わない手はない。
   （板の回復は REST snapshot で実装済みなので、trades側の欠落が論点）
2. 実測継続: `pings=` ログから **接続寿命の分布**（venue別）を取る。24時間制限や定期再起動の
   周期が読めれば、**切られる前に張り直す**運用が可能か判断できる。
3. 30秒無音クラスの生存確認（`no message for ... (pings=...)`）を取得して、venue別の閾値設計に進む。
4. 変更をまとめたら **Astra の最終監査**（1回）。

## 6. 全venue 対応表（2026-09-23 時点・調査進行中）

「サーバーの挙動」「クライアントの義務」「切断/回復」「出典」を1行に集約。**未確認は未確認と書く**。

| venue | サーバー ping/hb | クライアントの義務 | 切断・寿命 | 回復手段 | 出典 |
|---|---|---|---|---|---|
| Binance Spot | protocol ping **20秒毎** | 1分以内に pong（`ws` の autoPong が実施） | **24時間で切断**／受信5msg/秒超過で切断・IP BAN | 購読の再開＋RESTスナップショット | 公式 docs（web-socket-streams） |
| Binance USD-M | **未確認**（新docsがJS描画） | 未確認 | 未確認（Spotと同種の可能性はあるが**転用しない**） | 未確認 | — |
| Bybit v5 | protocol ping（応答必須）／アプリ層 heartbeat を送る仕様 | アプリ層 `{"op":"ping"}`（**未実装**） | ping-pong もデータも無ければ**10分で切断** | 再購読（ack確認が無い） | 公式 docs（v5/ws/connect） |
| OKX v5 | アプリ層 ping（`ping`→`pong`） | ping を送る／**データが30秒 push されないとサーバーが切る** | 接続要求3req/秒（IP）、購読要求数に上限、接続数上限30/チャンネル | snapshot（`books`）+ seq 突合 | 公式 docs（docs-v5） |
| Coinbase Exchange | **未確認** | 未確認 | 未確認 | 未確認 | — |
| Kraken WS v2 | heartbeat チャンネル（データ無し）／サーバー側の切断条件は**未確認** | アプリ層 ping 可（`ping`→`pong`） | set heartbeat（自動解除）等 | snapshot + checksum | 公式 APIリファレンス |
| Bitstamp | **サーバー心拍は無いと実測済み**（2026-08-16） | 自前 ping（実装済み） | 購読**1024上限**／1025件目で silent close／auth 60秒 | diff + snapshot の **microtimestamp** 突合／`event_id`/`pre_event_id`（**未使用**） | 公式 websocket v2 |
| Bitfinex | **15秒毎に heartbeat** | 再購読・snapshot 取り込み（実装あり） | サーバーが**「Stop/Restart Websocket Server (please reconnect)」**を送る＝定期再起動 | 再購読＋snapshot（既存） | 公式 docs（ws-general） |
| Hyperliquid | **未確認**（アプリ層 ping の要否） | 「切断を前提に graceful に再接続」 | 「**予告なく周期的に切断**」 | **再接続時の欠落は snapshot ack に入る**（ack は設計上無視している） | 公式 docs（websocket） |

### 進め方（調査の残り）
- 未確認3件（Binance USD-M / Coinbase Exchange / Kraken の切断条件）は、
  ① 並行調査（codex luna）の結果と突合、② ブラウザ（CDP）でJS描画ページを読む、の両方で埋める。
- 突合結果が揃ったら **Astra の最終監査**を受け、そのうえで実装（Byteb のアプリping／Bitfinex の info 対応など）に入る。

## 7. 突合で確定した「完全対応に必要な項目」（2026-09-23 / 一次資料で検証済み）

luna（codex gpt-6-luna, efflow high）の並行調査と私の調査を突合し、**食い違いは一次資料で決着**させた。
以下はすべて公式文書の文言を確認済み。

### 7-1. Bitfinex の info コード（検証済み・実装対象）
公式 `docs.bitfinex.com/docs/ws-general` の Info Codes:
- **20051**: Stop/Restart Websocket Server (please reconnect)
- **20060**: Entering in Maintenance mode. Please pause any activity and resume after receiving the info
  message **20061** (it should take 120 seconds at most)
- **20061**: Maintenance ended. You can resume normal activity. **It is advised to unsubscribe/subscribe again
  all channels.**
→ 対応: **20051 で再接続 / 20060 で処理抑制 / 20061 で全チャンネル再購読**。
  現状は `lib/bitfinex-connector.mjs:92` で `info` を**一律無視**している（＝未対応）。

### 7-2. Binance の `serverShutdown`（検証済み・実装対象）
公式 spot docs の General WSS information:
- 「A **`serverShutdown`** event will be sent when the server is about to shutdown, resulting in disconnection.
  **Please establish a new connection as soon as possible** to prevent interruption.」
- 「When you receive a ping, **you must send a pong with a copy of ping's payload** as soon as possible.」
  （`ws` の autoPong は payload をそのまま返す実装なので満たす。テストで担保する）
→ 対応: **`serverShutdown` を受けたら自分から新規接続**（現状は未対応の見込み）。
  24時間制限についても「24時間前に計画再接続」が推奨運用。

### 7-3. Hyperliquid の 60秒ルール（検証済み・実装対象）
公式 `.../websocket/timeouts-and-heartbeats`:
- 「The server will close any connection if it hasn't sent a message to it in the last **60 seconds**.」
- 「If you are subscribing to a channel that doesn't receive messages every 60 seconds, you can send
  heartbeat messages... `{ "method": "ping" }` → `{ "channel": "pong" }`」
→ 対応: **無送信が近づいたら JSON ping を送る**（現状は未対応）。

### 7-4. Bybit のアプリ層 ping（検証済み・実装対象）
公式 v5/ws/connect: ハートビートは **アプリ層** `{"op":"ping"}`。ping-pong もデータも無ければ **10分で切断**。
→ 対応: **20秒周期で送信**（`lib/bitmex-connector.mjs:74` に同型の前例あり）。

### 7-5. OKX のアイドル条件（検証済み・設計に反映）
公式 docs-v5: 「data has not been pushed for more than **30 seconds**」でサーバーが切断。
→ 対応: 受信ごとに更新するタイマーを持ち、**期限前にアプリ層 `ping`**。データ停滞の検知は30秒より短く。

### 7-6. Coinbase Exchange（検証済み・実装対象）
公式 `exchange/websocket-feed/overview`: 「**To receive feed messages, you must send a subscribe message or
you are disconnected in 5 seconds.**」／`heartbeat` チャンネルあり／公式サンプルはクライアントpingを無効化。
→ 対応: 購読は即時送信（実装済み）。**`heartbeat` チャンネルの要否は要検討**、seq/`last_trade_id` 監視を確認。

### 7-7. Kraken（未確認を明示）
v2 の ping/heartbeat は存在（アプリ層 `ping`→`pong`）。**サーバー側の無音切断条件は公式から確定できず**。
v1 の仕様を v2 に流用しない（luna の指摘）。

### 7-8. Bitstamp（一次資料は私の取得分で確定）
サーバー心拍は無い（実測 2026-08-16）／購読 1024 上限（1025件目で silent close）／
`bts:request_reconnect` を受ける仕様 → **受け取ったら再接続する必要がある**（実装の実効性を確認する）。

## 8. 次に実装する（確定分のみ）
1. Bitfinex: 20051/20060/20061 の分岐（再接続・抑制・再購読）
2. Binance: `serverShutdown` の受信で新規接続（+ payload付きpongのテスト）
3. Hyperliquid: 60秒対策の JSON ping
4. Bybit: アプリ層 `{"op":"ping"}` 20秒周期
5. OKX: アプリ層 `ping` + データ停滞検知
6. Bitstamp: `bts:request_reconnect` の実効性確認
