# bitstamp

対象: Bitstamp WebSocket API v2 の public market data。公式 API ページはリアルタイムデータについて WebSocket API を参照するよう案内するが、取得済み本文は遮断ページのみで、WebSocket の仕様本文を確認できなかった。

## 接続URL（public market data）

**UNKNOWN** — public market data 用の WebSocket 接続 URL を確認できない。保存本文 `/tmp/venue-docs/bitstamp.raw` は Incapsula の遮断 HTML であり、公式 [WebSocket API v2](https://www.bitstamp.net/websocket/v2/) ページも直接確認時に本文が取得できなかった。公式 [Bitstamp API](https://www.bitstamp.net/api/) はリアルタイムデータ用として WebSocket API を案内するだけで、接続 URL を示さない。

出典: `/tmp/venue-docs/bitstamp.raw`（遮断 HTML）; [Bitstamp API](https://www.bitstamp.net/api/)「Websocket」; [WebSocket API v2](https://www.bitstamp.net/websocket/v2/)（本文取得不可）。

## 購読メッセージ（正確な JSON／フィールド・複数チャンネル可否・購読上限）

**UNKNOWN** — 正確な購読 JSON、フィールド、public channel 名、複数 channel／market の指定可否、購読上限はいずれも確認できない。保存本文に仕様記述がない。

出典: `/tmp/venue-docs/bitstamp.raw`（遮断 HTML）; [WebSocket API v2](https://www.bitstamp.net/websocket/v2/)（本文取得不可）。

## 購読の応答（ack の形・失敗時の形・「既に購読済み」等の状態コード）

**UNKNOWN** — 成功 ack、失敗応答、状態コード、重複購読時の挙動を確認できない。保存本文に仕様記述がない。

出典: `/tmp/venue-docs/bitstamp.raw`（遮断 HTML）; [WebSocket API v2](https://www.bitstamp.net/websocket/v2/)（本文取得不可）。

## keep-alive（こちらから送るもの: 形式と推奨間隔）

**UNKNOWN** — クライアントから送る keep-alive の形式と推奨間隔を確認できない。

出典: `/tmp/venue-docs/bitstamp.raw`（遮断 HTML）; [WebSocket API v2](https://www.bitstamp.net/websocket/v2/)（本文取得不可）。

## サーバー側 ping/pong（送られてくるか・間隔・返答期限）

**UNKNOWN** — サーバー ping の有無・周期、pong の要否・返答期限を確認できない。

出典: `/tmp/venue-docs/bitstamp.raw`（遮断 HTML）; [WebSocket API v2](https://www.bitstamp.net/websocket/v2/)（本文取得不可）。

## 無音/切断の判断材料（サーバーが切る条件・こちらが切るべき条件）

**UNKNOWN** — サーバー切断条件、無音 timeout、クライアント側の推奨切断・再接続判定基準を確認できない。

出典: `/tmp/venue-docs/bitstamp.raw`（遮断 HTML）; [WebSocket API v2](https://www.bitstamp.net/websocket/v2/)（本文取得不可）。

## 計画停止・再接続の通知（コード/イベント名・意味・再購読の要否）

**UNKNOWN** — 計画停止通知のイベント名／コードと意味、切断後の再購読要否を確認できない。

出典: `/tmp/venue-docs/bitstamp.raw`（遮断 HTML）; [WebSocket API v2](https://www.bitstamp.net/websocket/v2/)（本文取得不可）。

## 更新の連番・欠落回復（sequence/prev/checksum/event_id・snapshot 取得手順）

**UNKNOWN** — 更新の連番・前連番・checksum・event ID、欠落検出方法、および snapshot の取得／再同期手順を確認できない。

出典: `/tmp/venue-docs/bitstamp.raw`（遮断 HTML）; [WebSocket API v2](https://www.bitstamp.net/websocket/v2/)（本文取得不可）。

## レート制限・接続数制限

**UNKNOWN** — WebSocket 接続数、再接続頻度、購読頻度・数の上限を確認できない。公式 API ページの REST API 用 rate limit 情報を WebSocket 制限として流用していない。

出典: `/tmp/venue-docs/bitstamp.raw`（遮断 HTML）; [WebSocket API v2](https://www.bitstamp.net/websocket/v2/)（本文取得不可）。

## 不明点（UNKNOWN と理由）

- public WebSocket URL: **UNKNOWN** — 公式 v2 ページ本文を取得できず、公式 API ページの WebSocket 案内にも URL がない。
- 購読 JSON、channel／market 指定、複数購読可否、上限: **UNKNOWN** — 保存本文が遮断 HTML。
- subscribe ack／error／重複購読の応答: **UNKNOWN** — 保存本文に仕様記述なし。
- keep-alive、server ping/pong、無音 timeout: **UNKNOWN** — 保存本文に仕様記述なし。
- 切断条件、計画停止通知、再購読: **UNKNOWN** — 保存本文に仕様記述なし。
- sequence／欠落回復／snapshot 手順: **UNKNOWN** — 保存本文に仕様記述なし。
- 接続数・頻度などの WebSocket rate limit: **UNKNOWN** — 保存本文に仕様記述なし。REST 制限からの推定はしていない。

確認した公式ページ: [Bitstamp API](https://www.bitstamp.net/api/)（WebSocket API への案内）; [Bitstamp WebSocket API v2](https://www.bitstamp.net/websocket/v2/)（直接確認時に本文取得不可）。
