# bybit

## 接続URL（public market data）

Mainnet の public URL は Spot `wss://stream.bybit.com/v5/public/spot`、USDT/USDC perpetual と USDT Futures `wss://stream.bybit.com/v5/public/linear`、Inverse `wss://stream.bybit.com/v5/public/inverse`。Testnet は同じパスで host が `stream-testnet.bybit.com`。本調査では Spot/Linear/Inverse が対象。出典: `/tmp/venue-docs/bybit.txt`「Connect」8–24行、[公式 Connect](https://bybit-exchange.github.io/docs/v5/ws/connect)「WebSocket public stream」。

## 購読メッセージ（正確な JSON／フィールド・複数チャンネル可否・購読上限）

```json
{
  "req_id": "test",
  "op": "subscribe",
  "args": [
    "orderbook.1.BTCUSDT",
    "publicTrade.BTCUSDT",
    "orderbook.1.ETHUSDT"
  ]
}
```

`req_id` は任意、`op` は `subscribe`、`args` は topic 文字列の配列。複数 topic と symbol を同じ要求で購読できる。接続あたり `args` 配列の長さは21,000文字以下。Spot は1要求あたり最大10 args、Options は接続あたり最大2,000 args、Futures/Spread は args 個数上限なしと記載。上限は接続カテゴリごとに異なるため、Spot/Linear/Inverse の購読数を一律の個数上限に換算できない。標準板 topic は `orderbook.{depth}.{symbol}`。Spot/Linear/Inverse の標準 depth は1/50/200/1000、記載 push 間隔は順に10/20/100/200 ms。出典: `/tmp/venue-docs/bybit.txt`「Public channel - Args limits」140–146行、「How to subscribe with a filter」186–208行、[公式 Connect](https://bybit-exchange.github.io/docs/v5/ws/connect)「Public channel - Args limits」「Understanding WebSocket Filters」、[公式 Orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)「Depths」「Topic」。

## 購読の応答（ack の形・失敗時の形・「既に購読済み」等の状態コード）

成功 ack の public Spot 例:

```json
{"success":true,"ret_msg":"subscribe","conn_id":"2324d924-aa4d-45b0-a858-7b8be29ab52b","req_id":"10001","op":"subscribe"}
```

成功 ack の Linear/Inverse 例では `ret_msg` が空文字、`req_id` は空文字、`conn_id` と `success:true`, `op:"subscribe"` を含む。成功時の値には例の差があるので `ret_msg` を固定値として扱わない。失敗時の ack 形式、重複購読時の挙動・状態コードは確認した Connect 資料に具体例なし (**UNKNOWN**)。出典: `/tmp/venue-docs/bybit.txt`「Topic subscription response message example」222–235行、[公式 Connect](https://bybit-exchange.github.io/docs/v5/ws/connect)「Understanding the Subscription Response」。

## keep-alive（こちらから送るもの: 形式と推奨間隔）

```json
{"req_id":"100001","op":"ping"}
```

`req_id` は任意。20秒ごとの送信を公式推奨。public pong の例は `{"success":true,"ret_msg":"pong","conn_id":"...","op":"ping"}`（Spot）および Linear/Inverse では加えて `req_id` を含む形。出典: `/tmp/venue-docs/bybit.txt`「How to Send the Heartbeat Packet」147–181行、[公式 Connect](https://bybit-exchange.github.io/docs/v5/ws/connect)「How to Send the Heartbeat Packet」。

## サーバー側 ping/pong（送られてくるか・間隔・返答期限）

サーバー起点の ping の有無・間隔・クライアントの返答期限は、確認した公式資料に記載なし (**UNKNOWN**)。確認できるのは、クライアントが JSON `op:"ping"` を送信し、サーバーから JSON pong 例が返る仕様。WebSocket protocol の ping frame と JSON ping は資料上区別して記載されていない。出典: `/tmp/venue-docs/bybit.txt`「How to Send the Heartbeat Packet」147–181行、[公式 Connect](https://bybit-exchange.github.io/docs/v5/ws/connect)「How to Send the Heartbeat Packet」。

## 無音/切断の判断材料（サーバーが切る条件・こちらが切るべき条件）

任意の切断があり得るため、切断時は速やかに再接続するよう案内される。無 ping-pong かつサーバー配信データなしで10分後に切断、`max_active_time` 30–600秒を設定可能との説明は private stream と order entry に限定され、public market data の切断条件としては確認できない (**public の条件は UNKNOWN**)。Public 接続では20秒ごとの client ping 推奨。出典: `/tmp/venue-docs/bybit.txt`「CUSTOMISE PRIVATE CONNECTION ALIVE TIME」59–65行、「CAUTION」131–140行、「How to Send the Heartbeat Packet」179–181行、[公式 Connect](https://bybit-exchange.github.io/docs/v5/ws/connect) 同見出し。

## 計画停止・再接続の通知（コード/イベント名・意味・再購読の要否）

Public market-data 接続の計画停止通知コード/イベント名は、確認した資料に記載なし (**UNKNOWN**)。Connect ページは別の WebSocket GET System Status URL `wss://stream.bybit.com/v5/public/misc/status` を示すが、この資料だけでは通知イベントや購読手順を確認できない。切断後の速やかな再接続は推奨される。再接続後の再購読要否を明文で定めた説明は見つからないが、購読要求は接続上で送る手順として記載されるため、新しい接続で購読要求を再送する前提が必要。出典: `/tmp/venue-docs/bybit.txt` 42–48行、131–140行、186–235行、[公式 Connect](https://bybit-exchange.github.io/docs/v5/ws/connect)「WebSocket GET System Status」「How to Subscribe to Topics」。

## 更新の連番・欠落回復（sequence/prev/checksum/event_id・snapshot 取得手順）

標準 `orderbook.{depth}.{symbol}` は購読後に `snapshot`、続いて `delta` を受信する。新たな snapshot を受けたらローカル板を置き換える。板側の問題時は最新データを含む snapshot が再送される。`data.u` は update ID、`data.seq` は cross sequence（板レベル間の生成順比較用）で、標準板について `prev_seq` / checksum / `event_id` による連続性検査は資料に記載なし。Linear/Inverse/Spot level 1 は板変更がなくても3秒後に同じ `u` の snapshot が再送される仕様がある。出典: [公式 Orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)「Process snapshot/delta」「Response Parameters」。

Full-depth `orderbook.full.{symbol}` は WS 初回 snapshot がなく delta のみ。REST full-orderbook snapshot を取得し、WS delta を先に buffer して同期する。Full-depth では `u` は同一 session 内で連続し、`seq` は単調増加だが連続とは限らない。`u` の飛びを検出したら buffer を破棄して取り直す。REST snapshot と buffer の `seq`/`u` を照合し、snapshot の `seq` が最初の delta より小さければ snapshot を再取得、snapshot と一致する位置より前の delta を捨て、一致した snapshot から残り delta を適用する。適用後も `u > local_u + 1` なら板を破棄して同期をやり直す。`u=1` は再同期合図。出典: [公式 Full Orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/full-ob)「Full Order Book Synchronization Procedure」「Order Book Update Procedure」。

## レート制限・接続数制限

WebSocket の新規接続は5分あたり500回以下（WebSocket domain 単位）。Market data 接続は IP あたり1,000接続以下で、Spot / Linear / Inverse / Options ごとに別カウント。Public connection の `args` 総文字数・カテゴリ別 args 件数制限は購読節を参照。出典: `/tmp/venue-docs/bybit.txt`「IP Limits」「Public channel - Args limits」137–146行、[公式 Connect](https://bybit-exchange.github.io/docs/v5/ws/connect) 同見出し、[公式 Rate Limit Rules](https://bybit-exchange.github.io/docs/v5/rate-limit)「Websocket IP limit」。

## 不明点（UNKNOWN と理由）

- 購読失敗 ack の形とコード、重複購読の応答・状態コード: Connect の購読応答節に成功例のみで、失敗・重複例なし。出典: `/tmp/venue-docs/bybit.txt` 222–235行、[公式 Connect](https://bybit-exchange.github.io/docs/v5/ws/connect)「Understanding the Subscription Response」。
- サーバー起点 ping の有無・周期・応答期限: heartbeat 節は client ping とそれへの pong 応答例のみ。出典: `/tmp/venue-docs/bybit.txt` 147–181行、[公式 Connect](https://bybit-exchange.github.io/docs/v5/ws/connect)「How to Send the Heartbeat Packet」。
- Public socket の無音切断時間・サーバー切断条件: 10分/max_active_time の説明対象は private stream と order entry。出典: `/tmp/venue-docs/bybit.txt` 59–65行、[公式 Connect](https://bybit-exchange.github.io/docs/v5/ws/connect)「Customise Private Connection Alive Time」。
- Public market-data の計画停止イベントと event code: Connect 資料に status URL はあるが、通知 payload と市場 WS への影響は記載なし。出典: `/tmp/venue-docs/bybit.txt` 42–48行、[公式 Connect](https://bybit-exchange.github.io/docs/v5/ws/connect)「WebSocket GET System Status」。
- Spot/Linear/Inverse 各カテゴリの購読 args 個数上限のうち Spot 以外: Linear/Futures は個数上限なしと記載、Inverse を個別に明示した個数上限はなく、21,000文字制限のみ共通。出典: `/tmp/venue-docs/bybit.txt` 140–146行、[公式 Connect](https://bybit-exchange.github.io/docs/v5/ws/connect)「Public channel - Args limits」。
