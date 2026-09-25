# okx

調査対象は OKX v5 の JSON public WebSocket (`wss://ws.okx.com:8443/ws/v5/public`)。出典の行番号は `/tmp/venue-docs/okx.txt`。公式ガイド: [OKX API guide](https://www.okx.com/docs-v5/en/)。公式ページを直接検索で確認したが、ページ全体を開く操作はサイズ制限で取得できなかったため、詳細はこの環境にある公式ページ本文を根拠とする。

## 接続URL（public market data）

本番 public market data URL は `wss://ws.okx.com:8443/ws/v5/public`。依頼にある接続 URL と一致する。出典: `okx.txt`「Production Trading Services」521–530行、公式 [API guide](https://www.okx.com/docs-v5/en/)。

## 購読メッセージ（正確な JSON／フィールド・複数チャンネル可否・購読上限）

板 `books` の例:

```json
{"id":"1512","op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"}]}
```

`id` は任意の文字列（英数字、最大32文字）、`op` は `subscribe`、`args` は購読対象 object の配列。object の必須項目は `channel`、板では `instId` も必須。複数チャンネルを一つの `args` 配列で指定でき、購読パラメーターの合計長は64 KB以下。配列要素数の数値上限は資料に明記なし (**UNKNOWN**)。

板 channel 名: `books` (400 levels)、`books5` (5)、`bbo-tbt` (1)、`books50-l2-tbt` (50)、`books-l2-tbt` (400)。同一銘柄では `books-l2-tbt` と `books50-l2-tbt` または `books` を同時購読できない。`books-l2-tbt` と `books50-l2-tbt` は VIP4 以上限定で、条件未達は code `64003`。資料は多数の50/400-level購読では接続あたり30 channel未満を推奨するが、これは推奨であり購読上限とは記されていない。

出典: `okx.txt`「Subscribe」323–375行、「WS / Order book channel」23813–23832行、23841–23870行。公式 [API guide](https://www.okx.com/docs-v5/en/#websocket-api)。

## 購読の応答（ack の形・失敗時の形・「既に購読済み」等の状態コード）

成功 ack:

```json
{"id":"1512","event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"},"connId":"a4d3ae55"}
```

失敗例は `event:"error"` に `code`, `msg`, `connId` が付き、`id` は任意。例では不正リクエストが `60012`。一般 WebSocket エラー一覧には不正 args `60013`、過頻リクエスト `60014`、URL/channel不正 `60018`、public/private等の endpoint 不一致 `60008`、VIP要件未達 `64003` がある。すでに購読済みの場合の専用 ack、エラー、状態コードは資料に記載なし (**UNKNOWN**)。

出典: `okx.txt`「Subscribe」377–411行、「WS / Order book channel」23872–23907行、「WebSocket / General Class」44203–44215行、44228行。公式 [API guide](https://www.okx.com/docs-v5/en/#websocket-api)。

## keep-alive（こちらから送るもの: 形式と推奨間隔）

最後に受信したメッセージごとに N 秒 timer を設定する（N<30）。timer 発火時に WebSocket 上のテキスト `ping` を送る。次の N 秒以内にテキスト `pong` を受け取ることを期待し、来なければ error 扱いまたは再接続する。具体的な N 値は規定されず、「30秒未満」とだけ記載。

出典: `okx.txt`「Connect」181–187行。公式 [API guide](https://www.okx.com/docs-v5/en/#websocket-api)。

## サーバー側 ping/pong（送られてくるか・間隔・返答期限）

サーバーから ping を開始する周期、ping frame/text のどちらか、pong の返答期限は資料に記載なし (**UNKNOWN**)。資料の keep-alive 手順はクライアントがテキスト `ping` を送り、テキスト `pong` を待つ方式。

出典: `okx.txt`「Connect」181–187行。公式 [API guide](https://www.okx.com/docs-v5/en/#websocket-api)。

## 無音/切断の判断材料（サーバーが切る条件・こちらが切るべき条件）

- サブスクリプションが成立しない場合、または30秒を超えてデータが push されない場合、接続は自動切断される。
- ネットワーク問題時、システムが接続を無効化することがある。
- market data の無音時は、受信ごとに N<30 秒の timer をリセットし、発火時に `ping`。N 秒以内の `pong` がなければエラー扱いまたは再接続。
- close reason 一覧には `4004` = 30秒間データ未受信、`4008` = 購読 channel 総数超過、`4009` = この接続の channel 数上限超過がある。`4009` の具体的な上限値は該当箇所で示されていない。

出典: `okx.txt`「Connect」177–187行、「WebSocket / Close Frame」44232–44242行。公式 [API guide](https://www.okx.com/docs-v5/en/#websocket-api)。

## 計画停止・再接続の通知（コード/イベント名・意味・再購読の要否）

サービス更新の60秒前に `{"event":"notice","code":"64008","msg":"The connection will soon be closed for a service upgrade. Please reconnect.","connId":"..."}` が送られる。接続を再確立するよう案内される。新規接続では必要な channel を再購読する必要がある。その他の計画停止通知名・コードはこの資料範囲では確認できず (**UNKNOWN**)。

出典: `okx.txt`「Websocket disconnect for service upgrade」494–511行。公式 [API guide](https://www.okx.com/docs-v5/en/#websocket-api)。

## 更新の連番・欠落回復（sequence/prev/checksum/event_id・snapshot 取得手順）

JSON 板 push は外側に `arg`, `action` (`snapshot` または `update`), `data` を持つ。初回は全板 `snapshot`、以降は `update`。`data` 内の `seqId` は現在の連番、`prevSeqId` は直前送信の連番で、通常は新しい `prevSeqId` が前回 `seqId` と一致する。新規購読時に WebSocket が full snapshot を push するため、これを基準にローカル板を初期化する。`checksum` は deprecated で常に0、整合性確認に使わず `seqId`/`prevSeqId` を使う。`books5`/`bbo-tbt` には checksum と prevSeqId がない。

更新が約60秒ない incremental channel では `asks:[]`, `bids:[]` の keep-alive update が届き、seq は据え置き（`prevSeqId == seqId`）。maintenance により seq が reset される場合は `seqId < prevSeqId` の update が一度届き、その後は通常の連番規則に戻る。欠落検出後に JSON public channel で snapshot を取り直す具体的な手順/APIは資料に記載なし (**UNKNOWN**); 初回 snapshot の仕組みだけでは、既存購読中の欠落回復手順までは確定できない。`event_id` はこの板仕様に記載なし。

出典: `okx.txt`「WS / Order book channel」23813–23829行、「Push Data Example / Push data parameters / Sequence ID」23909–24024行。公式 [API guide](https://www.okx.com/docs-v5/en/#websocket-api)。

## レート制限・接続数制限

- 接続試行: IPあたり毎秒3回。
- 接続ごとの `subscribe` + `unsubscribe` + `login`: 合計480 request/時。
- 64 KB超の複数 channel 購読パラメーターは不可。channel数の明示的な最大値は資料に記載なし (**UNKNOWN**); 多数の50/400-level depth channelは1接続30未満を推奨。
- 30接続/ sub-account / channel の接続数上限は `orders`, `account`, `positions`, `balance_and_position`, `position-risk-warning`, `account greeks` の private channel一覧に限定して記載。public market-data channel の接続数上限は記載なし (**UNKNOWN**)。

出典: `okx.txt`「Connect」169–205行、「Subscribe」338行、「WS / Order book channel」24007行。公式 [API guide](https://www.okx.com/docs-v5/en/#websocket-api)。

## 不明点（UNKNOWN と理由）

- 一つの購読要求で指定できる channel 数の最大値: 合計長64 KBと30未満の推奨のみで、ハード上限の数値なし（`okx.txt` 338行、24007行）。
- 重複購読時の応答/状態コード: ack と一般エラー例に挙動の記載なし（377–411行、23872–23907行）。
- サーバー起点 ping の有無・周期・応答期限: keep-alive 節はクライアント起点のテキスト ping/pong のみ（181–187行）。
- public market-data の接続数上限: 30接続制限は列挙された private channel に対する説明であり、public channel の上限を示していない（189–205行）。
- JSON 板の欠落後に snapshot を取り直す具体的な手順: 新規購読時の full snapshot と連番規則はあるが、gap発生後の recovery procedure は記載なし（23813–23829行、23909–24024行）。
