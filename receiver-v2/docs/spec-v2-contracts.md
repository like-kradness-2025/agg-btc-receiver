# v2 契約の確定 — Astra 監査 P1 への回答

この文書は `spec-v2.md` の補遺で、**実装前に確定させると決めた項目**を1つずつ決め切る。
Astra の指摘（P1×12）に対する決定であり、曖昧さを残さないことを目的とする。
状態: 決定（この文書が以後の実装・テストの正本。逸脱する実装は不可）。

## C1. 封筒のフィールド名と世代の輸送

- 名前は **snake_case に統一**する: `run_id` / `generation` / `connection_id` / `market` / `venue` /
  `stream` / `receive_seq` / `recv_ts_ms` / `recv_mono_ns` / `raw`。
- **正規化は受信側の1箇所だけで行う**。受信は `envelope.mjs` のファクトリ以外で封筒を作らない
  （camelCase を手書きしない）。整理・板は**受信が作った封筒をそのまま読む**。
- **`generation` は封筒に含めてシリアライズする**（IPC を越えても失われない）。世代の**発行は受信だけ**が行い、
  封筒はそれを運ぶだけ。板は封筒に付いた世代だけを信頼する。

## C2. 接続IDの一意性（再起動をまたぐ）

- `connection_id = "<run_id>:<venue>:<market>:<generation>"`。
- **`run_id` はプロセス起動ごとに新規生成**し、`run_marker` テーブルへ**受信開始前に fsync で書く**。
  「起動ごとに世代が 0 に戻る」ことは問題にしない — 一意性は `run_id` が担う。
- 整理は `accept(connection_id)` を**明示的に呼ばれた時だけ**世代交代する。配線の所有者は `structure`。
  未 `accept` の `connection_id` からのフレームは**受理しない（fail-closed）**。
- 旧世代の spool が残っている場合の順序は固定: ①旧世代の spool を流し切る → ②`applied_boundary` を確定 →
  ③新世代を `accept` → ④新規データを消費する。

## C3. 購読の成立判定

- **送信時に「期待購読の集合」を登録する**。成立は `期待集合 == ack済み集合` のときだけ。
  1件でも ack が来ていない購読がある状態を「成立」と呼ばない。
- `ackMode` を venue ごとに宣言する:
  - `explicit` — venue が購読 ack を返す（Kraken/Bitfinex/OKX/Bybit/Coinbase 等）。
  - `first-data` — ack を返さない（Binance 系）。**そのストリームの最初のデータ受信**をもって成立とみなす。
- **ack 期限**（既定 10000ms）を超えた未成立・拒否は `failed` として記録し、**帯域の状態に出す**。
  `failed` がある限り、その接続を「配信が成立している」と報告しない。
- 拒否を表す既知の状態（例: 「既に購読済み」）は **成立**として扱う。判断は**コード/イベント名**で行い、
  文面では行わない。

## C4. keep-alive の表現力

- アダプタは次のいずれかを返す（**単一メッセージ契約をやめる**）:
  - `{ intervalMs, payload() }` — 周期送信（Bybit 20秒・Bitstamp 15秒 等）
  - `{ noActivityMs, payload() }` — **無音時のみ送信**（OKX: 30秒未満の間隔で `ping` を送り、`pong` が
    期限内に来なければ切断扱い）
  - `null` — 何も送らない（Binance 系はサーバー ping に依存）
- **制御フレームの pong は WS 実装の自動応答に任せる**。受信側は「サーバー ping を観測した」ことを
  生存の証拠として記録する（送信義務を自分で実装しない）。
- `pong` の返答期限は **接続の生存判定**に使い、**市場更新の停滞判定とは分離**する（別の問い）。
- 沈黙の閾値は **venue ごとの値**を根拠つきで持つ。一律 15 秒は「他に根拠が無いときの既定」であり、
  venue ごとの実測・公式記述があるときはそちらを優先する。

## C5. snapshot と差分の区別

- `changesFor()` は次のいずれかを返す:
  - `{ replace: true, levels: [...] }` — **板を全置換**（既存レベルを捨てる。snapshot / 全量スナップショット）
  - `{ replace: false, changes: [...] }` — 差分適用（upsert。サイズ 0 は削除）
- **`replace: true` を差分適用として扱ってはいけない**（混同すると消えたはずの水準が残留し、
  板が crossed になる。現行系で実測済みの故障型）。
- 差分が `replace` を必要とする venue（snapshot を別フレームで送る型）は、アダプタが**境界を明示**して
  そのフレームだけ `replace: true` にする。

## C6. 境界の証明と、保証しない範囲の明文化

- `proveBoundary(connection_id, ...)` は**接続IDの一致だけでは成功にしない**。次のいずれかで証明する:
  - `prev_seq` / `seqId`+`prevSeqId` / `u`+`pu` による **連番の継続**
  - Kraken の **CRC32 checksum**（snapshot と更新の整合）
- **証明手段を持たない venue（Hyperliquid の snapshot feed 等）は `boundary: "unverifiable"` を宣言**する。
  その場合も running にはするが、**`data_complete` の主張には使わない**（「この範囲は証明していない」と
  記録する）。不明な連番・replay 保証を**仮定しない**。
- 証明に失敗した場合は fail-closed（適用しない・`SYNCING` へ戻す）。

## C7. 穴と readiness、世代の切り分け

- 穴を検出したら板の状態を **`SYNCING` へ戻す**（`RUNNING` のままにしない）。回復（snapshot 再取得 or
  連続性の回復）まで `running` を名乗らない。
- `closeGaps()` の判定条件に **`connection_id` を含める**。新世代の連番が旧世代の欠測を「埋まった」扱いに
  してはいけない。
- **「いまの板の回復」と「過去に生じた欠測の保持」を別々に持つ**（前者は状態、後者は記録）。
  欠測の記録は回復後も消さない（raw の正しさに関わる）。

## C8. 再配送（raw と板の間で失わない）

- 整理が持つのは **raw の重複排除（`applied_boundary` まで）**。板への配送は別の問いであり、
  **「耐久化済みだが未適用」のフレームは必ず配送する**（重複として捨てない）。
- 起動時は **`applied_boundary` から再送**する（`received_tail` ではなく）。板側は
  `(connection_id, receive_seq)` の適用済み集合を持つので**二重適用は no-op**（冪等）。
- 「raw に書けたか」と「板に適用したか」を1つの真偽値で表さない（別々の境界として持つ）。

## C9. 実装順（この順で進める）

1. 共通部分の是正 — C1（封筒の一本化）・C2（run_id と accept 配線）・C8（再配送）・C3（購読成立）・C4（keep-alive 契約）
2. **Kraken の採用版を決定**（v2 を採る。実装を v2 へ書き直し、v1 の知見は移植しない）
3. Bybit（snapshot 全置換・周期 ping） → Binance Spot（REST snapshot＋差分バッファ・control pong・`first-data`）
   → Binance 先物（`u`/`pu` の継続） → OKX（無音 ping・`seqId`/`prevSeqId`）
   → Hyperliquid（`unverifiable` の明示） → Coinbase（UNKNOWN 解消後） → Bitstamp（実測ベース）
4. 各段で「受信 → 整理 → 板」の通しテストと、障害訓練（受信再起動／spool 溢れ／板の単独再起動）

## C10. この文書の扱い

- 各項目は**テストで固定する**（決定が実装から離れたら赤くなる）。
- 実装が C1〜C8 に反する場合、**実装ではなくこの文書に合わせて直す**。
- 変更が必要になったら、理由とともにこの文書を更新し、監査に掛ける。
