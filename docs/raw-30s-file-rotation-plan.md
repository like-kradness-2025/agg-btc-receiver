# 生データ30秒ファイル分割の最小設計

## 結論

受信側の生データは **30秒ごとにファイルを切る**。

ただし受信側は
- 集計しない
- 保存単位を30秒で閉じる

だけにする。

---

## 目的

大きな生データを後段で処理しやすくする。

狙いは次の4つ。
- 時刻名だけで対象を拾える
- market ごとに並列で流せる
- 同じ market / kind の中では **窓の進行順** を保てる
- 集計済み管理と削除管理を単純にできる

> 注意: これは **同じ market / kind 内のファイル / 窓の進行順** を保つ意味であり、
> `.open` の current / previous に遅着を書き込む都合上、
> 1ファイル内部の event timestamp が厳密昇順になることまでは保証しない。

---

## 保存単位

### 基本単位
- 1 market
- 1 種類
- 1つの30秒窓

これを 1 ファイルにする。

### 種類
- raw trade
- raw depth update
- raw liquidation
- periodic snapshot

---

## 時間基準

### 基本ルール
- trade / depth update / liquidation は **event timestamp 基準** で30秒窓に振り分ける
- snapshot は **write call に渡す直前の保存時刻** で30秒窓に振り分ける

### タイムゾーン
- 窓計算も日付フォルダも **UTC 基準** に統一する
- `window_start_ms` から UTC の日付と時刻文字列を作る

### 30秒窓
- `12:00:00.000` 〜 `12:00:29.999`
- `12:00:30.000` 〜 `12:00:59.999`

### 窓計算
```text
window_start_ms = floor(ts_ms / 30000) * 30000
current_wall_clock_window_start_ms = floor(wall_clock_ms / 30000) * 30000
```

---

## timestamp 正規化

受信した timestamp は最初に **整数 ms** へ正規化する。

### 入力型
- 数値でない timestamp は drop + log
- numeric string は parse しない。drop + log
- `NaN` / `Infinity` / `-Infinity` は drop + log

### 受け入れる単位と判定
- `0 <= abs(ts) < 1e11` なら 秒 とみなして `ts_ms_raw = ts * 1000`
- `1e11 <= abs(ts) < 1e14` なら ms とみなして `ts_ms_raw = ts`
- `1e14 <= abs(ts) < 1e17` なら μs とみなして `ts_ms_raw = ts / 1000`
- `1e17 <= abs(ts) < 1e20` なら ns とみなして `ts_ms_raw = ts / 1000000`
- それ以外は invalid として drop + log

### 整数化
- 単位変換後に必ず `ts_ms = floor(ts_ms_raw)` を通す
- 以後の窓計算はこの整数 `ts_ms` だけを使う
- 古さ判定に使う値も `window_start_ms = floor(ts_ms / 30000) * 30000` に統一する

### 正常範囲ガード
正規化後の `ts_ms` / `window_start_ms` について次を適用する。
- 負数は drop + log
- `wall_clock_ms + future_skew_allowance_ms` より未来なら drop + log
- `window_start_ms > current_wall_clock_window_start_ms` なら drop + log
- `last_finalized_window_start_ms_by_market_kind` が `null` でない場合、`window_start_ms <= last_finalized_window_start_ms_by_market_kind` なら drop + log

### 初期値
- `future_skew_allowance_ms = 5000`

### writable-window 一貫性
- live ingest でも restart recovery でも、書き込み可能 window は **current wall-clock window と previous window のみ** とする
- したがって、timestamp が `future_skew_allowance_ms` 内に収まっていても、`window_start_ms > current_wall_clock_window_start_ms` の event は受け入れない
- これで live と recovery が同じ writable-window ルールを使う

### bootstrap
- まだその market / kind で finalize 済み窓が無い場合、`last_finalized_window_start_ms_by_market_kind` は `null`
- `null` の間は「finalized 済み窓以下なので drop」ルールは適用しない

### restart 後の watermark 復元
- 起動時は market / kind ごとに既存 `.jsonl` を走査し、最も新しい finalized 窓の `window_start_ms` を読む
- その値で `last_finalized_window_start_ms_by_market_kind` を初期化する
- 起動時 recovery で `.open` から `.jsonl` へ確定した窓も、この watermark 更新対象に含める
- **future quarantine した `.open` は watermark 更新対象に含めない**

### ログ
- 捨てた件数は kind / market ごとにログに残す

これで future 側に飛んだ異常 timestamp 1件で窓が先に進む事故を防ぐ。

---

## ディレクトリ案

```text
raw/
  trades/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
  book_updates/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
  liquidations/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
  snapshots/<market>/<YYYY-MM-DD>/<HH-MM-SS>.jsonl
  _quarantine/<market>/<kind>/<YYYY-MM-DD>/...
```

### 例
```text
raw/trades/binance_spot/2026-07-05/12-00-00.jsonl
raw/trades/binance_spot/2026-07-05/12-00-30.jsonl
raw/book_updates/binance_spot/2026-07-05/12-00-00.jsonl
raw/snapshots/binance_spot/2026-07-05/12-00-00.jsonl
raw/_quarantine/binance_spot/trades/2026-07-05/12-01-00.jsonl.open.future
```

---

## 命名規則

ファイル名は **窓の開始時刻** にする。

### 例
- `12-00-00.jsonl`
- `12-00-30.jsonl`

これで名前の昇順 = 窓順にできる。

---

## open / closed の扱い

最初は複雑にしない。

### 受信中
- 書き込み中は `.open` を付ける
- 例: `12-00-00.jsonl.open`

### 窓確定後
- close 後に `.jsonl` へ確定する
- 例: `12-00-00.jsonl`

### 集計側の読む条件
- `.jsonl` のみ読む
- `.open` は読まない

### no-clobber finalize primitive
- finalize は **既存 `.jsonl` を絶対に上書きしない原子的な no-clobber primitive** で行う
- 事前の「存在確認してから通常 rename」ではなく、**確定処理そのものが no-overwrite** を保証しなければならない
- destination が既に存在する場合は `EEXIST` 相当として扱う

### no-clobber quarantine primitive
- quarantine 移動も **既存 quarantine artifact を絶対に上書きしない no-clobber primitive** で行う
- quarantine destination が既に存在する場合は、連番や timestamp suffix を足した **新しい一意名** を生成する
- quarantine 先でも既存ファイルを上書きしてはいけない

### conflict ルール
- destination `.jsonl` が既に存在する場合、その `.jsonl` を上書きしてはいけない
- この場合は `.open` を quarantine へ移し、conflict log を残す
- conflict 発生時は finalized とみなさず、watermark も進めない

### live finalize 手順
live 中の finalize は次の順で固定する。
1. buffered write をすべて drain / acknowledge する
2. writer close
3. no-clobber finalize primitive を実行
4. 成功時のみ manager の finalized watermark 更新
5. `EEXIST` など失敗時は no-clobber quarantine + log

### finalize failure ルール
- drain / close / no-clobber finalize primitive のどれかが失敗したら、その窓は finalized とみなさない
- **watermark は no-clobber finalize 成功後にだけ進める**

### recovery finalize 手順
再起動後に見つけた stale `.open` は live writer を持たないので、別手順にする。
1. `.open` ファイルの存在確認
2. no-clobber finalize primitive を実行
3. 成功時のみ recovery 側の finalized watermark 更新
4. `EEXIST` など失敗時は no-clobber quarantine + log

つまり recovery path では `flush / close` を要求しない。

---

## 窓を確定する条件

ここは一本化する。

### writable 窓
ある market / kind について、書き込み可能なのは最大2窓。
- 現在窓
- 直前窓

この2窓はどちらも `.open` のまま保持してよい。

### finalized 窓
**直前窓よりさらに古くなった時点** で、その窓は確定する。

つまり:
- 現在窓: writable
- 直前窓: writable
- 直前より古い窓: close + finalize して finalized

### 重要
- 「新しい窓が来た瞬間に前窓を即 finalize」はしない
- late event を直前1窓まで許容するため、前窓も `.open` で持つ

### 補助タイマーの正確条件
イベントが止まる market もあるので、補助で
`wall_clock_window_start_ms >= open_window_start_ms + 60000`
になった `.open` は finalize してよい。

これは「その窓が current / previous の範囲外になった」ことと同じ意味で使う。

---

## late event の扱い

### 方針
最初の実装では **遅着を少しだけ許容** する。

### ルール
- 同一 market / kind で **現在窓 + 直前1窓** までだけ書き込み可
- それより古い窓に属する late event は drop + log
- finalized 済み窓に属する event は reopen しない

### 理由
- 無限に過去窓を開き直すと順序と管理が崩れる
- 直前1窓までなら現実的な遅延を拾いやすい
- 後段がすでに読んだ過去窓を書き換え続ける事故を減らせる

### 後段側の前提
このルールを採るなら、後段集計は
**最新2窓はまだ読む対象にしない**。

---

## market 直列性

### 原則
- market ごとに並列
- 同じ market / kind の中では窓順直列

### writer 管理
同じ market / kind については
**1つの writer manager** が窓切り替えを管理する。

### 直列化の不変条件
- 同じ market / kind に対する **すべての mutation** は 1 本の直列キューで処理する
- mutation には少なくとも次を含む
  - event write
  - timer-driven finalize
  - startup recovery
  - quarantine 移動
- 実装形は promise chain / queue のどちらでもよいが、同一 market / kind 内で並行 mutation を起こしてはいけない
- **startup recovery は、その market / kind の最初の live mutation より先に完了するか、同じ直列キューの先頭で必ず処理する**

### manager が持つ状態
1本の管理役が
- 現在窓
- 直前窓
- 最終確定済み窓番号
- 直列キューの先頭状態

だけを持つ。

これで finalize 順序逆転を防ぐ。

---

## snapshot 扱い

### ルール
- snapshot は保存時刻基準で窓へ入れる
- ここでいう保存時刻は **serialized write path の中で、bucketing と `writer.write(snapshot_row)` の直前に採る `Date.now()`** を指す
- snapshot は wall clock 基準の例外扱いでよい
- 1窓に複数 snapshot が入ってよい
- snapshot row には **`write_time_ms`** を保存し、実際に bucketing に使った時刻を後から監査できるようにする
- snapshot payload に source timestamp があるなら、その値は **同じ正規化関数で整数 ms に直した上で** `source_time_ms` として行データに残す

### 理由
snapshot は「その時点の板状態」を保存するものだから、
trade/depth のような event timestamp より、
**保存した瞬間** を基準にした方が自然。

---

## 日跨ぎ

### 基準
- UTC 日付でフォルダを切る
- `window_start_ms` から日付ディレクトリを作る

### 境界窓
- `23:59:30` 窓はその UTC 日付のフォルダに入る
- `00:00:00` 窓から次の日のフォルダに入る
- たとえば `23:59:58` の event が遅れて `00:00:01` に到着しても、event timestamp 基準なら前日 `23-59-30` 窓へ入る

### restart 時の探索範囲
- restart の `.open` 探索は market / kind 配下を **再帰走査** して行う
- 「今日の UTC ディレクトリだけ」「current / previous dir だけ」で済ませてはいけない
- これで >1日停止や古い stranded `.open` も拾える

---

## 再起動復旧

これは live 運用と同じ不変条件でそろえる。

### 基準時刻
- 起動時に `startup_now_ms = Date.now()` を取る
- `startup_now_window_start_ms = floor(startup_now_ms / 30000) * 30000` を計算する

### startup recovery の順序
market / kind ごとに次の順で処理する。
1. 既存 `.jsonl` を走査して finalized watermark を復元する
2. 再帰走査で `.open` を列挙する
3. `.open` を `window_start_ms` 昇順で並べる
4. 各 `.open` について keep / recovery-finalize / quarantine を判定する
5. keep した `.open` を manager の current / previous へ注入する

### keep できる `.open`
market / kind ごとに `.open` を走査して、次の2窓だけ keep 可。
- `startup_now_window_start_ms`
- `startup_now_window_start_ms - 30000`

ただし次の場合は keep しない。
- `window_start_ms <= finalized watermark`
- 同名 `.jsonl` が既に存在する

この場合は no-clobber quarantine へ移し、log を残す。

### finalize する `.open`
- keep 条件を満たさず、かつ future-named でもない `.open` は recovery finalize で即 finalize する
- recovery finalize は **`window_start_ms` 昇順** に実行する
- recovery 側の finalized watermark 更新は **`watermark = max(watermark, window_start_ms)`** で行う

### future-named `.open`
- `startup_now_window_start_ms + 30000` 以上の future-named `.open` は non-writable とみなす
- keep しない
- `.jsonl` finalized へ昇格させない
- watermark 更新対象にも入れない
- 最初の実装では **no-clobber quarantine 用サフィックスへ rename して log** する
  - 例: `12-01-00.jsonl.open.future`

### 追加ルール
- keep する `.open` は最大2窓
- それ以外は writable に戻さない
- ダウンタイム中に欠けた30秒窓は **欠番許容**
- 空ファイルは作らない
- 後段集計側で欠番検出できるようにする

---

## リソース上限

writer を増やしすぎない。

### 最小ルール
- 同一 market / kind で同時に保持する open writer は最大2窓
  - 現在窓
  - 直前窓

これを超える過去窓は reopen しない。

---

## 集計済み印の最小案

最初は sidecar でよい。

### 例
- 生データ: `12-00-00.jsonl`
- 集計中: `12-00-00.jsonl.processing`
- 集計完了: `12-00-00.jsonl.processed`

ただし今回は **受信側の設計だけ固定** が主目的なので、
実装は後段集計フェーズで入れる。

---

## 削除ルールの前提

削除はまだ入れない。

将来ルール:
- `.processed` がある
- 最新2窓ではない
- 保留時間を過ぎた

この3条件を満たしたときだけ削除する。

---

## 最初の実装範囲

### 今回やる対象
- 30秒窓の定義を固定
- path / filename ルールを固定
- `.open` → `.jsonl` の考え方を固定
- late event の最小ルールを固定
- 再起動時の `.open` 回収ルールを固定
- timestamp 正規化と future skew ガードを固定

### まだやらない
- 後段集計本体
- `.processing` / `.processed` 実装
- 自動削除
- parquet 化

---

## 利点

- ファイル名だけで時刻選別できる
- 巨大1本を先頭から舐めなくてよい
- 再実行範囲が30秒に閉じる
- market ごと並列が自然
- 受信と集計の責務が分かれる

---

## 注意点

- exchange timestamp と wall clock のずれはゼロではない
- late event をどこまで拾うかで open 窓数が決まる
- 後段は最新2窓を避けて読む前提にする
- 欠番は起こりうるので、後段で検出できる必要がある

---

## 次の実作業

1. `orderflow_monitor.mjs` の writer path を 30秒窓対応にする設計
2. writer manager を market + kind 単位で作る
3. 現在窓と直前窓だけ保持する
4. 起動時 `.open` 回収を入れる
5. smoke で 30秒ファイルが並ぶことを確認する
