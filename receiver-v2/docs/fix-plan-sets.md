# receiver v2 修正計画（セット2 以降）— Astra 監査 2026-09-25 反映版

**状態: 承認待ち（実装しない）。** この計画に対する Astra の APPROVE が出るまで、コードは書かない。
実装は1セット=1コミットで進め、各セットで「実装 → 全テスト → コミット → gpt-5.6-luna(xhigh) の厳格レビュー」を回す。

契約の正本: `spec-v2-contracts.md`（C1〜C11）／本計画はその残件をどう閉じるかだけを扱う。

## 1. いま解消すべき欠陥（優先度・根拠行つき）

### P0（データ経路が閉じない）
- **未適用フレームの台帳がメモリのみ**（`structure.mjs:87,268`）— 起動時に `applied_boundary` 起点の再送が無い。C8/C11 違反。
- **同一接続の `firstSeq` 補完が不能**（`state.mjs:242`）— 板が既知の接続に錨を取り直さないため `first sequence unknown` の保留が解消しない。

### P1（既存）
- run 所有権が復元されず、`runId=null` が認可を迂回する（`state.mjs:100-105,218`）。
- 再配送が seq 順でなく、適用済み・恒久不能が pending に残る（`structure.mjs:228-248`）。
- `skipped` の重複判定が理由を見ず、記録もメモリのみ（`structure.mjs:89,132-133,253`）。
- 整理が初見フレームで暗黙 accept し ACK まで返す（`watermark.mjs:131`）— C2 の fail-closed 違反。
- `structure.api.accept` が整理に `firstSeq` を渡さない（`structure.mjs:205`）。

### P1（Astra 監査で追加・メモリ内SQLiteで再現済み）
- **引退 run の拒否履歴が再オープンで消える**（`state.mjs:171,232`）。
- **`apply` が run・generation・対象板を検証していない**（`state.mjs:281-288`）。
- **venue 連番／checksum なしで境界証明が成功する**（`state.mjs:346-374`／契約 C6・C11）。
- **RUNNING 後に穴を検出しても SYNCING に戻らない**（`state.mjs:309-313`／契約 C7）。

## 2. セット2 の実装手順（Astra 指定・触る行番号つき）

1. `state.mjs:28-55, 89-117` — **冪等 migration**（重複列は許容）で `applied_boundary` に `run_id` / `first_seq` を追加し、**8列での保存・復元**、**引退 run の履歴**を持たせる。
   方式は **ALTER TABLE で8列**（別テーブル案は不採用）。`appliedBoundary` は既に全フィールドをコピーするので accessor 追加は不要。
2. `state.mjs:131-158, 170-171, 212-252` — `accept` を規則へ変更: 同一 run は世代の厳密増加／別 run は世代を比較せず `takeover` 認可のみ／**引退 run は拒否**／初回・復元時は所有者未確定として扱い、**`runId=null` をワイルドカードにしない**。**失敗時は DB・メモリとも旧状態を維持**。
3. `state.mjs:280-316` — `apply` で所有情報（run・generation・market/stream の板）を検証。
4. `connection.mjs:241-246` ＋ `structure.mjs:185-209` — `onGeneration` に `runId`/`venue` を載せ、structure が板へ渡す。`firstSeq` 連携も同経路。
5. `watermark.mjs:104-117, 130-160` — **暗黙 accept を除去**。同一接続の再 accept で `outOfOrder` を消さない。起点不明なら raw 耐久化と ACK 可能性を分け、**NULL を算術比較しない**。
6. テストの更新（下記）。

`resumeFrom()`（`state.mjs:268-270`）は所有情報を返さないため、**これだけで復元・認可を済ませない**。

## 3. このセットで固定する回帰テスト（1本）

`book-run-authority.test.mjs:35` を拡張し、次の順を1本で通す:
旧6列スキーマ → migration → run-A/generation=5 を**起点不明**で accept → 同一接続に `firstSeq` 補完 → 1件適用 → 再オープン → **owner・firstSeq・板・位置**を確認 → **NULL と無認可 run-B を拒否** → run-B/generation=1 の**明示 takeover** → 再オープン → **引退 run-A を拒否** → **不一致封筒を拒否** → **保存失敗時に DB・メモリ不変**。

期待値を変える既存テスト:
- `structure-redeliver.test.mjs` — 「**明示 accept 済み・起点未確定 → 補完 → 再配送**」の形へ置き換える。
- `structure-holds-losses.test.mjs:61` — 現在は**不具合を期待値として固定**しているため、補完後に自動再送される期待へ変更する。

## 4. このセットで閉じないもの（次のセット）

- セット3: 同一接続の起点補完（P0②の本体）。
- セット4: **配送台帳の耐久化**（`unapplied` の永続化・起動時 `applied_boundary` 起点の再送・ACK と板適用の耐久化）。raw 成功直後／ACK 直後／板 tx 中の停止点を1本の障害注入テストで回す。
- セット5: seq 順の共通分類と `skipped` の永続化。
- セット6: 起動復旧・spool 移送・上限（P0① 完了）。
- 別枠: venue 連番／checksum による境界証明（C6）と、穴検出時の SYNCING 復帰（C7）。

## 5. 進め方の約束

- **Astra の APPROVE まで実装しない。** 計画を変えるときは本ファイルを更新して再度承認に掛ける。
- 1セット=1コミット。**赤いツリーはコミットしない**。レビューは `gpt-5.6-luna` / effort `xhigh`。
- レビューが出す指摘は、その回で全部潰すか、本ファイルに残件として明示してから次へ進む。
