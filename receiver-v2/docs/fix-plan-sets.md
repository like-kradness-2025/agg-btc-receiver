# receiver v2 修正計画（セット2 以降）— Astra 監査 2026-09-25 反映版

**状態: APPROVED（2026-09-25 Astra APPROVE・実装可）。** 実装は下記 6節の注意点に従う。
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

## 2. セット2 の完了対象（範囲の明記・Astra 指摘①）

**セット2 で閉じるもの**:
1. 所有情報（`run_id` / `first_seq`）の永続化・復元（旧6列からの冪等 migration を含む）と、引退 run 履歴の永続化。
2. `accept` の規則（同一 run=世代の厳密増加／別 run=世代を比較せず takeover 認可のみ／引退 run=拒否／`runId=null` の扱い）。
3. **同一接続の起点補完**（下記 2.2 の条件つき）。
4. `apply` の所有情報検証（run・generation・対象板）。
5. 整理の暗黙 accept 除去と `firstSeq` の伝達。
6. `runId` の受信 → 板への伝達。

**セット2 で閉じないもの（残件として明示）**: 配送台帳の耐久化と起動時 `applied_boundary` 起点の再送（= 既存 P0①の本体・**セット3**）／再配送の seq 順分類と `skipped` の永続化（= 既存 P1②③・**セット4**）／spool 移送と上限（**セット5**）／venue 連番・checksum による境界証明（C6）と穴検出時の SYNCING 復帰（C7）（**セット6**）。
したがって**セット2 の完了は「上記1〜6が閉じたこと」**を指し、列挙した欠陥全体の解消ではない。

## 2.1 引退履歴の保存と原子性（Astra 指摘③）

- 保存先は**専用テーブル** `retired_run (market, stream, run_id, retired_at_ms, PRIMARY KEY (market, stream, run_id))`。板のキー（market/stream）で引き、**複数の引退 run を保持**できる。
- **run 名を持たない所有者も同じ表に引退記録する**（run 名は非空なので、**空文字**＝どの run も名乗れない値として保存する。NULL は主キーにできず、SQLite では複数行が並んで引退が効かなくなる）。復元時は空文字を「run 名を持たない所有者」として読み、その owner からの **takeover も拒否**する（引退は所有の話なので、名前の有無で扱いを変えない）。
- takeover の確定手順は固定する: **①旧 run の引退記録 ②新しい owner（`run_id`/`first_seq`）の保存 ③境界行の更新**を**同一トランザクション**で行い、**成功した後にだけ**メモリ（`applied`・履歴集合）を更新する。
  現状は `state.mjs:232` で保存前に履歴と所有状態を書き換えているため、**この順序を逆にする**。
- 失敗時は DB・メモリとも旧状態を維持（部分適用を作らない）。

## 2.2 起点補完の規則（Astra 指摘②）

同一接続への `firstSeq` 補完は、次を**すべて**満たすときだけ許す:
- **同一 run・同一 generation・同一 connection_id** であること。
- その接続の起点が**まだ未確定**であること（確定済み起点の**変更は禁止**）。
- 補完は**位置・待機列・穴を動かさない**こと。**到着済みの seq へ起点を移すのは穴を飛び越えるため禁止**。

世代比較の一律緩和はしない（同一 run では厳密増加のまま）。

## 2.3 復元時の owner の扱い（Astra 指摘④）

- **保存済み `run_id` がある復元**: その run を**権威として復元**する（未確立扱いにはしない。未確立にすると保存した owner による認可が無効になる）。
- **旧6列由来で `run_id` が NULL の復元**: 所有者**未確立**として扱う。確立条件は「その板への最初の明示 `accept`」で、takeover は要求せず、与えられた世代を**基準として記録**する。
- いずれの場合も **NULL を「任意の run と同一」とは扱わない**。

## 2.4 セット2 の実装手順（Astra 指定・触る行番号つき）

1. `state.mjs:28-55, 89-117` — **冪等 migration**（重複列は許容）で `applied_boundary` に `run_id` / `first_seq` を追加し、**8列での保存・復元**、**引退 run の履歴**を持たせる。
   方式は **ALTER TABLE で8列**（別テーブル案は不採用）。`appliedBoundary` は既に全フィールドをコピーするので accessor 追加は不要。
2. `state.mjs:131-158, 170-171, 212-252` — `accept` を規則へ変更: 同一 run は世代の厳密増加／別 run は世代を比較せず `takeover` 認可のみ／**引退 run は拒否**／**所有者未確立として扱うのは「初回」と「旧6列由来で `run_id` が NULL の復元」だけ**（保存済み `run_id` がある復元は 2.3 のとおり確立済みとして扱う）／**`runId=null` をワイルドカードにしない**。**失敗時は DB・メモリとも旧状態を維持**。
3. `state.mjs:280-316` — `apply` で所有情報（run・generation・market/stream の板）を検証。
4. `connection.mjs:241-246` ＋ `structure.mjs:185-209` — `onGeneration` に `runId`/`venue` を載せ、structure が板へ渡す。`firstSeq` 連携も同経路。
5. `watermark.mjs:104-117, 130-160` — **暗黙 accept を除去**。同一接続の再 accept で `outOfOrder` を消さない。起点不明なら raw 耐久化と ACK 可能性を分け、**NULL を算術比較しない**。
6. テストの更新（下記）。

`resumeFrom()`（`state.mjs:268-270`）は所有情報を返さないため、**これだけで復元・認可を済ませない**。

## 3. このセットで固定する回帰テスト（1本）

`book-run-authority.test.mjs:35` を拡張し、次の順を1本で通す:
旧6列スキーマ → migration → run-A/generation=5 を**起点不明**で accept → 同一接続に `firstSeq` 補完 → 1件適用 → 再オープン → **owner・firstSeq・板・位置**を確認 → **NULL と無認可 run-B を拒否** → run-B/generation=1 の**明示 takeover** → 再オープン → **引退 run-A を拒否** → **不一致封筒を拒否** → **保存失敗時に DB・メモリ不変**。

追加で固定する検証（Astra 指摘⑤）:
- **失敗注入は3点**で行う: ①引退履歴の更新後 ②owner 保存時 ③板更新後の境界保存時。各点で再オープンし、**DB とメモリの不変**（板・位置・owner・phase・待機列）を確認する。
- **引退 run は**「複数回 takeover の後」「再オープンの後」「高い generation に takeover を付けた場合」でも拒否されること。
- **不一致封筒は** connection を一致させ、`run` / `generation` / `market` / `stream` を**1つずつ変えて**拒否と無変更を確認する。
- **structure 経路**で: 未 accept 時に ACK を出さない／起点 NULL のとき raw は成功し ACK は保留される／補完後も穴を飛び越えない。

期待値を変える既存テスト:
- `structure-redeliver.test.mjs` — 「**明示 accept 済み・起点未確定 → 補完 → 再配送**」の形へ置き換える。
- `structure-holds-losses.test.mjs:61` — 現在は**不具合を期待値として固定**しているため、補完後に自動再送される期待へ変更する。

## 4. セット2 の外に残るもの（第2節の範囲と一致させる）

第2節で「セット2 で閉じるもの」に挙げた1〜6（所有情報の永続化・復元／引退履歴／`accept` の規則／**同一接続の起点補完**／`apply` の所有情報検証／整理の暗黙 accept 除去と `runId` 伝達）は、**すべてセット2 の中で閉じる**。

セット2 の外に残るのは次だけ:
- **セット3**: **配送台帳の耐久化** — `unapplied` の永続化と、**起動時に `applied_boundary` 起点で再送する配線**、および ACK と板適用の耐久化（= 既存 P0①の本体）。
- **セット4**: 再配送の **seq 順分類**（適用済み・保留・恒久不能・未認可を排他に分類）と **`skipped` の永続化**（= 既存 P1②③）。
- **セット5**: **spool の移送（drain）と上限**、溢れ時に既存分を消さない停止（= P0①の残り）。
- **セット6**: **境界証明（C6）** = venue 連番・checksum の検証と `unverifiable` の明示、および **穴検出時の SYNCING 復帰（C7）**。

### 4.1 セット3 の実装で確定したこと（2026-10-01）

- 台帳は `delivery_ledger (market, stream, connection_id, receive_seq, run_id, venue, generation, recv_ts_ms, recv_mono_ns, raw BLOB, meta, reason, state, recorded_at_ms)`（板のキー market/stream で引く）。**フレームの実体も一緒に保存する**: raw writer は呼び出し側のフックで読み出し口が無く、「raw から読み直して再送する」ができないため。
- entry は**2状態**（raw 書き込みは store のトランザクションの外にあるため）:
  - `intent` = raw 書き込みの**直前**に書く。クラッシュで残る最悪は「raw に無いかもしれない frame の entry」＝**再判定できる** ✓（逆順にすると「raw にはあるが誰も債権を知らない frame」＝永遠に配達されない ✗）。
  - `owed` = 耐久化を主張する**同一トランザクションで confirm** ✓（entry が「raw が持っている」ことと、受理したという主張が分離しない ✓）。
  - **板へ配達してよいのは `owed` だけ** ✓（intent を配ると板が正本より先へ進む ✗）。
- **再送は板へ直接**（organizer を通さない）＋ **構造を開いた時点（`resume`）に実行** ✓。再送される frame は「その接続の番号がどこから始まりうるか」の上限を決める一部でもあるため（§2.2）、accept が先に走れると完了がそれを飛び越える ✗。起動時の organizer への接続採用は台帳の intent を再判定するときだけ行う ✓。
- **解放は `firstSeq ≤ receive_seq ≤ upToSeq`** ✓。`firstSeq` 未満の frame は永久に適用不能なので**解放せず損失の記録として残す** ✓（連続上限のみ動くので穴の上を解放しない ✓）。接続が交代した entry も消さずに残す ✓。
- **取り消せるのは、その試行が新規に作った `intent` だけ** ✓。既存の entry は raw が持っているかもしれず、書き込みを拒否したという一事実で消してよい記録ではない ✗（C8）。既存の `owed` は raw を再書き込みせず配送する ✓。
- **起動時の順序は「`owed` の配送 → `intent` の再判定」** ✓。`owed` の frame は板の起点上限を決める一部なので（§2.2）、intent の宣言を先に通すと飛び越えが成立する ✗。
- **同じキーの `owed` がある受信フレームは raw に書き直さない** ✓。かつ、その近道でも**受信してきた frame 自身の申告**（板・接続・run・generation）を検証する ✓（台帳から配ることが「別の run に板の話を聞かせる」経路になってはいけない ✗）。届けるのは**保存済みの frame**（raw が持っている内容）✓。
- **復元は行があれば必ず起点を戻す** ✓（位置が NULL でも ✓）。位置と起点は別の記録で、位置がまだ無いこと（フレームを保留している状態 ✓）を起点不明と取り違えない ✗。
- **移行が要求する列は表ごとに決める** ✓（`organized_watermark` は `stream`＋`first_seq`、`organize_gap` は `stream` のみ ✓）。揃えると、**他の板を開くたびに生きた板の欠測が退避される** ✗。
- **起点（`first_seq`）も organizer の保存に含める** ✓。復元は保存値を優先し、**1 と仮定しない** ✗（仮定すると起点より下の frame まで「番号内」に見え、重複として捨てられる ✗）。
- **起点未満の frame は ceiling の有無にかかわらず 1 つの分岐で扱う** ✓（`upToSeq === null` の判定より前 ✓、`write: true, durable: true` ✓）。分岐が 2 つあると、最初の frame が来ていない間だけ古い（重複と答える）扱いが残る ✗。
- **板を持たない古いスキーマは移行して退避する** ✓（退避先の名前は連番で確保し、**再移行でも消さない** ✓。新表の作成も同じトランザクション ✓）。: 旧 `organized_watermark`／`organize_gap` は**どの板のものか決められない**（接続名は 2 つの板で共有される ✗）ので `*_legacy` に改名して**どの板にも割り当てない** ✓。位置は raw が実際に持っているものから再確立する ✓（`rawWriter` の冪等契約が担保 ✓）。書き込みロック下で検査・改名する ✓。
- **起点未満の frame は「耐久済み」ではない** ✓: 重複判定は `firstSeq ≤ seq ≤ upToSeq` に限定する ✓。起点未満は **raw に書いたうえで恒久不能として記録**する ✓（「適用済み」と答えると、正本に無い frame が板にも無いまま消える ✗）。organizer と book の両方で同じ規則 ✓。復元時の起点は**伝えられた値**を使う ✓（1 と仮定すると起点より下まで番号内に見える ✗）。
- **organizer の保存は板（market + stream）で分離する** ✓。接続名は run・venue・market・世代で作られ **stream を含まない**（C2）ので、同じ market の book と trades は正当に同名を持つ ✓。位置を読むときは market/stream を照合し、**他の板の位置を自分の耐久として扱わない** ✗（扱うと raw が取っていない frame を受理する ✗）。`organized_watermark`／`organize_gap` の主キー・列に stream を追加した ✓。
- **rawWriter の契約（冪等）**: 既に持っている frame の再書き込みを拒否してはならない ✓。同じキー・内容なら `true` を返す ✓。**拒否は「耐久していない」を意味しなければならない** ✗（さもないと confirm 失敗後の再試行が恒久的に失敗し、raw にある frame が板に届かない ✗）。
- **復旧の順序は保存順（`rowid`）で取り、壁時計に依存しない** ✓。`recorded_at_ms` 順にすると時計が戻ったときに逆順になり、**拒否された起点を誤採用**して、下の frame を永久に適用不能にする ✗。
- **解放は raw の連続耐久上限が NULL のときは行わない** ✓（NULL は「何も保証していない」✓）。境界が動いたあとに解放する ✓。
- **再配送（`redeliverPending`）も `deliver()` を通す** ✓。板が再配送で起点を確定することがあり、その起点は**解放の前に** organizer へ届いていなければならない ✗（届かないと raw の上限が置き去りになり、次の再送が二重書き込みになる ✗）。
- **spool に退避するのは「そのキーが意味するフレーム」**（保存済み ✓）。到着した再送を退避すると、raw と spool が別内容になる ✗。
- **同じキーの entry がある受信は、entry が書かれた時のフレームが「そのフレーム」** ✓。raw 書き込み・confirm・板配送のすべてに保存済み内容を使い、再送の内容（raw・meta・identity）で置き換えない ✗（同じキーは同じフレーム ✓）。
- **解放は「板の適用上限」と「raw の連続耐久上限」の小さい方まで** ✓。raw が連続位置より上に持つ frame は**台帳が唯一の記録**なので、板が持っただけでは解放しない ✗（解放すると次の再送が二重書き込みになり、その拒否で受信が止まる ✗）。
- **起動時に「raw が持っている分」を organizer に教える**（`note(..., { rawAlreadyHolds })`）✓、**解放より前に** ✓。ここを飛ばすと organizer の連続上限が追いつかず、台帳の entry が永久に解放されない ✗。
- **organizer の追従先は板の現在の境界から取る** ✓（受信 frame の接続名からではない ✗）。板が拒否した frame が organizer の接続を書き換えると、板が持っている接続の frame が raw より手前で拒否される ✗。上の identity 検証と重なる防御だが、両者は独立に成立させる ✓。。raw が既に持っている frame にとって「書けたか」は問いではなく、拒否は耐久性について何も言わない ✓。保存済みの frame を板へ届け、spool／停止の経路には入れない ✓（入れないと、二重書き込みの拒否が「失った」と誤解され、穴を埋める frame まで受信停止に巻き込まれる ✗）。
- **復旧中（`resume` 内）に停止したらソケットを開かない** ✓（`start()` は `resume()` の後にも `stopped` を検査 ✓）。
- **停止した構造は `start()` で再開しない** ✓（受信を閉じた理由は frame を扱えなかったことで、ソケットを開け直しても同じ状態に戻るだけ ✗。再開は新しい run の新しいプロセス ✓）。
- **受信の停止はソケットも閉じる** ✓（停止した構造は以後の frame をすべて拒否するので、開いたままだと「届くが使われない」を作る ✗）。非ゼロ終了への変換は entry point の仕事で未実装 ✓。
- **呼び出し側のフックの中で接続が交代した場合、引き継いだ接続の位置は更新しない** ✓。rawWriter は呼び出し側のフックなので、書き込みの最中に板を別の接続へ渡せる（`accept` を呼べる）。書き込みの前後で接続を固定して比較し、交代していたら**そのフレームの耐久と債務の記録だけ**を行い、**新接続の上限・ACK は出さない** ✓（出すと、新接続の次のフレームが「耐久済み」と答えられ、raw に無いまま板だけが持つ ✗）。

**セット3 の残件（次セット以降に明示）**: 台帳の**保持期限・上限**（仕様 §9.1 の 5分 or 1GB）は未実装 → セット5 の上限と同時に扱う／接続交代で永久に届かない entry の**確定記録（`skipped` の永続化）**はセット4 のまま（現状はメモリの `skipped` とギャップ通知のみで、entry は台帳に残る）／**同じ run・venue・market で独立したソケットが同じ世代番号を発行しうる**（接続名が衝突する ✗）→ 世代の発行を共有するのは entry point／supervisor（未実装）の仕事 ✓／`received_tail` も接続名だけをキーにしている（受信側の結線時に対応 ✓・現状は未結線 ✓）。

## 5. 進め方の約束

- **Astra の APPROVE まで実装しない。** 計画を変えるときは本ファイルを更新して再度承認に掛ける。
- 1セット=1コミット。**赤いツリーはコミットしない**。レビューは `gpt-5.6-luna` / effort `xhigh`。
- レビューが出す指摘は、その回で全部潰すか、本ファイルに残件として明示してから次へ進む。

## 6. 実装時の注意点（Astra 実装前レビュー 2026-09-25）

1. **着手順（`state.mjs:89` から）**: migration → 復元 → **8列の保存文** → `persistAcceptance` と `commitRange` → `accept` → `apply`。
   `commitRange`（136行）を後回しにすると、適用時に owner 情報を消す。確認の区切りは「保存・復元一式」「認可・伝達・fixture 一式」「整理・再配送期待値一式」。
2. **SQLite**: `BEGIN IMMEDIATE` の後、`PRAGMA table_info(applied_boundary)` の `name` を集合化し、**欠けた列だけ** `ADD COLUMN`。
   両方ある・片方だけある場合も通す。**重複列を含む全例外の握り潰しは禁止**。新列を使う statement は migration 完了後に prepare。
   migration と通常の accept は**別トランザクション**（`BEGIN` を入れ子にしない）。`INSERT OR REPLACE` は削除＋再挿入なので、**全保存経路で8列を指定**（6列のままだと新列が NULL に戻る）。
   `DatabaseSync` は同期 API、SQL の NULL は JS の `null`、**トランザクション内に `await` を挟まない**。
3. **ロールバックは「COMMIT 前にメモリを変えない」構造にする**: `persistAcceptance(next, retiredRun)` の形で候補を渡し、
   **引退行 INSERT → owner・位置を含む境界行保存 → COMMIT → メモリ（`applied`・履歴・`phase`・`waiting`）更新**。
   先行する `supersededRuns.add` / `applied=` / `waiting.clear()` を残さない。BEGIN 失敗時に ROLLBACK しない。ロールバックの失敗で元例外を隠さない。
4. **`accept` の契約**: 戻り値は `{ accepted, reason }` を維持。同一 connection ID でも run／generation が違えば「same connection」で通さない。
   起点補完は**同一 owner・generation・connection かつ `firstSeq` 未確定**のときだけ `next = { ...applied, firstSeq }` を保存し、
   **`upToSeq`・`waiting`・穴・`phase` を変えず、drain も呼ばない**。確定済み起点への異なる値は拒否。
   封筒由来の起点（294行）を採用して適用するなら `next.firstSeq` にも保存する。**所有情報の検証は重複判定・穴記録より前**。
5. **structure／organizer**: `connection.mjs:246` → `structure.mjs:185` に `runId`／`venue` を伝達。
   板の accept 成功後に `organizer.accept(connectionId, { firstSeq: book.appliedBoundary.firstSeq })` → 再配送 の順。省略値で既存起点を上書きしない。
   `watermark.mjs:104` は同一接続なら watermark と `outOfOrder` を維持し、**未確定の baseline だけ補完**。131行の暗黙 accept を除去。
   起点 NULL は比較演算より前に分岐し、raw 成功なら **`durable:true, ack:null`** を返す。起点不明中の耐久済み seq も保持し、起点到着時に連続分だけ進める。
   **`redeliverPending` は板にしか再配送しないので、それだけで organizer の ACK が進むと考えない。**
6. **赤くなるはずのテスト（Astra の静的読解・推測）**: `structure-redeliver`（明示accept・起点NULLから）/ `structure-refusal`（未acceptは `accepted:false`・ACKなし）/
   `organize-redelivery`（初回 note 前に明示 accept）/ `structure-holds-losses`（`stillPending:1` → `0`）/ `book-gap-scope`（accept と封筒に同じ run/generation）。
   `book-run-authority` は維持。`runId=null` を**初回も拒否**する実装にすると `book-state` の8本と `organize-watermark` の共通準備も対象（推測）。
7. **コミットは1つ**: migration・認可・伝達・整理・対応テストをまとめる。セット3以降（台帳耐久化・再配送分類・境界証明）を混ぜない。
8. **手で確認する**: 使い捨てDBで旧6列→再migration、8列の値、板別の引退履歴、再オープン後の owner を SQL で照合。失敗注入3点は getter だけでなくDBも確認。

**未決（実装前に決める）**: `structure.mjs:189` は現在 `takeover` を渡していない。**再起動時に明示 takeover を発行するのは誰か**を決める（無条件 `takeover:true` は不可）。
受信封筒の `adapter.stream` と板の stream の一致も確認する。

## 7. ④の認可配線と ①の再試行契約（Astra 判定 2026-09-25 を反映）

**④ 認可を「実際の受信開始」へ渡す（Astra 指摘の要修正を反映）**
- 発行は `structure.accept` ✓。条件は `!admitted && options.runId === runId && ownerRun !== runId` ✓
  （**Astra 判定 2026-10-01**により `ownerRun != null` を外した: 保存済みの NULL 所有者は「未確立」ではなく
  「run 名を持たない記録済みの所有者」であり、そこから通常 run へ移るには C11 の明示 takeover が要る。
  この条件を外さないと、その板は受信を永久に拒否し続ける）。
  **呼び出し元の `options.takeover` では上書きさせない** ✓。`runId` は非空必須、`ownerRun` は板の現在値。
  未確立扱い（初回 accept・旧6列由来で run_id が NULL の行）は §2.3 のまま: takeover は要求しない。
- **旧復旧の完了が前提**: entry point が ①新 run 生成＋`beginRun()` ②旧 spool を流し切る ③旧境界を確定 してから `accept` を呼ぶ ✓。
- **実際の `onGeneration` で新 run を認可し、**`accept` が成功するまで**受信開始を阻止**する ✓。
  （現状は `structure.mjs:185` が拒否をログするだけで、`connection.mjs:246` から socket 開始へ進む ✗）
- **受理できない接続は黙って止まらない**: structure は拒否を `onStop` にも上げる ✓。
  それを停止・非ゼロ終了へ変換するのは supervisor／entry point の仕事であり、両者は未実装なので
  そこはセット3以降の完了条件として残る（受信が止まったまま誰も気づかない、を作らない）。
- `admitted` は**板ごとの「今回の新 run」**に限定する ✓。**旧 run の復旧 accept では消費しない** ✓
  （消費すると新 run への交代が拒否され得る）。
- `admitted` は**板の保存が成功した直後にだけ**立てる ✓（保存失敗では立てず、DB・メモリとも旧状態を維持 ✓）。
- 再起動では新 run を生成し、**保存済み owner から再判定**する ✓。

**① 保存後・途中失敗の再試行契約**
- 板の保存が成功した後に organizer／再配送で失敗しても、**takeover を再発行しない** ✓。
- 再試行は**同一 owner・generation・connection**で行い、後続（organizer の accept・ACK 通知・再配送）だけを完了させる ✓。

**③ の精度（Astra 明示）**: 通知するのは「**補完によって進み、保存できた連続耐久上限だけ**」✓。
耐久済み `{1,3}` に起点 `1` を補完したら **ACK は 1**、穴 `2` は残す ✓。上限 NULL・不変・保存失敗では通知しない ✓。
「1回」は**当該補完処理内**の意味で、クラッシュをまたぐ厳密な一度限りの保証ではない ✓。

**⑤ チェックの位置**: `structure.stream` と `adapter.stream` の不一致は **`openBook`・spool 生成より前**に throw ✓。

**セット2 で完成扱いにしないもの**: spool drain と起動時の `applied_boundary` 起点再送は**セット3・5 のまま** ✓。
