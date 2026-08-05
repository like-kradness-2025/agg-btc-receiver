# Receiver運用

## systemd

```bash
systemctl --user status agg-btc-receiver.service
systemctl --user restart agg-btc-receiver.service
journalctl --user -u agg-btc-receiver.service -f
```

本番serviceは`--storage sqlite --database-dir data/sqlite --retention-days 90`で起動します。

## 後工程

次のtimerは現行運用では無効です。

```text
agg-btc-receiver-archive.timer
agg-btc-receiver-tfp.timer
agg-btc-receiver-book-snapshots.timer
agg-btc-receiver-cleanup-raw.timer
```

Receiverの再起動でTFPやBook Snapshotを起動させないため、Receiver serviceから後工程timerをWantedにしません。

## 条件付き自動復旧（市場モジュール単位）

`agg-btc-receiver-watchdog.timer` は1分ごとにread-only監視します。異常が3回連続した場合、systemdサービス全体ではなく、対象市場を担当するworkerへIPC経由で再接続・再同期を要求します。兄弟市場のsocket・writer・PIDは維持します。

- `health.jsonl` stale、depth stale、market state error/reconnecting、writer I/O failure
- cooldown 10分、1時間あたり最大3回
- worker module restartに失敗しても、worker内の通常reconnectへ移行
- process死は従来どおりsystemd `Restart=on-failure` が担当

raw bookの価格距離は復旧条件にしません。深い板やsequence検証前のcandidate
更新を破損と区別できず、正常な市場workerを再同期させるためです。

制御経路は同一ユーザーのruntime request file + `SIGUSR2` → main → owning worker → connector disconnect/connect/_syncBookです。サービス全体再起動は自動復旧経路ではありません。

安全弁として、復旧間隔は10分、最大3回/1時間。上限到達後は自動復旧せず、journalへ原因を残します。監視状態は`$XDG_RUNTIME_DIR/agg-btc-receiver-watchdog.json`に保存します。

インストール・有効化（既存Receiverは再起動しない）:

```bash
mkdir -p ~/.config/systemd/user
cp systemd/agg-btc-receiver-watchdog.{service,timer} ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now agg-btc-receiver-watchdog.timer
```

dry-run確認:

```bash
python3 scripts/receiver-auto-restart-watchdog.py --dry-run
```

## 容量確認

```bash
du -h data/sqlite/*.sqlite data/sqlite/*.sqlite-wal 2>/dev/null
df -h /home/weed420/Tool/agg-btc-receiver
```

容量はSQLite本体とWALを合算して確認します。今回の移行実績はSQLite約3.8GB、移行元DuckDB約4.5GBでした。90日分の将来容量は実流量とgzip圧縮率で変わるため、定期的に実測します。

## 切替前データの扱い

旧`data/live_v4`と`data/derived`は証拠保全のため自動削除しません。削除やDB移行を行う場合は、対象期間と検証方法を先に決めます。
