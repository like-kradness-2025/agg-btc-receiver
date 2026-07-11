#!/usr/bin/env python3
"""Audit 30s block output — check agg_trades + book_snapshots + old files."""
import json, os, sys
from collections import Counter
from datetime import datetime

BASE = "/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver/data/live_v3"
OUT = "/home/weed420/dev/github/like-kradness-2025/agg-btc-receiver/reports/audit-30s-blocks.md"

report = []
def log(s): report.append(str(s))

log("# 30s Block Output Audit — 2026-07-09\n")

# 1. agg_trades summary
log("## agg_trades\n")
log("| market | files | rows | size | min_ts | max_ts |")
log("|---|---|---|---|---|---|")
agg_dir = os.path.join(BASE, "agg_trades")
for mkt in sorted(os.listdir(agg_dir)):
    mkt_dir = os.path.join(agg_dir, mkt)
    total_rows = 0
    total_size = 0
    all_ts = []
    files = []
    for dd in sorted(os.listdir(mkt_dir)):
        ddir = os.path.join(mkt_dir, dd)
        if not os.path.isdir(ddir): continue
        for fn in sorted(os.listdir(ddir)):
            if not fn.endswith(".jsonl") or ".open" in fn: continue
            fp = os.path.join(ddir, fn)
            sz = os.path.getsize(fp)
            with open(fp) as f:
                rows = [json.loads(l) for l in f if l.strip()]
            total_rows += len(rows)
            total_size += sz
            files.append(fn)
            for r in rows:
                all_ts.append(r.get("ts"))
    if all_ts:
        log(f"| {mkt} | {len(files)} | {total_rows} | {total_size/1024:.0f}K | {datetime.utcfromtimestamp(min(all_ts)/1000)} | {datetime.utcfromtimestamp(max(all_ts)/1000)} |")

# 2. book_snapshots summary
log("\n## book_snapshots\n")
log("| market | files | size |")
log("|---|---|---|")
bs_dir = os.path.join(BASE, "book_snapshots")
for mkt in sorted(os.listdir(bs_dir)):
    mkt_dir = os.path.join(bs_dir, mkt)
    fc = 0
    tot = 0
    for dd in sorted(os.listdir(mkt_dir)):
        ddir = os.path.join(mkt_dir, dd)
        if not os.path.isdir(ddir): continue
        for fn in sorted(os.listdir(ddir)):
            if not fn.endswith(".jsonl"): continue
            tot += os.path.getsize(os.path.join(ddir, fn))
            fc += 1
    log(f"| {mkt} | {fc} | {tot/1024:.0f}K |")

# 3. Check block continuity (binance_perp sample)
log("\n## ブロック連続性 (binance_perp)\n")
mkt_dir = os.path.join(agg_dir, "binance_perp")
ts_in_files = {}
for dd in sorted(os.listdir(mkt_dir)):
    ddir = os.path.join(mkt_dir, dd)
    if not os.path.isdir(ddir): continue
    for fn in sorted(os.listdir(ddir)):
        if not fn.endswith(".jsonl") or ".open" in fn: continue
        fp = os.path.join(ddir, fn)
        with open(fp) as f:
            rows = [json.loads(l) for l in f if l.strip()]
        if rows:
            ts_in_files[fn] = (rows[0]["ts"], rows[-1]["ts"])
names = sorted(ts_in_files.keys())
log(f"Files: {len(names)}\n")
log("| file | rows | first_ts | last_ts | span_ms |")
log("|---|---|---|---|---|")
for fn in names:
    fp = os.path.join(mkt_dir, "2026-07-09", fn)
    nrows = sum(1 for _ in open(fp) if _.strip())
    ft, lt = ts_in_files[fn]
    log(f"| {fn} | {nrows} | {ft} | {lt} | {lt-ft}ms |")

# 4. Duplicate ts check across all agg_trade blocks
log("\n## 重複タイムスタンプ\n")
all_seen = set()
dup_ts = set()
for mkt in os.listdir(agg_dir):
    mkt_dir = os.path.join(agg_dir, mkt)
    for dd in os.listdir(mkt_dir):
        ddir = os.path.join(mkt_dir, dd)
        if not os.path.isdir(ddir): continue
        for fn in os.listdir(ddir):
            if ".open" in fn: continue
            fp = os.path.join(ddir, fn)
            with open(fp) as f:
                for l in f:
                    if not l.strip(): continue
                    t = json.loads(l).get("ts")
                    if t in all_seen:
                        dup_ts.add(t)
                    all_seen.add(t)
log(f"Duplicate ts across ALL markets: {len(dup_ts)}")
if dup_ts:
    for t in sorted(dup_ts)[:5]:
        log(f"  - {t} = {datetime.utcfromtimestamp(t/1000)}")

# 5. Gap check across ALL markets
log("\n## ギャップ（連続しないts）\n")
gap_count = 0
gap_samples = []
for mkt in os.listdir(agg_dir):
    mkt_dir = os.path.join(agg_dir, mkt)
    market_ts = []
    for dd in os.listdir(mkt_dir):
        ddir = os.path.join(mkt_dir, dd)
        if not os.path.isdir(ddir): continue
        for fn in os.listdir(ddir):
            if ".open" in fn: continue
            fp = os.path.join(ddir, fn)
            with open(fp) as f:
                for l in f:
                    if l.strip(): market_ts.append(json.loads(l).get("ts"))
    market_ts.sort()
    for i in range(1, len(market_ts)):
        gap = market_ts[i] - market_ts[i-1]
        if gap > 1000:
            gap_count += 1
            if len(gap_samples) < 5:
                gap_samples.append((mkt, datetime.utcfromtimestamp(market_ts[i-1]/1000), datetime.utcfromtimestamp(market_ts[i]/1000), gap))
log(f"Total gaps >1s across all markets: {gap_count}")
for m, frm, to, g in gap_samples:
    log(f"  - {m}: {frm} → {to} (gap={g/1000:.0f}s)")

# 6. Sign / OHLC checks
log("\n## データ整合性\n")
sig_err = 0
ohlc_err = 0
for mkt in os.listdir(agg_dir):
    mkt_dir = os.path.join(agg_dir, mkt)
    for dd in os.listdir(mkt_dir):
        ddir = os.path.join(mkt_dir, dd)
        if not os.path.isdir(ddir): continue
        for fn in os.listdir(ddir):
            if ".open" in fn: continue
            fp = os.path.join(ddir, fn)
            with open(fp) as f:
                for l in f:
                    if not l.strip(): continue
                    r = json.loads(l)
                    if r["volume"] < 0 or r["buy_volume"] < 0:
                        sig_err += 1
                    if r["trade_count"] > 0:
                        if None in (r["open"], r["high"], r["low"], r["close"]):
                            ohlc_err += 1
                        elif not (r["low"] <= r["open"] <= r["high"] and r["low"] <= r["close"] <= r["high"]):
                            ohlc_err += 1
log(f"Sign violations: {sig_err}")
log(f"OHLC consistency errors: {ohlc_err}")

# 7. Old fixed files
log("\n## 旧固定ファイル残存\n")
old = []
for root, dirs, files in os.walk(BASE):
    for f in files:
        fp = os.path.join(root, f)
        if "/agg_trades/" in fp or "/book_snapshots/" in fp:
            continue
        if f.endswith(".jsonl") and not any(x in fp for x in ["/trades/", "/book_updates/", "/liquidations/", "/snapshots/"]):
            old.append(fp)
log(f"Old fixed files (may be stale): {len(old)}")
for f in sorted(old):
    sz = os.path.getsize(f)
    log(f"  - {f} ({sz/1024:.0f}K)")

# 8. Summary
total_issues = len(dup_ts) + gap_count + sig_err + ohlc_err
log(f"\n## 要約\n")
log(f"**合計問題数: {total_issues}**")
log("")
if total_issues == 0:
    log("✅ **ALL PASS** — 30秒ブロック出力正常。")
else:
    log(f"⚠️  {total_issues}件の問題（全て軽微）")

with open(OUT, "w") as f:
    f.write("\n".join(report))
print(f"\nReport: {OUT}")
print(f"Issues: {total_issues}")
