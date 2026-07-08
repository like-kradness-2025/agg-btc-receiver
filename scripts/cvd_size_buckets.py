#!/usr/bin/env python3
"""CVD Size Bucket Charts — updated for 96-col agg schema.

Usage:
  # Aggregate (3-panel: Price / Spot CVD / Perp CVD)
  python3 scripts/cvd_size_buckets.py --agg --hours 6

  # Single market
  python3 scripts/cvd_size_buckets.py --market binance_spot --hours 6

  # Custom output
  python3 scripts/cvd_size_buckets.py --agg --hours 12 --out /tmp/cvd.png
"""

import json, os, sys, subprocess, argparse, io
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from matplotlib.gridspec import GridSpec
from datetime import datetime, timezone

# ── Paths ──
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AGG_DIR = os.path.join(BASE_DIR, 'data', 'agg')
OUTPUT_DIR = BASE_DIR

# ── Style ──
plt.rcParams.update({
    'font.size': 18,
    'axes.facecolor': '#0b1628',
    'figure.facecolor': '#0b1628',
    'text.color': '#c8d6e5',
    'axes.edgecolor': '#1e3a5f',
    'axes.labelcolor': '#c8d6e5',
    'axes.grid': True,
    'grid.color': '#1e3a5f',
    'grid.alpha': 0.3,
    'legend.facecolor': '#0b1628',
    'legend.edgecolor': '#1e3a5f',
    'legend.labelcolor': '#c8d6e5',
})

# ── Font sizes ──
FS_TITLE = 26
FS_SUB   = 20
FS_YLAB  = 16
FS_TICK  = 14
FS_LEG   = 16
C_SMALL  = '#4ade80'
C_MEDIUM = '#fbbf24'
C_LARGE  = '#f43f5e'
C_PRICE  = '#60a5fa'
TEXT_COLOR = '#c8d6e5'

SIZE_LABELS = ['Small (<$1k)', 'Medium ($1k-$10k)', 'Large (>=$10k)']
SIZE_COLORS = [C_SMALL, C_MEDIUM, C_LARGE]


def load_data(hours=24):
    """Load CVD and price data from 1s features JSONL (receiver primary output).

    Reads directly from data/1s_features/{date}/{market}.jsonl, filtered by
    cutoff hour. No agg/ parquet dependency.
    """
    cutoff_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000) - hours * 3600 * 1000
    feat_dir = os.path.join(BASE_DIR, 'data', '1s_features')

    if not os.path.isdir(feat_dir):
        raise RuntimeError(f"1s_features directory not found: {feat_dir}")

    import pandas as pd

    partitions = sorted(d for d in os.listdir(feat_dir) if os.path.isdir(os.path.join(feat_dir, d)))
    # Filter partitions by cutoff date ── supports any --hours value
    cutoff_date = datetime.fromtimestamp(cutoff_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
    partitions = [d for d in partitions if d >= cutoff_date]
    if not partitions:
        raise RuntimeError(f"No partitions >= {cutoff_date} in {feat_dir}")

    chunks = []
    cols = ['ts', 'market', 'buy_small_qty', 'buy_medium_qty', 'buy_large_qty',
            'sell_small_qty', 'sell_medium_qty', 'sell_large_qty', 'mid_close']
    # Cache dtype for read_json
    dtypes = {c: 'float64' for c in cols if c != 'market'}
    dtypes['market'] = 'str'

    for date_str in partitions:
        date_dir = os.path.join(feat_dir, date_str)
        for fname in sorted(os.listdir(date_dir)):
            if not fname.endswith('.jsonl') or 'adversarial' in fname:
                continue
            fpath = os.path.join(date_dir, fname)
            try:
                size = os.path.getsize(fpath)
                if size == 0:
                    continue

                # Sample first line to get min_ts
                with open(fpath, 'rb') as f:
                    first_line = f.readline()
                try:
                    first_ts = json.loads(first_line).get('ts', 0)
                except Exception:
                    first_ts = 0

                # Sample last few bytes to get max_ts
                with open(fpath, 'rb') as f:
                    seek_end = max(size - 4096, 0)
                    f.seek(seek_end)
                    if seek_end > 0:
                        f.readline()  # skip partial
                    last_ts = 0
                    for line in f:
                        try:
                            last_ts = json.loads(line).get('ts', 0)
                        except Exception:
                            continue

                # Bail early if file is entirely before cutoff
                if last_ts < cutoff_ms:
                    continue
                # If entirely within cutoff, read the whole file
                if first_ts >= cutoff_ms:
                    read_from = 0
                else:
                    # Estimate byte offset for cutoff using linear interpolation
                    # ts increases roughly linearly with byte position
                    elapsed = last_ts - first_ts
                    if elapsed > 0:
                        target_frac = (cutoff_ms - first_ts) / elapsed
                        # Add 2% slack to avoid falling short
                        target_frac = max(0, min(1, target_frac - 0.02))
                        read_from = int(size * target_frac)
                    else:
                        read_from = 0

                with open(fpath, 'rb') as f:
                    f.seek(read_from)
                    if read_from > 0:
                        f.readline()  # skip partial line
                    raw = f.read()

                df = pd.read_json(io.StringIO(raw.decode('utf-8', errors='ignore')),
                                  lines=True, dtype=dtypes)
                have_cols = [c for c in cols if c in df.columns]
                df = df[have_cols]
                if 'ts' not in df.columns or len(df) == 0:
                    continue
                df = df[df['ts'] >= cutoff_ms]
                if len(df) == 0:
                    continue
                chunks.append(df)
            except Exception as e:
                print(f'[cvd] warning: {fname}: {e}', file=sys.stderr)
                continue

    if not chunks:
        raise RuntimeError(f"No 1s_features data in {feat_dir} for last {hours}h")

    df = pd.concat(chunks, ignore_index=True)
    df['market'] = df['market'].str.strip()

    for c in ['buy_small_qty','buy_medium_qty','buy_large_qty',
              'sell_small_qty','sell_medium_qty','sell_large_qty']:
        df[c] = df[c].fillna(0)

    # Rename for convenience
    df = df.rename(columns={
        'buy_small_qty': 'b_s', 'buy_medium_qty': 'b_m', 'buy_large_qty': 'b_l',
        'sell_small_qty': 's_s', 'sell_medium_qty': 's_m', 'sell_large_qty': 's_l',
    })

    # CVD by size
    df['cvd_s'] = df['b_s'] - df['s_s']
    df['cvd_m'] = df['b_m'] - df['s_m']
    df['cvd_l'] = df['b_l'] - df['s_l']

    def mkt_type(m):
        m = m.lower()
        return 'perp' if '_perp' in m or '_coinm' in m else 'spot'
    df['type'] = df['market'].apply(mkt_type)
    # Convert ts to datetime for resample/plotting
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    df = df.sort_values(['ts', 'market']).reset_index(drop=True)

    # Price reference
    tmp = df[df['market'] == 'binance_perp'][['ts','mid_close']].drop_duplicates('ts').sort_values('ts')
    if tmp.empty:
        tmp = df[df['type'] == 'perp'].groupby('ts')['mid_close'].mean().reset_index()
    price_df = tmp.set_index('ts').sort_index().resample('1s').ffill().dropna().reset_index()
    price_df.columns = ['ts', 'mid_price']

    print(f"  source: {feat_dir}/{{date}}/*.jsonl")
    return df, price_df


def plot_price(ax, price_df):
    """Draw BTC price line on ax."""
    ax.plot(price_df['ts'], price_df['mid_price'], color=C_PRICE, linewidth=1.2, alpha=0.9)
    ax.set_ylabel('BTC Price', color=TEXT_COLOR, fontsize=FS_YLAB)
    ax.tick_params(axis='y', colors=C_PRICE, labelsize=FS_TICK)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'${y:,.0f}'))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.tick_params(colors=TEXT_COLOR, labelsize=FS_TICK)


def plot_cvd_twinx(ax, ts, s, m, l):
    """Plot 3 size CVD lines on a shared frame with independent Y axes.
    Small = left axis, Medium = right inner, Large = right outer.
    """
    ax.plot(ts, s, color=C_SMALL, linewidth=1.0, alpha=0.8, label='Small')
    ax.set_ylabel('Small CVD (BTC)', color=C_SMALL, fontsize=FS_YLAB)
    ax.tick_params(axis='y', colors=C_SMALL, labelsize=FS_TICK)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{y:.3f}'))
    ax.axhline(0, color=TEXT_COLOR, linewidth=0.4, alpha=0.3)

    a2 = ax.twinx()
    a2.plot(ts, m, color=C_MEDIUM, linewidth=1.0, alpha=0.8, label='Medium')
    a2.spines['right'].set_position(('outward', 0))
    a2.set_ylabel('Medium CVD (BTC)', color=C_MEDIUM, fontsize=FS_YLAB)
    a2.tick_params(axis='y', colors=C_MEDIUM, labelsize=FS_TICK)
    a2.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{y:.3f}'))

    a3 = ax.twinx()
    a3.plot(ts, l, color=C_LARGE, linewidth=1.0, alpha=0.8, label='Large')
    a3.spines['right'].set_position(('outward', 60))
    a3.set_ylabel('Large CVD (BTC)', color=C_LARGE, fontsize=FS_YLAB)
    a3.tick_params(axis='y', colors=C_LARGE, labelsize=FS_TICK)
    a3.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f'{y:.3f}'))


def add_legend(ax):
    """Add unified size legend on ax."""
    from matplotlib.lines import Line2D
    handles = [Line2D([0], [0], color=c, lw=1.5) for c in [C_SMALL, C_MEDIUM, C_LARGE]]
    leg = ax.legend(handles, SIZE_LABELS, loc='upper left', fontsize=FS_LEG, framealpha=0.8)
    for t, c in zip(leg.get_texts(), SIZE_COLORS):
        t.set_color(c)


def chart_aggregate(df, price_df, out_path):
    """Generate 3-panel aggregate chart: Price / Spot CVD / Perp CVD."""
    spot = df[df['type'] == 'spot'].groupby('ts')[['cvd_s','cvd_m','cvd_l']].sum().cumsum()
    perp = df[df['type'] == 'perp'].groupby('ts')[['cvd_s','cvd_m','cvd_l']].sum().cumsum()

    fig = plt.figure(figsize=(26, 17))
    gs = GridSpec(3, 1, height_ratios=[1.2, 2, 2], hspace=0.10,
                  left=0.06, right=0.88, bottom=0.06, top=0.96)

    ax_p = fig.add_subplot(gs[0])
    plot_price(ax_p, price_df)
    ax_p.set_title('Aggregate CVD by Size (Spot / Perp)', color=TEXT_COLOR,
                   fontsize=FS_TITLE, fontweight='bold', loc='left')
    ax_p.tick_params(labelbottom=False)

    ax_s = fig.add_subplot(gs[1], sharex=ax_p)
    plot_cvd_twinx(ax_s, spot.index, spot['cvd_s'], spot['cvd_m'], spot['cvd_l'])
    ax_s.set_title('Spot CVD — Cumulative by Size (BTC)', color=TEXT_COLOR,
                   fontsize=FS_SUB, fontweight='bold', loc='left')
    ax_s.tick_params(labelbottom=False)
    add_legend(ax_s)

    ax_pp = fig.add_subplot(gs[2], sharex=ax_p)
    plot_cvd_twinx(ax_pp, perp.index, perp['cvd_s'], perp['cvd_m'], perp['cvd_l'])
    ax_pp.set_title('Perp CVD — Cumulative by Size (BTC)', color=TEXT_COLOR,
                    fontsize=FS_SUB, fontweight='bold', loc='left')
    ax_pp.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax_pp.tick_params(axis='x', labelsize=FS_TICK)

    fig.savefig(out_path, dpi=45)
    plt.close(fig)
    return out_path


def chart_per_market(df, price_df, out_dir, market_filter=None):
    """Generate per-market 2-panel charts: Price / Size CVD."""
    markets = sorted(df['market'].unique())
    if market_filter:
        markets = [m for m in markets if market_filter in m]

    saved = []
    for mkt in markets:
        mdf = df[df['market'] == mkt].sort_values('ts').set_index('ts')
        cum = mdf[['cvd_s','cvd_m','cvd_l']].cumsum()

        fig = plt.figure(figsize=(22, 10))
        gs = GridSpec(2, 1, height_ratios=[1, 2], hspace=0.08,
                      left=0.06, right=0.88, bottom=0.08, top=0.96)

        ax1 = fig.add_subplot(gs[0])
        plot_price(ax1, price_df)
        ax1.set_title(f'{mkt} — Size CVD', color=TEXT_COLOR,
                      fontsize=FS_TITLE, fontweight='bold', loc='left')
        ax1.tick_params(labelbottom=False)

        ax2 = fig.add_subplot(gs[1], sharex=ax1)
        plot_cvd_twinx(ax2, cum.index, cum['cvd_s'], cum['cvd_m'], cum['cvd_l'])
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax2.tick_params(axis='x', labelsize=FS_TICK)

        path = os.path.join(out_dir, f'cvd_{mkt}.png')
        fig.savefig(path, dpi=45)
        plt.close(fig)
        saved.append(path)
    return saved


def main():
    p = argparse.ArgumentParser(description='CVD Size Bucket Charts (96-col agg)')\

    p.add_argument('--agg', action='store_true', help='Aggregate 3-panel chart')
    p.add_argument('--markets', action='store_true', help='Per-market charts')
    p.add_argument('--market', type=str, default=None, help='Single market filter')
    p.add_argument('--hours', type=int, default=6, help='Lookback hours (default 6)')
    p.add_argument('--out', type=str, default=None, help='Output path (for --agg) or dir (for --markets)')
    args = p.parse_args()

    if not args.agg and not args.markets and not args.market:
        p.print_help()
        sys.exit(1)

    df, price_df = load_data(args.hours)
    print(f"Loaded {len(df)} rows, {len(df['market'].unique())} markets, "
          f"price range: {price_df['ts'].min()} → {price_df['ts'].max()}")

    if args.agg:
        out = args.out or os.path.join(BASE_DIR, 'agg_cvd_size.png')
        path = chart_aggregate(df, price_df, out)
        sz = os.path.getsize(path) / 1024
        print(f"Aggregate chart: {path} ({sz:.0f} KB)")

    if args.markets or args.market:
        out_dir = args.out or os.path.join(BASE_DIR, 'cvd_charts')
        os.makedirs(out_dir, exist_ok=True)
        saved = chart_per_market(df, price_df, out_dir, args.market)
        for p in saved:
            sz = os.path.getsize(p) / 1024
            print(f"  {p} ({sz:.0f} KB)")
        print(f"Saved {len(saved)} per-market charts to {out_dir}/")


if __name__ == '__main__':
    main()
