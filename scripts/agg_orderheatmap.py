#!/usr/bin/env python3
"""Order depth heatmap from agg-btc-receiver parquet data.

Draws a time × bps distance heatmap of bid/ask volume using the ring depth
columns from agg parquet (bid_0_1bps, ask_0_1bps, etc.).

Usage:
  python3 scripts/agg_orderheatmap.py --market binance_spot --hours 8 --out agg_heatmap.png
  python3 scripts/agg_orderheatmap.py --market binance_perp --agg --hours 4
  python3 scripts/agg_orderheatmap.py --markets  # all markets
"""

import json, os, sys, subprocess, argparse
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
AGG_DIR = os.path.join(BASE_DIR, 'data', 'agg')

# ── Style ──
plt.rcParams.update({
    'font.size': 8,
    'axes.facecolor': '#0b1628',
    'figure.facecolor': '#0b1628',
    'text.color': '#c8d6e5',
    'axes.edgecolor': '#1e3a5f',
    'axes.labelcolor': '#c8d6e5',
    'axes.grid': False,
    'legend.facecolor': '#0b1628',
    'legend.edgecolor': '#1e3a5f',
    'legend.labelcolor': '#c8d6e5',
})

BPS_RINGS = [
    (0, 1, '0-1bps'),
    (1, 2, '1-2bps'),
    (2, 5, '2-5bps'),
    (5, 25, '5-25bps'),
    (25, 100, '25-100bps'),
]
# Center y position for each ring (0 = mid)
RING_CENTERS = [0.5, 1.5, 3.5, 15, 62.5]  # midpoint of each ring
RING_HALF_HEIGHTS = [0.5, 0.5, 1.5, 10, 37.5]  # half the height of each ring


def load_ring_data(market, hours=8):
    """Fetch ring depth + mid price from agg parquet via DuckDB (Node.js)."""
    cutoff_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000) - hours * 3600 * 1000
    tmp_path = os.path.join(BASE_DIR, 'tmp_heatmap_export.json')

    # Build column list for bid/ask ring depths
    ring_cols = []
    for lo, hi, name in BPS_RINGS:
        ring_cols.append(f"CAST(bid_{name.replace('-','_')} AS DOUBLE) as bid_{name.replace('-','_')}")
        ring_cols.append(f"CAST(ask_{name.replace('-','_')} AS DOUBLE) as ask_{name.replace('-','_')}")

    col_list = ',\n      '.join(ring_cols)

    script = f"""
    const fs = require('fs');
    const duckdb = require('duckdb');
    const db = new duckdb.Database(':memory:');
    db.all(`SELECT
      CAST(ts AS DOUBLE) as ts,
      CAST(mid_close AS DOUBLE) as mid_close,
      CAST(microprice_close AS DOUBLE) as microprice_close,
      CAST(best_bid_close AS DOUBLE) as best_bid_close,
      CAST(best_ask_close AS DOUBLE) as best_ask_close,
      CAST(best_bid_size_close_qty AS DOUBLE) as best_bid_sz,
      CAST(best_ask_size_close_qty AS DOUBLE) as best_ask_sz,
      CAST(spread_bps_close AS DOUBLE) as spread_bps,
      {col_list}
      FROM read_parquet('{AGG_DIR}/{market}.parquet')
      WHERE ts >= {cutoff_ms}
      ORDER BY ts`, (err, rows) => {{
      if (err) {{ fs.writeFileSync('{tmp_path}', JSON.stringify({{error: err.message}})); process.exit(1); }}
      const out = rows.map(r => ({{
        ts: Number(r.ts), mid: Number(r.mid_close) || 0,
        micro: Number(r.microprice_close) || 0,
        bb: Number(r.best_bid_close) || 0, ba: Number(r.best_ask_close) || 0,
        bbs: Number(r.best_bid_sz) || 0, bas: Number(r.best_ask_sz) || 0,
        spd: Number(r.spread_bps) || 0,
        bid_0: Number(r.bid_0_1bps) || 0, ask_0: Number(r.ask_0_1bps) || 0,
        bid_1: Number(r.bid_1_2bps) || 0, ask_1: Number(r.ask_1_2bps) || 0,
        bid_2: Number(r.bid_2_5bps) || 0, ask_2: Number(r.ask_2_5bps) || 0,
        bid_5: Number(r.bid_5_25bps) || 0, ask_5: Number(r.ask_5_25bps) || 0,
        bid_25: Number(r.bid_25_100bps) || 0, ask_25: Number(r.ask_25_100bps) || 0,
      }}));
      fs.writeFileSync('{tmp_path}', JSON.stringify(out));
      process.exit(0);
    }});
    """
    result = subprocess.run(
        ['node', '-e', script],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        cwd=BASE_DIR, timeout=60
    )
    if result.returncode != 0:
        raise RuntimeError(f"DuckDB export failed (exit {result.returncode})")
    with open(tmp_path) as f:
        raw = json.load(f)
    os.unlink(tmp_path)
    if isinstance(raw, dict) and 'error' in raw:
        raise RuntimeError(f"DuckDB error: {raw['error']}")
    if not raw:
        raise RuntimeError(f"No data for {market} in last {hours}h")
    # Parse into structured arrays
    df = pd.DataFrame(raw)
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    return df


def make_heatmap(df, ax, ring_col, label, cmap='RdYlGn_r', vmax=None):
    """Draw bid/ask heatmap for one side.

    Y axis = bps distance from mid (bid side inverted to show below mid).
    """
    times = df['ts'].values
    n = len(times)

    # Build arrays for each ring
    bid_data = np.zeros((len(BPS_RINGS), n))
    ask_data = np.zeros((len(BPS_RINGS), n))

    ring_keys = ['0', '1', '2', '5', '25']  # aliases from Node.js mapping

    for i, (lo, hi, name) in enumerate(BPS_RINGS):
        k = ring_keys[i]
        bid_data[i] = df[f'bid_{k}'].values
        ask_data[i] = df[f'ask_{k}'].values

    if ring_col.startswith('bid'):
        data = bid_data
        side_label = 'Bid Volume'
        sign = -1  # bid below mid
    else:
        data = ask_data
        side_label = 'Ask Volume'
        sign = 1   # ask above mid

    if vmax is None:
        vmax = np.percentile(data[data > 0], 95) if data.max() > 0 else 1
        vmax = max(vmax, 1)

    # For each ring, plot as a horizontal band with color = avg volume
    # Y positions: negative for bid (below), positive for ask (above)
    y_positions = [sign * c for c in RING_CENTERS]
    half_heights = RING_HALF_HEIGHTS

    # Use pcolormesh for a proper heatmap
    # Build 2D grid: x = time, y = bps distance
    y_edges_bid = [-100, -25, -5, -2, -1, 0]
    y_edges_ask = [0, 1, 2, 5, 25, 100]
    y_edges = y_edges_bid + y_edges_ask[1:]  # skip 0 dup

    # Create full grid
    X = np.tile(np.arange(n), (len(y_edges), 1))
    Y = np.tile(np.array(y_edges)[:, None], (1, n))

    # Build Z data
    Z = np.zeros((len(y_edges) - 1, n))
    for i in range(len(BPS_RINGS)):
        lo_dn = -(BPS_RINGS[-(i+1)][1])  # reverse for bid side
        Z[len(BPS_RINGS) - 1 - i] = bid_data[i]  # bid rings (bottom)

    for i in range(len(BPS_RINGS)):
        Z[len(BPS_RINGS) + i] = ask_data[i]  # ask rings (top)

    # Actually let me simplify with pcolormesh on binned data
    # Create a 2D grid where rows are bps distance bins
    bps_bins = [-100, -25, -5, -2, -1, 0, 1, 2, 5, 25, 100]
    n_bins = len(bps_bins) - 1  # 10 bins

    Z_simple = np.zeros((n_bins, n))
    # Map: bid_25 (idx 0) -> bin 0 (-100 to -25)
    # bid_5 (idx 1) -> bin 1 (-25 to -5)
    # bid_2 (idx 2) -> bin 2 (-5 to -2)
    # bid_1 (idx 3) -> bin 3 (-2 to -1)
    # bid_0 (idx 4) -> bin 4 (-1 to 0)
    # ask_0 (idx 5) -> bin 5 (0 to 1)
    # ask_1 (idx 6) -> bin 6 (1 to 2)
    # ask_2 (idx 7) -> bin 7 (2 to 5)
    # ask_5 (idx 8) -> bin 8 (5 to 25)
    # ask_25 (idx 9) -> bin 9 (25 to 100)
    Z_simple[0] = bid_data[4]   # bid 25-100bps
    Z_simple[1] = bid_data[3]   # bid 5-25bps
    Z_simple[2] = bid_data[2]   # bid 2-5bps
    Z_simple[3] = bid_data[1]   # bid 1-2bps
    Z_simple[4] = bid_data[0]   # bid 0-1bps
    Z_simple[5] = ask_data[0]   # ask 0-1bps
    Z_simple[6] = ask_data[1]   # ask 1-2bps
    Z_simple[7] = ask_data[2]   # ask 2-5bps
    Z_simple[8] = ask_data[3]   # ask 5-25bps
    Z_simple[9] = ask_data[4]   # ask 25-100bps

    # Clip for visual clarity
    vclip = np.percentile(Z_simple[Z_simple > 0], 97) if Z_simple.max() > 0 else 1
    Z_clipped = np.clip(Z_simple, 0, vclip)

    # Use a diverging colormap: white for zero, dark red for high bid, dark green for high ask
    colors_bid = plt.cm.RdYlGn_r(np.linspace(0, 1, 128))  # red=high bid
    colors_ask = plt.cm.RdYlGn(np.linspace(0, 1, 128))     # green=high ask
    # White band in the middle
    white_bar = np.ones((16, 4))
    all_colors = np.vstack([colors_bid, white_bar, colors_ask])
    custom_cmap = mcolors.LinearSegmentedColormap.from_list('depth', all_colors, N=256)

    # Create a 2D array with bid negative, ask positive
    Z_display = np.zeros_like(Z_simple)
    Z_display[:5] = -Z_clipped[:5]  # bid: negative
    Z_display[5:] = Z_clipped[5:]   # ask: positive

    # Extent: [xmin, xmax, ymin, ymax]
    extent = [0, n, -100, 100]

    im = ax.imshow(Z_display, aspect='auto', cmap=custom_cmap,
                   extent=extent, interpolation='nearest',
                   vmin=-vclip, vmax=vclip)

    # Y axis labels
    yticks = [-62.5, -15, -3.5, -1.5, -0.5, 0.5, 1.5, 3.5, 15, 62.5]
    yticklabels = ['25-100', '5-25', '2-5', '1-2', '0-1', '0-1', '1-2', '2-5', '5-25', '25-100']
    ax.set_yticks(yticks)
    ax.set_yticklabels(yticklabels, fontsize=6)
    ax.axhline(y=0, color='#555', linewidth=0.8)
    ax.set_ylabel('BPS from Mid', color='#8b949e', fontsize=9)

    # X axis: show time labels
    if n > 1:
        step = max(1, n // 12)
        tick_idx = np.arange(0, n, step)
        tick_times = times[tick_idx]
        ax.set_xticks(tick_idx)
        ax.set_xticklabels([pd.Timestamp(t).strftime('%H:%M') for t in tick_times],
                          fontsize=6, rotation=0)

    return im, vclip


def chart_single(df, market, out_path, hours):
    """Generate single-market order heatmap."""
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(16, 7),
        gridspec_kw={'height_ratios': [1, 4]},
        sharex=True
    )
    fig.patch.set_facecolor('#0b1628')

    # ── Top panel: Price ──
    ax1.plot(range(len(df)), df['mid'].values, color='#60a5fa', linewidth=1.2, alpha=0.9)
    # Best bid/ask
    ax1.fill_between(range(len(df)), df['bb'].values, df['ba'].values, alpha=0.15, color='#60a5fa')
    ax1.set_ylabel('Price', color='#60a5fa', fontsize=9)
    ax1.tick_params(axis='y', colors='#60a5fa', labelsize=7)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'${y:,.0f}'))
    ax1.set_title(f'{market} — Order Depth Heatmap ({hours}h)',
                  color='#c8d6e5', fontsize=11, fontweight='bold', pad=8)
    ax1.set_facecolor('#0b1628')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['bottom'].set_color('#1e3a5f')
    ax1.spines['left'].set_color('#1e3a5f')
    ax1.tick_params(labelbottom=False)
    ax1.grid(True, alpha=0.15, color='#8b949e', linewidth=0.3)

    # ── Bottom panel: Heatmap ──
    ax2.set_facecolor('#0b1628')
    im, vmax = make_heatmap(df, ax2, 'bid', 'Bid Volume')
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.spines['bottom'].set_color('#1e3a5f')
    ax2.spines['left'].set_color('#1e3a5f')

    # Colorbar
    cbar = fig.colorbar(im, ax=ax2, orientation='vertical', pad=0.01, shrink=0.8)
    cbar.set_label('Volume (BTC)', color='#8b949e', fontsize=8)
    cbar.ax.tick_params(colors='#8b949e', labelsize=6)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, facecolor='#0b1628')
    plt.close(fig)
    return out_path


def main():
    p = argparse.ArgumentParser(description='Agg Order Depth Heatmap')
    p.add_argument('--market', type=str, default='binance_spot', help='Market name')
    p.add_argument('--markets', action='store_true', help='Render all available markets')
    p.add_argument('--hours', type=int, default=8, help='Lookback hours')
    p.add_argument('--out', type=str, default=None, help='Output path')
    args = p.parse_args()

    if args.markets:
        # Auto-detect markets from agg parquets
        import glob
        markets = sorted([
            os.path.splitext(os.path.basename(f))[0]
            for f in glob.glob(os.path.join(AGG_DIR, '*.parquet'))
        ])
        out_dir = args.out or os.path.join(BASE_DIR, 'heatmaps')
        os.makedirs(out_dir, exist_ok=True)
    elif args.market:
        markets = [args.market]
        out_dir = os.path.dirname(args.out) if args.out else os.path.join(BASE_DIR, 'heatmaps')
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
    else:
        markets = ['binance_spot']
        out_dir = os.path.join(BASE_DIR, 'heatmaps')
        os.makedirs(out_dir, exist_ok=True)

    for mkt in markets:
        fp = os.path.join(AGG_DIR, f'{mkt}.parquet')
        if not os.path.exists(fp):
            print(f"  skip {mkt} — no parquet")
            continue
        try:
            df = load_ring_data(mkt, args.hours)
            print(f"  {mkt}: {len(df)} rows, "
                  f"price={df['mid'].min():.0f}-{df['mid'].max():.0f}")
        except (RuntimeError, subprocess.TimeoutExpired) as e:
            print(f"  {mkt}: {e}")
            continue

        # Determine output path
        if args.markets:
            out_path = os.path.join(out_dir, f'heatmap_{mkt}.png')
        elif args.market and args.out:
            out_path = args.out  # single market with explicit path
        else:
            out_path = os.path.join(out_dir, f'heatmap_{mkt}.png')

        chart_single(df, mkt, out_path, args.hours)
        sz = os.path.getsize(out_path) / 1024
        print(f"    → {out_path} ({sz:.0f} KB)")

    print(f"Done. {len(markets)} market(s).")


if __name__ == '__main__':
    main()
