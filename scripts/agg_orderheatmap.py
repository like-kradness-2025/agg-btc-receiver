#!/usr/bin/env python3
"""Order depth heatmap from agg-btc-receiver parquet data.

Draws a time × bps distance heatmap of bid/ask volume using the ring depth
columns from agg parquet (bid_0_1bps, ask_0_1bps, etc.).

Usage:
  python3 scripts/agg_orderheatmap.py --market binance_spot --hours 8 --out agg_heatmap.png
  python3 scripts/agg_orderheatmap.py --market binance_perp --agg --hours 4
  python3 scripts/agg_orderheatmap.py --markets  # all markets
"""

import json, os, sys, subprocess, argparse, glob
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
import matplotlib.ticker as mticker
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

JST = ZoneInfo('Asia/Tokyo')

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


TFP_DERIVED_ROOT = os.environ.get(
    'AGG_TFP_DERIVED_ROOT',
    os.path.join(BASE_DIR, 'data', 'derived', 'burst_features_v2'),
)
SNAPSHOT_HEATMAP_DIR = os.environ.get(
    'AGG_ORDERHEATMAP_ROOT',
    os.path.join(BASE_DIR, 'data', 'derived', 'burst_features_v2', 'orderheatmap_1s'),
)
FEATURES_1S_DIR = os.path.join(TFP_DERIVED_ROOT, 'features_1s')


def load_snapshot_heatmap(market, hours=8, from_ms=None, to_ms=None):
    """Load the strict absolute-price Book Snapshot consumer output.

    This is the production source for the chart. The old data/agg parquet ring
    loader remains above only as a diagnostic reference and is not used here.
    """
    cutoff_ms = (
        int(from_ms)
        if from_ms is not None
        else int(datetime.now(tz=timezone.utc).timestamp() * 1000) - hours * 3600 * 1000
    )
    root = os.path.join(SNAPSHOT_HEATMAP_DIR, f'market={market}')
    paths = sorted(glob.glob(os.path.join(root, '*', '*.jsonl')))
    rows = []
    for path in paths:
        try:
            with open(path, encoding='utf-8') as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    ts = int(row.get('ts', -1))
                    if ts >= cutoff_ms and (to_ms is None or ts < int(to_ms)):
                        rows.append(row)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
    if not rows:
        raise RuntimeError(f'No strict OrderHeatmap data for {market} in last {hours}h')
    rows.sort(key=lambda row: row.get('ts', 0))
    return rows


def load_feature_rows(market, hours=8, from_ms=None, to_ms=None):
    cutoff_ms = (
        int(from_ms)
        if from_ms is not None
        else int(datetime.now(tz=timezone.utc).timestamp() * 1000) - hours * 3600 * 1000
    )
    root = os.path.join(FEATURES_1S_DIR, market)
    rows = []
    for path in sorted(glob.glob(os.path.join(root, '*', '*.jsonl'))):
        try:
            with open(path, encoding='utf-8') as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    row = json.loads(line)
                    ts = int(row.get('ts', -1))
                    if ts >= cutoff_ms and (to_ms is None or ts < int(to_ms)):
                        rows.append(row)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
    rows.sort(key=lambda row: row.get('ts', 0))
    return rows


def derive_ohlcv_from_features(feature_rows, freq='15min'):
    """Build candles only from canonical TFP features_1s rows.

    Empty seconds remain empty; no forward-filled or orderbook-mid candles are
    invented here.  This keeps the chart's candle source identical across
    venues and makes the TFP conversion the single OHLCV authority.
    """
    required = ('trade_open_1s', 'trade_high_1s', 'trade_low_1s', 'trade_close_1s')
    if not feature_rows or not all(any(field in row for row in feature_rows) for field in required):
        return pd.DataFrame()

    frame = pd.DataFrame(feature_rows)
    if 'ts' not in frame:
        return pd.DataFrame()
    frame['timestamp'] = pd.to_datetime(pd.to_numeric(frame['ts'], errors='coerce'), unit='ms', utc=True)
    frame = frame.dropna(subset=['timestamp']).sort_values('timestamp').set_index('timestamp')
    for field in (*required, 'traded_qty_1s', 'traded_notional_1s', 'trade_count_1s'):
        if field in frame:
            frame[field] = pd.to_numeric(frame[field], errors='coerce')

    traded = frame[list(required)].notna().any(axis=1)
    frame = frame.loc[traded]
    if frame.empty:
        return pd.DataFrame()

    candles = pd.DataFrame({
        'open': frame['trade_open_1s'].resample(freq).first(),
        'high': frame['trade_high_1s'].resample(freq).max(),
        'low': frame['trade_low_1s'].resample(freq).min(),
        'close': frame['trade_close_1s'].resample(freq).last(),
    })
    if 'traded_qty_1s' in frame:
        candles['volume'] = frame['traded_qty_1s'].resample(freq).sum(min_count=1)
    else:
        candles['volume'] = np.nan
    if 'traded_notional_1s' in frame:
        candles['quote_volume'] = frame['traded_notional_1s'].resample(freq).sum(min_count=1)
    if 'trade_count_1s' in frame:
        candles['trade_count'] = frame['trade_count_1s'].resample(freq).sum(min_count=1)
    return candles.dropna(subset=['open', 'high', 'low', 'close']).sort_index()


def _heatmap_bin(rows):
    prices = []
    for row in rows:
        prices.extend(float(value) for value in row.get('bid_prices', []))
        prices.extend(float(value) for value in row.get('ask_prices', []))
        if row.get('mid') is not None:
            prices.append(float(row['mid']))
    if not prices:
        return 1
    span = max(prices) - min(prices)
    target_rows = 260
    candidates = (1, 2, 5, 10, 25, 50, 100, 250, 500)
    return next((value for value in candidates if span / value <= target_rows), candidates[-1])


def detect_time_gaps(rows, expected_interval_ms=None, multiplier=1.75):
    """Return interior missing-data spans as ``(start_ms, end_ms)`` tuples.

    The renderer intentionally does not synthesize rows.  A gap is inferred
    from the observed, sorted timestamps and begins one normal interval after
    the last known row, so the known samples themselves are not painted as
    missing.  ``multiplier`` tolerates normal timestamp jitter while still
    exposing a skipped display bucket.
    """
    timestamps = sorted({
        int(row['ts'])
        for row in rows
        if row.get('ts') is not None
    })
    if len(timestamps) < 2:
        return []

    diffs = np.diff(np.asarray(timestamps, dtype=np.int64))
    positive_diffs = diffs[diffs > 0]
    if positive_diffs.size == 0:
        return []
    expected = int(expected_interval_ms or np.median(positive_diffs))
    if expected <= 0:
        return []
    threshold = max(expected * float(multiplier), expected + 60_000)

    spans = []
    for previous, current, diff in zip(timestamps, timestamps[1:], diffs):
        if diff > threshold:
            start = previous + expected
            if start < current:
                spans.append((start, current))
    return spans


def format_gap_duration(start_ms, end_ms):
    """Format a gap duration for compact chart annotations."""
    seconds = max(0, int(end_ms - start_ms) // 1000)
    if seconds >= 3600:
        return f'{seconds / 3600:.1f}h'
    if seconds >= 60:
        return f'{seconds / 60:.1f}m'
    return f'{seconds}s'


def derive_display_cvd(rows, feature_rows, interval='15min'):
    """Return trade-flow CVD aligned to displayed book rows, in BTC."""
    values = np.full(len(rows), np.nan, dtype=float)
    if not rows or not feature_rows:
        return values
    frame = pd.DataFrame(feature_rows)
    if 'ts' not in frame:
        return values
    frame['timestamp'] = pd.to_datetime(pd.to_numeric(frame['ts'], errors='coerce'), unit='ms', utc=True)
    frame = frame.dropna(subset=['timestamp']).sort_values('timestamp').set_index('timestamp')
    buy_values = pd.to_numeric(
        frame['buy_qty'] if 'buy_qty' in frame else pd.Series(0.0, index=frame.index),
        errors='coerce',
    ).fillna(0.0)
    sell_values = pd.to_numeric(
        frame['sell_qty'] if 'sell_qty' in frame else pd.Series(0.0, index=frame.index),
        errors='coerce',
    ).fillna(0.0)
    bucket_delta = (buy_values - sell_values).groupby(frame.index.floor(interval)).sum()
    running_cvd = 0.0
    applied_buckets = set()
    for idx, row in enumerate(rows):
        bucket = pd.Timestamp(row['ts'], unit='ms', tz='UTC').floor(interval)
        if bucket in bucket_delta.index and bucket not in applied_buckets:
            running_cvd += float(bucket_delta.loc[bucket])
            applied_buckets.add(bucket)
        if bucket in applied_buckets:
            values[idx] = running_cvd
    return values


def chart_snapshot_heatmap(rows, market, out_path, period_label, feature_rows=None, ohlc_df=None):
    """Render agg data with the server1 production OrderHeatmap composition."""
    feature_rows = feature_rows or []
    raw_rows = list(rows)
    if len(rows) > 1 and (int(rows[-1]['ts']) - int(rows[0]['ts'])) >= 30 * 60 * 1000:
        display = pd.DataFrame(rows)
        display['bucket'] = pd.to_datetime(display['ts'], unit='ms', utc=True).dt.floor('15min')
        display['has_book'] = (
            display['finalized'].fillna(False)
            & display['mid'].notna()
            & (display['bid_prices'].map(bool) | display['ask_prices'].map(bool))
        )
        rows = (
            display.sort_values(['bucket', 'has_book', 'ts'])
            .groupby('bucket', sort=True)
            .tail(1)
            .drop(columns=['bucket', 'has_book'])
            .to_dict('records')
        )
    gap_spans = detect_time_gaps(rows)
    mid_values = [float(row['mid']) for row in rows if row.get('finalized') and row.get('mid') is not None]
    if not mid_values:
        raise RuntimeError(f'No finalized mid prices for {market}')

    center = float(mid_values[-1])
    price_bin = 20.0
    # Keep the active price action readable. A fixed +/-$4,000 range made
    # normal 15m candle bodies disappear inside a mostly empty heatmap.
    display_half_range = 1500.0
    price_min = np.floor((center - display_half_range) / price_bin) * price_bin
    price_max = np.ceil((center + display_half_range) / price_bin) * price_bin
    price_edges = np.arange(price_min, price_max + price_bin, price_bin)
    x_dt = pd.to_datetime([row['ts'] for row in rows], unit='ms', utc=True)
    x_num = mdates.date2num(x_dt.to_pydatetime())
    if len(x_num) > 1:
        x_midpoints = (x_num[:-1] + x_num[1:]) / 2.0
        x_edges = np.r_[
            x_num[0] - (x_midpoints[0] - x_num[0]),
            x_midpoints,
            x_num[-1] + (x_num[-1] - x_midpoints[-1]),
        ]
    else:
        x_edges = np.array([x_num[0] - 1 / 86400, x_num[0] + 1 / 86400])
    bid_grid = np.full((len(price_edges) - 1, len(rows)), np.nan, dtype=float)
    ask_grid = np.full_like(bid_grid, np.nan)
    mids = np.full(len(rows), np.nan, dtype=float)
    best_bids = np.full(len(rows), np.nan, dtype=float)
    best_asks = np.full(len(rows), np.nan, dtype=float)
    latest_book = None
    clipped_levels = 0
    clipped_qty = 0.0

    for col, row in enumerate(rows):
        if not row.get('finalized') or row.get('mid') is None:
            continue
        mids[col] = float(row['mid'])
        if row.get('best_bid') is not None:
            best_bids[col] = float(row['best_bid'])
        if row.get('best_ask') is not None:
            best_asks[col] = float(row['best_ask'])
        latest_book = row
        for prices_key, qtys_key, grid in (
            ('bid_prices', 'bid_qtys_btc', bid_grid),
            ('ask_prices', 'ask_qtys_btc', ask_grid),
        ):
            for price, qty in zip(row.get(prices_key, []), row.get(qtys_key, [])):
                price = float(price)
                qty = float(qty)
                idx = int(np.floor((price - price_min) / price_bin))
                if 0 <= idx < grid.shape[0] and qty > 0:
                    grid[idx, col] = (0.0 if np.isnan(grid[idx, col]) else grid[idx, col]) + qty
                elif qty > 0:
                    clipped_levels += 1
                    clipped_qty += qty

    valid_qty = np.concatenate([
        grid[np.isfinite(grid) & (grid > 0)] for grid in (bid_grid, ask_grid)
    ]) if any(np.any(np.isfinite(grid) & (grid > 0)) for grid in (bid_grid, ask_grid)) else np.array([1.0])
    # Keep the server1 visual language, but suppress the long tail of tiny
    # aggregated levels.  The agg receiver stores BTC-sized quantities, so
    # server1's absolute 100-unit threshold is not portable here.
    qty_vmin = max(float(np.percentile(valid_qty, 85)), 1e-6)
    qty_vmax = max(float(np.percentile(valid_qty, 99)), qty_vmin * 1.01)
    norm = mcolors.PowerNorm(gamma=0.5, vmin=qty_vmin, vmax=qty_vmax, clip=True)
    bid_img = np.where(np.isfinite(bid_grid) & (bid_grid >= qty_vmin), bid_grid, np.nan)
    ask_img = np.where(np.isfinite(ask_grid) & (ask_grid >= qty_vmin), ask_grid, np.nan)

    market_type = 'Futures' if 'perp' in market else 'Spot'
    fig = plt.figure(figsize=(12.8, 8.0), facecolor='#151515')
    outer = gridspec.GridSpec(
        2, 1, height_ratios=[6.6, 1.35], hspace=0.06,
        left=0.055, right=0.955, bottom=0.085, top=0.925,
    )
    top_outer = gridspec.GridSpecFromSubplotSpec(
        1, 2, subplot_spec=outer[0], width_ratios=[0.08, 4.30], wspace=0.054,
    )
    cbar_anchor = fig.add_subplot(top_outer[0])
    top_inner = gridspec.GridSpecFromSubplotSpec(
        1, 2, subplot_spec=top_outer[1], width_ratios=[3.72, 0.58], wspace=0.02,
    )
    ax_main = fig.add_subplot(top_inner[0])
    ax_profile = fig.add_subplot(top_inner[1])
    bottom_outer = gridspec.GridSpecFromSubplotSpec(
        1, 2, subplot_spec=outer[1], width_ratios=[0.08, 4.30], wspace=0.054,
    )
    spacer = fig.add_subplot(bottom_outer[0])
    bottom_inner = gridspec.GridSpecFromSubplotSpec(
        1, 2, subplot_spec=bottom_outer[1], width_ratios=[3.72, 0.58], wspace=0.02,
    )
    ax_depth = fig.add_subplot(bottom_inner[0], sharex=ax_main)
    fig.add_subplot(bottom_inner[1]).set_axis_off()
    spacer.set_axis_off()
    for ax in (cbar_anchor, ax_main, ax_profile, ax_depth):
        ax.set_facecolor('#151515')

    ax_main.set_ylim(price_min, price_max)
    ax_main.set_xlim(x_edges[0], x_edges[-1])
    ax_main.yaxis.tick_right()
    ax_main.yaxis.set_label_position('right')
    ax_main.yaxis.set_major_locator(mticker.MultipleLocator(500))
    ax_main.yaxis.set_major_formatter(plt.FuncFormatter(lambda value, _: f'{value:.0f}'))
    ax_main.tick_params(axis='y', colors='white', labelsize=9)
    ax_main.tick_params(axis='x', labelbottom=False)
    ax_main.grid(True, axis='x', linestyle=':', alpha=0.18, color='gray')
    bid_cmap = plt.get_cmap('Blues_r')
    ask_cmap = plt.get_cmap('OrRd_r')
    # pcolormesh preserves real elapsed time when snapshots are irregular;
    # imshow would stretch every column to the same width and hide outages.
    bid_im = ax_main.pcolormesh(x_edges, price_edges, bid_img, shading='flat', cmap=bid_cmap, norm=norm, alpha=0.8, zorder=1)
    ask_im = ax_main.pcolormesh(x_edges, price_edges, ask_img, shading='flat', cmap=ask_cmap, norm=norm, alpha=0.8, zorder=1)

    # Mark missing intervals without filling, carrying forward, or visually
    # attributing depth to a period for which no valid snapshot exists.
    gap_color = '#F6C453'
    for start_ms, end_ms in gap_spans:
        start_num = mdates.date2num(pd.Timestamp(start_ms, unit='ms', tz='UTC').to_pydatetime())
        end_num = mdates.date2num(pd.Timestamp(end_ms, unit='ms', tz='UTC').to_pydatetime())
        for axis in (ax_main,):
            axis.axvspan(
                start_num, end_num, facecolor=gap_color, edgecolor=gap_color,
                alpha=0.12, hatch='///', linewidth=0.8, zorder=9,
            )

    # Match server1's candle/marker layer when agg feature rows are available.
    fdf = pd.DataFrame(feature_rows)
    if not fdf.empty and 'ts' in fdf:
        fdf['timestamp'] = pd.to_datetime(fdf['ts'], unit='ms', utc=True)
        fdf = fdf.sort_values('timestamp').set_index('timestamp')
    else:
        fdf = pd.DataFrame()
    if ohlc_df is None or ohlc_df.empty:
        raise RuntimeError(f'No transformed TFP OHLCV for {market}')
    ohlc = ohlc_df[(ohlc_df.index >= x_dt[0]) & (ohlc_df.index <= x_dt[-1])]
    width_days = 15 * 60 / 86400 * 0.72
    # Agg derived features can have a very small 15m body (or a flat VWAP
    # candle). Keep the candle visible over the absolute-price heatmap.
    candle_body_floor = max(price_bin * 0.5, 20.0)
    if not ohlc.empty:
        for up, color in ((ohlc['close'] >= ohlc['open'], '#3bb2e5'), (ohlc['close'] < ohlc['open'], '#e9546c')):
            part = ohlc[up]
            if part.empty:
                continue
            idx = mdates.date2num(part.index.to_pydatetime())
            bottoms = np.minimum(part['open'], part['close'])
            heights = np.maximum((part['close'] - part['open']).abs(), candle_body_floor)
            ax_main.vlines(idx, part['low'], part['high'], color='#050505', linewidth=4.0, zorder=12.0)
            ax_main.vlines(idx, part['low'], part['high'], color=color, linewidth=2.0, zorder=12.1)
            ax_main.bar(idx, heights, width_days, bottom=bottoms, color=color, edgecolor='#ffffff', linewidth=1.8, alpha=1.0, zorder=12.2)
    if not fdf.empty:
        buys = pd.to_numeric(fdf.get('buy_notional_1s', 0), errors='coerce').fillna(0)
        sells = pd.to_numeric(fdf.get('sell_notional_1s', 0), errors='coerce').fillna(0)
        for values, color, label in ((buys, '#2E8B57', 'Buy Volume'), (sells, '#DC143C', 'Sell Volume')):
            part = fdf.loc[values.nlargest(12).index]
            if part.empty:
                continue
            sizes = np.interp(values.loc[part.index], [max(values.min(), 0), max(values.max(), 1)], [25, 420])
            prices = pd.to_numeric(part.get('book_mid_price', np.nan), errors='coerce')
            ax_main.scatter(mdates.date2num(part.index.to_pydatetime()), prices, s=sizes, color=color, alpha=0.45, edgecolors='white', linewidths=0.2, label=label, zorder=5)

    latest_price = float(mids[np.isfinite(mids)][-1])
    price_color = '#3bb2e5'
    ax_main.text(0.015, 0.975, f'{latest_price:.2f}', transform=ax_main.transAxes, fontsize=24, fontweight='bold', color=price_color, va='top', ha='left', bbox=dict(boxstyle='round,pad=0.28', fc='black', ec=price_color, lw=1.0, alpha=0.82), zorder=6)
    range_note = f'View ±${display_half_range:,.0f}'
    if clipped_levels:
        range_note += f'  |  clipped {clipped_levels} levels ({clipped_qty:.2f} BTC)'
    ax_main.text(
        0.015, 0.905, range_note, transform=ax_main.transAxes,
        fontsize=8, color='#d6d3d1', va='top', ha='left',
        bbox=dict(boxstyle='round,pad=0.16', fc='#151515', ec='#555555', lw=0.5, alpha=0.78),
        zorder=20,
    )
    ax_main.set_ylabel('')

    # Current orderbook profile on the right, matching server1's side panel.
    ax_profile.set_ylim(price_min, price_max)
    ax_profile.yaxis.tick_right()
    ax_profile.yaxis.set_major_locator(mticker.MultipleLocator(500))
    ax_profile.yaxis.set_major_formatter(plt.FuncFormatter(lambda value, _: f'{value:.0f}'))
    ax_profile.tick_params(axis='y', colors='white', labelsize=9)
    ax_profile.xaxis.set_ticks_position('top')
    ax_profile.xaxis.set_label_position('top')
    ax_profile.set_xlabel('Order Book Qty (BTC)', color='white', fontsize=9, labelpad=3)
    if latest_book is not None:
        profile_edges = np.arange(price_min, price_max + price_bin, price_bin)
        bid_profile = np.zeros(len(profile_edges) - 1)
        ask_profile = np.zeros_like(bid_profile)
        for prices_key, qtys_key, target in (('bid_prices', 'bid_qtys_btc', bid_profile), ('ask_prices', 'ask_qtys_btc', ask_profile)):
            for price, qty in zip(latest_book.get(prices_key, []), latest_book.get(qtys_key, [])):
                idx = int(np.floor((float(price) - price_min) / price_bin))
                if 0 <= idx < len(target):
                    target[idx] += float(qty)
        max_qty = max(float(bid_profile.max()), float(ask_profile.max()), 1e-9)
        centers = profile_edges[:-1] + price_bin / 2
        ax_profile.barh(centers, bid_profile, height=price_bin * 0.9, color=bid_cmap(0.8), alpha=0.7)
        ax_profile.barh(centers, ask_profile, height=price_bin * 0.9, color=ask_cmap(0.8), alpha=0.7)

        def format_profile_qty(value: float) -> str:
            if value >= 1000:
                return f'{value / 1000:.1f}k'
            if value >= 100:
                return f'{value:.0f}'
            if value >= 10:
                return f'{value:.1f}'
            if value >= 1:
                return f'{value:.2f}'
            return f'{value:.3f}'

        # Label only the largest, separated levels on each side so the profile
        # stays readable even when several walls sit close together.
        label_pad = max_qty * 0.045

        def pick_label_indices(values: np.ndarray, limit: int = 4, min_separation: int = 3) -> np.ndarray:
            candidates = list(np.argsort(values)[::-1])
            selected: list[int] = []
            for idx in candidates:
                if values[idx] <= 0:
                    continue
                if all(abs(int(idx) - chosen) >= min_separation for chosen in selected):
                    selected.append(int(idx))
                if len(selected) >= limit:
                    break
            return np.array(sorted(selected), dtype=int)

        label_indices = {
            'bid': pick_label_indices(bid_profile),
            'ask': pick_label_indices(ask_profile),
        }
        for side, values, color, y_offset in (
            ('bid', bid_profile, bid_cmap(0.95), -price_bin * 0.22),
            ('ask', ask_profile, ask_cmap(0.95), price_bin * 0.22),
        ):
            for idx in label_indices[side]:
                qty = float(values[idx])
                ax_profile.text(
                    qty + label_pad, centers[idx] + y_offset, format_profile_qty(qty),
                    color=color, fontsize=7.0, ha='left', va='center', clip_on=True,
                    bbox=dict(boxstyle='round,pad=0.10', fc='#151515', ec=color, lw=0.45, alpha=0.88),
                    zorder=5,
                )
        profile_limit = max_qty * 1.72
    else:
        profile_limit = 1.0
        format_profile_qty = lambda value: f'{value:.1f}'
    ax_profile.set_xlim(0, profile_limit)
    ax_profile.xaxis.set_major_locator(mticker.MaxNLocator(3))
    ax_profile.xaxis.set_major_formatter(plt.FuncFormatter(lambda value, _: format_profile_qty(value)))
    ax_profile.tick_params(axis='x', colors='white', labelsize=8)
    ax_profile.grid(True, axis='y', linestyle=':', linewidth=0.5, color='gray', alpha=0.18)
    ax_profile.axhline(latest_price, color='#3bb2e5', linestyle='--', linewidth=0.8, alpha=0.7)

    # Two separate left colorbars, exactly like server1.
    cbar_grid = gridspec.GridSpecFromSubplotSpec(2, 1, subplot_spec=cbar_anchor.get_subplotspec(), hspace=0.1)
    cbar_anchor.remove()
    ask_cax = fig.add_subplot(cbar_grid[0])
    bid_cax = fig.add_subplot(cbar_grid[1])
    for cax, image, label in ((ask_cax, ask_im, 'Ask Quantity'), (bid_cax, bid_im, 'Bid Quantity')):
        cbar = fig.colorbar(image, cax=cax, orientation='vertical')
        cbar.set_label(label, fontsize=9, color='white')
        cbar.ax.tick_params(labelsize=8, colors='white')
        cax.yaxis.set_ticks_position('left')
        cax.yaxis.set_label_position('left')

    # Lower panel: show the bid/ask fight from the same book snapshots used by
    # the heatmap above.  A stacked 0-100% view makes the neutral point and
    # the changing side dominance immediately visible; total depth alone does
    # not distinguish which side is applying the pressure.
    ax_depth.grid(True, linestyle=':', alpha=0.16, color='gray')
    ax_depth.tick_params(axis='x', colors='white', labelsize=9)
    ax_depth.tick_params(axis='y', colors='white', labelsize=9)
    ax_depth.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d\n%H:%M', tz=JST))
    bid_depth = np.zeros(len(rows), dtype=float)
    ask_depth = np.zeros(len(rows), dtype=float)
    for idx, row in enumerate(rows):
        if not row.get('finalized'):
            bid_depth[idx] = np.nan
            ask_depth[idx] = np.nan
            continue
        try:
            bid_depth[idx] = sum(max(float(qty), 0.0) for qty in row.get('bid_qtys_btc', []))
            ask_depth[idx] = sum(max(float(qty), 0.0) for qty in row.get('ask_qtys_btc', []))
        except (TypeError, ValueError):
            bid_depth[idx] = np.nan
            ask_depth[idx] = np.nan

    total_depth = bid_depth + ask_depth
    valid_pressure = np.isfinite(total_depth) & (total_depth > 0)
    bid_ratio = np.divide(
        bid_depth * 100.0, total_depth,
        out=np.full(len(rows), np.nan), where=valid_pressure,
    )
    ask_ratio = np.divide(
        ask_depth * 100.0, total_depth,
        out=np.full(len(rows), np.nan), where=valid_pressure,
    )
    finite_ratio = bid_ratio[np.isfinite(bid_ratio)]
    if finite_ratio.size:
        # Zoom around the neutral 50% line so small changes in side
        # dominance remain visible. Expand automatically when the market is
        # genuinely imbalanced; never clip the observed range.
        observed_min = float(finite_ratio.min())
        observed_max = float(finite_ratio.max())
        half_span = max(5.0, (observed_max - observed_min) * 0.70)
        half_span = max(half_span, 50.0 - observed_min + 2.0, observed_max - 50.0 + 2.0)
        pressure_min = max(0.0, 50.0 - half_span)
        pressure_max = min(100.0, 50.0 + half_span)
    else:
        pressure_min, pressure_max = 0.0, 100.0
    pressure_x = x_num
    bid_pressure_color = '#13B8E6'  # cyan / bid
    ask_pressure_color = '#F05A5A'  # coral red / ask
    ax_depth.fill_between(
        pressure_x, pressure_min, bid_ratio, color=bid_pressure_color, alpha=0.30,
        linewidth=0, label='Bid pressure', step='post', zorder=2,
    )
    ax_depth.fill_between(
        pressure_x, bid_ratio, pressure_max, color=ask_pressure_color, alpha=0.30,
        linewidth=0, label='Ask pressure', step='post', zorder=2,
    )
    for start_ms, end_ms in gap_spans:
        start_num = mdates.date2num(pd.Timestamp(start_ms, unit='ms', tz='UTC').to_pydatetime())
        end_num = mdates.date2num(pd.Timestamp(end_ms, unit='ms', tz='UTC').to_pydatetime())
        ax_depth.axvspan(
            start_num, end_num, facecolor=gap_color, edgecolor=gap_color,
            alpha=0.12, hatch='///', linewidth=0.8, zorder=8,
        )
    ax_depth.plot(
        pressure_x, bid_ratio, color=bid_cmap(0.95), linewidth=0.8,
        alpha=0.95, zorder=3,
    )

    # CVD is trade-flow cumulative delta in BTC, independent of book depth.
    # Aggregate the canonical 1s feature deltas into the same 15m display
    # buckets, then reset the visible baseline at the start of this chart.
    cvd_values = derive_display_cvd(rows, feature_rows)

    ax_cvd = ax_depth.twinx()
    ax_cvd.set_facecolor('none')
    ax_cvd.tick_params(axis='y', colors='#F6C453', labelsize=8)
    ax_cvd.spines['right'].set_color('#F6C453')
    ax_cvd.set_ylabel('CVD (BTC)', color='#F6C453', fontsize=9)
    finite_cvd = cvd_values[np.isfinite(cvd_values)]
    if finite_cvd.size:
        cvd_min = min(0.0, float(finite_cvd.min()))
        cvd_max = max(0.0, float(finite_cvd.max()))
        cvd_pad = max((cvd_max - cvd_min) * 0.10, 1e-6)
        ax_cvd.set_ylim(cvd_min - cvd_pad, cvd_max + cvd_pad)
        ax_cvd.plot(
            pressure_x, cvd_values, color='#F6C453', linewidth=1.35,
            alpha=0.95, label='CVD', zorder=6,
        )
        ax_cvd.axhline(0.0, color='#F6C453', linewidth=0.6, linestyle=':', alpha=0.45, zorder=5)
        ax_cvd.text(
            0.995, 0.06, f'CVD {float(finite_cvd[-1]):+.2f} BTC',
            transform=ax_cvd.transAxes, ha='right', va='bottom',
            color='#F6C453', fontsize=9,
            bbox=dict(boxstyle='round,pad=0.16', fc='#151515', ec='#F6C453', lw=0.65, alpha=0.82),
            zorder=10,
        )
    else:
        ax_cvd.set_ylim(-1.0, 1.0)
    ax_cvd.yaxis.set_major_formatter(plt.FuncFormatter(lambda value, _: f'{value:.2f}'))
    ax_cvd.grid(False)

    ax_depth.axhline(50, color='#d6d3d1', linewidth=0.8, linestyle='--', alpha=0.58, zorder=4)
    ax_depth.set_ylim(pressure_min, pressure_max)
    ax_depth.set_ylabel('Bid / Ask %', color='#e5e7eb', fontsize=10)
    ax_depth.yaxis.set_major_formatter(plt.FuncFormatter(lambda value, _: f'{value:.0f}%'))
    label_box = dict(boxstyle='round,pad=0.16', fc='#151515', alpha=0.82, lw=0.7)
    ax_depth.text(0.012, 0.86, 'ASK', transform=ax_depth.transAxes, color='#FFFFFF', fontsize=9, fontweight='bold', va='top', bbox={**label_box, 'ec': ask_pressure_color})
    ax_depth.text(0.012, 0.06, 'BID', transform=ax_depth.transAxes, color='#FFFFFF', fontsize=9, fontweight='bold', va='bottom', bbox={**label_box, 'ec': bid_pressure_color})
    if finite_ratio.size:
        latest_bid = float(finite_ratio[-1])
        latest_ask = 100.0 - latest_bid
        ax_depth.text(
            0.995, 0.90, f'Bid {latest_bid:.0f}%  |  Ask {latest_ask:.0f}%',
            transform=ax_depth.transAxes, ha='right', va='top',
            color='#e5e7eb', fontsize=10,
            bbox=dict(boxstyle='round,pad=0.18', fc='#151515', ec='#555555', alpha=0.78),
        )

    handles, labels = ax_main.get_legend_handles_labels()
    if handles:
        ax_main.legend(handles=handles, labels=labels, fontsize=9, loc='lower left', bbox_to_anchor=(0.005, 0.005), framealpha=0.7, labelcolor='white').get_frame().set_facecolor('black')
    title_kind = 'Futures' if 'perp' in market else 'Spot'
    title_interval = '15m'
    quote = 'USDC' if 'usdc' in market else ('USD' if any(name in market for name in ('coinbase', 'kraken', 'bitstamp', 'bitfinex', 'crypto_com')) else 'USDT')
    venue = market.rsplit('_', 1)[0].upper()
    title_time = x_dt[-1].tz_convert('Asia/Tokyo').strftime('%Y-%m-%d %H:%M')
    gap_summary = ''
    if gap_spans:
        total_gap_ms = sum(end - start for start, end in gap_spans)
        gap_summary = f' | gaps {len(gap_spans)} ({format_gap_duration(0, total_gap_ms)})'
        ax_main.text(
            0.995, 0.975, f'DATA GAP  {len(gap_spans)}  /  {format_gap_duration(0, total_gap_ms)}',
            transform=ax_main.transAxes, fontsize=9, fontweight='bold',
            color=gap_color, va='top', ha='right',
            bbox=dict(boxstyle='round,pad=0.20', fc='#151515', ec=gap_color, lw=0.8, alpha=0.88),
            zorder=20,
        )
    fig.suptitle(f'BTC_{quote} {venue} {title_kind} OrderHeatmap v3.41 | {title_interval} | {title_time} JST{gap_summary}', color='white', fontsize=13, y=0.96)
    fig.savefig(out_path, dpi=150, facecolor='#151515')
    plt.close(fig)
    return out_path


def main():
    p = argparse.ArgumentParser(description='Strict absolute-price OrderHeatmap')
    p.add_argument('--market', type=str, default='binance_perp', help='Market name (server1-compatible default)')
    p.add_argument('--markets', action='store_true', help='Render all available markets')
    p.add_argument('--hours', type=int, default=12, help='Lookback hours')
    p.add_argument('--from-ms', type=int, default=None, help='Inclusive UTC start timestamp in milliseconds')
    p.add_argument('--to-ms', type=int, default=None, help='Exclusive UTC end timestamp in milliseconds')
    p.add_argument('--out', type=str, default=None, help='Output path')
    args = p.parse_args()

    if args.markets:
        import glob
        markets = sorted([
            os.path.basename(f).split('=', 1)[1]
            for f in glob.glob(os.path.join(SNAPSHOT_HEATMAP_DIR, 'market=*'))
            if '=' in os.path.basename(f)
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
        try:
            rows = load_snapshot_heatmap(mkt, args.hours, args.from_ms, args.to_ms)
            feature_rows = load_feature_rows(mkt, args.hours, args.from_ms, args.to_ms)
            ohlc_df = derive_ohlcv_from_features(feature_rows)
            print(f"  {mkt}: {len(rows)} rows, strict snapshot source, {len(feature_rows)} feature rows, {len(ohlc_df)} candles")
            if ohlc_df.empty:
                print(f"    skip: no transformed TFP OHLCV rows for {mkt}")
                continue
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

        if args.from_ms is not None or args.to_ms is not None:
            start = pd.Timestamp(args.from_ms, unit='ms', tz='UTC').strftime('%H:%M:%S') if args.from_ms is not None else 'start'
            end = pd.Timestamp(args.to_ms, unit='ms', tz='UTC').strftime('%H:%M:%S') if args.to_ms is not None else 'now'
            period_label = f'{start}–{end} UTC'
        else:
            period_label = f'{args.hours}h'
        chart_snapshot_heatmap(rows, mkt, out_path, period_label, feature_rows, ohlc_df)
        sz = os.path.getsize(out_path) / 1024
        print(f"    → {out_path} ({sz:.0f} KB)")

    print(f"Done. {len(markets)} market(s).")


if __name__ == '__main__':
    main()
