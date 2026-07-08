#!/usr/bin/env python3
"""Plot CVD from existing agg parquet data (96-column format)."""

import duckdb
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from datetime import datetime, timezone
import sys, os

def main():
    agg_dir = os.path.join(os.path.dirname(__file__), 'data', 'agg')
    markets = [
        'binance_spot',
        'binance_perp',
        'coinbase_spot',
        'okx_perp',
        'bybit_perp',
    ]

    con = duckdb.connect(':memory:')

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(16, 9),
        gridspec_kw={'height_ratios': [3, 1]},
        sharex=True
    )
    fig.patch.set_facecolor('#0d1117')

    colors = ['#00d2ff', '#ff6b6b', '#50fa7b', '#ffb347', '#c678dd']
    cvd_max = 0
    cvd_min = 0

    # Store all delta data for the bar chart
    all_deltas = {}

    for i, market in enumerate(markets):
        fp = os.path.join(agg_dir, f'{market}.parquet')
        if not os.path.exists(fp):
            print(f"  skipping {market} — no parquet")
            continue

        rows = con.execute(f"""
            SELECT
                CAST(ts AS DOUBLE) as ts,
                CAST(delta_notional AS DOUBLE) as delta_notional,
                CAST(trade_count AS DOUBLE) as trade_count,
                CAST(buy_notional AS DOUBLE) as buy_notional,
                CAST(sell_notional AS DOUBLE) as sell_notional
            FROM read_parquet('{fp}')
            ORDER BY ts
        """).fetchall()

        if not rows:
            continue

        ts_arr = np.array([r[0] for r in rows]) / 1000.0  # ms → sec
        delta_arr = np.array([r[1] for r in rows])
        tc_arr = np.array([r[2] for r in rows])

        # Cumulative CVD
        cvd = np.cumsum(delta_arr)

        dt_arr = np.array([datetime.fromtimestamp(t, tz=timezone.utc) for t in ts_arr])

        # Plot CVD line
        color = colors[i % len(colors)]
        ax1.plot(dt_arr, cvd, label=f'{market} (CVD={cvd[-1]:+.0f})', color=color,
                 linewidth=1.0, alpha=0.85)

        cvd_max = max(cvd_max, np.max(cvd))
        cvd_min = min(cvd_min, np.min(cvd))

        all_deltas[market] = (dt_arr, delta_arr, color)

    # Delta notional bar chart (binance_spot only, or first available)
    primary = markets[0]
    if primary in all_deltas:
        dt_arr, delta_arr, color = all_deltas[primary]
        # Color bars: green for positive, red for negative
        colors_bar = np.where(delta_arr >= 0, '#26a69a', '#ef5350')
        ax2.bar(dt_arr, delta_arr, width=0.0008, color=colors_bar, alpha=0.6, linewidth=0)
        ax2.axhline(y=0, color='#444', linewidth=0.5)
        ax2.set_ylabel('Δ Notional /s', color='#8b949e', fontsize=10)

    # Style
    for ax in [ax1, ax2]:
        ax.set_facecolor('#0d1117')
        ax.tick_params(colors='#8b949e', labelsize=9)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_color('#30363d')
        ax.spines['left'].set_color('#30363d')
        ax.grid(True, alpha=0.15, color='#8b949e', linewidth=0.4)

    ax1.legend(loc='upper left', fontsize=9, labelcolor='#c9d1d9',
               framealpha=0.3, facecolor='#0d1117', edgecolor='#30363d')
    ax1.set_ylabel('CVD (USD)', color='#8b949e', fontsize=11)
    ax1.set_title('Cumulative Volume Delta — BTC Multi-Market',
                  color='#c9d1d9', fontsize=13, fontweight='bold', pad=12)

    # Time axis
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M', tz=timezone.utc))
    ax2.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=0)
    ax2.set_xlabel('Time (UTC)', color='#8b949e', fontsize=10)

    fig.tight_layout()
    out_path = os.path.join(os.path.dirname(__file__), 'data', 'cvd_chart.png')
    fig.savefig(out_path, dpi=150, facecolor='#0d1117')
    print(f"Saved: {out_path}")
    con.close()

if __name__ == '__main__':
    main()
