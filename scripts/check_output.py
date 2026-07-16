"""Quick sanity check: read Parquet with DuckDB."""
import duckdb
import sys
sys.path.insert(0, ".")
from pathlib import Path

con = duckdb.connect(':memory:')

r = con.execute("""
    SELECT count(*) as rows,
           count(DISTINCT market) as markets,
           min(ts) as first_ts,
           max(ts) as last_ts,
           sum(burst_count_1s) as total_bursts,
           round(avg(total_burst_notional_1s), 2) as avg_notional
    FROM read_parquet('data/derived/burst_features_v1/features_1s/**/*.parquet', hive_partitioning=true)
""").fetchone()
print(f"Summary: rows={r[0]}, markets={r[1]}, ts_range=[{r[2]},{r[3]}], total_bursts={r[4]}, avg_notional={r[5]}")

r2 = con.execute("""
    SELECT ts, burst_count_1s, total_burst_notional_1s,
           burst_imbalance_ratio_1s, burst_notional_vs_30s_traded_notional,
           market, date
    FROM read_parquet('data/derived/burst_features_v1/features_1s/**/*.parquet', hive_partitioning=true)
    WHERE burst_count_1s > 0
    ORDER BY ts
""").fetchall()
print("Burst seconds:")
for row in r2:
    print(f"  {row}")
