#!/usr/bin/env python3
"""
Quick diagnostics for xref parquet files to assess schema and fan-out risk.

Analyzes:
- data/xrefs/xref_pg_member_npi.parquet
- data/xrefs/xref_pg_member_tin.parquet

Reports rows, columns, estimated memory, unique counts, fan-out distribution,
and top offenders by fan-out per pg_uid.
"""

from pathlib import Path
import polars as pl


def inspect_file(name: str, path: Path) -> None:
    print("\n" + "=" * 100)
    print(f"Inspecting {name}: {path}")
    if not path.exists():
        print("Missing file")
        return

    df = pl.read_parquet(path)
    print(f"Rows: {df.height:,}  Cols: {len(df.columns)}")
    print("Columns:", df.columns)
    try:
        mem_mb = df.estimated_size() / (1024 * 1024)
    except Exception:
        mem_mb = float('nan')
    print(f"Estimated size: {mem_mb:.1f} MB")

    if 'pg_uid' in df.columns:
        nunique_pg = df.select(pl.col('pg_uid').n_unique()).item()
        print("Unique pg_uid:", f"{nunique_pg:,}")
        # fan-out per pg_uid
        fan = df.group_by('pg_uid').len().select([
            pl.col('len').quantile(0.5).alias('p50'),
            pl.col('len').quantile(0.9).alias('p90'),
            pl.col('len').max().alias('max'),
            pl.col('len').mean().alias('mean'),
        ])
        p50, p90, max_v, mean_v = fan.row(0)
        print("Fan-out per pg_uid (rows per pg_uid):",
              {"p50": p50, "p90": p90, "max": max_v, "mean": mean_v})
        # top offenders
        top = df.group_by('pg_uid').len().sort('len', descending=True).head(10)
        print("Top 10 pg_uid by fan-out:")
        print(top)

    if 'npi' in df.columns:
        nunique_npi = df.select(pl.col('npi').n_unique()).item()
        print("Unique npi:", f"{nunique_npi:,}")

    if 'tin' in df.columns:
        nunique_tin = df.select(pl.col('tin').n_unique()).item()
        print("Unique tin:", f"{nunique_tin:,}")


def main() -> None:
    base = Path('data/xrefs')
    files = {
        'pg_member_npi': base / 'xref_pg_member_npi.parquet',
        'pg_member_tin': base / 'xref_pg_member_tin.parquet',
    }
    for name, path in files.items():
        inspect_file(name, path)
    print("\nDone.")


if __name__ == '__main__':
    main()



