# utils_nppes.py
"""
NPPES → dim_npi upsert utilities
--------------------------------
- Fetch essential NPI info from NPPES Registry API (v2.x).
- Normalize into two small tables:
    data/dims/dim_npi.parquet              (1 row per NPI)
    data/dims/dim_npi_address.parquet      (N rows per NPI: LOCATION/MAILING)

Design goals:
- Memory efficient: read only the needed columns for dedup; use DuckDB to merge.
- Idempotent upserts: safe to re-run; last_updated drives replace behavior.
- Works on small laptops and scales as data grows.

CLI:
    python utils_nppes.py --data-dir data --add 1295795499 1639195834
    python utils_nppes.py --data-dir data --file npis.txt
"""

from __future__ import annotations
import os, time, json, hashlib, argparse, sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import polars as pl
import duckdb

# ------------- CONFIG DEFAULTS -------------
NPPES_BASE = "https://npiregistry.cms.hhs.gov/api/"
NPPES_VERSION = "2.1"   # commonly used at time of writing
REQUEST_TIMEOUT = 15
RETRY_MAX = 5
RETRY_BACKOFF = 1.6

# ------------- PATH HELPERS ----------------
def ensure_dirs(base: Path) -> Tuple[Path, Path]:
    # In the new structure, dim_npi files go directly in the dims directory
    dim_dir = base / "dims"
    addr_dir = base / "dims"  # Both files go in dims directory
    dim_dir.mkdir(parents=True, exist_ok=True)
    return dim_dir, addr_dir

def dim_paths(data_dir: Path, dim_npi_filename: str = "dim_npi.parquet") -> Tuple[Path, Path]:
    """Get paths for dim_npi and dim_npi_address files.
    
    Args:
        data_dir: Path to data directory (contains dims subdirectory)
        dim_npi_filename: Custom filename for dim_npi.parquet (default: "dim_npi.parquet")
    """
    dim_dir, addr_dir = ensure_dirs(data_dir)
    return dim_dir / dim_npi_filename, addr_dir / "dim_npi_address.parquet"

# ------------- UTILITIES -------------------
def md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()

def _co(s: Any) -> str:
    return "" if s is None else str(s)

def _safe_get(d: Dict, *keys, default=None):
    cur = d
    for k in keys:
        try:
            cur = cur[k]
        except Exception:
            return default
    return cur

def _phone_clean(x: Optional[str]) -> Optional[str]:
    if not x:
        return None
    digits = "".join(ch for ch in x if ch.isdigit())
    return digits or None

# ------------- NPPES FETCH -----------------
def fetch_nppes_record(npi: str) -> Optional[Dict[str, Any]]:
    """
    Fetch a single NPI from NPPES Registry API, return the first (and only) result dict or None.
    """
    # Build query: single NPI (avoid over-optimistic batching limits)
    params = {"version": NPPES_VERSION, "number": npi}
    url = NPPES_BASE
    attempt, delay = 0, RETRY_BACKOFF

    while attempt < RETRY_MAX:
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                results = data.get("results") or []
                return results[0] if results else None
            elif r.status_code in (429, 500, 502, 503, 504):
                time.sleep(delay)
                delay *= RETRY_BACKOFF
                attempt += 1
                continue
            else:
                # Other client/server error; give up gracefully
                sys.stderr.write(f"[WARN] NPPES {npi} HTTP {r.status_code}: {r.text[:200]}\n")
                return None
        except requests.RequestException as e:
            time.sleep(delay)
            delay *= RETRY_BACKOFF
            attempt += 1

    sys.stderr.write(f"[ERROR] NPPES fetch failed after retries for NPI={npi}\n")
    return None

# ------------- NORMALIZATION ---------------
def _extract_primary_taxonomy(rec: Dict[str, Any]) -> Dict[str, Optional[str]]:
    tax = rec.get("taxonomies") or []
    primary = None
    for t in tax:
        if t.get("primary"):
            primary = t
            break
    if primary is None and tax:
        primary = tax[0]
    return {
        "primary_taxonomy_code": _safe_get(primary or {}, "code"),
        "primary_taxonomy_desc": _safe_get(primary or {}, "desc"),
        "primary_taxonomy_state": _safe_get(primary or {}, "state"),
        "primary_taxonomy_license": _safe_get(primary or {}, "license"),
    }

def _extract_dim_npi_row(npi: str, rec: Dict[str, Any], nppes_fetched: bool = True) -> Dict[str, Any]:
    basic = rec.get("basic") or {}
    enum_type = rec.get("enumeration_type")
    org_name = basic.get("organization_name")
    first_name = basic.get("first_name")
    last_name = basic.get("last_name")
    credential = basic.get("credential")
    status = basic.get("status")
    sole_prop = basic.get("sole_proprietor")
    enum_date = basic.get("enumeration_date")
    last_updated = basic.get("last_updated")
    replacement_npi = basic.get("replacement_npi")

    tx = _extract_primary_taxonomy(rec)

    return {
        "npi": str(npi),
        "enumeration_type": enum_type,       # NPI-1 / NPI-2
        "status": status,                    # A/I
        "organization_name": org_name,       # present for NPI-2
        "first_name": first_name,            # present for NPI-1
        "last_name": last_name,              # present for NPI-1
        "credential": credential,
        "sole_proprietor": sole_prop,
        "enumeration_date": enum_date,
        "last_updated": last_updated,
        "replacement_npi": replacement_npi,
        "nppes_fetched": nppes_fetched,      # Flag indicating if data was fetched from NPPES
        "nppes_fetch_date": last_updated if nppes_fetched else None,  # When it was fetched
        **tx,
    }

def _extract_addresses(npi: str, rec: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for a in (rec.get("addresses") or []):
        row = {
            "npi": str(npi),
            "address_purpose": a.get("address_purpose"),    # LOCATION / MAILING
            "address_type": a.get("address_type"),          # DOM / FOREIGN
            "address_1": a.get("address_1"),
            "address_2": a.get("address_2"),
            "city": a.get("city"),
            "state": a.get("state"),
            "postal_code": a.get("postal_code"),
            "country_code": a.get("country_code"),
            "telephone_number": _phone_clean(a.get("telephone_number")),
            "fax_number": _phone_clean(a.get("fax_number")),
            "last_updated": _safe_get(rec, "basic", "last_updated"),
        }
        # stable address hash for dedup
        key = "|".join([
            _co(row["address_purpose"]),
            _co(row["address_type"]),
            _co(row["address_1"]),
            _co(row["address_2"]),
            _co(row["city"]),
            _co(row["state"]),
            _co(row["postal_code"]),
            _co(row["country_code"]),
        ])
        row["address_hash"] = md5(key)
        out.append(row)
    return out

def normalize_nppes_result(npi: str, rec: Dict[str, Any], nppes_fetched: bool = True) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Return (dim_npi_df, dim_npi_address_df) for a single NPI record."""
    dim_row = _extract_dim_npi_row(npi, rec, nppes_fetched)
    addr_rows = _extract_addresses(npi, rec)
    return pl.DataFrame([dim_row]), pl.DataFrame(addr_rows) if addr_rows else pl.DataFrame(
        schema={
            "npi": pl.Utf8, "address_purpose": pl.Utf8, "address_type": pl.Utf8,
            "address_1": pl.Utf8, "address_2": pl.Utf8, "city": pl.Utf8, "state": pl.Utf8,
            "postal_code": pl.Utf8, "country_code": pl.Utf8, "telephone_number": pl.Utf8,
            "fax_number": pl.Utf8, "last_updated": pl.Utf8, "address_hash": pl.Utf8
        }
    )

# ------------- UPSERTS ---------------------
def _append_unique_parquet(df_new: pl.DataFrame, path: Path, keys: List[str]) -> None:
    """Small-table upsert using Polars (loads old fully). OK for up to a few million rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        df_old = pl.read_parquet(str(path))
        df_all = pl.concat([df_old, df_new], how="vertical_relaxed").unique(subset=keys, keep="last")
    else:
        df_all = df_new.unique(subset=keys, keep="last")
    df_all.write_parquet(str(path), compression="zstd")

def _duckdb_merge_parquet(df_new: pl.DataFrame, path: Path, keys: List[str]) -> None:
    """Big-table upsert: uses DuckDB to merge without loading everything into RAM."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        df_new.write_parquet(str(path), compression="zstd")
        return

    # Write the delta file
    delta_path = str(path) + ".delta.parquet"
    df_new.write_parquet(delta_path, compression="zstd")

    # Build a merged output using DuckDB distinct on keys, preferring latest last_updated if present
    tmp_out = str(path) + ".next.parquet"
    con = duckdb.connect()
    # Use ROW_NUMBER() over keys ordering by last_updated to pick the newest if available
    con.execute(f"""
    CREATE OR REPLACE TABLE __merge AS
    SELECT *
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY {", ".join(keys)}
               ORDER BY COALESCE(try_cast(last_updated as timestamp), '1900-01-01'::timestamp) DESC
             ) AS rn
      FROM (
        SELECT * FROM read_parquet('{path}')
        UNION ALL
        SELECT * FROM read_parquet('{delta_path}')
      )
    )
    WHERE rn = 1;
    """)
    con.execute(f"""
    COPY (SELECT * FROM __merge) TO '{tmp_out}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)
    con.close()

    os.replace(tmp_out, path)
    os.remove(delta_path)

def upsert_dim_npi(
    dim_npi_new: pl.DataFrame,
    out_path: Path,
    big_table_threshold_rows: int = 3_000_000
) -> None:
    """
    Upsert into dim_npi.parquet keyed by ['npi'].
    Uses DuckDB merge when table is large to stay memory-efficient.
    """
    keys = ["npi"]
    if out_path.exists():
        # small optimization: anti-join by 'npi' first to shrink delta
        try:
            existing_ids = pl.read_parquet(str(out_path), columns=["npi"]).unique()
            dim_npi_new = dim_npi_new.join(existing_ids, on="npi", how="anti")
        except Exception:
            pass

    if dim_npi_new.is_empty():
        return

    # choose engine based on current size
    use_duckdb = out_path.exists()
    if use_duckdb:
        # Rough size heuristic by rowcount-only read
        try:
            n_old = pl.read_parquet(str(out_path), columns=["npi"]).height
        except Exception:
            n_old = big_table_threshold_rows + 1
        if n_old >= big_table_threshold_rows:
            _duckdb_merge_parquet(dim_npi_new, out_path, keys)
        else:
            _append_unique_parquet(dim_npi_new, out_path, keys)
    else:
        _append_unique_parquet(dim_npi_new, out_path, keys)

def upsert_dim_npi_address(
    addr_new: pl.DataFrame,
    out_path: Path,
    big_table_threshold_rows: int = 5_000_000
) -> None:
    """
    Upsert into dim_npi_address.parquet keyed by ['npi','address_purpose','address_hash'].
    Merges via DuckDB for large tables.
    """
    keys = ["npi","address_purpose","address_hash"]
    if out_path.exists():
        try:
            existing = pl.read_parquet(str(out_path), columns=keys).unique()
            addr_new = addr_new.join(existing, on=keys, how="anti")
        except Exception:
            pass

    if addr_new.is_empty():
        return

    # choose engine
    use_duckdb = out_path.exists()
    if use_duckdb:
        try:
            n_old = pl.read_parquet(str(out_path), columns=["npi"]).height
        except Exception:
            n_old = big_table_threshold_rows + 1
        if n_old >= big_table_threshold_rows:
            _duckdb_merge_parquet(addr_new, out_path, keys)
        else:
            _append_unique_parquet(addr_new, out_path, keys)
    else:
        _append_unique_parquet(addr_new, out_path, keys)

# ------------- PUBLIC API ------------------
def add_npi_to_dims(npi: str, data_dir: str | Path, dim_npi_filename: str = "dim_npi.parquet") -> bool:
    """
    High-level helper:
    - fetch NPPES record for `npi`
    - normalize to dim rows
    - upsert dim_npi + dim_npi_address

    Args:
        npi: NPI number to fetch
        data_dir: Path to data directory (contains dims subdirectory)
        dim_npi_filename: Custom filename for dim_npi.parquet (default: "dim_npi.parquet")

    Returns True if anything was written; False if nothing new or not found.
    """
    rec = fetch_nppes_record(str(npi))
    if not rec:
        sys.stderr.write(f"[INFO] NPI {npi} not found in NPPES.\n")
        return False

    dim_df, addr_df = normalize_nppes_result(str(npi), rec)
    dim_path, addr_path = dim_paths(Path(data_dir), dim_npi_filename)

    before_n = (pl.read_parquet(str(dim_path), columns=["npi"]).height
                if dim_path.exists() else 0)
    upsert_dim_npi(dim_df, dim_path)

    before_a = (pl.read_parquet(str(addr_path), columns=["npi"]).height
                if addr_path.exists() else 0)
    upsert_dim_npi_address(addr_df, addr_path)

    after_n = pl.read_parquet(str(dim_path), columns=["npi"]).height
    after_a = pl.read_parquet(str(addr_path), columns=["npi"]).height

    wrote = (after_n > before_n) or (after_a > before_a)
    return wrote

def add_many_npis(npis: List[str], data_dir: str | Path, sleep: float = 0.0, dim_npi_filename: str = "dim_npi.parquet") -> Tuple[int,int]:
    """
    Fetch/insert a list of NPIs sequentially (low memory).
    
    Args:
        npis: List of NPI numbers to fetch
        data_dir: Path to data directory (contains dims subdirectory)
        sleep: Sleep time between requests (seconds)
        dim_npi_filename: Custom filename for dim_npi.parquet (default: "dim_npi.parquet")
    
    Returns (count_found, count_written).
    """
    found = 0
    wrote = 0
    for npi in npis:
        rec = fetch_nppes_record(str(npi))
        if not rec:
            continue
        found += 1
        dim_df, addr_df = normalize_nppes_result(str(npi), rec)
        dim_path, addr_path = dim_paths(Path(data_dir), dim_npi_filename)
        upsert_dim_npi(dim_df, dim_path)
        upsert_dim_npi_address(addr_df, addr_path)
        wrote += 1
        if sleep > 0:
            time.sleep(sleep)
    return found, wrote

# ------------- NOTEBOOK CONVENIENCE FUNCTIONS -----------------
def get_npi_info(npi: str, data_dir: str | Path = "data", dim_npi_filename: str = "dim_npi.parquet") -> Optional[Dict[str, Any]]:
    """
    Get NPI information from existing dim_npi table or fetch from NPPES if not found.
    
    Args:
        npi: NPI number to look up
        data_dir: Path to data directory (contains dims subdirectory)
        dim_npi_filename: Custom filename for dim_npi.parquet
    
    Returns:
        Dictionary with NPI information or None if not found
    """
    dim_path, _ = dim_paths(Path(data_dir), dim_npi_filename)
    
    # First check if NPI exists in our dim table
    if dim_path.exists():
        try:
            existing = pl.read_parquet(str(dim_path)).filter(pl.col("npi") == str(npi))
            if existing.height > 0:
                return existing.to_dicts()[0]
        except Exception:
            pass
    
    # If not found, fetch from NPPES
    rec = fetch_nppes_record(str(npi))
    if not rec:
        return None
    
    dim_df, _ = normalize_nppes_result(str(npi), rec)
    return dim_df.to_dicts()[0] if dim_df.height > 0 else None

def list_available_npis(data_dir: str | Path = "data", dim_npi_filename: str = "dim_npi.parquet", limit: int = 100) -> pl.DataFrame:
    """
    List available NPIs in the dim_npi table.
    
    Args:
        data_dir: Path to data directory (contains dims subdirectory)
        dim_npi_filename: Custom filename for dim_npi.parquet
        limit: Maximum number of NPIs to return
    
    Returns:
        Polars DataFrame with NPI information
    """
    dim_path, _ = dim_paths(Path(data_dir), dim_npi_filename)
    
    if not dim_path.exists():
        return pl.DataFrame()
    
    return pl.read_parquet(str(dim_path)).head(limit)

def batch_update_npis_from_fact(data_dir: str | Path = "data", 
                               fact_path: str = "data/gold/fact_rate.parquet",
                               dim_npi_filename: str = "dim_npi.parquet",
                               sleep: float = 0.1) -> Tuple[int, int]:
    """
    Extract unique NPIs from fact table and update dim_npi table.
    
    Args:
        data_dir: Path to data directory (contains dims subdirectory)
        fact_path: Path to fact_rate.parquet
        dim_npi_filename: Custom filename for dim_npi.parquet
        sleep: Sleep time between NPPES requests
    
    Returns:
        Tuple of (found_count, written_count)
    """
    # Read fact table and extract unique NPIs from xref_pg_member_npi
    xref_path = Path(data_dir) / "xrefs" / "xref_pg_member_npi.parquet"
    
    if not xref_path.exists():
        print(f"xref_pg_member_npi not found at {xref_path}")
        return 0, 0
    
    xref_df = pl.read_parquet(str(xref_path))
    unique_npis = xref_df.select("npi").unique().to_series().to_list()
    
    print(f"Found {len(unique_npis)} unique NPIs in xref_pg_member_npi")
    
    # Update dim_npi with these NPIs
    return add_many_npis(unique_npis, data_dir, sleep=sleep, dim_npi_filename=dim_npi_filename)

def create_npi_placeholder(npi: str) -> pl.DataFrame:
    """Create a placeholder NPI record with nppes_fetched=False."""
    placeholder_row = {
        "npi": str(npi),
        "enumeration_type": None,
        "status": None,
        "organization_name": None,
        "first_name": None,
        "last_name": None,
        "credential": None,
        "sole_proprietor": None,
        "enumeration_date": None,
        "last_updated": None,
        "replacement_npi": None,
        "nppes_fetched": False,
        "nppes_fetch_date": None,
        "primary_taxonomy_code": None,
        "primary_taxonomy_desc": None,
        "primary_taxonomy_state": None,
        "primary_taxonomy_license": None,
    }
    return pl.DataFrame([placeholder_row])

def add_npi_placeholders(npis: List[str], data_dir: str | Path, dim_npi_filename: str = "dim_npi.parquet") -> int:
    """
    Add placeholder records for NPIs that don't exist in dim_npi yet.
    These placeholders have nppes_fetched=False and can be backfilled later.
    
    Args:
        npis: List of NPI numbers to add as placeholders
        data_dir: Path to data directory (contains dims subdirectory)
        dim_npi_filename: Custom filename for dim_npi.parquet
    
    Returns:
        Number of placeholders added
    """
    dim_path, _ = dim_paths(Path(data_dir), dim_npi_filename)
    
    # Get existing NPIs
    existing_npis = set()
    needs_migration = False
    
    if dim_path.exists():
        try:
            existing_df = pl.read_parquet(str(dim_path), columns=["npi"])
            existing_npis = set(existing_df.to_series().to_list())
            
            # Check if the file has the new columns
            full_df = pl.read_parquet(str(dim_path))
            if "nppes_fetched" not in full_df.columns:
                needs_migration = True
                print("Warning: dim_npi file needs migration to new format with nppes_fetched column")
                print("Run migrate_dim_npi_to_new_format() first, or the new functions won't work properly")
        except Exception as e:
            print(f"Error reading existing dim_npi: {e}")
            pass
    
    # Find NPIs that need placeholders
    new_npis = [npi for npi in npis if npi not in existing_npis]
    
    if not new_npis:
        return 0
    
    # Create placeholders
    placeholders = []
    for npi in new_npis:
        placeholders.append(create_npi_placeholder(npi))
    
    if placeholders:
        combined_placeholders = pl.concat(placeholders, how="vertical_relaxed")
        upsert_dim_npi(combined_placeholders, dim_path)
    
    return len(new_npis)

def backfill_missing_npis(data_dir: str | Path, dim_npi_filename: str = "dim_npi.parquet", sleep: float = 0.1) -> Tuple[int, int]:
    """
    Find NPIs with nppes_fetched=False and backfill them with NPPES data.
    
    Args:
        data_dir: Path to data directory (contains dims subdirectory)
        dim_npi_filename: Custom filename for dim_npi.parquet
        sleep: Sleep time between NPPES requests
    
    Returns:
        Tuple of (found_count, updated_count)
    """
    dim_path, _ = dim_paths(Path(data_dir), dim_npi_filename)
    
    if not dim_path.exists():
        print(f"dim_npi file not found at {dim_path}")
        return 0, 0
    
    # Find NPIs that need backfilling
    try:
        df = pl.read_parquet(str(dim_path))
        missing_npis = df.filter(
            (pl.col("nppes_fetched") == False) | (pl.col("nppes_fetched").is_null())
        ).select("npi").to_series().to_list()
    except Exception as e:
        print(f"Error reading dim_npi file: {e}")
        return 0, 0
    
    if not missing_npis:
        print("No NPIs need backfilling")
        return 0, 0
    
    print(f"Found {len(missing_npis)} NPIs that need backfilling")
    
    # Backfill each NPI
    found = 0
    updated = 0
    
    for npi in missing_npis:
        rec = fetch_nppes_record(str(npi))
        if rec:
            found += 1
            dim_df, addr_df = normalize_nppes_result(str(npi), rec, nppes_fetched=True)
            upsert_dim_npi(dim_df, dim_path)
            upsert_dim_npi_address(addr_df, dim_path.parent / "dim_npi_address.parquet")
            updated += 1
        else:
            print(f"NPI {npi} not found in NPPES")
        
        if sleep > 0:
            time.sleep(sleep)
    
    return found, updated

def sync_npis_from_xref(data_dir: str | Path, dim_npi_filename: str = "dim_npi.parquet", sleep: float = 0.1) -> Tuple[int, int, int]:
    """
    Complete sync: Add placeholders for missing NPIs, then backfill all unfetched ones.
    
    Args:
        data_dir: Path to data directory (contains dims subdirectory)
        dim_npi_filename: Custom filename for dim_npi.parquet
        sleep: Sleep time between NPPES requests
    
    Returns:
        Tuple of (placeholders_added, found_count, updated_count)
    """
    # Step 1: Get all NPIs from xref_pg_member_npi
    xref_path = Path(data_dir) / "xrefs" / "xref_pg_member_npi.parquet"
    
    if not xref_path.exists():
        print(f"xref_pg_member_npi not found at {xref_path}")
        return 0, 0, 0
    
    xref_df = pl.read_parquet(str(xref_path))
    all_npis = xref_df.select("npi").unique().to_series().to_list()
    
    print(f"Found {len(all_npis)} unique NPIs in xref_pg_member_npi")
    
    # Step 2: Add placeholders for missing NPIs
    placeholders_added = add_npi_placeholders(all_npis, data_dir, dim_npi_filename)
    print(f"Added {placeholders_added} placeholder records")
    
    # Step 3: Backfill unfetched NPIs
    found, updated = backfill_missing_npis(data_dir, dim_npi_filename, sleep)
    
    return placeholders_added, found, updated

# ------------- CLI -------------------------
def _load_npis_from_file(path: str) -> List[str]:
    out: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = "".join(ch for ch in line.strip() if ch.isdigit())
            if s:
                out.append(s)
    return out

def main():
    ap = argparse.ArgumentParser(description="NPPES → dim_npi upsert with backfill support")
    ap.add_argument("--data-dir", required=True, help="Path to your data directory (contains dims subdirectory)")
    ap.add_argument("--add", nargs="*", help="One or more NPIs to add")
    ap.add_argument("--file", help="Path to a text file with one NPI per line")
    ap.add_argument("--sleep", type=float, default=0.0, help="Optional sleep between requests (seconds)")
    ap.add_argument("--dim-npi-filename", default="dim_npi.parquet", help="Custom filename for dim_npi.parquet")
    ap.add_argument("--backfill", action="store_true", help="Backfill NPIs with nppes_fetched=False")
    ap.add_argument("--sync", action="store_true", help="Complete sync: add placeholders from xref_pg_member_npi, then backfill")
    ap.add_argument("--placeholders-only", action="store_true", help="Only add placeholders for NPIs from xref_pg_member_npi")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"Creating data directory {data_dir} ...")
        data_dir.mkdir(parents=True, exist_ok=True)

    # Handle sync operations
    if args.sync:
        placeholders, found, updated = sync_npis_from_xref(data_dir, args.dim_npi_filename, args.sleep)
        print(f"Sync complete. Placeholders added: {placeholders}, Found: {found}, Updated: {updated}")
        return 0
    
    if args.placeholders_only:
        xref_path = data_dir / "xrefs" / "xref_pg_member_npi.parquet"
        if not xref_path.exists():
            print(f"xref_pg_member_npi not found at {xref_path}")
            return 1
        
        xref_df = pl.read_parquet(str(xref_path))
        all_npis = xref_df.select("npi").unique().to_series().to_list()
        placeholders_added = add_npi_placeholders(all_npis, data_dir, args.dim_npi_filename)
        print(f"Added {placeholders_added} placeholder records")
        return 0
    
    if args.backfill:
        found, updated = backfill_missing_npis(data_dir, args.dim_npi_filename, args.sleep)
        print(f"Backfill complete. Found: {found}, Updated: {updated}")
        return 0

    # Handle regular add operations
    npis: List[str] = []
    if args.add:
        npis.extend(["".join(ch for ch in x if ch.isdigit()) for x in args.add])
    if args.file:
        npis.extend(_load_npis_from_file(args.file))

    if not npis:
        print("Nothing to do. Provide --add NPIs, --file path, --backfill, --sync, or --placeholders-only.")
        return 0

    found, wrote = add_many_npis(npis, data_dir, sleep=args.sleep, dim_npi_filename=args.dim_npi_filename)
    print(f"Done. Found={found}, Wrote={wrote}.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
