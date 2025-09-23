# Cell 1: imports & config
import os, json, time, hashlib, re
from typing import Optional, Dict, Any, Tuple

import polars as pl
import requests
from tqdm.auto import tqdm

# ---- paths (edit as needed) ----
# Get the project root directory (2 levels up from this script)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

DIM_NPI_ADDRESS_PARQUET = os.path.join(PROJECT_ROOT, "data", "dims", "dim_npi_address.parquet")         # input
ENRICHED_PARQUET        = os.path.join(PROJECT_ROOT, "data", "dims", "dim_npi_address_geolocation.parquet")  # output
CACHE_JSONL             = os.path.join(PROJECT_ROOT, "data", "dims", "dim_npi_geocoder_cache.jsonl")     # local cache

# ---- census geocoder settings ----
CENSUS_ENDPOINT = (
    "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
)
BENCHMARK = "Public_AR_Current"
VINTAGE   = "Current_Current"

# polite rate limit: ~5 req/sec
SLEEP_BETWEEN_CALLS_SEC = 0.2

# request settings
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "dim-npi-address-enricher/1.0"})
TIMEOUT = 15
MAX_RETRIES = 3


# Cell 2: helpers

ZIP9_RE = re.compile(r"^\s*(\d{5})-?(\d{4})\s*$")

def normalize_zip(z: Optional[str]) -> Optional[str]:
    if not z:
        return None
    z = str(z).strip()
    m = ZIP9_RE.match(z)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    # keep 5-digit or pass-through if weird; Census is fairly tolerant
    return z[:5] if z.isdigit() and len(z) >= 5 else z

def oneline_address(address_1: str,
                    city: Optional[str],
                    state: Optional[str],
                    zip_code: Optional[str]) -> str:
    parts = [address_1 or ""]
    if city:  parts.append(str(city))
    if state: parts.append(str(state))
    if zip_code: parts.append(str(zip_code))
    # collapse multiple spaces
    return " ".join(" ".join(parts).split())

# ---- simple JSONL cache keyed by address_hash (or oneline fallback) ----

def load_cache() -> Dict[str, Dict[str, Any]]:
    cache = {}
    if os.path.exists(CACHE_JSONL):
        with open(CACHE_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    cache[obj["key"]] = obj["value"]
                except Exception:
                    pass
    return cache

def append_cache_entry(key: str, value: Dict[str, Any]) -> None:
    with open(CACHE_JSONL, "a", encoding="utf-8") as f:
        f.write(json.dumps({"key": key, "value": value}) + "\n")


# Cell 3: API caller & parser

def call_census(oneline: str) -> Optional[dict]:
    params = {
        "address": oneline,
        "benchmark": BENCHMARK,
        "vintage": VINTAGE,
        "format": "json",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(CENSUS_ENDPOINT, params=params, timeout=TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            # transient?
            time.sleep(SLEEP_BETWEEN_CALLS_SEC * attempt)
        except requests.RequestException:
            time.sleep(SLEEP_BETWEEN_CALLS_SEC * attempt)
    return None

def pick_first(arr: list, key: str) -> Optional[dict]:
    if isinstance(arr, list) and arr:
        return arr[0]
    return None

def parse_geographies(j: dict) -> Dict[str, Any]:
    """
    Extracts:
      - latitude, longitude (float)
      - county_name, county_fips
      - stat_area_name (MSA/Micro preferred; CSA fallback)
    Returns empty fields (None) if something is missing.
    """
    out = {
        "latitude":  None,
        "longitude": None,
        "county_name": None,
        "county_fips": None,
        "stat_area_name": None,   # MSA/Micro else CSA
        "stat_area_code": None,   # CBSA code if available, else CSA code
        "matched_address": None,
    }
    try:
        matches = j.get("result", {}).get("addressMatches", [])
        if not matches:
            return out

        m0 = matches[0]
        coords = m0.get("coordinates") or {}
        out["longitude"] = coords.get("x")
        out["latitude"]  = coords.get("y")
        out["matched_address"] = m0.get("matchedAddress")

        geos = m0.get("geographies") or {}

        # County
        county = pick_first(geos.get("Counties", []), "NAME")
        if county:
            out["county_name"] = county.get("NAME")
            # county GEOID is 5-digit (state+county)
            out["county_fips"] = county.get("GEOID") or (county.get("STATE","") + county.get("COUNTY",""))

        # Statistical area (prefer MSA/Micro; fallback CSA)
        msa_key = "Metropolitan Statistical Areas/Micropolitan Statistical Areas"
        msa = pick_first(geos.get(msa_key, []), "NAME")
        if msa:
            out["stat_area_name"] = msa.get("NAME")
            out["stat_area_code"] = msa.get("GEOID")  # CBSA code
        else:
            csa = pick_first(geos.get("Combined Statistical Areas", []), "NAME")
            if csa:
                out["stat_area_name"] = csa.get("NAME")
                out["stat_area_code"] = csa.get("GEOID")  # CSA code

    except Exception:
        # leave Nones if anything odd happens
        pass

    return out


# Cell 4: load parquet and determine which rows need enrichment
df_input = pl.read_parquet(DIM_NPI_ADDRESS_PARQUET)

# Drop all MAILING address purpose rows
if "address_purpose" in df_input.columns:
    original_count = df_input.height
    df_input = df_input.filter(pl.col("address_purpose") != "MAILING")
    filtered_count = df_input.height
    print(f"Filtered out {original_count - filtered_count:,} MAILING address rows. Remaining: {filtered_count:,}")
else:
    print("Warning: 'address_purpose' column not found. No filtering applied.")

# ensure expected columns exist
required_cols = ["npi","address_1","city","state","postal_code","address_hash"]
missing = [c for c in required_cols if c not in df_input.columns]
if missing:
    raise ValueError(f"Missing required column(s) in {DIM_NPI_ADDRESS_PARQUET}: {missing}")

# normalize ZIP as a helper column (not persisted unless you want it)
df_input = df_input.with_columns(
    pl.col("postal_code").cast(pl.Utf8).map_elements(normalize_zip).alias("zip_norm")
)

# Check if enriched file exists and load it
df_enriched = None
if os.path.exists(ENRICHED_PARQUET):
    print(f"Found existing enriched file: {ENRICHED_PARQUET}")
    df_enriched = pl.read_parquet(ENRICHED_PARQUET)
    print(f"Existing enriched rows: {df_enriched.height:,}")
else:
    print(f"No existing enriched file found. Will create: {ENRICHED_PARQUET}")

# Define enrichment columns
enrichment_cols = {
    "latitude": pl.Float64,
    "longitude": pl.Float64,
    "county_name": pl.Utf8,
    "county_fips": pl.Utf8,
    "stat_area_name": pl.Utf8,
    "stat_area_code": pl.Utf8,
    "matched_address": pl.Utf8,
}

# Determine which rows need enrichment
if df_enriched is not None:
    # Get NPIs that are already enriched (check for both NPI and address_hash for more precision)
    enriched_npis = set(df_enriched.select("npi").to_series().to_list())
    # Filter input to only rows not yet enriched
    to_enrich = df_input.filter(~pl.col("npi").is_in(enriched_npis))
    print(f"Already enriched NPIs: {len(enriched_npis):,}")
    
    # Additional check: if someone wants to re-enrich failed geocodes (null lat/long), uncomment below
    # already_enriched_but_failed = df_enriched.filter(pl.col("latitude").is_null())
    # if already_enriched_but_failed.height > 0:
    #     failed_npis = set(already_enriched_but_failed.select("npi").to_series().to_list())
    #     to_retry = df_input.filter(pl.col("npi").is_in(failed_npis))
    #     to_enrich = pl.concat([to_enrich, to_retry])
    #     print(f"Re-enriching {to_retry.height:,} previously failed geocodes")
else:
    # No existing enriched data, enrich all input rows
    to_enrich = df_input

print(f"Total input rows: {df_input.height:,} | To enrich: {to_enrich.height:,}")


# Cell 5: apply enrichment with cache + polite rate limiting

cache = load_cache()

# Only proceed if there are rows to enrich
if to_enrich.height == 0:
    print("No rows need enrichment. All data is already enriched.")
    df_final = df_enriched if df_enriched is not None else df_input
else:
    print(f"Starting enrichment of {to_enrich.height:,} rows...")

    def make_cache_key(row: dict) -> str:
        """
        Prefer your existing address_hash, else hash the oneline string.
        """
        ah = row.get("address_hash")
        if ah and isinstance(ah, str) and len(ah) >= 16:
            return f"ah::{ah}"
        oneline = oneline_address(row.get("address_1",""), row.get("city"), row.get("state"), row.get("zip_norm"))
        return "ol::" + hashlib.md5(oneline.encode("utf-8")).hexdigest()

    updates = []

    for row in tqdm(to_enrich.to_dicts(), total=to_enrich.height):
        key = make_cache_key(row)
        if key in cache:
            enriched = cache[key]
        else:
            # build the address line
            ol = oneline_address(row.get("address_1",""), row.get("city"), row.get("state"), row.get("zip_norm"))
            if not ol.strip():
                continue  # nothing to do
            j = call_census(ol)
            enriched = parse_geographies(j) if j else {
                "latitude": None, "longitude": None, "county_name": None, "county_fips": None,
                "stat_area_name": None, "stat_area_code": None, "matched_address": None
            }
            cache[key] = enriched
            append_cache_entry(key, enriched)
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

        updates.append({
            "address_hash": row.get("address_hash"),
            "npi": row.get("npi"),
            **enriched
        })

    # Create enriched dataframe for new rows
    if updates:
        new_enriched = pl.DataFrame(updates)
        # Add all original columns to the enriched data
        new_enriched = to_enrich.join(new_enriched, on=["address_hash", "npi"], how="left")
        
        # Combine with existing enriched data if it exists
        if df_enriched is not None:
            df_final = pl.concat([df_enriched, new_enriched])
        else:
            df_final = new_enriched
    else:
        df_final = df_enriched if df_enriched is not None else df_input

    print("Enrichment complete.")


# Cell 6: save
df_final.write_parquet(ENRICHED_PARQUET)
print(f"Wrote: {ENRICHED_PARQUET} with {df_final.height:,} rows.")


# Cell 7: quick sanity check and summary
display_cols = [
    "npi","address_purpose","address_type","address_1","city","state","postal_code",
    "latitude","longitude","county_name","county_fips","stat_area_name","stat_area_code","matched_address"
]

print("\n" + "="*50)
print("ENRICHMENT SUMMARY")
print("="*50)
print(f"Total rows in final dataset: {df_final.height:,}")
if 'latitude' in df_final.columns:
    successful_geocodes = df_final.filter(pl.col("latitude").is_not_null()).height
    failed_geocodes = df_final.filter(pl.col("latitude").is_null()).height
    success_rate = (successful_geocodes / df_final.height * 100) if df_final.height > 0 else 0
    print(f"Successful geocodes: {successful_geocodes:,} ({success_rate:.1f}%)")
    print(f"Failed geocodes: {failed_geocodes:,} ({100-success_rate:.1f}%)")
print("="*50)

# Show sample of enriched data
print("\nSample of enriched data:")
df_final.select([c for c in display_cols if c in df_final.columns]).head(10)
