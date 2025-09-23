#!/usr/bin/env python3
"""
Fast NPPES data fetcher with parallel processing

This version uses threading to fetch multiple NPIs simultaneously,
significantly reducing the total time. It reads NPIs from the xref file
and only fetches those that haven't been fetched before.

Usage:
    python fetch_npi_data_fast.py --threads 5
    python fetch_npi_data_fast.py --threads 10 --limit 1000
"""

import sys
import time
import argparse
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import polars as pl

# Add current directory to path for imports
sys.path.append(str(Path(__file__).parent))

from utils_nppes import (
    fetch_nppes_record, 
    normalize_nppes_result, 
    upsert_dim_npi_address
)

class NPPESFetcher:
    def __init__(self, threads: int = 5, sleep: float = 0.1):
        # Set up paths relative to this script's location (ETL/utils)
        # Back out two directories to get to project root, then down to data/
        base_path = Path(__file__).resolve().parent.parent.parent  # ETL/utils/../..
        self.data_dir = base_path / "data"
        self.dims_path = self.data_dir / "dims"
        self.xrefs_path = self.data_dir / "xrefs"
        self.dim_path = self.dims_path / "dim_npi_temp.parquet"
        self.addr_path = self.dims_path / "dim_npi_address.parquet"
        self.xref_path = self.xrefs_path / "xref_pg_member_npi.parquet"
        self.threads = threads
        self.sleep = sleep
        self.results = []
        self.lock = threading.Lock()
        self.processed = 0
        self.found = 0
        self.errors = 0

    def get_npis_to_fetch(self):
        """Get NPIs from xref file that haven't been fetched yet."""
        # Read xref file to get all NPIs
        xref_df = pl.read_parquet(str(self.xref_path))
        all_npis = set(xref_df.select("npi").to_series().to_list())
        
        print(f"📊 Found {len(all_npis)} unique NPIs in xref file")
        
        # Check if dim_npi file exists (try both original and temp versions)
        original_dim_path = self.dims_path / "dim_npi.parquet"
        if not self.dim_path.exists() and not original_dim_path.exists():
            print("📝 No existing dim_npi file found - will fetch all NPIs")
            return list(all_npis)
        
        # Read existing dim_npi data (try temp first, then original)
        if self.dim_path.exists():
            existing_df = pl.read_parquet(str(self.dim_path))
        elif original_dim_path.exists():
            existing_df = pl.read_parquet(str(original_dim_path))
            print("📝 Using original dim_npi.parquet as starting point")
        else:
            print("📝 No existing dim_npi file found - will fetch all NPIs")
            return list(all_npis)
        
        # Get NPIs that have already been fetched
        if "nppes_fetched" in existing_df.columns:
            fetched_npis = set(
                existing_df.filter(pl.col("nppes_fetched") == True)
                .select("npi").to_series().to_list()
            )
        else:
            # If no nppes_fetched column, assume all existing NPIs have been fetched
            fetched_npis = set(existing_df.select("npi").to_series().to_list())
        
        # Find NPIs that need fetching
        npis_to_fetch = all_npis - fetched_npis
        
        print(f"✅ Already fetched: {len(fetched_npis)} NPIs")
        print(f"🔄 Need to fetch: {len(npis_to_fetch)} NPIs")
        
        return list(npis_to_fetch)
        
    def fetch_single_npi(self, npi: str):
        """Fetch data for a single NPI."""
        try:
            # Fetch from NPPES
            rec = fetch_nppes_record(str(npi))
            if rec:
                # Normalize the data
                dim_df, addr_df = normalize_nppes_result(str(npi), rec, nppes_fetched=True)
                
                with self.lock:
                    self.results.append((dim_df, addr_df, npi))
                    self.found += 1
            else:
                with self.lock:
                    print(f"⚠️  NPI {npi} not found in NPPES")
            
            # Sleep to be respectful to API
            if self.sleep > 0:
                time.sleep(self.sleep)
                
        except Exception as e:
            with self.lock:
                self.errors += 1
                print(f"❌ Error fetching NPI {npi}: {e}")
    
    def update_dim_npi(self, results_batch):
        """Update dim_npi file with a batch of results - MEMORY EFFICIENT VERSION."""
        if not results_batch:
            return
            
        try:
            # Ensure dims directory exists
            self.dims_path.mkdir(parents=True, exist_ok=True)
            
            # Collect all new data first
            new_dim_dataframes = []
            new_addr_dataframes = []
            npis_to_remove = set()
            
            for dim_df, addr_df, npi in results_batch:
                new_dim_dataframes.append(dim_df)
                if addr_df.height > 0:
                    new_addr_dataframes.append(addr_df)
                npis_to_remove.add(str(npi))
            
            # Process dim_npi updates
            if new_dim_dataframes:
                new_combined_dim_df = pl.concat(new_dim_dataframes, how="vertical_relaxed")
                
                # Read existing dim_npi data only once
                if self.dim_path.exists():
                    existing_dim_df = pl.read_parquet(str(self.dim_path))
                    
                    # Remove all NPIs that we're updating
                    if npis_to_remove:
                        existing_dim_df = existing_dim_df.filter(~pl.col("npi").is_in(list(npis_to_remove)))
                    
                    # Align schemas and concatenate
                    all_columns = list(set(existing_dim_df.columns + new_combined_dim_df.columns))
                    
                    for col in all_columns:
                        if col not in existing_dim_df.columns:
                            existing_dim_df = existing_dim_df.with_columns(pl.lit(None).alias(col))
                        if col not in new_combined_dim_df.columns:
                            new_combined_dim_df = new_combined_dim_df.with_columns(pl.lit(None).alias(col))
                    
                    existing_dim_df = existing_dim_df.select(all_columns)
                    new_combined_dim_df = new_combined_dim_df.select(all_columns)
                    
                    final_dim_df = pl.concat([existing_dim_df, new_combined_dim_df], how="vertical_relaxed")
                else:
                    final_dim_df = new_combined_dim_df
                
                # Write dim_npi data once
                final_dim_df.write_parquet(str(self.dim_path))
            
            # Process address updates - OPTIMIZED APPROACH
            if new_addr_dataframes:
                new_combined_addr_df = pl.concat(new_addr_dataframes, how="vertical_relaxed")
                
                # Fast append approach (no deduplication during processing for speed)
                if self.addr_path.exists():
                    try:
                        existing_addr_df = pl.read_parquet(str(self.addr_path))
                        combined_addr_df = pl.concat([existing_addr_df, new_combined_addr_df], how="vertical_relaxed")
                        combined_addr_df.write_parquet(str(self.addr_path), compression="zstd")
                    except Exception as e:
                        # If there's any issue, just write new data
                        print(f"⚠️  Address file issue, writing new data: {e}")
                        new_combined_addr_df.write_parquet(str(self.addr_path), compression="zstd")
                else:
                    new_combined_addr_df.write_parquet(str(self.addr_path), compression="zstd")
                
                print(f"📝 Updated address file with {len(new_addr_dataframes)} records")
                    
        except Exception as e:
            print(f"❌ Error updating dim_npi: {e}")
    
    def fetch_npis_parallel(self, npis: list, batch_size: int = 1000):
        """Fetch NPIs in parallel with optimized chunking."""
        total_npis = len(npis)
        print(f"🚀 Starting parallel fetch for {total_npis} NPIs")
        print(f"🧵 Using {self.threads} threads")
        print(f"⏱️  Sleep between requests: {self.sleep}s")
        print(f"📦 Chunk size: {batch_size} (optimized for fewer file I/O operations)")
        print()
        
        # Process in larger chunks to minimize file I/O
        for i in range(0, total_npis, batch_size):
            chunk_npis = npis[i:i + batch_size]
            chunk_num = i // batch_size + 1
            total_chunks = (total_npis + batch_size - 1) // batch_size
            
            print(f"📦 Processing chunk {chunk_num}/{total_chunks} ({len(chunk_npis)} NPIs)")
            
            # Clear results for this chunk
            self.results = []
            
            # Process chunk in parallel
            with ThreadPoolExecutor(max_workers=self.threads) as executor:
                futures = [executor.submit(self.fetch_single_npi, npi) for npi in chunk_npis]
                
                # Wait for completion
                for future in as_completed(futures):
                    self.processed += 1
                    if self.processed % 200 == 0:
                        progress = (self.processed / total_npis) * 100
                        print(f"📊 Progress: {self.processed}/{total_npis} ({progress:.1f}%) - Found: {self.found}, Errors: {self.errors}")
            
            # Update files with this chunk (single file I/O per chunk)
            if self.results:
                print(f"💾 Updating files with {len(self.results)} results...")
                self.update_dim_npi(self.results)
            
            print(f"✅ Chunk {chunk_num} complete")
            print()
        
        print("🎉 Parallel fetch complete!")
        print(f"   📈 Total processed: {self.processed}")
        print(f"   ✅ Found: {self.found}")
        print(f"   ❌ Errors: {self.errors}")
        
        # Address files are already merged during processing

def main():
    parser = argparse.ArgumentParser(description="Fast parallel NPPES data fetcher")
    parser.add_argument("--threads", type=int, default=5, help="Number of parallel threads")
    parser.add_argument("--sleep", type=float, default=0.1, help="Sleep between requests (seconds)")
    parser.add_argument("--limit", type=int, help="Limit number of NPIs to process")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for processing")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    
    args = parser.parse_args()
    
    # Check if we're in the right directory (data/dims must exist)
    base_path = Path(__file__).resolve().parent.parent.parent
    data_dir = base_path / "data"
    dims_dir = data_dir / "dims"
    if not dims_dir.exists():
        print(f"❌ Error: {dims_dir} directory not found")
        print("   Please run this script from within the repo so that data/dims exists")
        return 1
    
    # Create fetcher to get NPIs that need fetching
    fetcher = NPPESFetcher(
        threads=args.threads,
        sleep=args.sleep
    )
    
    # Get NPIs that need fetching
    npis_to_fetch = fetcher.get_npis_to_fetch()
    
    if args.limit:
        npis_to_fetch = npis_to_fetch[:args.limit]
    
    if not npis_to_fetch:
        print("✅ No NPIs need fetching")
        return 0
    
    # Confirmation prompt
    if not args.yes:
        limit_text = f" (limited to {args.limit})" if args.limit else ""
        estimated_time = len(npis_to_fetch) * args.sleep / args.threads / 60
        
        print(f"⚠️  WARNING: This will fetch NPPES data for {len(npis_to_fetch)} NPIs{limit_text}")
        print(f"🧵 Using {args.threads} parallel threads")
        print(f"⏰ Estimated time: {estimated_time:.1f} minutes")
        print(f"🌐 This will make {len(npis_to_fetch)} API calls to NPPES")
        print()
        
        response = input("Do you want to continue? (y/N): ")
        if response.lower() != 'y':
            print("❌ Cancelled by user")
            return 0
    
    try:
        fetcher.fetch_npis_parallel(npis_to_fetch, args.batch_size)
        return 0
    except KeyboardInterrupt:
        print("\n⏹️  Interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
