#!/usr/bin/env python3
"""
Simple CSV-based NPPES data fetcher - replacement for fetch_npi_data.py

This version reads from the NPPES CSV file on-demand without loading it into memory.
It's a drop-in replacement for the API-based fetcher.

Usage:
    python fetch_npi_data_csv_simple.py --threads 5
    python fetch_npi_data_csv_simple.py --threads 10 --limit 1000
"""

import sys
import time
import argparse
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import polars as pl
import pandas as pd
import zipfile
from typing import Dict, List, Optional, Tuple
import logging

# Add current directory to path for imports
sys.path.append(str(Path(__file__).parent))

from utils_nppes import (
    normalize_nppes_result, 
    upsert_dim_npi_address,
    upsert_dim_npi
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/nppes_csv_fetch_simple.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NPPESCSVSimpleFetcher:
    def __init__(self, csv_path: str, threads: int = 5):
        # Set up paths relative to this script's location (ETL/utils)
        base_path = Path(__file__).resolve().parent.parent.parent  # ETL/utils/../..
        self.data_dir = base_path / "data"
        self.dims_path = self.data_dir / "dims"
        self.xrefs_path = self.data_dir / "xrefs"
        self.dim_path = self.dims_path / "dim_npi.parquet"
        self.addr_path = self.dims_path / "dim_npi_address.parquet"
        self.xref_path = self.xrefs_path / "xref_pg_member_npi.parquet"
        
        self.csv_path = csv_path
        self.threads = threads
        self.results = []
        self.lock = threading.Lock()
        self.processed = 0
        self.found = 0
        self.errors = 0
        
        # Create logs directory
        Path("logs").mkdir(exist_ok=True)

    def find_npi_in_csv(self, npi: str) -> Optional[pd.Series]:
        """Find a specific NPI in the CSV file without loading everything into memory."""
        try:
            if self.csv_path.endswith('.zip'):
                with zipfile.ZipFile(self.csv_path) as z:
                    csv_filename = "npidata_pfile_20050523-20250810.csv"
                    with z.open(csv_filename) as f:
                        # Read CSV in chunks to find the NPI
                        chunk_size = 50000  # Larger chunks for better performance
                        for chunk in pd.read_csv(f, chunksize=chunk_size, dtype=str):
                            if npi in chunk['NPI'].values:
                                # Found the NPI, return the row
                                row = chunk[chunk['NPI'] == npi].iloc[0]
                                return row
            else:
                # Read CSV directly in chunks
                chunk_size = 50000
                for chunk in pd.read_csv(self.csv_path, chunksize=chunk_size, dtype=str):
                    if npi in chunk['NPI'].values:
                        # Found the NPI, return the row
                        row = chunk[chunk['NPI'] == npi].iloc[0]
                        return row
            
            return None  # NPI not found
            
        except Exception as e:
            logger.error(f"Error searching for NPI {npi} in CSV: {e}")
            return None

    def csv_row_to_nppes_record(self, row: pd.Series) -> Dict:
        """Convert a CSV row to NPPES API-like record format."""
        # This mimics the structure that normalize_nppes_result expects
        basic = {
            "organization_name": row.get('Provider Organization Name (Legal Business Name)'),
            "first_name": row.get('Provider First Name'),
            "last_name": row.get('Provider Last Name (Legal Name)'),
            "credential": row.get('Provider Credential Text'),
            "sole_proprietor": row.get('Provider Sole Proprietor'),
            "status": "A" if pd.isna(row.get('NPI Deactivation Date')) else "I",
            "enumeration_date": row.get('Provider Enumeration Date'),
            "last_updated": row.get('Last Update Date'),
            "replacement_npi": row.get('Replacement NPI')
        }
        
        # Clean up None values
        for key, value in basic.items():
            if pd.isna(value):
                basic[key] = None
        
        # Determine enumeration type
        entity_type = row.get('Entity Type Code')
        if pd.isna(entity_type):
            enumeration_type = None
        elif entity_type == 1:
            enumeration_type = "NPI-1"
        elif entity_type == 2:
            enumeration_type = "NPI-2"
        else:
            enumeration_type = f"NPI-{entity_type}"
        
        # Taxonomy information
        taxonomies = []
        for i in range(1, 16):  # NPPES has up to 15 taxonomy codes
            code_col = f'Healthcare Provider Taxonomy Code_{i}'
            desc_col = f'Healthcare Provider Taxonomy Description_{i}'
            license_col = f'Provider License Number_{i}'
            state_col = f'Provider License Number State Code_{i}'
            
            if code_col in row and pd.notna(row[code_col]):
                taxonomies.append({
                    "code": row[code_col],
                    "desc": row.get(desc_col, ''),
                    "license": row.get(license_col),
                    "state": row.get(state_col),
                    "primary": i == 1
                })
        
        # Address information
        addresses = []
        
        # Mailing address
        mailing_addr = {
            "address_1": row.get('Provider First Line Business Mailing Address'),
            "address_2": row.get('Provider Second Line Business Mailing Address'),
            "city": row.get('Provider Business Mailing Address City Name'),
            "state": row.get('Provider Business Mailing Address State Name'),
            "postal_code": row.get('Provider Business Mailing Address Postal Code'),
            "country_code": row.get('Provider Business Mailing Address Country Code (If outside U.S.)', 'US'),
            "telephone_number": row.get('Provider Business Mailing Address Telephone Number'),
            "fax_number": row.get('Provider Business Mailing Address Fax Number'),
            "address_purpose": "MAILING"
        }
        
        # Only add if we have at least address_1
        if pd.notna(mailing_addr["address_1"]):
            addresses.append(mailing_addr)
        
        # Location address
        location_addr = {
            "address_1": row.get('Provider First Line Business Practice Location Address'),
            "address_2": row.get('Provider Second Line Business Practice Location Address'),
            "city": row.get('Provider Business Practice Location Address City Name'),
            "state": row.get('Provider Business Practice Location Address State Name'),
            "postal_code": row.get('Provider Business Practice Location Address Postal Code'),
            "country_code": row.get('Provider Business Practice Location Address Country Code (If outside U.S.)', 'US'),
            "telephone_number": row.get('Provider Business Practice Location Address Telephone Number'),
            "fax_number": row.get('Provider Business Practice Location Address Fax Number'),
            "address_purpose": "LOCATION"
        }
        
        # Only add if we have at least address_1
        if pd.notna(location_addr["address_1"]):
            addresses.append(location_addr)
        
        # Clean up None values in addresses
        for addr in addresses:
            for key, value in addr.items():
                if pd.isna(value):
                    addr[key] = None
        
        return {
            "basic": basic,
            "enumeration_type": enumeration_type,
            "taxonomies": taxonomies,
            "addresses": addresses
        }

    def get_npis_to_fetch(self):
        """Get NPIs from xref file that haven't been fetched yet."""
        # Read xref file to get all NPIs
        xref_df = pl.read_parquet(str(self.xref_path))
        all_npis = set(xref_df.select("npi").to_series().to_list())
        
        logger.info(f"Found {len(all_npis)} unique NPIs in xref file")
        
        # Check if dim_npi file exists
        if not self.dim_path.exists():
            logger.info("No existing dim_npi file found - will fetch all NPIs")
            return list(all_npis)
        
        # Read existing dim_npi data
        existing_df = pl.read_parquet(str(self.dim_path))
        
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
        
        logger.info(f"Already fetched: {len(fetched_npis)} NPIs")
        logger.info(f"Need to fetch: {len(npis_to_fetch)} NPIs")
        
        return list(npis_to_fetch)
        
    def fetch_single_npi(self, npi: str):
        """Fetch data for a single NPI from CSV."""
        try:
            # Search for NPI in CSV without loading everything into memory
            row = self.find_npi_in_csv(npi)
            
            if row is not None:
                # Convert to NPPES API-like format
                nppes_record = self.csv_row_to_nppes_record(row)
                
                # Normalize the data using existing function
                dim_df, addr_df = normalize_nppes_result(str(npi), nppes_record, nppes_fetched=True)
                
                with self.lock:
                    self.results.append((dim_df, addr_df, npi))
                    self.found += 1
            else:
                with self.lock:
                    logger.warning(f"NPI {npi} not found in CSV")
            
        except Exception as e:
            with self.lock:
                self.errors += 1
                logger.error(f"Error fetching NPI {npi}: {e}")
    
    def update_dim_npi_simple(self, results_batch):
        """Simple update method that avoids complex merging."""
        if not results_batch:
            return
            
        try:
            # Ensure dims directory exists
            self.dims_path.mkdir(parents=True, exist_ok=True)
            
            # Process each result individually to avoid schema conflicts
            for dim_df, addr_df, npi in results_batch:
                try:
                    # Simple append for dim_npi - let Polars handle duplicates
                    if self.dim_path.exists():
                        # Read existing data
                        existing_df = pl.read_parquet(str(self.dim_path))
                        
                        # Remove any existing record for this NPI
                        existing_df = existing_df.filter(pl.col("npi") != str(npi))
                        
                        # Append new record
                        combined_df = pl.concat([existing_df, dim_df], how="vertical_relaxed")
                        
                        # Write back
                        combined_df.write_parquet(str(self.dim_path))
                    else:
                        # First time - just write the new data
                        dim_df.write_parquet(str(self.dim_path))
                    
                    # Simple append for addresses
                    if addr_df.height > 0:
                        if self.addr_path.exists():
                            # Read existing addresses
                            existing_addr_df = pl.read_parquet(str(self.addr_path))
                            
                            # Remove any existing addresses for this NPI
                            existing_addr_df = existing_addr_df.filter(pl.col("npi") != str(npi))
                            
                            # Append new addresses
                            combined_addr_df = pl.concat([existing_addr_df, addr_df], how="vertical_relaxed")
                            
                            # Write back
                            combined_addr_df.write_parquet(str(self.addr_path))
                        else:
                            # First time - just write the new data
                            addr_df.write_parquet(str(self.addr_path))
                            
                except Exception as e:
                    logger.error(f"Error updating data for NPI {npi}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error in update_dim_npi_simple: {e}")
    
    def fetch_npis_parallel(self, npis: list, batch_size: int = 50):
        """Fetch NPIs in parallel with batching."""
        total_npis = len(npis)
        logger.info(f"Starting parallel fetch for {total_npis} NPIs")
        logger.info(f"Using {self.threads} threads")
        logger.info(f"Batch size: {batch_size}")
        
        # Process in batches to avoid memory issues
        for i in range(0, total_npis, batch_size):
            batch_npis = npis[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_npis + batch_size - 1) // batch_size
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_npis)} NPIs)")
            
            # Clear results for this batch
            self.results = []
            
            # Process batch in parallel
            with ThreadPoolExecutor(max_workers=self.threads) as executor:
                futures = [executor.submit(self.fetch_single_npi, npi) for npi in batch_npis]
                
                # Wait for completion
                for future in as_completed(futures):
                    self.processed += 1
                    if self.processed % 50 == 0:
                        progress = (self.processed / total_npis) * 100
                        logger.info(f"Progress: {self.processed}/{total_npis} ({progress:.1f}%) - Found: {self.found}, Errors: {self.errors}")
            
            # Update dim_npi file with this batch
            if self.results:
                logger.info(f"Updating dim_npi with {len(self.results)} results...")
                self.update_dim_npi_simple(self.results)
            
            logger.info(f"Batch {batch_num} complete")
        
        logger.info("Parallel fetch complete!")
        logger.info(f"Total processed: {self.processed}")
        logger.info(f"Found: {self.found}")
        logger.info(f"Errors: {self.errors}")

def main():
    parser = argparse.ArgumentParser(description="Simple CSV-based NPPES data fetcher")
    parser.add_argument("--csv-path", type=str, 
                       default=r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip",
                       help="Path to NPPES CSV file or zip file")
    parser.add_argument("--threads", type=int, default=5, help="Number of parallel threads")
    parser.add_argument("--limit", type=int, help="Limit number of NPIs to process")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for processing")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    
    args = parser.parse_args()
    
    # Check if we're in the right directory (data/dims must exist)
    base_path = Path(__file__).resolve().parent.parent.parent
    data_dir = base_path / "data"
    dims_dir = data_dir / "dims"
    if not dims_dir.exists():
        logger.error(f"{dims_dir} directory not found")
        logger.error("Please run this script from within the repo so that data/dims exists")
        return 1
    
    # Create fetcher
    fetcher = NPPESCSVSimpleFetcher(
        csv_path=args.csv_path,
        threads=args.threads
    )
    
    # Get NPIs that need fetching
    npis_to_fetch = fetcher.get_npis_to_fetch()
    
    if args.limit:
        npis_to_fetch = npis_to_fetch[:args.limit]
    
    if not npis_to_fetch:
        logger.info("No NPIs need fetching")
        return 0
    
    # Confirmation prompt
    if not args.yes:
        limit_text = f" (limited to {args.limit})" if args.limit else ""
        
        print(f"⚠️  WARNING: This will fetch NPPES data for {len(npis_to_fetch)} NPIs{limit_text}")
        print(f"🧵 Using {args.threads} parallel threads")
        print(f"📁 Using CSV file: {args.csv_path}")
        print(f"💾 Output: data/dims/dim_npi.parquet & dim_npi_address.parquet")
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
        logger.error(f"Error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())


