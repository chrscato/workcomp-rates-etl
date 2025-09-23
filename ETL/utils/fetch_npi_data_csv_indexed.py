#!/usr/bin/env python3
"""
Memory-efficient NPPES data fetcher using CSV with index file

This version creates an index file of NPI positions in the CSV, allowing
fast random access without loading the entire CSV into memory.

Usage:
    python fetch_npi_data_csv_indexed.py --csv-path "path/to/nppes.csv" --threads 5
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
import pickle
import struct

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
        logging.FileHandler('logs/nppes_csv_fetch_indexed.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NPPESCSVIndexedFetcher:
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
        
        # Index file for fast NPI lookup
        self.index_file = Path("data/nppes_csv_index.pkl")
        self.npi_index = {}
        
        # Create logs directory
        Path("logs").mkdir(exist_ok=True)

    def build_index(self, force_rebuild: bool = False):
        """Build an index of NPI positions in the CSV file."""
        if self.index_file.exists() and not force_rebuild:
            logger.info(f"Loading existing index from {self.index_file}")
            try:
                with open(self.index_file, 'rb') as f:
                    self.npi_index = pickle.load(f)
                logger.info(f"Loaded index with {len(self.npi_index)} NPIs")
                return
            except Exception as e:
                logger.warning(f"Failed to load existing index: {e}")
                logger.info("Rebuilding index...")
        
        logger.info("Building NPI index from CSV...")
        self.npi_index = {}
        
        try:
            if self.csv_path.endswith('.zip'):
                with zipfile.ZipFile(self.csv_path) as z:
                    csv_filename = "npidata_pfile_20050523-20250810.csv"
                    with z.open(csv_filename) as f:
                        # Read CSV in chunks to build index
                        chunk_size = 50000
                        chunk_num = 0
                        total_rows = 0
                        
                        for chunk in pd.read_csv(f, chunksize=chunk_size, dtype=str):
                            chunk_num += 1
                            total_rows += len(chunk)
                            
                            # Index NPIs in this chunk
                            for idx, row in chunk.iterrows():
                                npi = str(row['NPI'])
                                # Store chunk number and row index within chunk
                                self.npi_index[npi] = (chunk_num, idx)
                            
                            if chunk_num % 10 == 0:
                                logger.info(f"Indexed {total_rows:,} rows, found {len(self.npi_index):,} unique NPIs")
                        
                        logger.info(f"Index complete: {total_rows:,} total rows, {len(self.npi_index):,} unique NPIs")
                        
            else:
                # Read CSV directly in chunks
                chunk_size = 50000
                chunk_num = 0
                total_rows = 0
                
                for chunk in pd.read_csv(self.csv_path, chunksize=chunk_size, dtype=str):
                    chunk_num += 1
                    total_rows += len(chunk)
                    
                    # Index NPIs in this chunk
                    for idx, row in chunk.iterrows():
                        npi = str(row['NPI'])
                        # Store chunk number and row index within chunk
                        self.npi_index[npi] = (chunk_num, idx)
                    
                    if chunk_num % 10 == 0:
                        logger.info(f"Indexed {total_rows:,} rows, found {len(self.npi_index):,} unique NPIs")
                
                logger.info(f"Index complete: {total_rows:,} total rows, {len(self.npi_index):,} unique NPIs")
            
            # Save index to file
            self.data_dir.mkdir(parents=True, exist_ok=True)
            with open(self.index_file, 'wb') as f:
                pickle.dump(self.npi_index, f)
            logger.info(f"Index saved to {self.index_file}")
            
        except Exception as e:
            logger.error(f"Error building index: {e}")
            raise

    def get_npi_row(self, npi: str) -> Optional[pd.Series]:
        """Get a specific NPI row from the CSV using the index."""
        if npi not in self.npi_index:
            return None
        
        try:
            chunk_num, row_idx = self.npi_index[npi]
            
            if self.csv_path.endswith('.zip'):
                with zipfile.ZipFile(self.csv_path) as z:
                    csv_filename = "npidata_pfile_20050523-20250810.csv"
                    with z.open(csv_filename) as f:
                        # Read the specific chunk
                        chunk_size = 50000
                        chunk_count = 0
                        
                        for chunk in pd.read_csv(f, chunksize=chunk_size, dtype=str):
                            chunk_count += 1
                            if chunk_count == chunk_num:
                                # Found the right chunk, return the specific row
                                if row_idx < len(chunk):
                                    return chunk.iloc[row_idx]
                                else:
                                    logger.error(f"Row index {row_idx} out of range for chunk {chunk_num}")
                                    return None
                        
                        logger.error(f"Chunk {chunk_num} not found")
                        return None
            else:
                # Read CSV directly
                chunk_size = 50000
                chunk_count = 0
                
                for chunk in pd.read_csv(self.csv_path, chunksize=chunk_size, dtype=str):
                    chunk_count += 1
                    if chunk_count == chunk_num:
                        # Found the right chunk, return the specific row
                        if row_idx < len(chunk):
                            return chunk.iloc[row_idx]
                        else:
                            logger.error(f"Row index {row_idx} out of range for chunk {chunk_num}")
                            return None
                
                logger.error(f"Chunk {chunk_num} not found")
                return None
                
        except Exception as e:
            logger.error(f"Error getting row for NPI {npi}: {e}")
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
            # Get NPI row using index
            row = self.get_npi_row(npi)
            
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
    
    def update_dim_npi(self, results_batch):
        """Update dim_npi file with a batch of results."""
        if not results_batch:
            return
            
        try:
            # Ensure dims directory exists
            self.dims_path.mkdir(parents=True, exist_ok=True)
            
            # Process each result
            for dim_df, addr_df, npi in results_batch:
                # Update dim_npi
                upsert_dim_npi(dim_df, self.dim_path)
                
                # Update addresses
                if addr_df.height > 0:
                    upsert_dim_npi_address(addr_df, self.addr_path)
                    
        except Exception as e:
            logger.error(f"Error updating dim_npi: {e}")
    
    def fetch_npis_parallel(self, npis: list, batch_size: int = 100):
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
                    if self.processed % 100 == 0:
                        progress = (self.processed / total_npis) * 100
                        logger.info(f"Progress: {self.processed}/{total_npis} ({progress:.1f}%) - Found: {self.found}, Errors: {self.errors}")
            
            # Update dim_npi file with this batch
            if self.results:
                logger.info(f"Updating dim_npi with {len(self.results)} results...")
                self.update_dim_npi(self.results)
            
            logger.info(f"Batch {batch_num} complete")
        
        logger.info("Parallel fetch complete!")
        logger.info(f"Total processed: {self.processed}")
        logger.info(f"Found: {self.found}")
        logger.info(f"Errors: {self.errors}")

def main():
    parser = argparse.ArgumentParser(description="Memory-efficient NPPES data fetcher using CSV with index")
    parser.add_argument("--csv-path", type=str, 
                       default=r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip",
                       help="Path to NPPES CSV file or zip file")
    parser.add_argument("--threads", type=int, default=5, help="Number of parallel threads")
    parser.add_argument("--limit", type=int, help="Limit number of NPIs to process")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for processing")
    parser.add_argument("--rebuild-index", action="store_true", help="Force rebuild of index file")
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
    fetcher = NPPESCSVIndexedFetcher(
        csv_path=args.csv_path,
        threads=args.threads
    )
    
    # Build index
    try:
        fetcher.build_index(force_rebuild=args.rebuild_index)
    except Exception as e:
        logger.error(f"Failed to build index: {e}")
        return 1
    
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
        print(f"📊 Index contains {len(fetcher.npi_index)} NPIs")
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


