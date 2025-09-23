#!/usr/bin/env python3
"""
ETL script to process the large NPPES CSV file into dim_npi and dim_npi_address parquet files.
Handles the 10GB+ CSV file in memory-efficient chunks.
"""

import pandas as pd
import polars as pl
import zipfile
import hashlib
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime
import sys
import os

# Add the parent directory to the path to import utils
sys.path.append(str(Path(__file__).parent.parent.parent))
from ETL.utils.utils_nppes import upsert_dim_npi, upsert_dim_npi_address

# Set up logging
def setup_logging():
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(logs_dir / 'nppes_csv_etl.log'),
            logging.StreamHandler()
        ]
    )
logger = logging.getLogger(__name__)

class NPPESCSVETL:
    def __init__(self, data_dir: str = "data", chunk_size: int = 10000):
        # Set up logging first
        setup_logging()
        
        self.data_dir = Path(data_dir)
        self.dims_dir = self.data_dir / "dims"
        self.dims_dir.mkdir(parents=True, exist_ok=True)
        self.chunk_size = chunk_size
        
        # Output file paths
        self.dim_npi_path = self.dims_dir / "dim_npi.parquet"
        self.dim_npi_address_path = self.dims_dir / "dim_npi_address.parquet"
        
        logger.info(f"Initialized NPPES CSV ETL with chunk size: {chunk_size}")
        logger.info(f"Output directory: {self.dims_dir}")
    
    def generate_address_hash(self, address_data: Dict[str, str]) -> str:
        """Generate a hash for address deduplication."""
        # Create a string from address components for hashing
        address_string = "|".join([
            str(address_data.get('address_1', '')),
            str(address_data.get('address_2', '')),
            str(address_data.get('city', '')),
            str(address_data.get('state', '')),
            str(address_data.get('postal_code', ''))
        ])
        return hashlib.md5(address_string.encode()).hexdigest()
    
    def map_csv_to_dim_npi(self, row: pd.Series) -> Dict[str, any]:
        """Map CSV row to dim_npi structure."""
        # Determine enumeration type
        entity_type = row.get('Entity Type Code')
        if pd.isna(entity_type):
            enumeration_type = None
        elif entity_type == 1:
            enumeration_type = "NPI-1"  # Individual
        elif entity_type == 2:
            enumeration_type = "NPI-2"  # Organization
        else:
            enumeration_type = f"NPI-{entity_type}"
        
        # Get primary taxonomy (first one)
        taxonomy_code = row.get('Healthcare Provider Taxonomy Code_1')
        taxonomy_desc = row.get('Healthcare Provider Taxonomy Description_1', '')
        license_number = row.get('Provider License Number_1')
        license_state = row.get('Provider License Number State Code_1')
        
        # Determine status
        status = "A"  # Active by default
        if pd.notna(row.get('NPI Deactivation Date')):
            status = "I"  # Inactive
        
        return {
            "npi": str(row['NPI']),
            "enumeration_type": enumeration_type,
            "status": status,
            "organization_name": row.get('Provider Organization Name (Legal Business Name)'),
            "first_name": row.get('Provider First Name'),
            "last_name": row.get('Provider Last Name (Legal Name)'),
            "credential": row.get('Provider Credential Text'),
            "sole_proprietor": row.get('Provider Sole Proprietor', '').upper() if pd.notna(row.get('Provider Sole Proprietor')) else None,
            "enumeration_date": row.get('Provider Enumeration Date'),
            "last_updated": row.get('Last Update Date'),
            "replacement_npi": row.get('Replacement NPI'),
            "nppes_fetched": True,  # Since we're loading from NPPES CSV
            "nppes_fetch_date": row.get('Last Update Date'),
            "primary_taxonomy_code": taxonomy_code,
            "primary_taxonomy_desc": taxonomy_desc,
            "primary_taxonomy_state": license_state,
            "primary_taxonomy_license": license_number
        }
    
    def map_csv_to_addresses(self, row: pd.Series) -> List[Dict[str, any]]:
        """Map CSV row to dim_npi_address structure (both mailing and location addresses)."""
        addresses = []
        npi = str(row['NPI'])
        last_updated = row.get('Last Update Date')
        
        # Mailing address
        mailing_address = {
            "npi": npi,
            "address_purpose": "MAILING",
            "address_type": "DOM",  # Domestic by default
            "address_1": row.get('Provider First Line Business Mailing Address'),
            "address_2": row.get('Provider Second Line Business Mailing Address'),
            "city": row.get('Provider Business Mailing Address City Name'),
            "state": row.get('Provider Business Mailing Address State Name'),
            "postal_code": row.get('Provider Business Mailing Address Postal Code'),
            "country_code": row.get('Provider Business Mailing Address Country Code (If outside U.S.)', 'US'),
            "telephone_number": row.get('Provider Business Mailing Address Telephone Number'),
            "fax_number": row.get('Provider Business Mailing Address Fax Number'),
            "last_updated": last_updated
        }
        
        # Generate hash for mailing address
        mailing_address["address_hash"] = self.generate_address_hash(mailing_address)
        
        # Only add if we have at least address_1
        if pd.notna(mailing_address["address_1"]):
            addresses.append(mailing_address)
        
        # Location address
        location_address = {
            "npi": npi,
            "address_purpose": "LOCATION",
            "address_type": "DOM",  # Domestic by default
            "address_1": row.get('Provider First Line Business Practice Location Address'),
            "address_2": row.get('Provider Second Line Business Practice Location Address'),
            "city": row.get('Provider Business Practice Location Address City Name'),
            "state": row.get('Provider Business Practice Location Address State Name'),
            "postal_code": row.get('Provider Business Practice Location Address Postal Code'),
            "country_code": row.get('Provider Business Practice Location Address Country Code (If outside U.S.)', 'US'),
            "telephone_number": row.get('Provider Business Practice Location Address Telephone Number'),
            "fax_number": row.get('Provider Business Practice Location Address Fax Number'),
            "last_updated": last_updated
        }
        
        # Generate hash for location address
        location_address["address_hash"] = self.generate_address_hash(location_address)
        
        # Only add if we have at least address_1
        if pd.notna(location_address["address_1"]):
            addresses.append(location_address)
        
        return addresses
    
    def process_chunk(self, chunk_df: pd.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """Process a chunk of CSV data into dim_npi and dim_npi_address DataFrames."""
        dim_npi_rows = []
        dim_npi_address_rows = []
        
        for _, row in chunk_df.iterrows():
            try:
                # Process dim_npi row
                dim_npi_row = self.map_csv_to_dim_npi(row)
                dim_npi_rows.append(dim_npi_row)
                
                # Process addresses
                addresses = self.map_csv_to_addresses(row)
                dim_npi_address_rows.extend(addresses)
                
            except Exception as e:
                logger.error(f"Error processing NPI {row.get('NPI', 'unknown')}: {e}")
                continue
        
        # Convert to Polars DataFrames with explicit schema
        if dim_npi_rows:
            dim_npi_df = pl.DataFrame(dim_npi_rows, infer_schema_length=None)
        else:
            dim_npi_df = pl.DataFrame()
            
        if dim_npi_address_rows:
            dim_npi_address_df = pl.DataFrame(dim_npi_address_rows, infer_schema_length=None)
        else:
            dim_npi_address_df = pl.DataFrame()
        
        # Ensure column order matches existing schema
        if not dim_npi_df.is_empty():
            expected_columns = [
                'credential', 'nppes_fetched', 'enumeration_date', 'primary_taxonomy_desc', 
                'last_name', 'organization_name', 'npi', 'primary_taxonomy_code', 
                'first_name', 'last_updated', 'status', 'primary_taxonomy_license', 
                'enumeration_type', 'nppes_fetch_date', 'sole_proprietor', 
                'primary_taxonomy_state', 'replacement_npi'
            ]
            
            # Reorder columns to match existing schema
            available_columns = [col for col in expected_columns if col in dim_npi_df.columns]
            dim_npi_df = dim_npi_df.select(available_columns)
        
        # Ensure address column order matches existing schema
        if not dim_npi_address_df.is_empty():
            expected_address_columns = [
                'npi', 'address_purpose', 'address_type', 'address_1', 'address_2', 
                'city', 'state', 'postal_code', 'country_code', 'telephone_number', 
                'fax_number', 'last_updated', 'address_hash'
            ]
            
            # Reorder columns to match existing schema
            available_address_columns = [col for col in expected_address_columns if col in dim_npi_address_df.columns]
            dim_npi_address_df = dim_npi_address_df.select(available_address_columns)
        
        return dim_npi_df, dim_npi_address_df
    
    def etl_csv_file(self, zip_path: str, csv_filename: str):
        """ETL the CSV file from the zip archive."""
        logger.info(f"Starting ETL of {csv_filename} from {zip_path}")
        
        total_rows_processed = 0
        total_dim_npi_rows = 0
        total_address_rows = 0
        
        try:
            with zipfile.ZipFile(zip_path) as z:
                with z.open(csv_filename) as f:
                    # Read CSV in chunks
                    chunk_iter = pd.read_csv(f, chunksize=self.chunk_size, dtype=str)
                    
                    for chunk_num, chunk_df in enumerate(chunk_iter):
                        logger.info(f"Processing chunk {chunk_num + 1} with {len(chunk_df)} rows")
                        
                        # Process the chunk
                        dim_npi_chunk, dim_npi_address_chunk = self.process_chunk(chunk_df)
                        
                        # Upsert to parquet files
                        if not dim_npi_chunk.is_empty():
                            upsert_dim_npi(dim_npi_chunk, self.dim_npi_path)
                            total_dim_npi_rows += len(dim_npi_chunk)
                        
                        if not dim_npi_address_chunk.is_empty():
                            upsert_dim_npi_address(dim_npi_address_chunk, self.dim_npi_address_path)
                            total_address_rows += len(dim_npi_address_chunk)
                        
                        total_rows_processed += len(chunk_df)
                        
                        # Log progress every 10 chunks
                        if (chunk_num + 1) % 10 == 0:
                            logger.info(f"Processed {total_rows_processed} rows, "
                                      f"created {total_dim_npi_rows} dim_npi records, "
                                      f"{total_address_rows} address records")
        
        except Exception as e:
            logger.error(f"Error during ETL: {e}")
            raise
        
        logger.info(f"ETL completed successfully!")
        logger.info(f"Total rows processed: {total_rows_processed}")
        logger.info(f"Total dim_npi records: {total_dim_npi_rows}")
        logger.info(f"Total address records: {total_address_rows}")
        
        return total_rows_processed, total_dim_npi_rows, total_address_rows

def main():
    """Main function to run the ETL process."""
    # Configuration
    zip_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
    csv_filename = "npidata_pfile_20050523-20250810.csv"
    chunk_size = 10000  # Adjust based on available memory
    
    # Initialize ETL processor
    etl = NPPESCSVETL(chunk_size=chunk_size)
    
    # Run ETL
    try:
        start_time = datetime.now()
        total_rows, dim_npi_count, address_count = etl.etl_csv_file(zip_path, csv_filename)
        end_time = datetime.now()
        
        duration = end_time - start_time
        logger.info(f"ETL completed in {duration}")
        logger.info(f"Performance: {total_rows / duration.total_seconds():.2f} rows/second")
        
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
