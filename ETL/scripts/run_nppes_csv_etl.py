#!/usr/bin/env python3
"""
Runner script for the NPPES CSV ETL process.
This script will process the full 10GB+ NPPES CSV file into dim_npi and dim_npi_address parquet files.
"""

import sys
from pathlib import Path

# Add the parent directory to the path to import the ETL script
sys.path.append(str(Path(__file__).parent.parent.parent))
from ETL.scripts.etl_nppes_csv import NPPESCSVETL

def main():
    """Run the full NPPES CSV ETL process."""
    print("🚀 Starting NPPES CSV ETL Process")
    print("=" * 50)
    
    # Configuration
    zip_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
    csv_filename = "npidata_pfile_20050523-20250810.csv"
    
    # Adjust chunk size based on available memory
    # Start with 10,000 rows per chunk - adjust if you have memory issues
    chunk_size = 10000
    
    print(f"📁 Source: {zip_path}")
    print(f"📄 CSV file: {csv_filename}")
    print(f"📊 Chunk size: {chunk_size:,} rows")
    print(f"💾 Output: data/dims/dim_npi.parquet & dim_npi_address.parquet")
    print()
    
    # Initialize ETL processor
    etl = NPPESCSVETL(chunk_size=chunk_size)
    
    # Run ETL
    try:
        print("⏳ Processing CSV file...")
        total_rows, dim_npi_count, address_count = etl.etl_csv_file(zip_path, csv_filename)
        
        print()
        print("✅ ETL completed successfully!")
        print("=" * 50)
        print(f"📊 Total rows processed: {total_rows:,}")
        print(f"👥 dim_npi records created: {dim_npi_count:,}")
        print(f"🏠 Address records created: {address_count:,}")
        print()
        print("📁 Output files:")
        print(f"   - data/dims/dim_npi.parquet")
        print(f"   - data/dims/dim_npi_address.parquet")
        print()
        print("🎉 Ready to use in your ETL pipeline!")
        
    except Exception as e:
        print(f"❌ ETL failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()


