#!/usr/bin/env python3
"""
Runner script for the NPPES CSV-based fetcher.
This script will look up NPIs from your xref file in the NPPES CSV data
and populate dim_npi and dim_npi_address parquet files.
"""

import sys
from pathlib import Path

# Add the parent directory to the path to import the fetcher
sys.path.append(str(Path(__file__).parent.parent.parent))
from ETL.utils.fetch_npi_data_csv import NPPESCSVFetcher

def main():
    """Run the NPPES CSV-based fetcher."""
    print("🚀 Starting NPPES CSV-based Fetcher")
    print("=" * 50)
    
    # Configuration
    csv_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
    threads = 10  # Adjust based on your system
    batch_size = 100  # Process in batches
    
    print(f"📁 CSV Source: {csv_path}")
    print(f"🧵 Threads: {threads}")
    print(f"📦 Batch size: {batch_size}")
    print(f"💾 Output: data/dims/dim_npi.parquet & dim_npi_address.parquet")
    print()
    
    # Create fetcher
    fetcher = NPPESCSVFetcher(
        csv_path=csv_path,
        threads=threads
    )
    
    try:
        # Load CSV data
        print("📊 Loading CSV data...")
        fetcher.load_csv_data()
        print(f"✅ Loaded {len(fetcher.csv_data)} records from CSV")
        print()
        
        # Get NPIs that need fetching
        print("🔍 Finding NPIs to fetch...")
        npis_to_fetch = fetcher.get_npis_to_fetch()
        
        if not npis_to_fetch:
            print("✅ No NPIs need fetching - all are already in dim_npi")
            return 0
        
        print(f"🔄 Found {len(npis_to_fetch)} NPIs to fetch")
        print()
        
        # Run the fetcher
        print("⏳ Starting parallel fetch...")
        fetcher.fetch_npis_parallel(npis_to_fetch, batch_size)
        
        print()
        print("✅ Fetch completed successfully!")
        print("=" * 50)
        print(f"📊 Total processed: {fetcher.processed}")
        print(f"✅ Found: {fetcher.found}")
        print(f"❌ Errors: {fetcher.errors}")
        print()
        print("📁 Output files updated:")
        print(f"   - data/dims/dim_npi.parquet")
        print(f"   - data/dims/dim_npi_address.parquet")
        print()
        print("🎉 Ready to use in your ETL pipeline!")
        
    except Exception as e:
        print(f"❌ Fetch failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())


