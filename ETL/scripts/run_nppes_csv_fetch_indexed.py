#!/usr/bin/env python3
"""
Runner script for the memory-efficient NPPES CSV-based fetcher with index.
This script builds an index of NPI positions in the CSV file for fast random access.
"""

import sys
from pathlib import Path

# Add the parent directory to the path to import the fetcher
sys.path.append(str(Path(__file__).parent.parent.parent))
from ETL.utils.fetch_npi_data_csv_indexed import NPPESCSVIndexedFetcher

def main():
    """Run the memory-efficient NPPES CSV-based fetcher."""
    print("🚀 Starting Memory-Efficient NPPES CSV Fetcher")
    print("=" * 60)
    
    # Configuration
    csv_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
    threads = 10  # Adjust based on your system
    batch_size = 100  # Process in batches
    rebuild_index = False  # Set to True to force rebuild index
    
    print(f"📁 CSV Source: {csv_path}")
    print(f"🧵 Threads: {threads}")
    print(f"📦 Batch size: {batch_size}")
    print(f"🔍 Rebuild index: {rebuild_index}")
    print(f"💾 Output: data/dims/dim_npi.parquet & dim_npi_address.parquet")
    print()
    
    # Create fetcher
    fetcher = NPPESCSVIndexedFetcher(
        csv_path=csv_path,
        threads=threads
    )
    
    try:
        # Build index (this may take a while the first time)
        print("🔍 Building/loading NPI index...")
        fetcher.build_index(force_rebuild=rebuild_index)
        print(f"✅ Index ready with {len(fetcher.npi_index):,} NPIs")
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
        print("=" * 60)
        print(f"📊 Total processed: {fetcher.processed}")
        print(f"✅ Found: {fetcher.found}")
        print(f"❌ Errors: {fetcher.errors}")
        print()
        print("📁 Output files updated:")
        print(f"   - data/dims/dim_npi.parquet")
        print(f"   - data/dims/dim_npi_address.parquet")
        print()
        print("📊 Index file saved:")
        print(f"   - data/nppes_csv_index.pkl")
        print()
        print("🎉 Ready to use in your ETL pipeline!")
        
    except Exception as e:
        print(f"❌ Fetch failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())


