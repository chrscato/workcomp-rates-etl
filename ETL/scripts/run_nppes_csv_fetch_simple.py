#!/usr/bin/env python3
"""
Runner script for the simple CSV-based NPPES fetcher.
This is a drop-in replacement for the API-based fetch_npi_data.py
"""

import sys
from pathlib import Path

# Add the parent directory to the path to import the fetcher
sys.path.append(str(Path(__file__).parent.parent.parent))
from ETL.utils.fetch_npi_data_csv_simple import NPPESCSVSimpleFetcher

def main():
    """Run the simple CSV-based NPPES fetcher."""
    print("🚀 Starting Simple CSV-based NPPES Fetcher")
    print("=" * 50)
    print("📝 This replaces fetch_npi_data.py with CSV lookups")
    print()
    
    # Configuration
    csv_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
    threads = 10  # Adjust based on your system
    batch_size = 50  # Smaller batches for stability
    
    print(f"📁 CSV Source: {csv_path}")
    print(f"🧵 Threads: {threads}")
    print(f"📦 Batch size: {batch_size}")
    print(f"💾 Output: data/dims/dim_npi.parquet & dim_npi_address.parquet")
    print()
    
    # Create fetcher
    fetcher = NPPESCSVSimpleFetcher(
        csv_path=csv_path,
        threads=threads
    )
    
    try:
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
        print("📊 This will look up each NPI in the CSV file")
        print("💡 Memory usage will stay low (<1GB)")
        print()
        
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
        print("💡 This approach is much faster than API calls!")
        
    except Exception as e:
        print(f"❌ Fetch failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())


