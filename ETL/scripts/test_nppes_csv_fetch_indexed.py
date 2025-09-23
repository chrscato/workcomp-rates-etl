#!/usr/bin/env python3
"""
Test script for the memory-efficient NPPES CSV fetcher with index.
Tests the index building and NPI lookup functionality.
"""

import sys
from pathlib import Path
import pandas as pd
import polars as pl

# Add the parent directory to the path to import the fetcher
sys.path.append(str(Path(__file__).parent.parent.parent))
from ETL.utils.fetch_npi_data_csv_indexed import NPPESCSVIndexedFetcher

def test_index_building():
    """Test building the NPI index."""
    print("Testing index building...")
    
    csv_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
    
    try:
        fetcher = NPPESCSVIndexedFetcher(csv_path=csv_path, threads=1)
        
        # Build index (this will take a while for the full file)
        print("Building index (this may take several minutes)...")
        fetcher.build_index(force_rebuild=True)
        
        print(f"✅ Index built successfully")
        print(f"   Indexed NPIs: {len(fetcher.npi_index):,}")
        print(f"   Index file: {fetcher.index_file}")
        
        # Test loading existing index
        print("Testing index loading...")
        fetcher2 = NPPESCSVIndexedFetcher(csv_path=csv_path, threads=1)
        fetcher2.build_index(force_rebuild=False)
        
        print(f"✅ Index loaded successfully")
        print(f"   Loaded NPIs: {len(fetcher2.npi_index):,}")
        
        return True
        
    except Exception as e:
        print(f"❌ Index building failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_npi_lookup():
    """Test looking up specific NPIs using the index."""
    print("\nTesting NPI lookup...")
    
    csv_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
    
    try:
        fetcher = NPPESCSVIndexedFetcher(csv_path=csv_path, threads=1)
        
        # Load existing index or build if needed
        if not fetcher.index_file.exists():
            print("Index not found, building...")
            fetcher.build_index(force_rebuild=True)
        else:
            fetcher.build_index(force_rebuild=False)
        
        # Get some sample NPIs from the index
        sample_npis = list(fetcher.npi_index.keys())[:3]
        print(f"Testing lookup for NPIs: {sample_npis}")
        
        for npi in sample_npis:
            print(f"\nTesting NPI: {npi}")
            
            # Test the lookup
            row = fetcher.get_npi_row(npi)
            if row is not None:
                print(f"  ✅ Found in CSV")
                print(f"  Organization: {row.get('Provider Organization Name (Legal Business Name)', 'N/A')}")
                print(f"  First Name: {row.get('Provider First Name', 'N/A')}")
                print(f"  Last Name: {row.get('Provider Last Name (Legal Name)', 'N/A')}")
                print(f"  Entity Type: {row.get('Entity Type Code', 'N/A')}")
                
                # Test conversion to NPPES record
                nppes_record = fetcher.csv_row_to_nppes_record(row)
                print(f"  Converted to NPPES record with {len(nppes_record.get('addresses', []))} addresses")
            else:
                print(f"  ❌ Not found in CSV")
        
        return True
        
    except Exception as e:
        print(f"❌ NPI lookup failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_xref_integration():
    """Test integration with xref file."""
    print("\nTesting xref integration...")
    
    csv_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
    
    try:
        fetcher = NPPESCSVIndexedFetcher(csv_path=csv_path, threads=1)
        
        # Load index
        if not fetcher.index_file.exists():
            print("Index not found, building...")
            fetcher.build_index(force_rebuild=True)
        else:
            fetcher.build_index(force_rebuild=False)
        
        # Test getting NPIs to fetch
        npis_to_fetch = fetcher.get_npis_to_fetch()
        
        print(f"✅ Found {len(npis_to_fetch)} NPIs to fetch from xref")
        
        if npis_to_fetch:
            # Test fetching a few NPIs
            test_npis = npis_to_fetch[:3]
            print(f"Testing fetch for: {test_npis}")
            
            for npi in test_npis:
                print(f"\nFetching NPI: {npi}")
                fetcher.fetch_single_npi(npi)
            
            print(f"✅ Fetch test completed")
            print(f"   Results: {len(fetcher.results)}")
            print(f"   Found: {fetcher.found}")
            print(f"   Errors: {fetcher.errors}")
        
        return True
        
    except Exception as e:
        print(f"❌ Xref integration failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_schema_compatibility():
    """Test that the generated data matches the expected schema."""
    print("\nTesting schema compatibility...")
    
    try:
        # Read existing parquet files to get expected schema
        existing_dim_npi = pl.read_parquet("data/dims/dim_npi.parquet")
        existing_dim_npi_address = pl.read_parquet("data/dims/dim_npi_address.parquet")
        
        print(f"Existing dim_npi columns: {existing_dim_npi.columns}")
        print(f"Existing dim_npi_address columns: {existing_dim_npi_address.columns}")
        
        # Test with sample data
        csv_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
        fetcher = NPPESCSVIndexedFetcher(csv_path=csv_path, threads=1)
        
        # Load index
        if not fetcher.index_file.exists():
            print("Index not found, building...")
            fetcher.build_index(force_rebuild=True)
        else:
            fetcher.build_index(force_rebuild=False)
        
        # Get a sample NPI and test the full process
        sample_npi = list(fetcher.npi_index.keys())[0]
        print(f"Testing with sample NPI: {sample_npi}")
        
        fetcher.fetch_single_npi(sample_npi)
        
        if fetcher.results:
            dim_df, addr_df, npi = fetcher.results[0]
            print(f"Generated dim_npi columns: {dim_df.columns}")
            print(f"Generated dim_npi_address columns: {addr_df.columns}")
            
            # Check for missing/extra columns
            missing_cols = set(existing_dim_npi.columns) - set(dim_df.columns)
            extra_cols = set(dim_df.columns) - set(existing_dim_npi.columns)
            print(f"Missing columns: {missing_cols}")
            print(f"Extra columns: {extra_cols}")
            
            missing_addr_cols = set(existing_dim_npi_address.columns) - set(addr_df.columns)
            extra_addr_cols = set(addr_df.columns) - set(existing_dim_npi_address.columns)
            print(f"Missing address columns: {missing_addr_cols}")
            print(f"Extra address columns: {extra_addr_cols}")
        
        print("✅ Schema compatibility test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Schema compatibility test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Starting Memory-Efficient NPPES CSV Fetcher tests...")
    print("=" * 60)
    
    # Run tests
    test1_passed = test_index_building()
    test2_passed = test_npi_lookup()
    test3_passed = test_xref_integration()
    test4_passed = test_schema_compatibility()
    
    print("\n" + "=" * 60)
    if test1_passed and test2_passed and test3_passed and test4_passed:
        print("🎉 All tests passed! Ready to run the indexed CSV fetcher.")
    else:
        print("❌ Some tests failed. Please review and fix issues before running the fetcher.")
        sys.exit(1)


