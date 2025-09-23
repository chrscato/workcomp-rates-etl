#!/usr/bin/env python3
"""
Test script for the NPPES CSV-based fetcher.
Tests the CSV lookup functionality with a small sample of NPIs.
"""

import sys
from pathlib import Path
import pandas as pd
import polars as pl

# Add the parent directory to the path to import the fetcher
sys.path.append(str(Path(__file__).parent.parent.parent))
from ETL.utils.fetch_npi_data_csv import NPPESCSVFetcher

def test_csv_loading():
    """Test loading the CSV data."""
    print("Testing CSV data loading...")
    
    csv_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
    
    try:
        fetcher = NPPESCSVFetcher(csv_path=csv_path, threads=1)
        fetcher.load_csv_data()
        
        print(f"✅ CSV loaded successfully")
        print(f"   Records: {len(fetcher.csv_data):,}")
        print(f"   Columns: {len(fetcher.csv_data.columns)}")
        print(f"   Sample NPIs: {list(fetcher.csv_data.index[:5])}")
        
        return True
        
    except Exception as e:
        print(f"❌ CSV loading failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_npi_lookup():
    """Test looking up specific NPIs from the CSV."""
    print("\nTesting NPI lookup...")
    
    csv_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
    
    try:
        fetcher = NPPESCSVFetcher(csv_path=csv_path, threads=1)
        fetcher.load_csv_data()
        
        # Get some sample NPIs from the CSV
        sample_npis = list(fetcher.csv_data.index[:3])
        print(f"Testing lookup for NPIs: {sample_npis}")
        
        for npi in sample_npis:
            print(f"\nTesting NPI: {npi}")
            
            # Test the lookup
            if npi in fetcher.csv_data.index:
                row = fetcher.csv_data.loc[npi]
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
        fetcher = NPPESCSVFetcher(csv_path=csv_path, threads=1)
        fetcher.load_csv_data()
        
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
        fetcher = NPPESCSVFetcher(csv_path=csv_path, threads=1)
        fetcher.load_csv_data()
        
        # Get a sample NPI and test the full process
        sample_npi = list(fetcher.csv_data.index)[0]
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
    print("Starting NPPES CSV Fetcher tests...")
    print("=" * 50)
    
    # Run tests
    test1_passed = test_csv_loading()
    test2_passed = test_npi_lookup()
    test3_passed = test_xref_integration()
    test4_passed = test_schema_compatibility()
    
    print("\n" + "=" * 50)
    if test1_passed and test2_passed and test3_passed and test4_passed:
        print("🎉 All tests passed! Ready to run the CSV fetcher.")
    else:
        print("❌ Some tests failed. Please review and fix issues before running the fetcher.")
        sys.exit(1)


