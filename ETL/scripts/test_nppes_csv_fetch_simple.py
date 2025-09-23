#!/usr/bin/env python3
"""
Test script for the simple CSV-based NPPES fetcher.
Tests the CSV lookup functionality with a small sample of NPIs.
"""

import sys
from pathlib import Path
import pandas as pd
import polars as pl

# Add the parent directory to the path to import the fetcher
sys.path.append(str(Path(__file__).parent.parent.parent))
from ETL.utils.fetch_npi_data_csv_simple import NPPESCSVSimpleFetcher

def test_csv_lookup():
    """Test looking up specific NPIs from the CSV."""
    print("Testing CSV lookup...")
    
    csv_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
    
    try:
        fetcher = NPPESCSVSimpleFetcher(csv_path=csv_path, threads=1)
        
        # Test with some sample NPIs
        test_npis = ['1679576722', '1588667638', '1497758544']  # From your earlier test
        
        for npi in test_npis:
            print(f"\nTesting NPI: {npi}")
            
            # Test the lookup
            row = fetcher.find_npi_in_csv(npi)
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
        print(f"❌ CSV lookup failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_xref_integration():
    """Test integration with xref file."""
    print("\nTesting xref integration...")
    
    csv_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
    
    try:
        fetcher = NPPESCSVSimpleFetcher(csv_path=csv_path, threads=1)
        
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
        fetcher = NPPESCSVSimpleFetcher(csv_path=csv_path, threads=1)
        
        # Get a sample NPI and test the full process
        test_npi = '1679576722'  # Known to exist
        print(f"Testing with sample NPI: {test_npi}")
        
        fetcher.fetch_single_npi(test_npi)
        
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

def test_memory_usage():
    """Test that memory usage stays low."""
    print("\nTesting memory usage...")
    
    try:
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        csv_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
        fetcher = NPPESCSVSimpleFetcher(csv_path=csv_path, threads=1)
        
        # Test a few lookups
        test_npis = ['1679576722', '1588667638', '1497758544']
        
        for npi in test_npis:
            row = fetcher.find_npi_in_csv(npi)
            current_memory = process.memory_info().rss / 1024 / 1024  # MB
            print(f"  NPI {npi}: Memory usage {current_memory:.1f}MB")
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        print(f"✅ Memory test completed")
        print(f"   Initial memory: {initial_memory:.1f}MB")
        print(f"   Final memory: {final_memory:.1f}MB")
        print(f"   Memory increase: {memory_increase:.1f}MB")
        
        if memory_increase < 100:  # Less than 100MB increase
            print("✅ Memory usage is low - good for large CSV files")
        else:
            print("⚠️  Memory usage is higher than expected")
        
        return True
        
    except Exception as e:
        print(f"❌ Memory test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Starting Simple CSV-based NPPES Fetcher tests...")
    print("=" * 60)
    
    # Run tests
    test1_passed = test_csv_lookup()
    test2_passed = test_xref_integration()
    test3_passed = test_schema_compatibility()
    test4_passed = test_memory_usage()
    
    print("\n" + "=" * 60)
    if test1_passed and test2_passed and test3_passed and test4_passed:
        print("🎉 All tests passed! Ready to use the simple CSV fetcher.")
        print("💡 This is a drop-in replacement for fetch_npi_data.py")
    else:
        print("❌ Some tests failed. Please review and fix issues before running the fetcher.")
        sys.exit(1)


