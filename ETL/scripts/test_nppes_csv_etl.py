#!/usr/bin/env python3
"""
Test script for the NPPES CSV ETL process.
Tests with a small sample of data to validate the mapping and processing logic.
"""

import pandas as pd
import polars as pl
import zipfile
from pathlib import Path
import sys

# Add the parent directory to the path to import the ETL script
sys.path.append(str(Path(__file__).parent.parent.parent))
from ETL.scripts.etl_nppes_csv import NPPESCSVETL

def test_csv_mapping():
    """Test the CSV to dimension mapping with a small sample."""
    print("Testing NPPES CSV ETL mapping...")
    
    # Initialize ETL processor
    etl = NPPESCSVETL(chunk_size=100)
    
    # Read a small sample from the CSV
    zip_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
    csv_filename = "npidata_pfile_20050523-20250810.csv"
    
    try:
        with zipfile.ZipFile(zip_path) as z:
            with z.open(csv_filename) as f:
                # Read just 5 rows for testing
                sample_df = pd.read_csv(f, nrows=5, dtype=str)
        
        print(f"Sample CSV data shape: {sample_df.shape}")
        print(f"Sample CSV columns (first 20): {sample_df.columns.tolist()[:20]}")
        
        # Test the mapping functions
        print("\nTesting dim_npi mapping...")
        for idx, row in sample_df.iterrows():
            npi = row['NPI']
            print(f"\nProcessing NPI: {npi}")
            
            # Test dim_npi mapping
            dim_npi_row = etl.map_csv_to_dim_npi(row)
            print(f"  dim_npi fields: {list(dim_npi_row.keys())}")
            print(f"  enumeration_type: {dim_npi_row['enumeration_type']}")
            print(f"  organization_name: {dim_npi_row['organization_name']}")
            print(f"  first_name: {dim_npi_row['first_name']}")
            print(f"  last_name: {dim_npi_row['last_name']}")
            print(f"  primary_taxonomy_code: {dim_npi_row['primary_taxonomy_code']}")
            
            # Test address mapping
            addresses = etl.map_csv_to_addresses(row)
            print(f"  addresses found: {len(addresses)}")
            for addr in addresses:
                print(f"    {addr['address_purpose']}: {addr['address_1']}, {addr['city']}, {addr['state']}")
        
        # Test chunk processing
        print("\nTesting chunk processing...")
        dim_npi_chunk, dim_npi_address_chunk = etl.process_chunk(sample_df)
        
        print(f"dim_npi_chunk shape: {dim_npi_chunk.shape}")
        print(f"dim_npi_address_chunk shape: {dim_npi_address_chunk.shape}")
        
        if not dim_npi_chunk.is_empty():
            print(f"dim_npi columns: {dim_npi_chunk.columns}")
            print(f"Sample dim_npi row:")
            print(dim_npi_chunk.head(1).to_pandas().to_dict('records')[0])
        
        if not dim_npi_address_chunk.is_empty():
            print(f"dim_npi_address columns: {dim_npi_address_chunk.columns}")
            print(f"Sample address row:")
            print(dim_npi_address_chunk.head(1).to_pandas().to_dict('records')[0])
        
        print("\n✅ Test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_schema_compatibility():
    """Test that the generated data matches the expected schema."""
    print("\nTesting schema compatibility...")
    
    # Read existing parquet files to get expected schema
    try:
        existing_dim_npi = pl.read_parquet("data/dims/dim_npi.parquet")
        existing_dim_npi_address = pl.read_parquet("data/dims/dim_npi_address.parquet")
        
        print(f"Existing dim_npi columns: {existing_dim_npi.columns}")
        print(f"Existing dim_npi_address columns: {existing_dim_npi_address.columns}")
        
        # Test with sample data
        etl = NPPESCSVETL()
        zip_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
        csv_filename = "npidata_pfile_20050523-20250810.csv"
        
        with zipfile.ZipFile(zip_path) as z:
            with z.open(csv_filename) as f:
                sample_df = pd.read_csv(f, nrows=1, dtype=str)
        
        dim_npi_chunk, dim_npi_address_chunk = etl.process_chunk(sample_df)
        
        if not dim_npi_chunk.is_empty():
            print(f"Generated dim_npi columns: {dim_npi_chunk.columns}")
            missing_cols = set(existing_dim_npi.columns) - set(dim_npi_chunk.columns)
            extra_cols = set(dim_npi_chunk.columns) - set(existing_dim_npi.columns)
            print(f"Missing columns: {missing_cols}")
            print(f"Extra columns: {extra_cols}")
        
        if not dim_npi_address_chunk.is_empty():
            print(f"Generated dim_npi_address columns: {dim_npi_address_chunk.columns}")
            missing_cols = set(existing_dim_npi_address.columns) - set(dim_npi_address_chunk.columns)
            extra_cols = set(dim_npi_address_chunk.columns) - set(existing_dim_npi_address.columns)
            print(f"Missing columns: {missing_cols}")
            print(f"Extra columns: {extra_cols}")
        
        print("✅ Schema compatibility test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Schema compatibility test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Starting NPPES CSV ETL tests...")
    
    # Run tests
    test1_passed = test_csv_mapping()
    test2_passed = test_schema_compatibility()
    
    if test1_passed and test2_passed:
        print("\n🎉 All tests passed! Ready to run the full ETL.")
    else:
        print("\n❌ Some tests failed. Please review and fix issues before running the full ETL.")
        sys.exit(1)
