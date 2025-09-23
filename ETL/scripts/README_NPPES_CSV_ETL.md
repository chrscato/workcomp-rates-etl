# NPPES CSV ETL Documentation

## Overview

This ETL process processes the large NPPES (National Plan and Provider Enumeration System) CSV file (10GB+) into the existing `dim_npi` and `dim_npi_address` parquet files used by your workcomp rates ETL pipeline.

## Files Created

1. **`etl_nppes_csv.py`** - Main ETL script that processes the CSV in memory-efficient chunks
2. **`test_nppes_csv_etl.py`** - Test script to validate the ETL logic with sample data
3. **`run_nppes_csv_etl.py`** - Simple runner script to execute the full ETL process

## CSV Structure Analysis

The NPPES CSV contains 330 columns with the following key mappings:

### dim_npi Mapping
- **NPI** → `npi` (National Provider Identifier)
- **Entity Type Code** → `enumeration_type` (1=NPI-1 individual, 2=NPI-2 organization)
- **Provider Organization Name** → `organization_name` (for NPI-2)
- **Provider First/Last Name** → `first_name`/`last_name` (for NPI-1)
- **Provider Credential Text** → `credential`
- **Healthcare Provider Taxonomy Code_1** → `primary_taxonomy_code`
- **Provider License Number_1** → `primary_taxonomy_license`
- **Provider License Number State Code_1** → `primary_taxonomy_state`
- **Provider Enumeration Date** → `enumeration_date`
- **Last Update Date** → `last_updated`
- **Replacement NPI** → `replacement_npi`

### dim_npi_address Mapping
The CSV contains both mailing and location addresses for each provider:

**Mailing Address:**
- Provider First Line Business Mailing Address → `address_1`
- Provider Second Line Business Mailing Address → `address_2`
- Provider Business Mailing Address City Name → `city`
- Provider Business Mailing Address State Name → `state`
- Provider Business Mailing Address Postal Code → `postal_code`
- Provider Business Mailing Address Telephone Number → `telephone_number`
- Provider Business Mailing Address Fax Number → `fax_number`

**Location Address:**
- Provider First Line Business Practice Location Address → `address_1`
- Provider Second Line Business Practice Location Address → `address_2`
- Provider Business Practice Location Address City Name → `city`
- Provider Business Practice Location Address State Name → `state`
- Provider Business Practice Location Address Postal Code → `postal_code`
- Provider Business Practice Location Address Telephone Number → `telephone_number`
- Provider Business Practice Location Address Fax Number → `fax_number`

## Key Features

### Memory Efficiency
- Processes the 10GB+ CSV in configurable chunks (default: 10,000 rows)
- Uses Polars for fast DataFrame operations
- Implements upsert logic to handle existing data

### Data Quality
- Generates address hashes for deduplication
- Handles missing/null values appropriately
- Validates data types and formats
- Comprehensive error handling and logging

### Schema Compatibility
- Maintains exact compatibility with existing `dim_npi` and `dim_npi_address` schemas
- Uses the same upsert functions as the existing ETL pipeline
- Preserves all existing columns and data types

## Usage

### 1. Test the ETL Logic
```bash
python ETL/scripts/test_nppes_csv_etl.py
```

### 2. Run the Full ETL
```bash
python ETL/scripts/run_nppes_csv_etl.py
```

### 3. Monitor Progress
The ETL process logs progress to:
- Console output
- `logs/nppes_csv_etl.log` file

## Configuration

### Chunk Size
Adjust the `chunk_size` parameter based on available memory:
- **8GB RAM**: 5,000 rows
- **16GB RAM**: 10,000 rows (default)
- **32GB+ RAM**: 20,000+ rows

### File Paths
Update the `zip_path` in the runner script if your NPPES file is in a different location:
```python
zip_path = r"C:\Users\ChristopherCato\Downloads\NPPES_Data_Dissemination_August_2025.zip"
```

## Expected Output

After successful completion, you'll have:
- **dim_npi.parquet**: All provider information (millions of records)
- **dim_npi_address.parquet**: All provider addresses (millions of records)
- **logs/nppes_csv_etl.log**: Detailed processing log

## Performance Expectations

Based on the 10GB+ CSV size:
- **Processing time**: 2-4 hours (depending on system specs)
- **Memory usage**: 2-4GB peak (with 10K chunk size)
- **Output size**: ~500MB-1GB for each parquet file

## Integration with Existing ETL

The generated parquet files are fully compatible with your existing ETL pipeline:
- Same schema as current `dim_npi` and `dim_npi_address` files
- Can be used directly in ETL_1, ETL_2, and ETL_3 processes
- Maintains all existing relationships and joins

## Troubleshooting

### Memory Issues
- Reduce `chunk_size` to 5,000 or 2,500 rows
- Close other applications to free up RAM
- Monitor memory usage during processing

### File Access Issues
- Ensure the NPPES zip file is not being used by other applications
- Check file permissions on the output directory
- Verify sufficient disk space (need ~2GB free)

### Processing Errors
- Check the log file for detailed error messages
- Verify the CSV file is not corrupted
- Ensure all required Python packages are installed

## Dependencies

- pandas
- polars
- pathlib
- logging
- zipfile (built-in)
- hashlib (built-in)

The script uses your existing `ETL/utils/utils_nppes.py` for upsert operations.


