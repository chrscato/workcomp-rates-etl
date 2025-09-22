#!/usr/bin/env python3
"""
Memory-efficient patch script to merge existing temp files using batched processing
This avoids re-running the entire ETL pipeline.
"""

import os
import sys
import logging
from pathlib import Path
import duckdb
import shutil

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

def merge_temp_files_batched(output_path: Path, table_name: str, keys: list, db_conn, batch_size: int = 100) -> None:
    """Merge all temporary files into final output file using batched processing"""
    temp_dir = output_path.parent / "temp_chunks"
    temp_pattern = f"{table_name}_chunk_*.parquet"
    
    # Find all temp files for this table
    temp_files = list(temp_dir.glob(temp_pattern))
    
    if not temp_files:
        print(f"No temp files found for {table_name}")
        return
    
    print(f"Merging {len(temp_files)} temp files for {table_name} in batches of {batch_size}")
    
    try:
        if len(temp_files) == 1:
            # Single file - just rename it
            os.rename(temp_files[0], output_path)
            print(f"Renamed single temp file to {output_path}")
        else:
            # Multiple files - merge them in batches
            temp_files.sort()  # Ensure consistent ordering
            
            if table_name == "fact_rate":
                # For fact table, just concatenate all files in batches
                batch_files = []
                for i in range(0, len(temp_files), batch_size):
                    batch = temp_files[i:i + batch_size]
                    temp_paths_str = "', '".join(str(f) for f in batch)
                    
                    # Create intermediate batch file
                    batch_file = temp_dir / f"{table_name}_batch_{i//batch_size:06d}.parquet"
                    db_conn.execute(f"""
                        COPY (
                            SELECT * FROM read_parquet(['{temp_paths_str}'])
                        ) TO '{batch_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
                    """)
                    batch_files.append(batch_file)
                
                # Merge all batch files
                if len(batch_files) == 1:
                    os.rename(batch_files[0], output_path)
                else:
                    batch_paths_str = "', '".join(str(f) for f in batch_files)
                    db_conn.execute(f"""
                        COPY (
                            SELECT * FROM read_parquet(['{batch_paths_str}'])
                        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
                    """)
                
                # Clean up batch files
                for batch_file in batch_files:
                    try:
                        os.remove(batch_file)
                    except (OSError, PermissionError):
                        pass
                        
            else:
                # For dimensions, deduplicate based on keys using batched processing
                partition_cols = ", ".join(keys) if keys else ""
                order_cols = ", ".join(keys) if keys else "1"
                
                # Process in batches and accumulate unique records
                all_unique_data = []
                
                for i in range(0, len(temp_files), batch_size):
                    batch = temp_files[i:i + batch_size]
                    temp_paths_str = "', '".join(str(f) for f in batch)
                    
                    print(f"  Processing batch {i//batch_size + 1}/{(len(temp_files) + batch_size - 1)//batch_size}")
                    
                    # Get unique records from this batch
                    batch_unique = db_conn.execute(f"""
                        WITH all_rows AS (
                            SELECT * FROM read_parquet(['{temp_paths_str}'])
                        )
                        SELECT * FROM (
                            SELECT 
                                all_rows.*, 
                                ROW_NUMBER() OVER (PARTITION BY {partition_cols} ORDER BY {order_cols}) AS rn
                            FROM all_rows
                        )
                        WHERE rn = 1
                    """).fetchall()
                    
                    if batch_unique:
                        all_unique_data.extend(batch_unique)
                
                # Write all unique data to final file
                if all_unique_data:
                    # Create a temporary file with all unique data
                    temp_final = output_path.with_suffix('.temp.parquet')
                    
                    # Write data in chunks to avoid memory issues
                    chunk_size = 10000
                    for i in range(0, len(all_unique_data), chunk_size):
                        chunk = all_unique_data[i:i + chunk_size]
                        
                        # Convert to DataFrame and write
                        import polars as pl
                        df = pl.DataFrame(chunk)
                        
                        if i == 0:
                            df.write_parquet(temp_final, compression="zstd")
                        else:
                            # Append to existing file
                            existing = pl.read_parquet(temp_final)
                            combined = pl.concat([existing, df])
                            combined.write_parquet(temp_final, compression="zstd")
                    
                    # Move temp file to final location
                    shutil.move(temp_final, output_path)
            
            print(f"Merged {len(temp_files)} temp files into {output_path}")
        
        # Clean up temp files
        for temp_file in temp_files:
            try:
                os.remove(temp_file)
            except (OSError, PermissionError):
                print(f"Could not remove temp file {temp_file}")
        
        # Remove temp directory if empty
        try:
            temp_dir.rmdir()
        except OSError:
            pass  # Directory not empty or other error
            
    except Exception as e:
        print(f"Failed to merge temp files for {table_name}: {e}")
        raise

def main():
    """Main entry point"""
    if len(sys.argv) != 2:
        print("Usage: python patch_merge_temp_files_batched.py <payer>")
        print("Example: python patch_merge_temp_files_batched.py aetna")
        sys.exit(1)
    
    payer = sys.argv[1]
    
    # Set up paths
    data_root = Path("data")
    dims_dir = data_root / "dims"
    xrefs_dir = data_root / "xrefs"
    gold_dir = data_root / "gold"
    
    output_files = {
        "dim_code": dims_dir / "dim_code.parquet",
        "dim_payer": dims_dir / "dim_payer.parquet",
        "dim_provider_group": dims_dir / "dim_provider_group.parquet",
        "dim_pos_set": dims_dir / "dim_pos_set.parquet",
        "xref_pg_npi": xrefs_dir / "xref_pg_member_npi.parquet",
        "xref_pg_tin": xrefs_dir / "xref_pg_member_tin.parquet",
        "fact_rate": gold_dir / "fact_rate.parquet",
    }
    
    # Key mappings for deduplication
    key_mapping = {
        "dim_code": ["code_type", "code"],
        "dim_payer": ["payer_slug"],
        "dim_provider_group": ["pg_uid"],
        "dim_pos_set": ["pos_set_id"],
        "xref_pg_npi": ["pg_uid", "npi"],
        "xref_pg_tin": ["pg_uid", "tin_value"],
    }
    
    print(f"🔧 Patching merge for payer: {payer} (batched processing)")
    print("=" * 60)
    
    # Connect to DuckDB
    db_conn = duckdb.connect()
    
    try:
        # Process each table
        for table_name, output_path in output_files.items():
            keys = key_mapping.get(table_name, [])
            print(f"\nProcessing {table_name}...")
            merge_temp_files_batched(output_path, table_name, keys, db_conn, batch_size=50)
        
        print("\n✅ All temp files merged successfully!")
        
    except Exception as e:
        print(f"\n❌ Patch failed: {e}")
        sys.exit(1)
    finally:
        db_conn.close()

if __name__ == "__main__":
    main()
