#!/usr/bin/env python3
"""
Simple patch script to merge existing temp files by concatenation
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

def merge_temp_files_simple(output_path: Path, table_name: str, db_conn, batch_size: int = 100) -> None:
    """Merge all temporary files into final output file using simple concatenation"""
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
            
            # Process in batches to avoid memory issues
            batch_files = []
            for i in range(0, len(temp_files), batch_size):
                batch = temp_files[i:i + batch_size]
                temp_paths_str = "', '".join(str(f) for f in batch)
                
                # Create intermediate batch file
                batch_file = temp_dir / f"{table_name}_batch_{i//batch_size:06d}.parquet"
                print(f"  Processing batch {i//batch_size + 1}/{(len(temp_files) + batch_size - 1)//batch_size}")
                
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
                print(f"  Final merge of {len(batch_files)} batch files...")
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
        print("Usage: python patch_merge_simple.py <payer>")
        print("Example: python patch_merge_simple.py aetna")
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
    
    print(f"🔧 Patching merge for payer: {payer} (simple concatenation)")
    print("=" * 60)
    
    # Connect to DuckDB
    db_conn = duckdb.connect()
    
    try:
        # Process each table
        for table_name, output_path in output_files.items():
            print(f"\nProcessing {table_name}...")
            merge_temp_files_simple(output_path, table_name, db_conn, batch_size=50)
        
        print("\n✅ All temp files merged successfully!")
        
    except Exception as e:
        print(f"\n❌ Patch failed: {e}")
        sys.exit(1)
    finally:
        db_conn.close()

if __name__ == "__main__":
    main()
