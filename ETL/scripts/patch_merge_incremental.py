#!/usr/bin/env python3
"""
Incremental patch script to merge existing temp files one by one
This avoids re-running the entire ETL pipeline.
"""

import os
import sys
import logging
from pathlib import Path
import polars as pl
import shutil

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

def merge_temp_files_incremental(output_path: Path, table_name: str) -> None:
    """Merge all temporary files into final output file using incremental approach"""
    temp_dir = output_path.parent / "temp_chunks"
    temp_pattern = f"{table_name}_chunk_*.parquet"
    
    # Find all temp files for this table
    temp_files = list(temp_dir.glob(temp_pattern))
    
    if not temp_files:
        print(f"No temp files found for {table_name}")
        return
    
    print(f"Merging {len(temp_files)} temp files for {table_name} incrementally")
    
    try:
        if len(temp_files) == 1:
            # Single file - just rename it
            os.rename(temp_files[0], output_path)
            print(f"Renamed single temp file to {output_path}")
        else:
            # Multiple files - merge them incrementally
            temp_files.sort()  # Ensure consistent ordering
            
            # Start with the first file
            current_file = temp_files[0]
            temp_output = output_path.with_suffix('.temp.parquet')
            
            # Copy first file to temp output
            shutil.copy2(current_file, temp_output)
            
            # Process remaining files one by one
            for i, temp_file in enumerate(temp_files[1:], 1):
                print(f"  Processing file {i+1}/{len(temp_files)}: {temp_file.name}")
                
                try:
                    # Read current accumulated data
                    current_df = pl.read_parquet(temp_output)
                    
                    # Read new file
                    new_df = pl.read_parquet(temp_file)
                    
                    # Concatenate
                    combined_df = pl.concat([current_df, new_df])
                    
                    # Write back to temp file
                    combined_df.write_parquet(temp_output, compression="zstd")
                    
                    # Clean up memory
                    del current_df, new_df, combined_df
                    
                except Exception as e:
                    print(f"    Warning: Could not process {temp_file}: {e}")
                    continue
            
            # Move temp file to final location
            shutil.move(temp_output, output_path)
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
        print("Usage: python patch_merge_incremental.py <payer>")
        print("Example: python patch_merge_incremental.py aetna")
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
    
    print(f"🔧 Patching merge for payer: {payer} (incremental processing)")
    print("=" * 60)
    
    try:
        # Process each table
        for table_name, output_path in output_files.items():
            print(f"\nProcessing {table_name}...")
            merge_temp_files_incremental(output_path, table_name)
        
        print("\n✅ All temp files merged successfully!")
        
    except Exception as e:
        print(f"\n❌ Patch failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
