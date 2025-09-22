#!/usr/bin/env python3
"""
Cleanup script for locked files that prevent ETL pipeline from running.

This script helps resolve file locking issues by:
1. Identifying processes that might be using the files
2. Attempting to remove locked files with retries
3. Cleaning up temporary files

Usage:
    python ETL/scripts/cleanup_locked_files.py --file data/gold/fact_rate.parquet
    python ETL/scripts/cleanup_locked_files.py --cleanup-all
"""

import os
import sys
import time
import argparse
import psutil
from pathlib import Path
from typing import List, Optional


def find_processes_using_file(file_path: Path) -> List[psutil.Process]:
    """Find processes that might be using the specified file"""
    processes = []
    
    try:
        for proc in psutil.process_iter(['pid', 'name', 'open_files']):
            try:
                if proc.info['open_files']:
                    for open_file in proc.info['open_files']:
                        if str(file_path) in str(open_file.path):
                            processes.append(proc)
                            break
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
    except Exception as e:
        print(f"Warning: Could not scan processes: {e}")
    
    return processes


def force_remove_file(file_path: Path, max_retries: int = 5) -> bool:
    """Attempt to remove a file with retries"""
    if not file_path.exists():
        print(f"File {file_path} does not exist")
        return True
    
    for attempt in range(max_retries):
        try:
            # Check for processes using the file
            processes = find_processes_using_file(file_path)
            if processes:
                print(f"Found {len(processes)} processes using {file_path}:")
                for proc in processes:
                    print(f"  - PID {proc.pid}: {proc.name()}")
                
                if attempt < max_retries - 1:
                    print(f"Waiting 2 seconds before retry {attempt + 2}/{max_retries}...")
                    time.sleep(2)
                    continue
            
            # Try to remove the file
            os.remove(file_path)
            print(f"Successfully removed {file_path}")
            return True
            
        except PermissionError as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Waiting 2 seconds before retry...")
                time.sleep(2)
            else:
                print(f"Failed to remove {file_path} after {max_retries} attempts")
                return False
        except Exception as e:
            print(f"Unexpected error removing {file_path}: {e}")
            return False
    
    return False


def cleanup_temp_files(base_dir: Path) -> None:
    """Clean up temporary files in the data directory"""
    temp_patterns = ["*.temp.parquet", "*.backup.parquet", "*.final.parquet"]
    
    for pattern in temp_patterns:
        temp_files = list(base_dir.rglob(pattern))
        for temp_file in temp_files:
            try:
                os.remove(temp_file)
                print(f"Cleaned up temp file: {temp_file}")
            except Exception as e:
                print(f"Could not clean up {temp_file}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Cleanup locked files for ETL pipeline")
    parser.add_argument("--file", help="Specific file to clean up")
    parser.add_argument("--cleanup-all", action="store_true", help="Clean up all temp files in data directory")
    parser.add_argument("--data-dir", default="../data", help="Data directory path (default: ../data)")
    
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    
    if args.file:
        # Clean up specific file
        file_path = Path(args.file)
        if not file_path.is_absolute():
            file_path = data_dir / file_path
        
        print(f"Attempting to clean up {file_path}")
        success = force_remove_file(file_path)
        if success:
            print("File cleanup completed successfully")
        else:
            print("File cleanup failed")
            sys.exit(1)
    
    if args.cleanup_all:
        # Clean up all temp files
        print("Cleaning up temporary files...")
        cleanup_temp_files(data_dir)
        print("Temp file cleanup completed")
    
    if not args.file and not args.cleanup_all:
        print("No action specified. Use --file or --cleanup-all")
        parser.print_help()


if __name__ == "__main__":
    main()
