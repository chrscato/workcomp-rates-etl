#!/usr/bin/env python3
"""
Write-only ETL1 runner script

This script runs ETL1 with the new write-only approach that avoids
reading existing large parquet files during processing.
"""

import subprocess
import sys
import argparse
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="Write-only ETL1 runner")
    parser.add_argument("--payer", required=True, help="Payer name (e.g., aetna, uhc)")
    parser.add_argument("--state", default="GA", help="State code (default: GA)")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Chunk size (default: 1000)")
    parser.add_argument("--memory-limit", type=int, default=1024, help="Memory limit in MB (default: 1024)")
    parser.add_argument("--force-cleanup", action="store_true", help="Force cleanup of existing files")
    
    args = parser.parse_args()
    
    # Build command
    cmd = [
        "python", "ETL/etl1_scalable.py",
        "--payer", args.payer,
        "--state", args.state,
        "--chunk-size", str(args.chunk_size),
        "--memory-limit", str(args.memory_limit)
    ]
    
    if args.force_cleanup:
        cmd.append("--force-cleanup")
    
    print(f"🚀 Running ETL1 with write-only approach:")
    print(f"   Payer: {args.payer}")
    print(f"   State: {args.state}")
    print(f"   Chunk size: {args.chunk_size:,}")
    print(f"   Memory limit: {args.memory_limit}MB")
    print(f"   Force cleanup: {args.force_cleanup}")
    print()
    print("📝 This approach will:")
    print("   - Write each chunk to separate temp files")
    print("   - Avoid reading existing large parquet files")
    print("   - Merge all temp files in batches at the end")
    print("   - Use minimal memory throughout processing")
    print("   - Process 1000 rows per chunk (memory-optimized)")
    print()
    
    try:
        # Run the ETL
        result = subprocess.run(cmd, check=True)
        print("✅ ETL1 completed successfully!")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"❌ ETL1 failed with exit code {e.returncode}")
        return e.returncode
    except KeyboardInterrupt:
        print("\n⏹️  ETL1 interrupted by user")
        return 1

if __name__ == "__main__":
    sys.exit(main())
