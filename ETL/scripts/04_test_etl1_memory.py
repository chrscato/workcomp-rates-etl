#!/usr/bin/env python3
"""
Test script to verify memory fixes in ETL1
"""

import subprocess
import sys
import time
import psutil
from pathlib import Path

def get_memory_usage():
    """Get current memory usage"""
    process = psutil.Process()
    memory_info = process.memory_info()
    return {
        "process_mb": memory_info.rss / (1024 * 1024),
        "process_percent": process.memory_percent()
    }

def test_etl1_memory():
    """Test ETL1 with memory monitoring"""
    print("🧪 Testing ETL1 memory fixes...")
    print(f"Initial memory: {get_memory_usage()['process_mb']:.1f}MB")
    
    # Test with very conservative settings
    cmd = [
        "python", "ETL/scripts/run_etl1_write_only.py",
        "--payer", "aetna",
        "--chunk-size", "500",  # Even smaller chunks
        "--memory-limit", "512",  # Very low memory limit
        "--force-cleanup"
    ]
    
    print(f"Running: {' '.join(cmd)}")
    print("Monitoring memory usage...")
    
    start_time = time.time()
    initial_memory = get_memory_usage()
    
    try:
        # Run the ETL process
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Monitor memory usage
        max_memory = initial_memory['process_mb']
        memory_samples = []
        
        while process.poll() is None:
            current_memory = get_memory_usage()
            memory_samples.append(current_memory['process_mb'])
            max_memory = max(max_memory, current_memory['process_mb'])
            
            if len(memory_samples) % 10 == 0:  # Print every 10 samples
                print(f"Memory: {current_memory['process_mb']:.1f}MB (max: {max_memory:.1f}MB)")
            
            time.sleep(2)  # Check every 2 seconds
        
        # Get final result
        stdout, stderr = process.communicate()
        return_code = process.returncode
        
        end_time = time.time()
        final_memory = get_memory_usage()
        
        print(f"\n📊 Results:")
        print(f"   Duration: {end_time - start_time:.1f} seconds")
        print(f"   Initial memory: {initial_memory['process_mb']:.1f}MB")
        print(f"   Final memory: {final_memory['process_mb']:.1f}MB")
        print(f"   Peak memory: {max_memory:.1f}MB")
        print(f"   Return code: {return_code}")
        
        if return_code == 0:
            print("✅ ETL1 completed successfully!")
        else:
            print("❌ ETL1 failed!")
            print("STDOUT:", stdout[-1000:])  # Last 1000 chars
            if stderr:
                print("STDERR:", stderr[-1000:])
        
        return return_code == 0
        
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        return False

if __name__ == "__main__":
    success = test_etl1_memory()
    sys.exit(0 if success else 1)
