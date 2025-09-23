#!/usr/bin/env python3
"""
Test script to monitor memory usage during ETL processing
"""

import psutil
import time
import subprocess
import sys
from pathlib import Path

def monitor_memory(duration_seconds=300):
    """Monitor memory usage for a specified duration"""
    print("🔍 Starting memory monitoring...")
    print(f"📊 Monitoring for {duration_seconds} seconds")
    print()
    
    start_time = time.time()
    max_memory = 0
    
    while time.time() - start_time < duration_seconds:
        process = psutil.Process()
        memory_info = process.memory_info()
        system_memory = psutil.virtual_memory()
        
        current_memory_mb = memory_info.rss / (1024 * 1024)
        max_memory = max(max_memory, current_memory_mb)
        
        system_available_gb = system_memory.available / (1024 * 1024 * 1024)
        
        print(f"⏰ {time.strftime('%H:%M:%S')} | "
              f"Process: {current_memory_mb:.1f}MB | "
              f"System Available: {system_available_gb:.1f}GB | "
              f"Max: {max_memory:.1f}MB")
        
        time.sleep(10)  # Check every 10 seconds
    
    print(f"\n📈 Maximum memory usage: {max_memory:.1f}MB")
    return max_memory

def main():
    if len(sys.argv) < 2:
        print("Usage: python test_memory_usage.py <command>")
        print("Example: python test_memory_usage.py 'python ETL/etl1_scalable.py --payer aetna --chunk-size 5000'")
        return 1
    
    command = sys.argv[1]
    print(f"🚀 Running command: {command}")
    print()
    
    # Start monitoring in background
    import threading
    monitor_thread = threading.Thread(target=monitor_memory, args=(1800,))  # 30 minutes
    monitor_thread.daemon = True
    monitor_thread.start()
    
    try:
        # Run the command
        result = subprocess.run(command, shell=True, check=True)
        print("\n✅ Command completed successfully!")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Command failed with exit code {e.returncode}")
        return e.returncode
    except KeyboardInterrupt:
        print("\n⏹️  Command interrupted by user")
        return 1

if __name__ == "__main__":
    sys.exit(main())
