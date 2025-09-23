#!/usr/bin/env python3
"""
Test script for memory-optimized ETL3 pipeline.
This script tests the memory fixes with very conservative settings.
"""

import os
import sys
import logging
import psutil
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "ETL" / "utils"))

from ETL.ETL_3 import ETL3Config
from ETL.utils.monitoring import ETLMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_memory_usage():
    """Get current memory usage"""
    process = psutil.Process()
    memory_info = process.memory_info()
    system_memory = psutil.virtual_memory()
    
    return {
        "process_mb": memory_info.rss / (1024 * 1024),
        "process_percent": process.memory_percent(),
        "system_available_gb": system_memory.available / (1024 * 1024 * 1024),
        "system_percent": system_memory.percent
    }


def test_memory_optimized_etl3():
    """Test ETL3 with memory monitoring"""
    print("🧪 Testing Memory-Optimized ETL3 Pipeline...")
    
    # Set environment variables for memory optimization
    os.environ['POLARS_MAX_THREADS'] = '1'
    os.environ['OMP_NUM_THREADS'] = '1'
    os.environ['OPENBLAS_NUM_THREADS'] = '1'
    os.environ['MKL_NUM_THREADS'] = '1'
    
    initial_memory = get_memory_usage()
    print(f"Initial memory: {initial_memory['process_mb']:.1f}MB")
    print(f"System memory available: {initial_memory['system_available_gb']:.1f}GB")
    
    # Create ultra-conservative configuration
    config = ETL3Config('ETL/config/etl3_config_memory_optimized.yaml')
    config.CHUNK_SIZE = 500  # Ultra-small chunks for testing
    config.MAX_WORKERS = 1   # Single worker
    config.MEMORY_LIMIT_MB = 512  # Very low memory limit
    
    print(f"\nConfiguration:")
    print(f"  Chunk size: {config.CHUNK_SIZE:,}")
    print(f"  Max workers: {config.MAX_WORKERS}")
    print(f"  Memory limit: {config.MEMORY_LIMIT_MB} MB")
    
    # Test configuration validation
    print(f"\nValidating configuration...")
    if not config.validate():
        print("❌ Configuration validation failed")
        return False
    
    print("✅ Configuration validation passed")
    
    # Test dimension loading (this is where memory issues often occur)
    print(f"\nTesting dimension loading...")
    try:
        dimensions = {}
        for name, path in config.DIM_PATHS.items():
            if path.exists():
                import polars as pl
                dimensions[name] = pl.read_parquet(path)
                print(f"  ✅ Loaded {dimensions[name].height:,} {name} records")
            else:
                print(f"  ⚠️  Missing: {name}")
        
        # Check memory after loading dimensions
        memory_after_dims = get_memory_usage()
        print(f"Memory after loading dimensions: {memory_after_dims['process_mb']:.1f}MB")
        print(f"Memory increase: {memory_after_dims['process_mb'] - initial_memory['process_mb']:.1f}MB")
        
        # Test fact table scanning (lazy evaluation)
        print(f"\nTesting fact table scanning...")
        fact_lazy = pl.scan_parquet(config.FACT_RATE_PATH)
        total_rows = fact_lazy.select(pl.count()).collect().item()
        print(f"  ✅ Fact table has {total_rows:,} rows")
        
        # Test chunk processing
        print(f"\nTesting chunk processing...")
        chunk_size = config.CHUNK_SIZE
        test_chunks = min(3, (total_rows + chunk_size - 1) // chunk_size)  # Test first 3 chunks
        
        for chunk_idx in range(test_chunks):
            chunk_start = chunk_idx * chunk_size
            chunk_data = fact_lazy.slice(chunk_start, chunk_size).collect()
            
            if chunk_data.height == 0:
                break
            
            print(f"  ✅ Chunk {chunk_idx + 1}: {chunk_data.height:,} rows")
            
            # Check memory after each chunk
            memory_after_chunk = get_memory_usage()
            print(f"    Memory: {memory_after_chunk['process_mb']:.1f}MB")
            
            # Cleanup
            del chunk_data
            import gc
            gc.collect()
        
        print("✅ Chunk processing test passed")
        
        # Final memory check
        final_memory = get_memory_usage()
        print(f"\nFinal memory: {final_memory['process_mb']:.1f}MB")
        print(f"Peak memory increase: {final_memory['process_mb'] - initial_memory['process_mb']:.1f}MB")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        final_memory = get_memory_usage()
        print(f"Memory at failure: {final_memory['process_mb']:.1f}MB")
        return False


def test_s3_operations():
    """Test S3 operations with memory monitoring"""
    print(f"\n🧪 Testing S3 operations...")
    
    try:
        from ETL.utils.s3_etl_utils import S3PartitionedETL
        
        # Initialize S3 ETL (this will test AWS credentials)
        s3_etl = S3PartitionedETL(
            bucket_name="healthcare-data-lake-prod",
            region="us-east-1"
        )
        
        print("✅ S3 ETL initialized successfully")
        
        # Test partition path creation
        test_partition = {
            'payer_slug': 'test-payer',
            'state': 'GA',
            'billing_class': 'professional',
            'procedure_set': 'test-set',
            'procedure_class': 'test-class',
            'primary_taxonomy_code': 'test-code',
            'stat_area_name': 'test-area',
            'year': '2025',
            'month': '01'
        }
        
        s3_path = s3_etl.create_s3_path(test_partition)
        print(f"✅ S3 path created: {s3_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ S3 test failed: {str(e)}")
        return False


def main():
    """Main test function"""
    print("="*60)
    print("ETL3 MEMORY OPTIMIZATION TEST")
    print("="*60)
    
    # Test 1: Memory-optimized ETL3
    test1_success = test_memory_optimized_etl3()
    
    # Test 2: S3 operations
    test2_success = test_s3_operations()
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Memory-optimized ETL3: {'✅ PASSED' if test1_success else '❌ FAILED'}")
    print(f"S3 operations: {'✅ PASSED' if test2_success else '❌ FAILED'}")
    
    if test1_success and test2_success:
        print("\n🎉 All tests passed! ETL3 is ready for memory-optimized processing.")
        print("\nRecommended command:")
        print("python ETL/scripts/run_etl3_memory_optimized.py --chunk-size 1000 --memory-limit 1024")
        return True
    else:
        print("\n❌ Some tests failed. Please check the errors above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
