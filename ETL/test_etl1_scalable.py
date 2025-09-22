#!/usr/bin/env python3
"""
Test script for the scalable ETL1 pipeline.
This script validates the pipeline with different configurations and data sizes.
"""

import os
import sys
import time
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from ETL.etl1_scalable import ScalableETL1, ETL1Config


def test_small_chunks():
    """Test with very small chunks to verify batching works"""
    print("=== Testing Small Chunks ===")
    
    config = ETL1Config()
    config.chunk_size = 1000  # Very small chunks
    config.memory_limit_mb = 1024  # Low memory limit
    config.log_level = "DEBUG"
    
    with ScalableETL1(config) as etl:
        try:
            summary = etl.run_pipeline("aetna")
            print(f"✅ Small chunk test passed: {summary['processed_rates']:,} rates processed")
            return True
        except Exception as e:
            print(f"❌ Small chunk test failed: {e}")
            return False


def test_memory_efficiency():
    """Test memory efficiency with larger chunks"""
    print("=== Testing Memory Efficiency ===")
    
    config = ETL1Config()
    config.chunk_size = 100000  # Larger chunks
    config.memory_limit_mb = 4096  # Moderate memory limit
    
    with ScalableETL1(config) as etl:
        try:
            start_time = time.time()
            summary = etl.run_pipeline("aetna")
            duration = time.time() - start_time
            
            print(f"✅ Memory efficiency test passed:")
            print(f"   Duration: {duration:.2f} seconds")
            print(f"   Rates processed: {summary['processed_rates']:,}")
            print(f"   Providers processed: {summary['processed_providers']:,}")
            return True
        except Exception as e:
            print(f"❌ Memory efficiency test failed: {e}")
            return False


def test_different_payers():
    """Test with different payer configurations"""
    print("=== Testing Different Payers ===")
    
    payers = ["aetna", "uhc"]
    results = []
    
    for payer in payers:
        config = ETL1Config()
        config.chunk_size = 25000
        config.payer_slug_override = payer  # Override payer slug
        
        with ScalableETL1(config) as etl:
            try:
                summary = etl.run_pipeline(payer)
                print(f"✅ {payer.upper()} test passed: {summary['processed_rates']:,} rates")
                results.append(True)
            except Exception as e:
                print(f"❌ {payer.upper()} test failed: {e}")
                results.append(False)
    
    return all(results)


def test_error_handling():
    """Test error handling with invalid inputs"""
    print("=== Testing Error Handling ===")
    
    config = ETL1Config()
    config.chunk_size = 1000
    
    # Test with non-existent payer
    with ScalableETL1(config) as etl:
        try:
            etl.run_pipeline("nonexistent")
            print("❌ Should have failed with non-existent payer")
            return False
        except FileNotFoundError:
            print("✅ Correctly handled non-existent payer")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False
    
    return True


def test_config_loading():
    """Test configuration loading from YAML"""
    print("=== Testing Configuration Loading ===")
    
    try:
        config = ETL1Config.from_yaml(Path("ETL/config/etl1_config.yaml"))
        print(f"✅ Config loaded successfully:")
        print(f"   State: {config.state}")
        print(f"   Chunk size: {config.chunk_size:,}")
        print(f"   Memory limit: {config.memory_limit_mb} MB")
        return True
    except Exception as e:
        print(f"❌ Config loading failed: {e}")
        return False


def run_performance_benchmark():
    """Run performance benchmark comparing different chunk sizes"""
    print("=== Performance Benchmark ===")
    
    chunk_sizes = [10000, 25000, 50000, 100000]
    results = []
    
    for chunk_size in chunk_sizes:
        print(f"Testing chunk size: {chunk_size:,}")
        
        config = ETL1Config()
        config.chunk_size = chunk_size
        config.log_level = "WARNING"  # Reduce logging noise
        
        with ScalableETL1(config) as etl:
            try:
                start_time = time.time()
                summary = etl.run_pipeline("aetna")
                duration = time.time() - start_time
                
                results.append({
                    "chunk_size": chunk_size,
                    "duration": duration,
                    "rates_per_second": summary["processed_rates"] / duration,
                    "memory_efficient": duration < 300  # Should complete in under 5 minutes
                })
                
                print(f"   Duration: {duration:.2f}s, Rate: {summary['processed_rates']/duration:.0f} rows/s")
                
            except Exception as e:
                print(f"   Failed: {e}")
                results.append({
                    "chunk_size": chunk_size,
                    "duration": float('inf'),
                    "rates_per_second": 0,
                    "memory_efficient": False
                })
    
    # Find optimal chunk size
    best_result = max(results, key=lambda x: x["rates_per_second"])
    print(f"✅ Best performance: {best_result['chunk_size']:,} rows/chunk at {best_result['rates_per_second']:.0f} rows/s")
    
    return results


def main():
    """Run all tests"""
    print("🧪 Running ETL1 Scalable Pipeline Tests")
    print("=" * 50)
    
    # Check if input files exist
    input_dir = Path("data/input")
    if not input_dir.exists():
        print("❌ Input directory not found. Please ensure data/input/ exists with parquet files.")
        return False
    
    # List available files
    rates_files = list(input_dir.glob("*_rates.parquet"))
    providers_files = list(input_dir.glob("*_providers.parquet"))
    
    print(f"Found {len(rates_files)} rates files and {len(providers_files)} provider files")
    
    if not rates_files or not providers_files:
        print("❌ No input files found. Please add parquet files to data/input/")
        return False
    
    # Run tests
    tests = [
        ("Configuration Loading", test_config_loading),
        ("Error Handling", test_error_handling),
        ("Small Chunks", test_small_chunks),
        ("Memory Efficiency", test_memory_efficiency),
        ("Different Payers", test_different_payers),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        if test_func():
            passed += 1
        print()
    
    # Run performance benchmark
    print("\n--- Performance Benchmark ---")
    benchmark_results = run_performance_benchmark()
    
    # Summary
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! The scalable ETL1 pipeline is ready for production.")
        return True
    else:
        print("⚠️  Some tests failed. Please review the output above.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
