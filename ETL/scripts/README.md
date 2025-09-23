# ETL Scripts Directory

This directory contains all the scripts needed to run and manage the ETL pipelines. The scripts have been cleaned up and organized for clarity.

## 🚀 Main ETL Scripts

### `01_run_etl1_main.py` ⭐ **RECOMMENDED**
**Primary ETL1 runner with memory fixes**
- **Purpose**: Main script to run ETL1 pipeline with optimized memory settings
- **Usage**: `python ETL/scripts/01_run_etl1_main.py --payer aetna --force-cleanup`
- **Features**: 
  - Memory-optimized processing (1000 row chunks, 1GB limit)
  - Batched temp file merging
  - Automatic cleanup
  - Progress monitoring
- **Best for**: Production runs, memory-constrained systems

### `02_run_etl1_advanced.py`
**Advanced ETL1 runner with more features**
- **Purpose**: Alternative ETL1 runner with additional configuration options
- **Usage**: `python ETL/scripts/02_run_etl1_advanced.py --payer aetna --config ETL/config/etl1_config.yaml`
- **Features**:
  - YAML configuration support
  - Dry-run mode
  - Verbose logging
  - Custom chunk sizes and memory limits
- **Best for**: Development, testing different configurations

## 🧪 Testing & Monitoring Scripts

### `03_test_memory_monitor.py`
**Memory usage monitoring tool**
- **Purpose**: Monitor memory usage during ETL processing
- **Usage**: `python ETL/scripts/03_test_memory_monitor.py`
- **Features**:
  - Real-time memory tracking
  - System memory monitoring
  - Peak memory detection
- **Best for**: Debugging memory issues, performance analysis

### `04_test_etl1_memory.py`
**ETL1 memory testing with conservative settings**
- **Purpose**: Test ETL1 with very conservative memory settings
- **Usage**: `python ETL/scripts/04_test_etl1_memory.py`
- **Features**:
  - Ultra-small chunks (500 rows)
  - Low memory limit (512MB)
  - Real-time memory monitoring
  - Automatic success/failure reporting
- **Best for**: Testing fixes, validating memory optimizations

## 🛠️ Utility Scripts

### `05_cleanup_locked_files.py`
**Clean up locked or corrupted files**
- **Purpose**: Remove files that may be locked or causing issues
- **Usage**: `python ETL/scripts/05_cleanup_locked_files.py`
- **Features**:
  - Force remove locked files
  - Clean up temp directories
  - Reset file permissions
- **Best for**: Troubleshooting file access issues

### `run_etl3.py` ⭐ **MAIN ETL3 RUNNER**
**Memory-optimized ETL3 pipeline runner**
- **Purpose**: Main ETL3 pipeline with built-in memory optimization
- **Usage**: `python ETL/scripts/run_etl3.py`
- **Features**:
  - Memory-optimized processing (1000 row chunks, 1GB limit)
  - Real-time memory monitoring
  - Automatic garbage collection
  - S3 partitioned data warehouse
  - Athena table creation
- **Best for**: Production ETL3 runs, memory-constrained systems

### `test_etl3_memory.py`
**ETL3 memory testing script**
- **Purpose**: Test ETL3 with memory monitoring and validation
- **Usage**: `python ETL/scripts/test_etl3_memory.py`
- **Features**:
  - Memory usage validation
  - Configuration testing
  - S3 operations testing
  - Ultra-conservative settings
- **Best for**: Testing memory optimizations, debugging ETL3

### `setup_aws_resources.py`
**AWS infrastructure setup**
- **Purpose**: Set up AWS resources for ETL processing
- **Usage**: `python ETL/scripts/setup_aws_resources.py`
- **Features**:
  - S3 bucket creation
  - IAM role setup
  - Resource configuration
- **Best for**: Initial AWS setup

## 📋 Quick Start Guide

### For ETL1 (Most Common):
```bash
# Basic run with memory fixes
python ETL/scripts/01_run_etl1_main.py --payer aetna --force-cleanup

# Test with conservative settings
python ETL/scripts/04_test_etl1_memory.py

# Monitor memory during processing
python ETL/scripts/03_test_memory_monitor.py
```

### For ETL3:
```bash
# Test memory optimizations first
python ETL/scripts/test_etl3_memory.py

# Run main ETL3 pipeline
python ETL/scripts/run_etl3.py

# Custom memory settings for constrained systems
python ETL/scripts/run_etl3.py --chunk-size 500 --memory-limit 512
```

## 🗑️ Removed Scripts

The following scripts were removed as they were duplicates or outdated:

- ❌ `run_etl1_memory_optimized.py` → Duplicate of `01_run_etl1_main.py`
- ❌ `patch_merge_simple.py` → Outdated (superseded by fixed ETL1)
- ❌ `patch_merge_polars.py` → Outdated (superseded by fixed ETL1)
- ❌ `patch_merge_incremental.py` → Outdated (superseded by fixed ETL1)
- ❌ `patch_merge_temp_files.py` → Outdated (superseded by fixed ETL1)
- ❌ `patch_merge_temp_files_batched.py` → Outdated (superseded by fixed ETL1)
- ❌ `finish_fact_rate_merge.py` → Outdated (superseded by fixed ETL1)

## 🔧 Memory Optimization Features

All ETL1 scripts now include:
- **Batched Processing**: Process files in small batches to avoid memory spikes
- **Conservative Chunk Sizes**: 1000 rows per chunk (vs 5000 before)
- **Memory Monitoring**: Real-time memory usage tracking
- **Automatic Cleanup**: Clean up temp files and force garbage collection
- **Error Recovery**: Better error handling and recovery mechanisms

## 📊 Expected Performance

With the memory fixes:
- **Memory Usage**: Stable under 1GB (vs 2GB+ before)
- **Processing Time**: 2-4 hours for 13.4M records
- **Success Rate**: Should complete without memory errors
- **Chunk Size**: 1000 rows (5x smaller than before)

## 🆘 Troubleshooting

If you encounter issues:

1. **Memory Errors**: Use `04_test_etl1_memory.py` to test with ultra-conservative settings
2. **File Locks**: Use `05_cleanup_locked_files.py` to clean up locked files
3. **Memory Monitoring**: Use `03_test_memory_monitor.py` to track memory usage
4. **Configuration Issues**: Use `02_run_etl1_advanced.py` with custom settings

## 📁 File Organization

```
ETL/scripts/
├── 01_run_etl1_main.py      # ⭐ Main ETL1 runner
├── 02_run_etl1_advanced.py  # Advanced ETL1 runner
├── 03_test_memory_monitor.py # Memory monitoring
├── 04_test_etl1_memory.py   # ETL1 memory testing
├── 05_cleanup_locked_files.py # File cleanup utility
├── run_etl3.py              # ETL3 pipeline
├── setup_aws_resources.py   # AWS setup
└── README.md                # This file
```

The scripts are numbered in order of importance and usage frequency.

