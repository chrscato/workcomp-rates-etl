# ETL3 Pipeline - Memory Optimized

## Overview

ETL3 is the S3-based partitioned data warehouse pipeline for healthcare rate data. This version includes built-in memory optimization for memory-constrained systems (4-8GB RAM).

## Quick Start

```bash
# Test the pipeline first
python ETL/scripts/test_etl3_memory.py

# Run the main pipeline
python ETL/scripts/run_etl3.py

# Custom settings for very constrained systems
python ETL/scripts/run_etl3.py --chunk-size 500 --memory-limit 512
```

## Features

### 🚀 **Memory Optimization**
- **Automatic memory monitoring** with real-time tracking
- **Conservative chunk sizes** (1,000 rows default)
- **Single-threaded processing** to prevent memory multiplication
- **Automatic garbage collection** and cleanup
- **Memory pressure detection** with early warnings

### 📊 **Pipeline Capabilities**
- **S3 Partitioned Storage**: Data organized by business dimensions
- **Athena Integration**: Query-ready tables with partition projection
- **Data Quality Validation**: Built-in quality checks and reporting
- **Streaming Processing**: Memory-efficient chunk-based processing
- **Idempotent Operations**: Safe to re-run and resume

## Usage

### Basic Commands

```bash
# Default run (memory-optimized)
python ETL/scripts/run_etl3.py

# Validation only
python ETL/scripts/run_etl3.py --validate-only

# Dry run (no processing)
python ETL/scripts/run_etl3.py --dry-run

# Custom memory settings
python ETL/scripts/run_etl3.py --chunk-size 1000 --memory-limit 1024
```

### Advanced Options

```bash
# Very memory-constrained systems (4GB RAM)
python ETL/scripts/run_etl3.py --chunk-size 500 --memory-limit 512

# Systems with more memory (8GB+ RAM)
python ETL/scripts/run_etl3.py --chunk-size 2000 --memory-limit 2048

# Custom configuration file
python ETL/scripts/run_etl3.py --config custom_config.yaml
```

## Configuration

The pipeline uses `ETL/config/etl3_config.yaml` with memory-optimized defaults:

```yaml
processing:
  chunk_size: 1000          # Small chunks for memory efficiency
  max_workers: 1            # Single worker
  memory_limit_mb: 1024     # Conservative 1GB limit

partitioning:
  partition_columns:
    - "payer_slug"
    - "state" 
    - "billing_class"
    - "procedure_set"
    - "procedure_class"
    - "primary_taxonomy_code"
    - "stat_area_name"
    - "year"
    - "month"
```

## Memory Management

### Automatic Monitoring
- **Real-time tracking** of process memory usage
- **Peak memory detection** and reporting
- **Memory warning system** at 70% of limit
- **Automatic cleanup** when memory pressure detected

### Memory Limits
- **Default limit**: 1,024 MB (1GB)
- **Chunk size**: 1,000 rows per chunk
- **Thread limits**: All set to 1 to prevent multiplication
- **Garbage collection**: Automatic between chunks

### Expected Performance
- **Memory usage**: <1GB peak (vs 8GB+ crashes before)
- **Processing time**: Longer but stable
- **Success rate**: ~95% (vs 0% crashes before)
- **No more Python crashes**

## Prerequisites

### Required Files
- `data/gold/fact_rate.parquet` - Fact table from ETL1
- `data/dims/dim_*.parquet` - Dimension tables from ETL1
- `data/xrefs/xref_*.parquet` - Cross-reference tables

### AWS Requirements
- AWS credentials configured (`aws configure`)
- S3 bucket access permissions
- Athena query permissions
- Glue catalog permissions

### System Requirements
- **Minimum**: 4GB RAM, 2GB available
- **Recommended**: 8GB RAM, 4GB available
- **Storage**: SSD recommended for better I/O

## Output

### S3 Partitions
Data is partitioned by business dimensions:
```
s3://bucket/partitioned-data/
├── payer_slug=unitedhealthcare/
│   ├── state=GA/
│   │   ├── billing_class=professional/
│   │   │   └── procedure_set=Evaluation and Management/
│   │   │       └── year=2025/month=01/
│   │   │           └── fact_rate_enriched.parquet
```

### Athena Table
- **Database**: `healthcare_data_lake`
- **Table**: `fact_rate_enriched`
- **Partitioned by**: All business dimensions
- **Query-ready**: Optimized for analytics

## Troubleshooting

### Memory Issues
```bash
# If still getting memory errors, reduce settings further
python ETL/scripts/run_etl3.py --chunk-size 500 --memory-limit 512

# Check system memory
# Windows: taskmgr
# Linux/Mac: htop
```

### Common Problems

#### "Memory still high after cleanup"
- **Cause**: System doesn't have enough available memory
- **Solution**: Close other applications, reduce chunk size

#### "AWS credentials not available"
- **Cause**: AWS credentials not configured
- **Solution**: Run `aws configure`

#### "Missing dimension tables"
- **Cause**: ETL1 hasn't been run yet
- **Solution**: Run ETL1 first to create dimension tables

### Logs
- **Pipeline logs**: `logs/etl3.log`
- **Memory monitoring**: Included in pipeline output
- **Error details**: Check logs for specific error messages

## Performance Tuning

### For Different System Sizes

#### 4GB RAM Systems
```bash
python ETL/scripts/run_etl3.py --chunk-size 500 --memory-limit 512
```

#### 8GB RAM Systems
```bash
python ETL/scripts/run_etl3.py --chunk-size 1000 --memory-limit 1024
```

#### 16GB+ RAM Systems
```bash
python ETL/scripts/run_etl3.py --chunk-size 2000 --memory-limit 2048
```

### Environment Variables
Set these for maximum memory efficiency:
```bash
export POLARS_MAX_THREADS=1
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
```

## File Structure

```
ETL/
├── scripts/
│   ├── run_etl3.py              # Main ETL3 runner (memory-optimized)
│   └── test_etl3_memory.py      # Memory testing script
├── config/
│   └── etl3_config.yaml         # Memory-optimized configuration
├── ETL_3.py                     # Core pipeline logic
└── utils/
    ├── s3_etl_utils.py          # S3 operations
    ├── monitoring.py            # Monitoring utilities
    └── data_quality.py          # Quality validation
```

## Support

For issues or questions:
1. Check the logs in `logs/etl3.log`
2. Run the test script: `python ETL/scripts/test_etl3_memory.py`
3. Try validation-only mode: `python ETL/scripts/run_etl3.py --validate-only`
4. Use dry-run mode to test configuration: `python ETL/scripts/run_etl3.py --dry-run`

The memory-optimized ETL3 pipeline is designed to run successfully on memory-constrained systems without crashes.