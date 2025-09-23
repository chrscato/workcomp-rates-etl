# ETL1 Memory Issues - Fixes Applied

## Problem Analysis

Your ETL1 pipeline was running into memory issues due to several factors:

1. **SQL Query Error**: The merge operation was using a complex window function with `rowid` that doesn't exist in parquet files
2. **Large Chunk Size**: 5,000 rows per chunk was too large for your memory-constrained system
3. **Memory Limit Too High**: 2GB limit was too aggressive for your available memory
4. **Inefficient Merge Operation**: Trying to load all temp files at once during merge

## Fixes Applied

### 1. Fixed SQL Query in Merge Operation ✅
- **Problem**: Complex window function with non-existent `rowid` column
- **Solution**: Simplified to `SELECT DISTINCT *` which is much more memory efficient
- **Impact**: Eliminates SQL errors and reduces memory usage during merge

### 2. Reduced Chunk Size ✅
- **Before**: 5,000 rows per chunk
- **After**: 1,000 rows per chunk
- **Impact**: 5x reduction in memory per chunk, much safer for constrained systems

### 3. Lowered Memory Limits ✅
- **Before**: 2,048 MB (2GB) limit
- **After**: 1,024 MB (1GB) limit
- **Impact**: More conservative memory usage, prevents system overload

### 4. Implemented Batched Merging ✅
- **Problem**: Loading all temp files at once (2,692 files for dim_code!)
- **Solution**: Process files in batches of 10, create intermediate files
- **Impact**: Prevents memory explosion during merge phase

### 5. Enhanced Memory Monitoring ✅
- **Before**: Check every 10 chunks, warn at 80% memory usage
- **After**: Check every 5 chunks, warn at 70% memory usage
- **Impact**: Earlier detection and intervention of memory pressure

## Recommended Settings for Your System

Based on your memory constraints, use these settings:

```bash
python ETL/scripts/run_etl1_write_only.py \
    --payer aetna \
    --chunk-size 1000 \
    --memory-limit 1024 \
    --force-cleanup
```

## Memory Usage Expectations

With these fixes:
- **Per chunk**: ~50-100MB (vs 250-500MB before)
- **Peak memory**: Should stay under 1GB
- **Merge phase**: Batched processing prevents memory spikes
- **Total processing time**: May be slightly longer due to smaller chunks, but much more stable

## Testing

Run the test script to verify fixes:

```bash
python ETL/scripts/test_memory_fixes.py
```

This will run ETL1 with very conservative settings (500 row chunks, 512MB limit) and monitor memory usage.

## Additional Recommendations

1. **Close other applications** while running ETL1
2. **Monitor system memory** during processing
3. **Consider running overnight** for large datasets
4. **Use SSD storage** for better I/O performance
5. **Set environment variables** to limit threading:
   ```bash
   set POLARS_MAX_THREADS=1
   set OMP_NUM_THREADS=1
   set OPENBLAS_NUM_THREADS=1
   ```

## Expected Results

With 13.4M rates records and 132K provider records:
- **Processing time**: 2-4 hours (vs previous failures)
- **Memory usage**: Stable under 1GB
- **Success rate**: Should complete without memory errors
- **Output files**: All dimension and fact tables created successfully

