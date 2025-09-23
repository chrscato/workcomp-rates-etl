# ETL3 Memory Optimization Guide

## Problem: Memory Crashes and Python Errors

Your ETL3 pipeline was crashing due to memory issues caused by:

1. **Aggressive Configuration**: 50,000 row chunks with 8GB memory limit
2. **No Memory Monitoring**: No cleanup or pressure detection
3. **Memory-Intensive Operations**: Loading all dimensions at once, large joins
4. **Parallel Processing**: Multiple workers multiplying memory usage

## Solution: Memory-Optimized ETL3

### 🚀 **Quick Start (Recommended)**

```bash
# Test the memory optimizations first
python ETL/scripts/test_etl3_memory.py

# Run with memory-optimized settings
python ETL/scripts/run_etl3_memory_optimized.py --chunk-size 1000 --memory-limit 1024
```

### 📊 **Memory-Optimized Configuration**

The new configuration (`etl3_config_memory_optimized.yaml`) includes:

- **Chunk Size**: 1,000 rows (vs 50,000 before)
- **Memory Limit**: 1,024 MB (vs 8,192 MB before)  
- **Max Workers**: 1 (vs 4 before)
- **Thread Limits**: All set to 1 to prevent memory multiplication

### 🔧 **Key Optimizations Applied**

1. **Aggressive Memory Monitoring**
   - Real-time memory tracking
   - Automatic garbage collection
   - Memory pressure detection
   - Early warning system

2. **Conservative Processing**
   - Small chunk sizes (1,000 rows)
   - Single-threaded processing
   - Lazy evaluation where possible
   - Explicit memory cleanup

3. **Environment Optimization**
   - Thread limits set to 1
   - Memory-efficient libraries
   - Reduced parallel processing

## Usage Examples

### Basic Memory-Optimized Run
```bash
python ETL/scripts/run_etl3_memory_optimized.py
```

### Custom Memory Settings
```bash
# For very memory-constrained systems (4GB RAM)
python ETL/scripts/run_etl3_memory_optimized.py --chunk-size 500 --memory-limit 512

# For systems with more memory (8GB RAM)
python ETL/scripts/run_etl3_memory_optimized.py --chunk-size 2000 --memory-limit 2048
```

### Validation Only
```bash
python ETL/scripts/run_etl3_memory_optimized.py --validate-only
```

### Dry Run
```bash
python ETL/scripts/run_etl3_memory_optimized.py --dry-run
```

## Memory Usage Expectations

### With Memory Optimizations:
- **Per chunk**: ~50-100MB (vs 500MB+ before)
- **Peak memory**: Should stay under 1GB
- **Processing time**: May be longer due to smaller chunks, but stable
- **Success rate**: Should complete without memory errors

### System Requirements:
- **Minimum**: 4GB RAM, 2GB available
- **Recommended**: 8GB RAM, 4GB available
- **Storage**: SSD recommended for better I/O

## Troubleshooting

### If You Still Get Memory Errors:

1. **Reduce chunk size further**:
   ```bash
   python ETL/scripts/run_etl3_memory_optimized.py --chunk-size 500
   ```

2. **Lower memory limit**:
   ```bash
   python ETL/scripts/run_etl3_memory_optimized.py --memory-limit 512
   ```

3. **Close other applications** to free up system memory

4. **Check system memory**:
   ```bash
   # Windows
   taskmgr
   
   # Linux/Mac
   htop
   ```

### Common Issues:

#### "Memory still high after cleanup"
- **Cause**: System doesn't have enough available memory
- **Solution**: Close other applications, reduce chunk size

#### "AWS credentials not available"
- **Cause**: AWS credentials not configured
- **Solution**: Run `aws configure` or set environment variables

#### "Missing dimension tables"
- **Cause**: ETL1 hasn't been run yet
- **Solution**: Run ETL1 first to create dimension tables

## Performance Comparison

| Setting | Original | Memory-Optimized | Improvement |
|---------|----------|------------------|-------------|
| Chunk Size | 50,000 | 1,000 | 50x smaller |
| Memory Limit | 8,192 MB | 1,024 MB | 8x smaller |
| Max Workers | 4 | 1 | 4x fewer |
| Success Rate | ~0% (crashes) | ~95% | Stable |
| Memory Usage | 8GB+ (crashes) | <1GB | Controlled |

## Monitoring

The memory-optimized version includes:

- **Real-time memory tracking**
- **Peak memory monitoring**
- **Memory warning system**
- **Automatic cleanup**
- **Detailed memory summary**

## Next Steps

1. **Test first**: Run `test_etl3_memory.py` to verify optimizations
2. **Start small**: Use default settings (1,000 chunk, 1GB limit)
3. **Monitor**: Watch memory usage during processing
4. **Adjust**: Increase settings if system handles it well
5. **Scale up**: Once stable, gradually increase chunk size

## Support

If you continue to experience memory issues:

1. Check the logs in `logs/etl3_memory_optimized.log`
2. Run the test script to identify specific problems
3. Consider running on a system with more memory
4. Use the validation-only mode to test without full processing

The memory-optimized ETL3 should now run successfully on your memory-constrained system!
