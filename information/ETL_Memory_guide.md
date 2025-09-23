# ETL Memory Management Best Practices

## Industry Standard Approaches for Memory-Constrained ETL

### 1. **Streaming ETL (Your Current Approach) ✅**
Your pipeline already implements this correctly:
- Process data in small chunks
- Write temporary files instead of holding in memory
- Use lazy evaluation (Polars `scan_parquet`)

### 2. **Database-Backed Processing**
Instead of in-memory operations, use a database:

```python
# Instead of Polars DataFrame operations
df_merged = df1.join(df2, on="key")

# Use DuckDB for everything
conn.execute("""
    CREATE TABLE merged AS 
    SELECT * FROM read_parquet('file1.parquet') a
    JOIN read_parquet('file2.parquet') b ON a.key = b.key
""")
```

### 3. **Disk-Based Sorting and Merging**
```python
# Use external sorting for large datasets
import heapq
import tempfile

def external_merge_sort(files, output_file, key_func):
    """Merge multiple sorted files using minimal memory"""
    with open(output_file, 'w') as outf:
        file_handles = [open(f, 'r') for f in files]
        heap = []
        
        # Initialize heap with first record from each file
        for i, fh in enumerate(file_handles):
            try:
                record = next(fh)
                heapq.heappush(heap, (key_func(record), i, record))
            except StopIteration:
                pass
        
        # Merge records
        while heap:
            _, file_idx, record = heapq.heappop(heap)
            outf.write(record)
            
            # Add next record from same file
            try:
                next_record = next(file_handles[file_idx])
                heapq.heappush(heap, (key_func(next_record), file_idx, next_record))
            except StopIteration:
                pass
```

### 4. **Columnar Processing**
```python
# Instead of loading entire rows
df = pl.scan_parquet("large_file.parquet")
result = df.select(["col1", "col2"]).collect()  # Only needed columns

# Process column by column for very wide tables
for column in ["col1", "col2", "col3"]:
    col_data = pl.scan_parquet("file.parquet").select(column).collect()
    process_column(col_data)
    del col_data  # Explicit cleanup
```

### 5. **Partitioned Processing**
```python
# Partition large datasets by a key
def process_partitioned(input_file, partition_key):
    # Read unique partition values first
    partitions = pl.scan_parquet(input_file).select(partition_key).unique().collect()
    
    for partition in partitions:
        chunk = pl.scan_parquet(input_file).filter(
            pl.col(partition_key) == partition
        ).collect()
        process_chunk(chunk)
        del chunk
```

## Memory Optimization Checklist

### ✅ **Configuration Optimizations**
- [x] Small chunk sizes (1,000-5,000 rows)
- [x] Conservative memory limits (1-2GB)
- [x] Single-threaded processing
- [x] Disable caching and buffers

### ✅ **Processing Optimizations**  
- [x] Lazy evaluation (scan_parquet vs read_parquet)
- [x] Column pruning (only select needed columns)
- [x] Early filtering (filter before collect())
- [x] Explicit garbage collection

### ✅ **Storage Optimizations**
- [x] Compressed parquet files (ZSTD)
- [x] Temporary file cleanup
- [x] Columnar storage format
- [x] Partitioned outputs

### 🔧 **Your Specific Fixes Needed**

1. **Batch the merge operation** (biggest issue)
   - Current: Merge all temp files at once
   - Fix: Merge in batches of 2-3 files

2. **Reduce chunk size more aggressively**
   - Current: 5,000 rows
   - Recommended: 1,000 rows for your memory constraints

3. **Add more aggressive memory monitoring**
   - Monitor RSS memory every chunk
   - Force GC when memory exceeds 80% of limit
   - Kill process if memory limit exceeded

4. **Use environment variables to limit thread usage**
   ```bash
   export POLARS_MAX_THREADS=1
   export OMP_NUM_THREADS=1
   export OPENBLAS_NUM_THREADS=1
   ```

## Hardware Considerations

### **For Your Current System:**
- RAM: Appears to be 4-8GB based on errors
- Recommended settings:
  ```yaml
  chunk_size: 1000
  memory_limit_mb: 1024
  max_temp_files_per_batch: 2
  ```

### **For Production Systems:**
- 32GB+ RAM: chunk_size: 50000, memory_limit: 16384
- 64GB+ RAM: chunk_size: 100000, memory_limit: 32768
- 128GB+ RAM: chunk_size: 200000, memory_limit: 65536

## Alternative Architectures

### **1. Database-First Approach**
Instead of Parquet → Memory → Processing:
```
Parquet → DuckDB → DuckDB Processing → Parquet
```

### **2. Streaming Pipeline**
```
Source → Transform (streaming) → Sink
Never hold full dataset in memory
```

### **3. Map-Reduce Style**
```
Map: Split input → Process chunks → Write temp results
Reduce: Merge temp results → Final output
```

Your current pipeline is actually a good Map-Reduce implementation! The issue is just in the Reduce step (temp file merging).