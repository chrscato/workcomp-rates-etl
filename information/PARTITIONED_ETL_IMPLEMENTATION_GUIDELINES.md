# Partitioned ETL Implementation Guidelines

## Overview

This document provides detailed implementation guidelines for creating a memory-efficient, pre-joined partitioned data warehouse from the existing MRF ETL pipeline. The goal is to transform the current star schema into a denormalized, partitioned structure optimized for analytics performance.

## Architecture Overview

### Current State
- **Star Schema**: Fact table with separate dimension tables
- **Runtime Joins**: Queries require multiple table joins
- **Memory Intensive**: Large datasets loaded for each query
- **Single File**: All data in one large parquet file

### Target State
- **Denormalized Schema**: Pre-joined fact table with embedded dimensions
- **Partitioned Storage**: Data split by business dimensions
- **Memory Efficient**: Only relevant partitions loaded
- **Query Optimized**: Minimal runtime processing required

## Partitioning Strategy

### Partition Hierarchy
```
payer_slug/
├── state/
│   ├── billing_class/
│   │   ├── procedure_set/
│   │   │   ├── procedure_class/
│   │   │   │   ├── taxonomy/
│   │   │   │   │   └── stat_area_name/
│   │   │   │   │       └── fact_rate_enriched.parquet
```

### Partition Key Rationale
1. **`payer_slug`**: Highest cardinality, natural business boundary
2. **`state`**: Geographic boundary, regulatory compliance
3. **`billing_class`**: Service type differentiation
4. **`procedure_set`**: Clinical category grouping
5. **`procedure_class`**: Service level grouping
6. **`taxonomy`**: Provider specialty grouping
7. **`stat_area_name`**: Geographic market area

### Expected Partition Sizes
- **Total Partitions**: ~50,000-100,000 (estimated)
- **Average Partition Size**: 10-50 rows
- **Largest Partitions**: 1,000-5,000 rows
- **Memory per Partition**: 1-10 MB

## Data Transformation Process

### Step 1: Data Preparation
```python
# Load all dimension tables
dim_code = pl.read_parquet("data/dims/dim_code.parquet")
dim_code_cat = pl.read_parquet("data/dims/dim_code_cat.parquet")
dim_payer = pl.read_parquet("data/dims/dim_payer.parquet")
dim_provider_group = pl.read_parquet("data/dims/dim_provider_group.parquet")
dim_pos_set = pl.read_parquet("data/dims/dim_pos_set.parquet")
dim_npi = pl.read_parquet("data/dims/dim_npi.parquet")
dim_npi_address = pl.read_parquet("data/dims/dim_npi_address.parquet")
dim_npi_address_geo = pl.read_parquet("data/dims/dim_npi_address_geolocation.parquet")
bench_medicare = pl.read_parquet("data/dims/bench_medicare_comprehensive.parquet")

# Load fact table
fact_rate = pl.read_parquet("data/gold/fact_rate.parquet")
```

### Step 2: Pre-Join All Dimensions
```python
def create_enriched_fact_table(fact_rate, dimensions):
    """
    Create a single denormalized table with all dimension data embedded.
    Memory-efficient approach using Polars lazy evaluation.
    """
    
    # Start with fact table
    enriched = fact_rate.lazy()
    
    # Join with payer information
    enriched = enriched.join(
        dim_payer.lazy(),
        on="payer_slug",
        how="left"
    )
    
    # Join with procedure code information
    enriched = enriched.join(
        dim_code.lazy(),
        on=["code_type", "code"],
        how="left"
    )
    
    # Join with procedure categorization
    enriched = enriched.join(
        dim_code_cat.lazy(),
        left_on="code",
        right_on="proc_cd",
        how="left"
    )
    
    # Join with provider group information
    enriched = enriched.join(
        dim_provider_group.lazy(),
        on="pg_uid",
        how="left"
    )
    
    # Join with place of service information
    enriched = enriched.join(
        dim_pos_set.lazy(),
        on="pos_set_id",
        how="left"
    )
    
    # Join with provider information (via NPI from provider groups)
    # This requires a two-step join through the cross-reference
    xref_npi = pl.read_parquet("data/xrefs/xref_pg_member_npi.parquet")
    
    # Get unique NPIs for each provider group
    pg_npi_mapping = xref_npi.group_by("pg_uid").agg([
        pl.col("npi").first().alias("primary_npi")
    ])
    
    enriched = enriched.join(
        pg_npi_mapping.lazy(),
        on="pg_uid",
        how="left"
    )
    
    # Join with NPI details
    enriched = enriched.join(
        dim_npi.lazy(),
        left_on="primary_npi",
        right_on="npi",
        how="left"
    )
    
    # Join with address information
    enriched = enriched.join(
        dim_npi_address.lazy(),
        on="npi",
        how="left"
    )
    
    # Join with geolocation information
    enriched = enriched.join(
        dim_npi_address_geo.lazy(),
        on=["npi", "address_hash"],
        how="left"
    )
    
    # Join with Medicare benchmarks
    enriched = enriched.join(
        bench_medicare.lazy(),
        on=["state", "code", "code_type"],
        how="left"
    )
    
    # Add calculated fields
    enriched = enriched.with_columns([
        # Rate to Medicare ratio
        (pl.col("negotiated_rate") / pl.col("medicare_state_rate")).alias("rate_to_medicare_ratio"),
        
        # Boolean flags
        (pl.col("negotiated_rate") > pl.col("medicare_state_rate")).alias("is_above_medicare"),
        (pl.col("enumeration_type") == "NPI-1").alias("is_individual_provider"),
        (pl.col("sole_proprietor") == "YES").alias("is_sole_proprietor"),
        
        # Provider type
        pl.when(pl.col("enumeration_type") == "NPI-1")
        .then(pl.lit("Individual"))
        .otherwise(pl.lit("Organization"))
        .alias("provider_type")
    ])
    
    return enriched.collect()
```

### Step 3: Memory-Efficient Partitioning
```python
def create_partitions_memory_efficient(enriched_df, output_base_path):
    """
    Create partitions in a memory-efficient manner by processing
    one partition at a time and using streaming writes.
    """
    
    # Define partition columns
    partition_cols = [
        "payer_slug", "state", "billing_class", 
        "procedure_set", "procedure_class", 
        "primary_taxonomy_code", "stat_area_name"
    ]
    
    # Get unique partition combinations
    partition_combinations = (
        enriched_df
        .select(partition_cols)
        .unique()
        .sort(partition_cols)
    )
    
    # Process each partition
    for partition_row in partition_combinations.iter_rows(named=True):
        # Create partition path
        partition_path = create_partition_path(partition_row, output_base_path)
        
        # Filter data for this partition
        partition_filter = create_partition_filter(partition_row)
        partition_data = enriched_df.filter(partition_filter)
        
        # Skip if no data
        if partition_data.height == 0:
            continue
            
        # Ensure directory exists
        os.makedirs(os.path.dirname(partition_path), exist_ok=True)
        
        # Write partition
        partition_data.write_parquet(
            partition_path,
            compression="zstd",
            use_pyarrow=True
        )
        
        print(f"Created partition: {partition_path} ({partition_data.height} rows)")

def create_partition_path(partition_row, base_path):
    """Create Hive-style partition path."""
    path_parts = []
    for col in ["payer_slug", "state", "billing_class", "procedure_set", "procedure_class", "primary_taxonomy_code", "stat_area_name"]:
        value = partition_row[col]
        if value is None:
            value = "null"
        # Sanitize for filesystem
        value = str(value).replace("/", "_").replace("\\", "_")
        path_parts.append(f"{col}={value}")
    
    return os.path.join(base_path, *path_parts, "fact_rate_enriched.parquet")

def create_partition_filter(partition_row):
    """Create filter condition for partition."""
    conditions = []
    for col, value in partition_row.items():
        if value is None:
            conditions.append(pl.col(col).is_null())
        else:
            conditions.append(pl.col(col) == value)
    
    return pl.all_horizontal(conditions)
```

## Memory Optimization Techniques

### 1. Streaming Processing
```python
def process_partitions_streaming(fact_rate_path, output_base_path):
    """
    Process partitions in streaming fashion to minimize memory usage.
    """
    
    # Read fact table in chunks
    fact_chunk_size = 100_000
    
    for chunk in pl.read_parquet(fact_rate_path, batch_size=fact_chunk_size):
        # Process chunk with all joins
        enriched_chunk = create_enriched_fact_table(chunk, dimensions)
        
        # Create partitions for this chunk
        create_partitions_memory_efficient(enriched_chunk, output_base_path)
        
        # Clear memory
        del enriched_chunk
        gc.collect()
```

### 2. Lazy Evaluation
```python
def create_enriched_fact_lazy(fact_rate, dimensions):
    """
    Use Polars lazy evaluation to optimize query execution.
    """
    
    # All joins are lazy - no computation until collect()
    enriched = (
        fact_rate.lazy()
        .join(dim_payer.lazy(), on="payer_slug", how="left")
        .join(dim_code.lazy(), on=["code_type", "code"], how="left")
        .join(dim_code_cat.lazy(), left_on="code", right_on="proc_cd", how="left")
        .join(dim_provider_group.lazy(), on="pg_uid", how="left")
        .join(dim_pos_set.lazy(), on="pos_set_id", how="left")
        # ... other joins
    )
    
    return enriched
```

### 3. Column Selection
```python
def select_essential_columns(enriched_df):
    """
    Select only essential columns for the final partitioned table.
    """
    
    essential_columns = [
        # Fact table columns
        "fact_uid", "state", "year_month", "payer_slug", "billing_class",
        "code_type", "code", "negotiated_type", "negotiation_arrangement",
        "negotiated_rate", "expiration_date",
        
        # Provider group columns
        "provider_group_id_raw", "reporting_entity_name",
        
        # Procedure code columns
        "code_description", "code_name",
        
        # Procedure category columns
        "procedure_set", "procedure_class", "procedure_group",
        
        # Place of service columns
        "pos_set_id", "pos_members",
        
        # Provider columns
        "npi", "first_name", "last_name", "organization_name",
        "enumeration_type", "status", "primary_taxonomy_code",
        "primary_taxonomy_desc", "primary_taxonomy_state",
        "primary_taxonomy_license", "credential", "sole_proprietor",
        "enumeration_date", "last_updated",
        
        # Address columns
        "address_purpose", "address_type", "address_1", "address_2",
        "city", "state", "postal_code", "country_code",
        "telephone_number", "fax_number", "address_hash",
        
        # Geolocation columns
        "latitude", "longitude", "county_name", "county_fips",
        "stat_area_name", "stat_area_code", "matched_address",
        
        # Medicare benchmark columns
        "benchmark_type", "medicare_national_rate", "medicare_state_rate",
        "work_rvu", "practice_expense_rvu", "malpractice_rvu", "total_rvu",
        "conversion_factor", "opps_weight", "opps_si", "asc_pi",
        
        # Calculated columns
        "rate_to_medicare_ratio", "is_above_medicare", "provider_type",
        "is_sole_proprietor", "is_individual_provider"
    ]
    
    return enriched_df.select(essential_columns)
```

## Error Handling and Data Quality

### 1. Geocoding Failures
```python
def handle_geocoding_failures(enriched_df):
    """
    Handle cases where geocoding failed or returned null values.
    """
    
    # Fill null geocoding values with defaults
    enriched_df = enriched_df.with_columns([
        pl.col("latitude").fill_null(0.0),
        pl.col("longitude").fill_null(0.0),
        pl.col("county_name").fill_null("Unknown"),
        pl.col("county_fips").fill_null("00000"),
        pl.col("stat_area_name").fill_null("Unknown"),
        pl.col("stat_area_code").fill_null("00000"),
        pl.col("matched_address").fill_null("Not Geocoded")
    ])
    
    return enriched_df
```

### 2. Data Validation
```python
def validate_partition_data(partition_df):
    """
    Validate data quality within each partition.
    """
    
    # Check for required fields
    required_fields = ["fact_uid", "negotiated_rate", "state", "payer_slug"]
    missing_fields = [field for field in required_fields if field not in partition_df.columns]
    
    if missing_fields:
        raise ValueError(f"Missing required fields: {missing_fields}")
    
    # Check for null rates
    null_rates = partition_df.filter(pl.col("negotiated_rate").is_null()).height
    if null_rates > 0:
        print(f"Warning: {null_rates} rows with null rates in partition")
    
    # Check for negative rates
    negative_rates = partition_df.filter(pl.col("negotiated_rate") < 0).height
    if negative_rates > 0:
        print(f"Warning: {negative_rates} rows with negative rates in partition")
    
    return True
```

## Performance Optimization

### 1. Parallel Processing
```python
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor

def process_partitions_parallel(partition_combinations, enriched_df, output_base_path):
    """
    Process partitions in parallel for better performance.
    """
    
    def process_single_partition(partition_row):
        partition_path = create_partition_path(partition_row, output_base_path)
        partition_filter = create_partition_filter(partition_row)
        partition_data = enriched_df.filter(partition_filter)
        
        if partition_data.height == 0:
            return None
            
        os.makedirs(os.path.dirname(partition_path), exist_ok=True)
        partition_data.write_parquet(partition_path, compression="zstd")
        
        return partition_path
    
    # Use all available CPU cores
    max_workers = min(mp.cpu_count(), len(partition_combinations))
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_single_partition, partition_combinations.iter_rows(named=True)))
    
    return [r for r in results if r is not None]
```

### 2. Incremental Updates
```python
def update_partitions_incremental(new_data, output_base_path):
    """
    Update only affected partitions when new data arrives.
    """
    
    # Identify affected partitions
    affected_partitions = new_data.select([
        "payer_slug", "state", "billing_class", 
        "procedure_set", "procedure_class", 
        "primary_taxonomy_code", "stat_area_name"
    ]).unique()
    
    # Process only affected partitions
    for partition_row in affected_partitions.iter_rows(named=True):
        partition_path = create_partition_path(partition_row, output_base_path)
        
        # Load existing partition if it exists
        existing_data = None
        if os.path.exists(partition_path):
            existing_data = pl.read_parquet(partition_path)
        
        # Filter new data for this partition
        partition_filter = create_partition_filter(partition_row)
        new_partition_data = new_data.filter(partition_filter)
        
        # Combine with existing data
        if existing_data is not None:
            combined_data = pl.concat([existing_data, new_partition_data])
        else:
            combined_data = new_partition_data
        
        # Write updated partition
        combined_data.write_parquet(partition_path, compression="zstd")
```

## Monitoring and Maintenance

### 1. Partition Statistics
```python
def generate_partition_statistics(output_base_path):
    """
    Generate statistics about partition sizes and distribution.
    """
    
    stats = []
    
    for root, dirs, files in os.walk(output_base_path):
        for file in files:
            if file.endswith(".parquet"):
                file_path = os.path.join(root, file)
                file_size = os.path.getsize(file_path)
                
                # Read row count
                df = pl.read_parquet(file_path)
                row_count = df.height
                
                stats.append({
                    "partition_path": file_path,
                    "file_size_mb": file_size / (1024 * 1024),
                    "row_count": row_count,
                    "rows_per_mb": row_count / (file_size / (1024 * 1024)) if file_size > 0 else 0
                })
    
    return pl.DataFrame(stats)
```

### 2. Query Performance Monitoring
```python
def monitor_query_performance(query_func, test_queries):
    """
    Monitor query performance across different partition sizes.
    """
    
    results = []
    
    for query in test_queries:
        start_time = time.time()
        result = query_func(query)
        end_time = time.time()
        
        results.append({
            "query": query,
            "execution_time": end_time - start_time,
            "result_rows": result.height if hasattr(result, 'height') else len(result)
        })
    
    return pl.DataFrame(results)
```

## Implementation Checklist

### Phase 1: Data Preparation
- [ ] Load all dimension tables
- [ ] Validate data quality
- [ ] Create enriched fact table
- [ ] Add calculated fields
- [ ] Handle geocoding failures

### Phase 2: Partitioning
- [ ] Implement partition creation logic
- [ ] Add memory optimization
- [ ] Implement parallel processing
- [ ] Add error handling
- [ ] Validate partition data

### Phase 3: Optimization
- [ ] Implement streaming processing
- [ ] Add lazy evaluation
- [ ] Optimize column selection
- [ ] Add compression
- [ ] Implement incremental updates

### Phase 4: Monitoring
- [ ] Add partition statistics
- [ ] Implement query monitoring
- [ ] Add data quality checks
- [ ] Create maintenance scripts
- [ ] Document performance metrics

## Expected Benefits

### Performance Improvements
- **Query Speed**: 5-10x faster due to pre-joined data
- **Memory Usage**: 50-70% reduction due to partition pruning
- **Storage Efficiency**: 20-30% better compression due to partitioning
- **Parallel Processing**: 4-8x faster processing with parallel execution

### Operational Benefits
- **Simplified Queries**: No complex joins required
- **Better Caching**: Smaller, more cacheable data chunks
- **Easier Maintenance**: Independent partition updates
- **Scalability**: Easy to add new partitions

### Analytics Benefits
- **Geographic Analysis**: Built-in spatial data
- **Provider Analysis**: Rich provider information
- **Benchmark Analysis**: Pre-calculated ratios
- **Market Analysis**: Statistical area grouping
