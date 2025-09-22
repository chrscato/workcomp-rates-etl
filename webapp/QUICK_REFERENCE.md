# Partition Navigation - Quick Reference

## Core Concepts

### Hierarchical Filtering
```
Required (Tier 1):  Payer → State → Billing Class
Optional (Tier 2):  Procedure Set, Specialty, Area
Temporal (Tier 3):  Year, Month
```

### Database Schema
```sql
-- Main table
partitions (id, partition_path, payer_slug, state, billing_class, ...)

-- Dimension tables
dim_payers, dim_states, dim_billing_classes, dim_taxonomies, ...

-- Navigation views
v_partition_navigation, v_partition_summary
```

## Implementation Steps

### 1. Database Setup
```python
# Create partition inventory
python s3_partition_inventory.py bucket-name --create-db

# Result: partition_navigation.db with all tables and views
```

### 2. Core Class
```python
class PartitionNavigator:
    def search_partitions(filters, require_top_levels=True)
    def combine_partitions_for_analysis(partition_paths, max_rows)
    def get_filter_options()
    def analyze_combined_data(df)
```

### 3. UI Pattern
```python
# Required filters
payer = st.selectbox("Payer *:", options)
state = st.selectbox("State *:", options)
billing_class = st.selectbox("Billing Class *:", options)

# Optional filters
procedure_set = st.selectbox("Procedure Set:", options)
taxonomy = st.selectbox("Specialty:", options)

# Validation
if not all([payer, state, billing_class]):
    st.error("Please select all required filters")
```

### 4. Results Processing
```python
# Search
results_df = navigator.search_partitions(filters)

# Combine for analysis
if combine_partitions:
    s3_paths = [f"s3://{row['s3_bucket']}/{row['s3_key']}" for _, row in results_df.iterrows()]
    combined_df = navigator.combine_partitions_for_analysis(s3_paths)
```

## Configuration Examples

### Healthcare
```python
config = {
    'required_filters': ['payer_slug', 'state', 'billing_class'],
    'optional_filters': ['procedure_set', 'taxonomy_code', 'stat_area_name'],
    'temporal_filters': ['year', 'month'],
    'max_rows': 10000,
    'storage_type': 's3'
}
```

### E-commerce
```python
config = {
    'required_filters': ['category', 'region', 'channel'],
    'optional_filters': ['brand', 'product_type', 'price_tier'],
    'temporal_filters': ['year', 'quarter', 'month'],
    'max_rows': 50000,
    'storage_type': 'gcs'
}
```

### IoT
```python
config = {
    'required_filters': ['device_type', 'location', 'sensor_type'],
    'optional_filters': ['manufacturer', 'model', 'firmware_version'],
    'temporal_filters': ['year', 'month', 'day'],
    'max_rows': 100000,
    'storage_type': 'azure'
}
```

## Key Methods

### Search Partitions
```python
filters = {
    'payer_slug': 'unitedhealthcare',
    'state': 'GA',
    'billing_class': 'institutional'
}
results = navigator.search_partitions(filters)
```

### Combine Partitions
```python
partition_paths = ['s3://bucket/path1.parquet', 's3://bucket/path2.parquet']
combined_df = navigator.combine_partitions_for_analysis(partition_paths, max_rows=10000)
```

### Analyze Data
```python
analysis = navigator.analyze_combined_data(combined_df)
print(f"Shape: {analysis['shape']}")
print(f"Columns: {analysis['columns']}")
```

### Export Data
```python
csv_data = navigator.export_data(combined_df, 'csv')
parquet_data = navigator.export_data(combined_df, 'parquet')
```

## Storage Adapters

### S3
```python
def _load_from_s3(self, s3_path):
    s3_client = self.connect_storage()
    bucket, key = s3_path.split('/', 1)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(response['Body'].read()))
```

### Google Cloud Storage
```python
def _load_from_gcs(self, gcs_path):
    gcs_client = self.connect_storage()
    bucket, key = gcs_path.split('/', 1)
    blob = gcs_client.bucket(bucket).blob(key)
    return pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
```

### Local Filesystem
```python
def _load_from_local(self, local_path):
    return pd.read_parquet(local_path)
```

## UI Components

### Form Layout
```python
# Required section
st.subheader("🔴 Required Filters")
col1, col2, col3 = st.columns(3)
# ... selectboxes

# Optional section  
st.subheader("🟡 Optional Filters")
col1, col2 = st.columns(2)
# ... selectboxes

# Analysis options
st.subheader("📊 Analysis Options")
combine_partitions = st.checkbox("Combine Multiple Partitions")
max_rows = st.number_input("Max Rows", 1000, 100000, 10000)
```

### Results Display
```python
# Summary metrics
col1, col2, col3 = st.columns(3)
with col1: st.metric("Partitions", len(results_df))
with col2: st.metric("Total Size", f"{total_size:.1f} MB")
with col3: st.metric("Records", f"{total_records:,}")

# Combined analysis
if combine_partitions and len(results_df) > 1:
    if st.button("🚀 Load & Combine All Partitions"):
        combined_df = navigator.combine_partitions_for_analysis(s3_paths)
        # ... display analysis
```

## Best Practices

1. **Memory Management**: Set reasonable row limits
2. **Error Handling**: Wrap operations in try-catch
3. **Progress Feedback**: Show loading indicators
4. **Validation**: Check required filters before processing
5. **Performance**: Create indexes on frequently queried columns
6. **User Experience**: Clear visual hierarchy for required vs optional

## Common Patterns

### Filter Validation
```python
required = ['payer_slug', 'state', 'billing_class']
if not all(filters.get(f) for f in required):
    st.error("Please select all required filters")
    return
```

### Progress Tracking
```python
with st.spinner("Loading partitions..."):
    combined_df = navigator.combine_partitions_for_analysis(paths)
```

### Error Handling
```python
try:
    result = navigator.search_partitions(filters)
except Exception as e:
    st.error(f"Search failed: {e}")
```

### Download Options
```python
csv_data = navigator.export_data(df, 'csv')
st.download_button("Download CSV", csv_data, "data.csv", "text/csv")
```
