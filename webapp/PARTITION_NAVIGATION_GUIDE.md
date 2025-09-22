# Partition Navigation System - Implementation Guide

## Overview

This guide documents the partition navigation system used in the Healthcare Partition Navigator webapp. The system provides hierarchical filtering, partition discovery, and in-memory data combination for analytics across partitioned datasets.

## Core Concepts

### 1. Hierarchical Filtering Architecture

The system implements a 3-tier hierarchical filtering approach:

```
Tier 1 (Required): Payer → State → Billing Class
Tier 2 (Optional):  Procedure Set, Medical Specialty, Statistical Area
Tier 3 (Temporal):  Year, Month
```

**Benefits:**
- Ensures meaningful data subsets
- Prevents overly broad queries
- Enables focused analysis
- Maintains data quality

### 2. Partition Discovery Pattern

```python
class PartitionNavigator:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.s3_client = None
    
    def search_partitions(self, filters: Dict, require_top_levels: bool = True) -> pd.DataFrame:
        """Search partitions with hierarchical requirements"""
        conn = self.connect_db()
        
        # Define required top-level filters
        required_filters = ['payer_slug', 'state', 'billing_class']
        
        # Build WHERE clause
        where_conditions = []
        params = []
        
        # Apply required filters
        for filter_key in required_filters:
            if filters.get(filter_key):
                where_conditions.append(f"p.{filter_key} = ?")
                params.append(filters[filter_key])
            elif require_top_levels:
                return pd.DataFrame()  # Return empty if required filters missing
        
        # Apply optional filters
        optional_filters = ['procedure_set', 'taxonomy_code', 'stat_area_name', 'year', 'month']
        for filter_key in optional_filters:
            if filters.get(filter_key):
                where_conditions.append(f"p.{filter_key} = ?")
                params.append(filters[filter_key])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
            SELECT p.*, dp.payer_display_name
            FROM partitions p
            LEFT JOIN dim_payers dp ON p.payer_slug = dp.payer_slug
            {where_clause}
            ORDER BY p.file_size_mb DESC
            LIMIT 1000
        """
        
        return pd.read_sql_query(query, conn, params=params)
```

### 3. In-Memory Partition Combination

```python
def combine_partitions_for_analysis(self, partition_paths: List[str], max_rows: int = 10000) -> Optional[pd.DataFrame]:
    """Combine multiple partitions in memory for analysis"""
    if not partition_paths:
        return None
    
    try:
        s3_client = self.connect_s3()
        combined_dfs = []
        total_rows = 0
        
        for i, s3_path in enumerate(partition_paths):
            if total_rows >= max_rows:
                break
            
            try:
                # Parse S3 path
                if s3_path.startswith('s3://'):
                    s3_path = s3_path[5:]
                
                bucket, key = s3_path.split('/', 1)
                
                # Read parquet file from S3
                response = s3_client.get_object(Bucket=bucket, Key=key)
                parquet_data = response['Body'].read()
                
                # Convert to DataFrame
                df = pd.read_parquet(io.BytesIO(parquet_data))
                
                # Add partition metadata
                df['_partition_source'] = s3_path
                df['_partition_index'] = i
                
                combined_dfs.append(df)
                total_rows += len(df)
                
            except Exception as e:
                print(f"Error loading partition {s3_path}: {e}")
                continue
        
        if combined_dfs:
            return pd.concat(combined_dfs, ignore_index=True)
        else:
            return None
            
    except Exception as e:
        print(f"Error combining partitions: {e}")
        return None
```

## Database Schema Design

### Core Tables

```sql
-- Main partitions table
CREATE TABLE partitions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    partition_path TEXT UNIQUE NOT NULL,
    payer_slug TEXT,
    state TEXT,
    billing_class TEXT,
    procedure_set TEXT,
    procedure_class TEXT,
    taxonomy_code TEXT,
    taxonomy_desc TEXT,
    stat_area_name TEXT,
    year INTEGER,
    month INTEGER,
    file_size_bytes INTEGER,
    file_size_mb REAL,
    last_modified TEXT,
    estimated_records INTEGER,
    s3_bucket TEXT,
    s3_key TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Dimension tables for filtering
CREATE TABLE dim_payers (
    payer_slug TEXT PRIMARY KEY,
    payer_display_name TEXT,
    partition_count INTEGER,
    total_size_mb REAL
);

CREATE TABLE dim_states (
    state_code TEXT PRIMARY KEY,
    state_name TEXT,
    partition_count INTEGER,
    total_size_mb REAL
);

CREATE TABLE dim_billing_classes (
    billing_class TEXT PRIMARY KEY,
    partition_count INTEGER,
    total_size_mb REAL
);

-- Additional dimension tables...
```

### Navigation Views

```sql
-- Main navigation view
CREATE VIEW v_partition_navigation AS
SELECT 
    p.id,
    p.partition_path,
    p.s3_bucket,
    p.s3_key,
    p.payer_slug,
    dp.payer_display_name,
    p.state,
    p.billing_class,
    p.procedure_set,
    p.taxonomy_code,
    p.taxonomy_desc,
    p.file_size_mb,
    p.estimated_records
FROM partitions p
LEFT JOIN dim_payers dp ON p.payer_slug = dp.payer_slug;

-- Summary view
CREATE VIEW v_partition_summary AS
SELECT 
    payer_slug,
    state,
    billing_class,
    procedure_set,
    COUNT(*) as partition_count,
    SUM(file_size_mb) as total_size_mb,
    SUM(estimated_records) as total_estimated_records
FROM partitions
GROUP BY payer_slug, state, billing_class, procedure_set
ORDER BY total_size_mb DESC;
```

## UI/UX Patterns

### 1. Hierarchical Form Layout

```python
# Required filters section
st.subheader("🔴 Required Filters (Top 3 Levels)")
col1, col2, col3 = st.columns(3)

with col1:
    payer = st.selectbox(
        "Payer *:",
        options,
        help="Required: Choose the primary dimension"
    )

# Optional filters section
st.subheader("🟡 Optional Filters (For Refinement)")
col1, col2 = st.columns(2)

with col1:
    procedure_set = st.selectbox(
        "Procedure Set:",
        options,
        help="Optional: Filter by secondary dimension"
    )
```

### 2. Validation Pattern

```python
# Validate required filters
if not all([payer, state, billing_class]):
    st.error("❌ Please select all required filters: Payer, State, and Billing Class")
    st.stop()
```

### 3. Results Display Pattern

```python
# Display partition summary
total_size = results_df['file_size_mb'].sum()
total_records = results_df['estimated_records'].sum()

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Partitions", len(results_df))
with col2:
    st.metric("Total Size", f"{total_size:.1f} MB")
with col3:
    st.metric("Estimated Records", f"{total_records:,}")
```

## Implementation Steps

### Step 1: Database Setup

1. **Create partition inventory script** that scans your data source
2. **Build dimension tables** for each filterable attribute
3. **Create navigation views** for easy querying
4. **Add indexes** for performance

### Step 2: Core Navigation Class

```python
class PartitionNavigator:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.storage_client = None  # S3, GCS, Azure, etc.
    
    def connect_db(self):
        """Connect to navigation database"""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
        return self.conn
    
    def connect_storage(self):
        """Connect to storage system (S3, GCS, etc.)"""
        # Implement based on your storage system
        pass
    
    def get_filter_options(self) -> Dict:
        """Get all available filter options"""
        # Implementation for populating filter dropdowns
        pass
    
    def search_partitions(self, filters: Dict, require_top_levels: bool = True) -> pd.DataFrame:
        """Search partitions with hierarchical requirements"""
        # Implementation as shown above
        pass
    
    def combine_partitions_for_analysis(self, partition_paths: List[str], max_rows: int = 10000) -> Optional[pd.DataFrame]:
        """Combine multiple partitions in memory"""
        # Implementation as shown above
        pass
```

### Step 3: UI Implementation

1. **Create hierarchical form** with required/optional sections
2. **Implement validation** for required fields
3. **Add results display** with summary metrics
4. **Create combination interface** for multi-partition analysis
5. **Add download options** for combined data

### Step 4: Analysis Features

```python
def analyze_combined_data(df: pd.DataFrame):
    """Analyze combined partition data"""
    
    # Data summary
    st.write("**Data Shape:**")
    st.write(f"- Rows: {len(df):,}")
    st.write(f"- Columns: {len(df.columns)}")
    
    # Show sample data
    st.dataframe(df.head(20))
    
    # Quick analysis
    categorical_cols = df.select_dtypes(include=['object']).columns
    for col in categorical_cols[:5]:
        if df[col].nunique() < 20:
            st.write(f"**{col} Distribution:**")
            value_counts = df[col].value_counts().head(10)
            st.bar_chart(value_counts)
    
    # Numeric summary
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        st.write("**Numeric Summary:**")
        st.dataframe(df[numeric_cols].describe())
```

## Configuration Patterns

### 1. Hierarchical Filter Configuration

```python
HIERARCHY_CONFIG = {
    'required_levels': ['payer_slug', 'state', 'billing_class'],
    'optional_levels': ['procedure_set', 'taxonomy_code', 'stat_area_name'],
    'temporal_levels': ['year', 'month']
}
```

### 2. Storage Configuration

```python
STORAGE_CONFIG = {
    'type': 's3',  # s3, gcs, azure, local
    'bucket': 'your-bucket',
    'region': 'us-east-1',
    'max_preview_rows': 10,
    'max_combination_rows': 10000
}
```

### 3. UI Configuration

```python
UI_CONFIG = {
    'max_search_results': 1000,
    'default_chart_height': 400,
    'enable_data_export': True,
    'enable_combination': True
}
```

## Best Practices

### 1. Memory Management

- **Set row limits** for partition combination
- **Implement progress tracking** for long operations
- **Add error handling** for failed partitions
- **Use efficient pandas operations**

### 2. Performance Optimization

- **Create indexes** on frequently queried columns
- **Use views** for complex queries
- **Implement pagination** for large result sets
- **Cache filter options** when possible

### 3. User Experience

- **Clear visual hierarchy** for required vs optional filters
- **Immediate feedback** on validation errors
- **Progress indicators** for long operations
- **Download options** for analysis results

### 4. Error Handling

```python
try:
    # Partition combination logic
    combined_df = combine_partitions_for_analysis(partition_paths, max_rows)
    if combined_df is not None:
        # Success handling
        st.success(f"✅ Successfully combined {len(partition_paths)} partitions")
    else:
        st.error("❌ Failed to combine partitions")
except Exception as e:
    st.error(f"❌ Error: {e}")
```

## Extension Points

### 1. Different Storage Systems

```python
# S3
def load_from_s3(bucket: str, key: str) -> pd.DataFrame:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(response['Body'].read()))

# Google Cloud Storage
def load_from_gcs(bucket: str, key: str) -> pd.DataFrame:
    blob = gcs_client.bucket(bucket).blob(key)
    return pd.read_parquet(io.BytesIO(blob.download_as_bytes()))

# Local filesystem
def load_from_local(file_path: str) -> pd.DataFrame:
    return pd.read_parquet(file_path)
```

### 2. Different Data Formats

```python
# Parquet
def load_parquet(data: bytes) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(data))

# CSV
def load_csv(data: bytes) -> pd.DataFrame:
    return pd.read_csv(io.StringIO(data.decode('utf-8')))

# JSON
def load_json(data: bytes) -> pd.DataFrame:
    return pd.read_json(io.StringIO(data.decode('utf-8')))
```

### 3. Different UI Frameworks

- **Streamlit**: As shown in the example
- **Dash**: For more complex interactivity
- **Flask/FastAPI**: For custom web applications
- **Jupyter**: For notebook-based analysis

## Conclusion

This partition navigation system provides a robust foundation for building data exploration tools across partitioned datasets. The hierarchical filtering approach ensures meaningful data subsets while the combination functionality enables comprehensive analysis across multiple partitions.

Key benefits:
- **Scalable**: Works with large partitioned datasets
- **Flexible**: Adaptable to different storage systems and data formats
- **User-friendly**: Intuitive hierarchical filtering interface
- **Analytical**: Built-in data combination and analysis features

Use this guide as a template for implementing similar systems in your own projects!
