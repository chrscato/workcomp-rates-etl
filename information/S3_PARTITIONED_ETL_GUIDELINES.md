# S3-Based Partitioned ETL Implementation Guidelines

## Overview

This document provides implementation guidelines for creating a memory-efficient, pre-joined partitioned data warehouse on AWS S3. The architecture leverages S3's scalability and integrates with AWS analytics services for optimal performance.

## S3 Architecture Benefits

### **Scalability & Cost**
- **Unlimited Storage**: No partition count or size limitations
- **Cost Optimization**: Intelligent tiering (Standard → IA → Glacier)
- **Pay-per-Use**: Only pay for data stored and accessed
- **Lifecycle Management**: Automatic data archival

### **Performance & Integration**
- **S3 Select**: Query data without downloading entire files
- **Athena Integration**: Serverless SQL queries on S3 data
- **Redshift Spectrum**: Query S3 data from Redshift
- **Glue Catalog**: Automatic schema discovery and metadata management

## S3 Partitioning Strategy

### **S3 Path Structure**
```
s3://your-data-lake-bucket/
├── partitioned-data/
│   ├── payer=unitedhealthcare-of-georgia-inc/
│   │   ├── state=GA/
│   │   │   ├── billing_class=professional/
│   │   │   │   ├── procedure_set=Evaluation and Management/
│   │   │   │   │   ├── procedure_class=Office/outpatient services/
│   │   │   │   │   │   ├── taxonomy=101YP2500X/
│   │   │   │   │   │   │   ├── stat_area_name=Atlanta-Sandy Springs-Alpharetta, GA/
│   │   │   │   │   │   │   │   ├── year=2025/
│   │   │   │   │   │   │   │   │   ├── month=08/
│   │   │   │   │   │   │   │   │   │   └── fact_rate_enriched.parquet
│   │   │   │   │   │   │   │   │   └── month=09/
│   │   │   │   │   │   │   │   └── fact_rate_enriched.parquet
│   │   │   │   │   │   │   └── stat_area_name=Augusta-Richmond County, GA-SC/
│   │   │   │   │   │   └── taxonomy=101Y00000X/
│   │   │   │   │   └── procedure_class=Emergency department services/
│   │   │   │   └── procedure_set=Surgery/
│   │   │   └── billing_class=institutional/
│   │   └── state=FL/
│   └── payer=anthem-blue-cross-blue-shield/
```

### **Enhanced Partition Keys for S3**
1. **`payer_slug`**: Business boundary
2. **`state`**: Geographic boundary
3. **`billing_class`**: Service type
4. **`procedure_set`**: Clinical category
5. **`procedure_class`**: Service level
6. **`taxonomy`**: Provider specialty
7. **`stat_area_name`**: Market area
8. **`year`**: Time partitioning (NEW)
9. **`month`**: Time partitioning (NEW)

## S3-Specific Implementation

### **1. S3 Client Configuration**
```python
import boto3
from botocore.config import Config
import polars as pl
from datetime import datetime
import os

class S3PartitionedETL:
    def __init__(self, bucket_name, region='us-east-1'):
        self.bucket_name = bucket_name
        self.region = region
        
        # Configure S3 client for optimal performance
        self.s3_config = Config(
            region_name=region,
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            max_pool_connections=50,
            s3={
                'addressing_style': 'virtual',
                'payload_signing_enabled': False
            }
        )
        
        self.s3_client = boto3.client('s3', config=self.s3_config)
        self.s3_resource = boto3.resource('s3', config=self.s3_config)
        
    def create_s3_path(self, partition_values):
        """Create S3 path for partition."""
        path_parts = []
        
        # Business dimensions
        for dim in ['payer_slug', 'state', 'billing_class', 'procedure_set', 'procedure_class', 'taxonomy', 'stat_area_name']:
            value = partition_values.get(dim)
            if value is None:
                value = "null"
            # S3-safe encoding
            value = str(value).replace("/", "_").replace("\\", "_").replace(" ", "_")
            path_parts.append(f"{dim}={value}")
        
        # Time dimensions
        year = partition_values.get('year', datetime.now().year)
        month = partition_values.get('month', datetime.now().month)
        path_parts.extend([f"year={year}", f"month={month:02d}"])
        
        return f"s3://{self.bucket_name}/partitioned-data/" + "/".join(path_parts) + "/fact_rate_enriched.parquet"
```

### **2. Memory-Efficient S3 Upload**
```python
def upload_partition_to_s3(self, partition_data, s3_path, compression='zstd'):
    """
    Upload partition data to S3 with memory optimization.
    """
    
    # Convert to parquet in memory
    parquet_buffer = io.BytesIO()
    
    # Write with compression
    partition_data.write_parquet(
        parquet_buffer,
        compression=compression,
        use_pyarrow=True
    )
    
    # Reset buffer position
    parquet_buffer.seek(0)
    
    # Upload to S3
    bucket, key = self._parse_s3_path(s3_path)
    
    self.s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_buffer.getvalue(),
        ContentType='application/octet-stream',
        ServerSideEncryption='AES256'
    )
    
    # Clean up
    parquet_buffer.close()
    
    return s3_path

def _parse_s3_path(self, s3_path):
    """Parse S3 path into bucket and key."""
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:]
    
    parts = s3_path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    
    return bucket, key
```

### **3. Streaming S3 Processing**
```python
def process_partitions_streaming_s3(self, fact_rate_path, dimensions):
    """
    Process partitions in streaming fashion and upload to S3.
    """
    
    # Process in chunks to manage memory
    chunk_size = 50_000  # Smaller chunks for S3 processing
    
    for chunk in pl.read_parquet(fact_rate_path, batch_size=chunk_size):
        print(f"Processing chunk of {chunk.height} rows...")
        
        # Enrich chunk with dimensions
        enriched_chunk = self.create_enriched_fact_table(chunk, dimensions)
        
        # Create partitions for this chunk
        self.create_s3_partitions(enriched_chunk)
        
        # Clear memory
        del enriched_chunk
        gc.collect()

def create_s3_partitions(self, enriched_df):
    """
    Create S3 partitions from enriched data.
    """
    
    # Define partition columns
    partition_cols = [
        "payer_slug", "state", "billing_class", 
        "procedure_set", "procedure_class", 
        "primary_taxonomy_code", "stat_area_name",
        "year", "month"
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
        # Create S3 path
        s3_path = self.create_s3_path(partition_row)
        
        # Filter data for this partition
        partition_filter = self.create_partition_filter(partition_row)
        partition_data = enriched_df.filter(partition_filter)
        
        # Skip if no data
        if partition_data.height == 0:
            continue
        
        # Upload to S3
        self.upload_partition_to_s3(partition_data, s3_path)
        
        print(f"Uploaded partition: {s3_path} ({partition_data.height} rows)")
```

## S3 Performance Optimization

### **1. S3 Select Integration**
```python
def query_s3_partition(self, s3_path, sql_expression):
    """
    Use S3 Select to query specific partition without downloading entire file.
    """
    
    bucket, key = self._parse_s3_path(s3_path)
    
    response = self.s3_client.select_object_content(
        Bucket=bucket,
        Key=key,
        ExpressionType='SQL',
        Expression=sql_expression,
        InputSerialization={
            'Parquet': {}
        },
        OutputSerialization={
            'JSON': {}
        }
    )
    
    # Process streaming response
    records = []
    for event in response['Payload']:
        if 'Records' in event:
            records.append(event['Records']['Payload'].decode('utf-8'))
    
    return records
```

### **2. Athena Integration**
```python
def create_athena_table(self, database_name, table_name):
    """
    Create Athena table for querying S3 partitioned data.
    """
    
    athena_client = boto3.client('athena', region_name=self.region)
    
    create_table_sql = f"""
    CREATE EXTERNAL TABLE {database_name}.{table_name} (
        fact_uid string,
        state string,
        year_month string,
        payer_slug string,
        billing_class string,
        code_type string,
        code string,
        negotiated_type string,
        negotiation_arrangement string,
        negotiated_rate double,
        expiration_date string,
        -- Add all other columns
        latitude double,
        longitude double,
        county_name string,
        stat_area_name string,
        -- ... other fields
    )
    PARTITIONED BY (
        payer_slug string,
        state string,
        billing_class string,
        procedure_set string,
        procedure_class string,
        taxonomy string,
        stat_area_name string,
        year int,
        month int
    )
    STORED AS PARQUET
    LOCATION 's3://{self.bucket_name}/partitioned-data/'
    TBLPROPERTIES (
        'projection.enabled' = 'true',
        'projection.payer_slug.type' = 'enum',
        'projection.payer_slug.values' = 'unitedhealthcare-of-georgia-inc,anthem-blue-cross-blue-shield',
        'projection.state.type' = 'enum',
        'projection.state.values' = 'GA,FL,CA,TX,NY',
        'projection.billing_class.type' = 'enum',
        'projection.billing_class.values' = 'professional,institutional',
        'projection.year.type' = 'integer',
        'projection.year.range' = '2020,2030',
        'projection.month.type' = 'integer',
        'projection.month.range' = '1,12',
        'storage.location.template' = 's3://{self.bucket_name}/partitioned-data/payer_slug=${{payer_slug}}/state=${{state}}/billing_class=${{billing_class}}/procedure_set=${{procedure_set}}/procedure_class=${{procedure_class}}/taxonomy=${{taxonomy}}/stat_area_name=${{stat_area_name}}/year=${{year}}/month=${{month}}/'
    )
    """
    
    response = athena_client.start_query_execution(
        QueryString=create_table_sql,
        ResultConfiguration={
            'OutputLocation': f's3://{self.bucket_name}/athena-results/'
        }
    )
    
    return response['QueryExecutionId']
```

### **3. Glue Catalog Integration**
```python
def create_glue_crawler(self, crawler_name, s3_path):
    """
    Create Glue crawler to automatically discover and catalog partitions.
    """
    
    glue_client = boto3.client('glue', region_name=self.region)
    
    crawler_config = {
        'Name': crawler_name,
        'Role': 'arn:aws:iam::123456789012:role/GlueServiceRole',
        'DatabaseName': 'healthcare_data_lake',
        'Targets': {
            'S3Targets': [
                {
                    'Path': s3_path,
                    'Exclusions': ['**/athena-results/**']
                }
            ]
        },
        'SchemaChangePolicy': {
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'LOG'
        },
        'RecrawlPolicy': {
            'RecrawlBehavior': 'CRAWL_EVERYTHING'
        }
    }
    
    response = glue_client.create_crawler(**crawler_config)
    return response
```

## S3 Cost Optimization

### **1. Intelligent Tiering**
```python
def setup_intelligent_tiering(self, bucket_name):
    """
    Configure S3 Intelligent Tiering for cost optimization.
    """
    
    s3_client = boto3.client('s3', region_name=self.region)
    
    # Enable Intelligent Tiering
    s3_client.put_bucket_intelligent_tiering_configuration(
        Bucket=bucket_name,
        Id='healthcare-data-tiering',
        IntelligentTieringConfiguration={
            'Id': 'healthcare-data-tiering',
            'Status': 'Enabled',
            'Filter': {
                'Prefix': 'partitioned-data/'
            },
            'Tierings': [
                {
                    'Days': 0,
                    'AccessTier': 'ARCHIVE_ACCESS'
                },
                {
                    'Days': 90,
                    'AccessTier': 'DEEP_ARCHIVE_ACCESS'
                }
            ]
        }
    )
```

### **2. Lifecycle Policies**
```python
def setup_lifecycle_policy(self, bucket_name):
    """
    Configure S3 lifecycle policies for data archival.
    """
    
    s3_client = boto3.client('s3', region_name=self.region)
    
    lifecycle_config = {
        'Rules': [
            {
                'ID': 'healthcare-data-lifecycle',
                'Status': 'Enabled',
                'Filter': {
                    'Prefix': 'partitioned-data/'
                },
                'Transitions': [
                    {
                        'Days': 30,
                        'StorageClass': 'STANDARD_IA'
                    },
                    {
                        'Days': 90,
                        'StorageClass': 'GLACIER'
                    },
                    {
                        'Days': 365,
                        'StorageClass': 'DEEP_ARCHIVE'
                    }
                ]
            }
        ]
    }
    
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration=lifecycle_config
    )
```

## Monitoring and Maintenance

### **1. S3 Metrics and Monitoring**
```python
def setup_cloudwatch_metrics(self, bucket_name):
    """
    Set up CloudWatch metrics for S3 monitoring.
    """
    
    cloudwatch = boto3.client('cloudwatch', region_name=self.region)
    
    # Create custom metrics
    metrics = [
        {
            'MetricName': 'PartitionCount',
            'Namespace': 'HealthcareDataLake',
            'Dimensions': [
                {
                    'Name': 'BucketName',
                    'Value': bucket_name
                }
            ]
        },
        {
            'MetricName': 'DataSize',
            'Namespace': 'HealthcareDataLake',
            'Dimensions': [
                {
                    'Name': 'BucketName',
                    'Value': bucket_name
                }
            ]
        }
    ]
    
    for metric in metrics:
        cloudwatch.put_metric_data(
            Namespace=metric['Namespace'],
            MetricData=[
                {
                    'MetricName': metric['MetricName'],
                    'Dimensions': metric['Dimensions'],
                    'Value': 0,
                    'Unit': 'Count'
                }
            ]
        )
```

### **2. Partition Health Monitoring**
```python
def monitor_partition_health(self, bucket_name):
    """
    Monitor partition health and data quality.
    """
    
    s3_client = boto3.client('s3', region_name=self.region)
    
    # List all partitions
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix='partitioned-data/')
    
    partition_stats = []
    
    for page in pages:
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                # Extract partition info from key
                partition_info = self._extract_partition_info(obj['Key'])
                
                partition_stats.append({
                    'partition_path': obj['Key'],
                    'size_bytes': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'partition_info': partition_info
                })
    
    return pl.DataFrame(partition_stats)
```

## Security and Compliance

### **1. S3 Security Configuration**
```python
def setup_s3_security(self, bucket_name):
    """
    Configure S3 security settings for healthcare data.
    """
    
    s3_client = boto3.client('s3', region_name=self.region)
    
    # Enable versioning
    s3_client.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={'Status': 'Enabled'}
    )
    
    # Enable server-side encryption
    s3_client.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES256'
                    }
                }
            ]
        }
    )
    
    # Block public access
    s3_client.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            'BlockPublicAcls': True,
            'IgnorePublicAcls': True,
            'BlockPublicPolicy': True,
            'RestrictPublicBuckets': True
        }
    )
```

## Expected S3 Benefits

### **Performance Improvements**
- **Query Speed**: 10-20x faster with Athena/Redshift Spectrum
- **Storage Cost**: 60-80% reduction with intelligent tiering
- **Scalability**: Unlimited partitions and storage
- **Availability**: 99.999999999% durability

### **Operational Benefits**
- **Serverless**: No infrastructure management
- **Auto-scaling**: Handles any data volume
- **Integration**: Native AWS analytics services
- **Compliance**: Built-in security and audit features

### **Analytics Benefits**
- **Athena Queries**: SQL on S3 data
- **ML Integration**: SageMaker compatibility
- **Data Lake**: Foundation for advanced analytics
- **Real-time**: Kinesis integration for streaming
