# ETL3: S3 Partitioned Data Warehouse Pipeline

## Overview

ETL3 is a memory-efficient, S3-based partitioned data warehouse pipeline designed for healthcare rate data. It transforms the existing star schema into a denormalized, partitioned structure optimized for analytics performance.

## Architecture

### Key Features
- **Pre-joined Data**: All dimension data embedded in fact table
- **S3 Partitioning**: Hierarchical partitioning by business dimensions
- **Memory Efficiency**: Streaming processing with chunked data
- **AWS Integration**: Native Athena, Glue, and CloudWatch integration
- **Data Quality**: Comprehensive validation and monitoring

### Partitioning Strategy
```
s3://bucket/partitioned-data/
├── payer=unitedhealthcare-of-georgia-inc/
│   ├── state=GA/
│   │   ├── billing_class=professional/
│   │   │   ├── procedure_set=Evaluation and Management/
│   │   │   │   ├── procedure_class=Office/outpatient services/
│   │   │   │   │   ├── taxonomy=101YP2500X/
│   │   │   │   │   │   ├── stat_area_name=Atlanta-Sandy Springs-Alpharetta, GA/
│   │   │   │   │   │   │   ├── year=2025/
│   │   │   │   │   │   │   │   └── month=08/
│   │   │   │   │   │   │   │       └── fact_rate_enriched.parquet
```

## Quick Start

### 1. Prerequisites
```bash
# Install dependencies
pip install polars boto3 pyyaml tqdm

# Set up AWS credentials
aws configure

# Set environment variables
export S3_BUCKET="your-healthcare-data-lake"
export S3_REGION="us-east-1"
```

### 2. Set up AWS Resources
```bash
# Create S3 bucket and other AWS resources
python ETL/scripts/setup_aws_resources.py \
    --bucket-name your-healthcare-data-lake \
    --region us-east-1 \
    --environment development
```

### 3. Run ETL3 Pipeline
```bash
# Run the complete pipeline
python ETL/scripts/run_etl3.py \
    --environment development \
    --monitor \
    --quality-check

# Or run in dry-run mode first
python ETL/scripts/run_etl3.py --dry-run
```

### 4. Query Data with Athena
```sql
-- Basic rate query
SELECT 
    negotiated_rate,
    code_description,
    reporting_entity_name,
    procedure_set,
    stat_area_name
FROM fact_rate_enriched
WHERE state = 'GA' 
  AND code = '99213'
  AND negotiated_rate > 100
LIMIT 100;
```

## File Structure

```
ETL/
├── ETL_3.ipynb                    # Main pipeline notebook
├── utils/
│   ├── s3_etl_utils.py           # S3 ETL utilities
│   ├── data_quality.py           # Data quality validation
│   └── monitoring.py             # Monitoring and metrics
├── config/
│   └── etl3_config.yaml          # Configuration file
├── scripts/
│   ├── run_etl3.py               # Pipeline runner script
│   └── setup_aws_resources.py    # AWS setup script
└── README_ETL3.md                # This file
```

## Configuration

### Environment Variables
- `S3_BUCKET`: S3 bucket name for data storage
- `S3_REGION`: AWS region for S3 bucket
- `CHUNK_SIZE`: Number of rows to process per chunk (default: 50000)
- `MAX_WORKERS`: Number of parallel workers (default: 4)
- `ENVIRONMENT`: Environment name (development/staging/production)

### Configuration File
Edit `ETL/config/etl3_config.yaml` to customize:
- S3 settings
- Processing parameters
- Partitioning strategy
- Data quality rules
- Monitoring settings

## Usage Examples

### Basic Pipeline Run
```bash
# Run with default settings
python ETL/scripts/run_etl3.py

# Run with custom settings
python ETL/scripts/run_etl3.py \
    --chunk-size 25000 \
    --max-workers 8 \
    --s3-bucket my-custom-bucket
```

### Validation Only
```bash
# Run data validation without processing
python ETL/scripts/run_etl3.py --validate-only
```

### Dry Run
```bash
# Test configuration without actual processing
python ETL/scripts/run_etl3.py --dry-run
```

## Data Quality

### Validation Rules
- **Required Fields**: fact_uid, negotiated_rate, state, payer_slug
- **Rate Validation**: Values between $0.01 and $1,000,000
- **State Validation**: Valid US state codes
- **NPI Validation**: 10-digit format
- **Geocoding Validation**: Valid latitude/longitude ranges

### Quality Metrics
- Completeness percentage for each field
- Rate statistics (mean, median, min, max)
- Geographic coverage (statistical areas, counties)
- Provider coverage (unique NPIs)
- Medicare benchmark coverage

## Monitoring

### CloudWatch Metrics
- `ChunkProcessingRate`: Rows processed per second
- `ChunkInputRows`: Input rows per chunk
- `ChunkOutputRows`: Output rows per chunk
- `PartitionsCreated`: Number of partitions created
- `DataQualityScore`: Overall data quality score
- `ErrorCount`: Number of errors encountered

### Alarms
- High error rate (>10 errors)
- Low processing rate (<100 rows/second)
- High memory usage (>80%)

### Dashboard
Access the CloudWatch dashboard to monitor:
- Processing performance
- Data quality metrics
- Error rates
- Resource utilization

## Query Examples

### Geographic Analysis
```sql
SELECT 
    stat_area_name,
    county_name,
    procedure_set,
    AVG(negotiated_rate) as avg_rate,
    COUNT(*) as rate_count
FROM fact_rate_enriched
WHERE state = 'GA' 
  AND stat_area_name != 'Unknown'
GROUP BY stat_area_name, county_name, procedure_set
ORDER BY avg_rate DESC;
```

### Medicare Benchmark Analysis
```sql
SELECT 
    code,
    code_description,
    negotiated_rate,
    medicare_state_rate,
    rate_to_medicare_ratio,
    benchmark_type
FROM fact_rate_enriched
WHERE state = 'GA' 
  AND negotiated_rate > 0
  AND medicare_state_rate > 0
  AND rate_to_medicare_ratio > 2.0
ORDER BY rate_to_medicare_ratio DESC;
```

### Provider Network Analysis
```sql
SELECT 
    provider_group_id_raw,
    primary_taxonomy_desc,
    stat_area_name,
    COUNT(DISTINCT npi) as provider_count,
    AVG(negotiated_rate) as avg_rate
FROM fact_rate_enriched
WHERE state = 'GA'
  AND enumeration_type = 'NPI-1'
GROUP BY provider_group_id_raw, primary_taxonomy_desc, stat_area_name
ORDER BY provider_count DESC;
```

## Performance Optimization

### Memory Management
- **Streaming Processing**: Process data in chunks
- **Lazy Evaluation**: Use Polars lazy evaluation
- **Garbage Collection**: Explicit memory cleanup
- **Partition Pruning**: Only load relevant partitions

### S3 Optimization
- **Compression**: ZSTD compression for optimal size
- **Parallel Uploads**: Multiple concurrent uploads
- **Intelligent Tiering**: Automatic cost optimization
- **Lifecycle Policies**: Automatic archival

### Query Performance
- **Partition Pruning**: Only scan relevant partitions
- **Column Pruning**: Only read required columns
- **Predicate Pushdown**: Filter at storage level
- **Caching**: Leverage Athena result caching

## Troubleshooting

### Common Issues

#### Memory Issues
```bash
# Reduce chunk size
python ETL/scripts/run_etl3.py --chunk-size 10000

# Increase memory limit
export MEMORY_LIMIT_MB=16384
```

#### S3 Permission Issues
```bash
# Check AWS credentials
aws sts get-caller-identity

# Verify S3 permissions
aws s3 ls s3://your-bucket-name
```

#### Data Quality Issues
```bash
# Run validation only
python ETL/scripts/run_etl3.py --validate-only

# Check specific partition
python -c "
from ETL.utils.s3_etl_utils import S3PartitionedETL
etl = S3PartitionedETL('your-bucket')
data = etl.download_partition('s3://your-bucket/path/to/partition.parquet')
print(data.describe())
"
```

### Logs
- **Pipeline Logs**: `logs/etl3.log`
- **CloudWatch Logs**: `/aws/glue/etl3-pipeline`
- **S3 Access Logs**: `s3://your-bucket/logs/`

## Cost Optimization

### S3 Costs
- **Intelligent Tiering**: Automatic movement to cheaper storage
- **Lifecycle Policies**: Archive old data to Glacier
- **Compression**: Reduce storage size by 20-30%
- **Partition Pruning**: Only query relevant data

### Athena Costs
- **Partition Pruning**: Reduce data scanned
- **Column Pruning**: Only read required columns
- **Result Caching**: Reuse query results
- **Compression**: Reduce data transfer

### Estimated Costs (Monthly)
- **S3 Storage**: $0.023/GB (Standard)
- **Athena Queries**: $5/TB scanned
- **Glue Crawler**: $0.44/DPU-hour
- **CloudWatch**: $0.30/metric

## Security

### Data Protection
- **Encryption**: AES-256 server-side encryption
- **Access Control**: IAM-based permissions
- **VPC Endpoints**: Private network access
- **Audit Logging**: CloudTrail integration

### Compliance
- **HIPAA Ready**: Healthcare data protection
- **SOC 2**: Security controls
- **GDPR**: Data privacy compliance
- **Audit Trails**: Complete activity logging

## Support

### Documentation
- **Data Dictionary**: `information/README_DATA_DICTIONARY_PARTITIONED.md`
- **Implementation Guide**: `information/S3_PARTITIONED_ETL_GUIDELINES.md`
- **API Reference**: `ETL/utils/` module documentation

### Monitoring
- **CloudWatch Dashboard**: Real-time metrics
- **Alarms**: Automated alerting
- **Logs**: Detailed execution logs
- **Reports**: Quality and performance reports

### Troubleshooting
1. Check logs for error messages
2. Verify AWS permissions
3. Run validation checks
4. Monitor CloudWatch metrics
5. Contact support if needed
