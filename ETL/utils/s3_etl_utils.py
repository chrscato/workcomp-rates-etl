"""
S3 ETL Utilities for Partitioned Data Warehouse

This module provides utilities for creating and managing S3-based partitioned data warehouses.
"""

import os
import io
import json
import time
import hashlib
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path

import polars as pl
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)


class S3Config:
    """Configuration for S3 operations."""
    
    def __init__(self, region='us-east-1'):
        self.region = region
        self.s3_config = Config(
            region_name=region,
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            max_pool_connections=50,
            s3={
                'addressing_style': 'virtual',
                'payload_signing_enabled': False
            }
        )


class S3PartitionedETL:
    """S3-based partitioned ETL operations."""
    
    def __init__(self, bucket_name: str, region: str = 'us-east-1'):
        self.bucket_name = bucket_name
        self.region = region
        self.config = S3Config(region)
        
        # Initialize S3 clients
        self.s3_client = boto3.client('s3', config=self.config.s3_config)
        self.s3_resource = boto3.resource('s3', config=self.config.s3_config)
        
        # Initialize other AWS clients
        self.athena_client = boto3.client('athena', region_name=region)
        self.glue_client = boto3.client('glue', region_name=region)
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=region)
        
        logger.info(f"S3 ETL initialized for bucket: {bucket_name}")
    
    def create_s3_path(self, partition_values: Dict[str, Any], prefix: str = 'partitioned-data') -> str:
        """Create S3 path for partition."""
        path_parts = [prefix]
        
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
        
        # Ensure month is properly formatted as string with zero padding
        if isinstance(month, str):
            month_str = month.zfill(2)
        else:
            month_str = f"{month:02d}"
            
        path_parts.extend([f"year={year}", f"month={month_str}"])
        
        return f"s3://{self.bucket_name}/" + "/".join(path_parts) + "/fact_rate_enriched.parquet"
    
    def upload_partition_to_s3(self, partition_data: pl.DataFrame, s3_path: str, compression: str = 'zstd') -> str:
        """Upload partition data to S3 with memory optimization."""
        
        logger.info(f"Uploading partition to S3: {s3_path}")
        
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
        
        try:
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream',
                ServerSideEncryption='AES256'
            )
            
            logger.info(f"[SUCCESS] Successfully uploaded {partition_data.height:,} rows to {s3_path}")
            return s3_path
            
        except ClientError as e:
            logger.error(f"[ERROR] Failed to upload to S3: {e}")
            raise
        finally:
            parquet_buffer.close()
    
    def create_s3_partitions(self, enriched_df: pl.DataFrame, partition_cols: List[str], prefix: str) -> List[str]:
        """Create S3 partitions from enriched data."""
        
        logger.info(f"Creating S3 partitions for {enriched_df.height:,} rows...")
        
        # Get unique partition combinations
        partition_combinations = (
            enriched_df
            .select(partition_cols)
            .unique()
            .sort(partition_cols)
        )
        
        created_partitions = []
        
        # Process each partition
        for partition_row in tqdm(partition_combinations.iter_rows(named=True), 
                                total=partition_combinations.height, 
                                desc="Creating partitions"):
            
            # Create S3 path
            s3_path = self.create_s3_path(partition_row, prefix)
            
            # Filter data for this partition
            partition_filter = self._create_partition_filter(partition_row)
            partition_data = enriched_df.filter(partition_filter)
            
            # Skip if no data
            if partition_data.height == 0:
                continue
            
            # Upload to S3
            self.upload_partition_to_s3(partition_data, s3_path)
            created_partitions.append(s3_path)
        
        logger.info(f"[SUCCESS] Created {len(created_partitions)} partitions")
        return created_partitions
    
    def _create_partition_filter(self, partition_row: Dict[str, Any]) -> pl.Expr:
        """Create filter condition for partition."""
        conditions = []
        for col, value in partition_row.items():
            if value is None:
                conditions.append(pl.col(col).is_null())
            else:
                conditions.append(pl.col(col) == value)
        
        return pl.all_horizontal(conditions)
    
    def _parse_s3_path(self, s3_path: str) -> Tuple[str, str]:
        """Parse S3 path into bucket and key."""
        if s3_path.startswith('s3://'):
            s3_path = s3_path[5:]
        
        parts = s3_path.split('/', 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ''
        
        return bucket, key
    
    def list_partitions(self, prefix: str) -> List[str]:
        """List all partitions in S3."""
        
        partitions = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        try:
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            for page in pages:
                for obj in page.get('Contents', []):
                    if obj['Key'].endswith('.parquet'):
                        partitions.append(f"s3://{self.bucket_name}/{obj['Key']}")
            
            logger.info(f"Found {len(partitions)} partitions in S3")
            return partitions
            
        except ClientError as e:
            logger.error(f"Failed to list partitions: {e}")
            return []
    
    def partition_exists(self, s3_path: str) -> bool:
        """Check if a partition exists in S3."""
        
        bucket, key = self._parse_s3_path(s3_path)
        
        try:
            self.s3_client.head_object(Bucket=bucket, Key=key)
            logger.debug(f"Partition exists: {s3_path}")
            return True
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.debug(f"Partition does not exist: {s3_path}")
                return False
            else:
                logger.error(f"Error checking partition existence {s3_path}: {e}")
                raise
    
    def download_partition(self, s3_path: str) -> pl.DataFrame:
        """Download and load a partition from S3."""
        
        bucket, key = self._parse_s3_path(s3_path)
        
        try:
            # Download object
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            
            # Read parquet from bytes
            parquet_data = response['Body'].read()
            df = pl.read_parquet(io.BytesIO(parquet_data))
            
            logger.info(f"Downloaded partition: {s3_path} ({df.height:,} rows)")
            return df
            
        except ClientError as e:
            logger.error(f"Failed to download partition {s3_path}: {e}")
            raise
    
    def read_partition(self, s3_path: str) -> pl.DataFrame:
        """Alias for download_partition for consistency."""
        return self.download_partition(s3_path)
    
    def create_athena_table(self, database_name: str, table_name: str, output_location: str) -> str:
        """Create Athena table for querying S3 partitioned data."""
        
        logger.info(f"Creating Athena table: {database_name}.{table_name}")
        
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
            provider_group_id_raw bigint,
            reporting_entity_name string,
            code_description string,
            code_name string,
            procedure_set string,
            procedure_class string,
            procedure_group string,
            pos_set_id string,
            pos_members array<string>,
            npi string,
            first_name string,
            last_name string,
            organization_name string,
            enumeration_type string,
            status string,
            primary_taxonomy_code string,
            primary_taxonomy_desc string,
            primary_taxonomy_state string,
            primary_taxonomy_license string,
            credential string,
            sole_proprietor string,
            enumeration_date string,
            last_updated string,
            address_purpose string,
            address_type string,
            address_1 string,
            address_2 string,
            city string,
            postal_code string,
            country_code string,
            telephone_number string,
            fax_number string,
            address_hash string,
            latitude double,
            longitude double,
            county_name string,
            county_fips string,
            stat_area_name string,
            stat_area_code string,
            matched_address string,
            benchmark_type string,
            medicare_national_rate double,
            medicare_state_rate double,
            work_rvu double,
            practice_expense_rvu double,
            malpractice_rvu double,
            total_rvu double,
            conversion_factor double,
            opps_weight double,
            opps_si string,
            asc_pi string,
            rate_to_medicare_ratio double,
            is_above_medicare boolean,
            provider_type string,
            is_sole_proprietor boolean,
            is_individual_provider boolean,
            year int,
            month int
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
        
        try:
            response = self.athena_client.start_query_execution(
                QueryString=create_table_sql,
                ResultConfiguration={
                    'OutputLocation': output_location
                }
            )
            
            query_execution_id = response['QueryExecutionId']
            logger.info(f"[SUCCESS] Athena table creation started: {query_execution_id}")
            
            # Wait for completion
            self._wait_for_athena_query(query_execution_id)
            
            return query_execution_id
            
        except ClientError as e:
            logger.error(f"Failed to create Athena table: {e}")
            raise
    
    def create_glue_crawler(self, crawler_name: str, s3_path: str) -> str:
        """Create Glue crawler to automatically discover and catalog partitions."""
        
        logger.info(f"Creating Glue crawler: {crawler_name}")
        
        try:
            crawler_config = {
                'Name': crawler_name,
                'Role': f'arn:aws:iam::{self._get_account_id()}:role/GlueServiceRole',
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
            
            response = self.glue_client.create_crawler(**crawler_config)
            logger.info(f"[SUCCESS] Glue crawler created: {crawler_name}")
            
            return crawler_name
            
        except ClientError as e:
            logger.error(f"Failed to create Glue crawler: {e}")
            raise
    
    def _wait_for_athena_query(self, query_execution_id: str, max_wait_time: int = 300) -> None:
        """Wait for Athena query to complete."""
        
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED']:
                logger.info(f"[SUCCESS] Athena query completed: {query_execution_id}")
                return
            elif status in ['FAILED', 'CANCELLED']:
                error_reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise Exception(f"Athena query failed: {error_reason}")
            
            time.sleep(5)
        
        raise Exception(f"Athena query timed out after {max_wait_time} seconds")
    
    def _get_account_id(self) -> str:
        """Get AWS account ID."""
        try:
            sts_client = boto3.client('sts', region_name=self.region)
            response = sts_client.get_caller_identity()
            return response['Account']
        except ClientError:
            return '123456789012'  # Fallback for testing
    
    def setup_s3_security(self) -> None:
        """Configure S3 security settings for healthcare data."""
        
        logger.info("Setting up S3 security configuration...")
        
        try:
            # Enable versioning
            self.s3_client.put_bucket_versioning(
                Bucket=self.bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            
            # Enable server-side encryption
            self.s3_client.put_bucket_encryption(
                Bucket=self.bucket_name,
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
            self.s3_client.put_public_access_block(
                Bucket=self.bucket_name,
                PublicAccessBlockConfiguration={
                    'BlockPublicAcls': True,
                    'IgnorePublicAcls': True,
                    'BlockPublicPolicy': True,
                    'RestrictPublicBuckets': True
                }
            )
            
            logger.info("[SUCCESS] S3 security configuration completed")
            
        except ClientError as e:
            logger.error(f"Failed to configure S3 security: {e}")
            raise
    
    def setup_intelligent_tiering(self) -> None:
        """Configure S3 Intelligent Tiering for cost optimization."""
        
        logger.info("Setting up S3 Intelligent Tiering...")
        
        try:
            self.s3_client.put_bucket_intelligent_tiering_configuration(
                Bucket=self.bucket_name,
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
            
            logger.info("[SUCCESS] S3 Intelligent Tiering configured")
            
        except ClientError as e:
            logger.error(f"Failed to configure Intelligent Tiering: {e}")
            raise
