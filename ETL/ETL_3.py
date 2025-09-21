"""
ETL3: S3 Partitioned Data Warehouse Pipeline

This module implements a memory-efficient, S3-based partitioned data warehouse for healthcare rate data.

Pipeline Overview:
1. Data Loading: Load fact and dimension tables
2. Data Enrichment: Pre-join all dimensions with fact data
3. Partitioning: Create S3 partitions by business dimensions
4. Upload: Stream data to S3 with optimization
5. Cataloging: Create Athena tables and Glue catalog
6. Monitoring: Track performance and data quality
"""

import os
import sys
import yaml
import time
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime

import polars as pl
import boto3
from botocore.exceptions import ClientError

# Add utils to path
sys.path.append(str(Path(__file__).parent / "utils"))
from s3_etl_utils import S3PartitionedETL, S3Config
from monitoring import ETLMonitor
from data_quality import DataQualityChecker

logger = logging.getLogger(__name__)


class ETL3Config:
    """Configuration class for ETL3 pipeline."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize configuration from YAML file or environment variables."""
        
        if config_path is None:
            config_path = os.environ.get('CONFIG_FILE', 'ETL/config/etl3_config.yaml')
        
        self.config_path = config_path
        self.config = self._load_config()
        
        # Set up paths
        self.project_root = Path(__file__).parent.parent
        self.data_root = self.project_root / "data"
        
        # S3 Configuration
        self.S3_BUCKET = os.environ.get('S3_BUCKET', self.config.get('s3', {}).get('bucket', 'healthcare-data-lake-prod'))
        self.S3_REGION = self.config.get('s3', {}).get('region', 'us-east-1')
        self.S3_PREFIX = self.config.get('s3', {}).get('prefix', 'partitioned-data')
        
        # Processing Configuration
        self.CHUNK_SIZE = int(os.environ.get('CHUNK_SIZE', self.config.get('processing', {}).get('chunk_size', 10000)))
        self.MAX_WORKERS = int(os.environ.get('MAX_WORKERS', self.config.get('processing', {}).get('max_workers', 2)))
        self.MEMORY_LIMIT_MB = int(os.environ.get('MEMORY_LIMIT_MB', self.config.get('processing', {}).get('memory_limit_mb', 2048)))
        
        # Data Paths
        self.FACT_RATE_PATH = self.data_root / "gold" / "fact_rate.parquet"
        self.DIM_DIR = self.data_root / "dims"
        self.XREF_DIR = self.data_root / "xrefs"
        
        # Dimension file paths
        self.DIM_PATHS = {
            'code': self.DIM_DIR / "dim_code.parquet",
            'code_cat': self.DIM_DIR / "dim_code_cat.parquet",
            'payer': self.DIM_DIR / "dim_payer.parquet",
            'provider_group': self.DIM_DIR / "dim_provider_group.parquet",
            'pos_set': self.DIM_DIR / "dim_pos_set.parquet",
            'npi': self.DIM_DIR / "dim_npi.parquet",
            'npi_address': self.DIM_DIR / "dim_npi_address.parquet",
            'npi_address_geo': self.DIM_DIR / "dim_npi_address_geolocation.parquet"
        }
        
        # Cross-reference file paths
        self.XREF_PATHS = {
            'pg_member_npi': self.XREF_DIR / "xref_pg_member_npi.parquet",
            'pg_member_tin': self.XREF_DIR / "xref_pg_member_tin.parquet"
        }
        
        # Partitioning configuration
        self.PARTITION_COLUMNS = self.config.get('partitioning', {}).get('partition_columns', [
            'payer_slug', 'state', 'billing_class', 'procedure_set', 
            'procedure_class', 'primary_taxonomy_code', 'stat_area_name', 'year', 'month'
        ])
        
        # Athena configuration
        self.ATHENA_DATABASE = self.config.get('athena', {}).get('database', 'healthcare_data_lake')
        self.ATHENA_TABLE = self.config.get('athena', {}).get('table', 'fact_rate_enriched')
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {self.config_path}. Using defaults.")
            return {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing config file: {e}")
            return {}
    
    def validate(self) -> bool:
        """Validate configuration and required files."""
        logger.info("Validating ETL3 configuration...")
        
        # Check required files exist
        if not self.FACT_RATE_PATH.exists():
            logger.error(f"Fact table not found: {self.FACT_RATE_PATH}")
            return False
        
        # Check dimension files
        missing_dims = []
        for name, path in self.DIM_PATHS.items():
            if not path.exists():
                missing_dims.append(str(path))
        
        if missing_dims:
            logger.error(f"Missing dimension files: {missing_dims}")
            return False
        
        # Check AWS credentials
        try:
            sts = boto3.client('sts')
            sts.get_caller_identity()
            logger.info("AWS credentials validated")
        except Exception as e:
            logger.error(f"AWS credentials not available: {e}")
            return False
        
        logger.info("Configuration validated successfully")
        return True


def run_etl3_pipeline(config: Optional[ETL3Config] = None) -> Dict[str, Any]:
    """
    Run the complete ETL3 pipeline for S3 partitioned data warehouse.
    Uses memory-efficient streaming approach for large datasets.
    
    Args:
        config: ETL3Config instance. If None, creates a new one.
        
    Returns:
        Dictionary with pipeline execution summary
    """
    
    if config is None:
        config = ETL3Config()
    
    # Validate configuration
    if not config.validate():
        raise ValueError("Configuration validation failed")
    
    logger.info("Starting ETL3 Pipeline (Memory-Efficient Mode)")
    start_time = time.time()
    
    try:
        # Initialize S3 ETL utilities
        s3_etl = S3PartitionedETL(
            bucket_name=config.S3_BUCKET,
            region=config.S3_REGION
        )
        
        # Load dimension tables (these are small, load once)
        logger.info("Loading dimension tables...")
        dimensions = {}
        for name, path in config.DIM_PATHS.items():
            if path.exists():
                dimensions[name] = pl.read_parquet(path)
                logger.info(f"Loaded {dimensions[name].height:,} {name} records")
        
        # Load cross-reference tables (these are small, load once)
        logger.info("Loading cross-reference tables...")
        xrefs = {}
        for name, path in config.XREF_PATHS.items():
            if path.exists():
                xrefs[name] = pl.read_parquet(path)
                logger.info(f"Loaded {xrefs[name].height:,} {name} records")
        
        # Process fact table in streaming chunks
        logger.info("Processing fact table in streaming mode...")
        fact_lazy = pl.scan_parquet(config.FACT_RATE_PATH)
        total_rows = fact_lazy.select(pl.count()).collect().item()
        logger.info(f"Total fact records to process: {total_rows:,}")
        
        # Process in chunks
        chunk_size = config.CHUNK_SIZE
        total_chunks = (total_rows + chunk_size - 1) // chunk_size
        processed_rows = 0
        total_partitions = 0
        
        logger.info(f"Processing {total_chunks} chunks of {chunk_size:,} rows each...")
        logger.info(f"Memory limit: {config.MEMORY_LIMIT_MB} MB")
        
        # Progress tracking
        start_time = time.time()
        last_progress_time = start_time
        
        for chunk_idx in range(total_chunks):
            chunk_start_time = time.time()
            logger.info(f"Processing chunk {chunk_idx + 1}/{total_chunks}...")
            
            try:
                # Load chunk
                chunk_start = chunk_idx * chunk_size
                chunk_data = fact_lazy.slice(chunk_start, chunk_size).collect()
                
                if chunk_data.height == 0:
                    logger.info("No more data to process")
                    break
                    
                # Enrich chunk with dimensions
                enriched_chunk = _enrich_fact_table(chunk_data, dimensions, xrefs)
                
                # Create partitions for this chunk
                chunk_partitions = _create_partitions_for_chunk(
                    enriched_chunk, 
                    config.PARTITION_COLUMNS,
                    s3_etl,
                    config.S3_PREFIX,
                    config
                )
                
                total_partitions += len(chunk_partitions)
                processed_rows += chunk_data.height
                
                # Calculate timing and progress
                chunk_time = time.time() - chunk_start_time
                progress_pct = (chunk_idx + 1) / total_chunks * 100
                elapsed_time = time.time() - start_time
                
                # Estimate remaining time
                if chunk_idx > 0:
                    avg_chunk_time = elapsed_time / (chunk_idx + 1)
                    remaining_chunks = total_chunks - (chunk_idx + 1)
                    estimated_remaining = remaining_chunks * avg_chunk_time
                else:
                    estimated_remaining = 0
                
                logger.info(f"Chunk {chunk_idx + 1} complete: {chunk_data.height:,} rows, "
                           f"{len(chunk_partitions)} partitions created. "
                           f"Progress: {progress_pct:.1f}% "
                           f"(~{estimated_remaining/60:.1f} min remaining)")
                
                # Memory cleanup
                del chunk_data, enriched_chunk, chunk_partitions
                
                # Progress update every 10 chunks or 5 minutes
                if (chunk_idx + 1) % 10 == 0 or (time.time() - last_progress_time) > 300:
                    logger.info(f"Progress update: {processed_rows:,} rows processed, "
                               f"{total_partitions:,} partitions created")
                    last_progress_time = time.time()
                
            except Exception as e:
                logger.error(f"Error processing chunk {chunk_idx + 1}: {str(e)}")
                # Continue with next chunk instead of failing completely
                continue
        
        # Create Athena table
        logger.info("Creating Athena table...")
        output_location = f"s3://{config.S3_BUCKET}/{config.S3_PREFIX}/athena-output/"
        athena_table = s3_etl.create_athena_table(
            database_name=config.ATHENA_DATABASE,
            table_name=config.ATHENA_TABLE,
            output_location=output_location
        )
        
        # Calculate execution metrics
        end_time = time.time()
        total_time = end_time - start_time
        
        summary = {
            'total_input_rows': processed_rows,
            'total_chunks_processed': total_chunks,
            'total_partitions_created': total_partitions,
            'total_processing_time': total_time,
            'rows_per_second': processed_rows / total_time if total_time > 0 else 0,
            's3_bucket': config.S3_BUCKET,
            's3_prefix': config.S3_PREFIX,
            'athena_database': config.ATHENA_DATABASE,
            'athena_table': config.ATHENA_TABLE,
            'status': 'SUCCESS'
        }
        
        logger.info("ETL3 Pipeline completed successfully!")
        return summary
        
    except Exception as e:
        logger.error(f"ETL3 Pipeline failed: {str(e)}")
        raise


def _enrich_fact_table(fact_rate: pl.DataFrame, dimensions: Dict[str, pl.DataFrame], 
                      xrefs: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """
    Enrich fact table with dimension data.
    
    Args:
        fact_rate: Fact table DataFrame
        dimensions: Dictionary of dimension DataFrames
        xrefs: Dictionary of cross-reference DataFrames
        
    Returns:
        Enriched fact table DataFrame
    """
    
    logger.info("Enriching fact table with dimension data...")
    
    # Start with fact table
    enriched = fact_rate
    
    # Join with code dimension
    if 'code' in dimensions:
        enriched = enriched.join(
            dimensions['code'],
            on=['code_type', 'code'],
            how='left'
        )
        logger.info("Joined with code dimension")
    
    # Join with code categorization dimension
    if 'code_cat' in dimensions:
        enriched = enriched.join(
            dimensions['code_cat'],
            left_on='code',
            right_on='proc_cd',
            how='left'
        )
        logger.info("Joined with code categorization dimension")
    
    # Join with payer dimension
    if 'payer' in dimensions:
        enriched = enriched.join(
            dimensions['payer'],
            on='payer_slug',
            how='left'
        )
        logger.info("Joined with payer dimension")
    
    # Join with provider group dimension
    if 'provider_group' in dimensions:
        enriched = enriched.join(
            dimensions['provider_group'],
            on='pg_uid',
            how='left'
        )
        logger.info("Joined with provider group dimension")
    
    # Join with POS set dimension
    if 'pos_set' in dimensions:
        enriched = enriched.join(
            dimensions['pos_set'],
            on='pos_set_id',
            how='left'
        )
        logger.info("Joined with POS set dimension")
    
    # Add NPI data through cross-reference
    if 'pg_member_npi' in xrefs and 'npi' in dimensions:
        # Join fact -> xref -> npi
        enriched = enriched.join(
            xrefs['pg_member_npi'],
            on='pg_uid',
            how='left'
        ).join(
            dimensions['npi'],
            on='npi',
            how='left'
        )
        logger.info("Joined with NPI data through cross-reference")
    
    # Add TIN data through cross-reference
    if 'pg_member_tin' in xrefs:
        enriched = enriched.join(
            xrefs['pg_member_tin'],
            on='pg_uid',
            how='left'
        )
        logger.info("Joined with TIN data through cross-reference")
    
    # Add geolocation data through NPI address (LOCATION addresses only)
    if 'npi_address_geo' in dimensions and 'npi' in enriched.columns:
        # Filter for LOCATION addresses only (not MAILING) and select only geo fields
        location_addresses = dimensions['npi_address_geo'].filter(
            pl.col('address_purpose') == 'LOCATION'
        ).select([
            'npi',  # Keep NPI for joining
            'state',  # Include state for partitioning
            'latitude', 'longitude', 'county_name', 'county_fips', 
            'stat_area_name', 'stat_area_code', 'matched_address'
        ])
        enriched = enriched.join(
            location_addresses,
            on='npi',
            how='left',
            suffix='_geo'
        )
        logger.info("Joined with NPI geolocation data (LOCATION addresses only, geo fields only)")
    
    # Add partitioning columns using the new extract_partition_keys function
    enriched = extract_partition_keys(enriched)
    
    logger.info(f"Enriched fact table: {enriched.height:,} rows, {len(enriched.columns)} columns")
    return enriched


def _create_partitions_for_chunk(chunk_data: pl.DataFrame, partition_columns: List[str], 
                                s3_etl: S3PartitionedETL, prefix: str, config: Optional[ETL3Config] = None) -> List[str]:
    """
    Create S3 partitions for a single chunk of data with idempotent writes.
    
    Args:
        chunk_data: Chunk of enriched data
        partition_columns: List of columns to partition by
        s3_etl: S3PartitionedETL instance
        prefix: S3 prefix for partitions
        config: ETL3Config instance for validation (optional)
        
    Returns:
        List of created/updated S3 partition paths
    """
    
    created_partitions = []
    
    # Validate partition data quality if config is provided
    if config is not None:
        try:
            validate_partition_data(chunk_data, config)
        except ValueError as e:
            logger.error(f"Partition validation failed for chunk: {e}")
            # Continue processing but log the validation failure
            logger.warning("Continuing with partition creation despite validation failures")
    
    # Get unique partition combinations for this chunk
    partition_combinations = (
        chunk_data
        .select(partition_columns)
        .unique()
        .sort(partition_columns)
    )
    
    logger.info(f"Processing {partition_combinations.height} unique partition combinations")
    
    # Process each partition combination
    for partition_row in partition_combinations.iter_rows(named=True):
        # Create S3 path
        s3_path = s3_etl.create_s3_path(partition_row, prefix)
        
        # Filter data for this partition
        partition_filter = _create_partition_filter(partition_row)
        partition_data = chunk_data.filter(partition_filter)
        
        # Skip if no data
        if partition_data.height == 0:
            logger.debug(f"No data for partition: {s3_path}")
            continue
        
        # Write partition idempotently (handles duplicates and existing partitions)
        try:
            write_partition_idempotent(partition_data, s3_path, s3_etl)
            created_partitions.append(s3_path)
            logger.debug(f"Successfully processed partition: {s3_path} ({partition_data.height:,} rows)")
        except Exception as e:
            logger.error(f"Failed to process partition {s3_path}: {e}")
            # Continue with next partition instead of failing completely
            continue
    
    logger.info(f"Successfully processed {len(created_partitions)} partitions")
    return created_partitions


def _create_partition_filter(partition_row: Dict[str, Any]) -> pl.Expr:
    """Create filter condition for partition."""
    conditions = []
    for col, value in partition_row.items():
        if value is None:
            conditions.append(pl.col(col).is_null())
        else:
            conditions.append(pl.col(col) == value)
    
    return pl.all_horizontal(conditions)


def merge_partition_data(existing_data: pl.DataFrame, new_data: pl.DataFrame) -> pl.DataFrame:
    """
    Merge partition data handling duplicates.
    
    Args:
        existing_data: Existing partition data
        new_data: New data to merge
        
    Returns:
        Merged DataFrame with duplicates removed
    """
    
    logger.info(f"Merging partition data: {existing_data.height:,} existing + {new_data.height:,} new rows")
    
    try:
        # Ensure both dataframes have the same schema
        if existing_data.columns != new_data.columns:
            logger.warning("Schema mismatch between existing and new data, aligning columns")
            # Get common columns
            common_columns = list(set(existing_data.columns) & set(new_data.columns))
            existing_data = existing_data.select(common_columns)
            new_data = new_data.select(common_columns)
        
        # Add unique row identifiers if not present
        if 'row_id' not in existing_data.columns:
            existing_data = existing_data.with_row_index('row_id', offset=0)
        
        if 'row_id' not in new_data.columns:
            # Offset by existing data size to avoid conflicts
            new_data = new_data.with_row_index('row_id', offset=existing_data.height)
        
        # Combine the data
        combined_data = pl.concat([existing_data, new_data])
        
        # Define deduplication key - use fact_uid as primary key if available, otherwise use all columns
        if 'fact_uid' in combined_data.columns:
            dedup_key = ['fact_uid']
            logger.info("Using fact_uid as deduplication key")
        else:
            # Use all columns except row_id for deduplication
            dedup_key = [col for col in combined_data.columns if col != 'row_id']
            logger.info(f"Using all columns except row_id as deduplication key: {dedup_key}")
        
        # Remove duplicates, keeping the last occurrence (newest data wins)
        merged_data = combined_data.unique(subset=dedup_key, keep='last')
        
        # Remove the temporary row_id column
        if 'row_id' in merged_data.columns:
            merged_data = merged_data.drop('row_id')
        
        logger.info(f"Merged partition data: {merged_data.height:,} rows (removed {combined_data.height - merged_data.height:,} duplicates)")
        return merged_data
        
    except Exception as e:
        logger.error(f"Error merging partition data: {e}")
        # Fallback: return new data if merge fails
        logger.warning("Merge failed, returning new data only")
        return new_data


def write_partition_idempotent(partition_data: pl.DataFrame, s3_path: str, s3_etl: S3PartitionedETL) -> str:
    """
    Write partition data idempotently.
    If partition exists, merge with existing data.
    
    Args:
        partition_data: Data to write
        s3_path: S3 path for the partition
        s3_etl: S3PartitionedETL instance
        
    Returns:
        S3 path of the written partition
    """
    
    logger.info(f"Writing partition idempotently: {s3_path}")
    
    try:
        # Check if partition already exists in S3
        if s3_etl.partition_exists(s3_path):
            logger.info(f"Partition exists, merging with existing data: {s3_path}")
            
            # Download existing partition
            existing_data = s3_etl.read_partition(s3_path)
            
            # Merge with new data (handle duplicates by unique key)
            merged_data = merge_partition_data(existing_data, partition_data)
            
            # Write back merged data
            s3_etl.upload_partition_to_s3(merged_data, s3_path)
            
            logger.info(f"Successfully merged and wrote partition: {s3_path}")
        else:
            # New partition, write directly
            logger.info(f"New partition, writing directly: {s3_path}")
            s3_etl.upload_partition_to_s3(partition_data, s3_path)
            
            logger.info(f"Successfully wrote new partition: {s3_path}")
        
        return s3_path
        
    except Exception as e:
        logger.error(f"Error writing partition idempotently {s3_path}: {e}")
        raise


def validate_partition_data(df: pl.DataFrame, config: ETL3Config) -> bool:
    """
    Validate that partition columns have real data, not defaults.
    
    Args:
        df: DataFrame with partition columns
        config: ETL3Config instance with PARTITION_COLUMNS
        
    Returns:
        True if validation passes
        
    Raises:
        ValueError: If partition validation fails
    """
    
    logger.info("Validating partition data quality...")
    
    issues = []
    
    for partition_col in config.PARTITION_COLUMNS:
        if partition_col not in df.columns:
            issues.append(f"Partition column {partition_col} not found in data")
            continue
            
        # Check for excessive defaults (Unknown values)
        unknown_count = df.filter(pl.col(partition_col) == 'Unknown').height
        unknown_ratio = unknown_count / df.height if df.height > 0 else 0
        
        if unknown_ratio > 0.1:  # More than 10% defaults
            issues.append(f"Column {partition_col} has {unknown_ratio:.1%} default values ({unknown_count:,}/{df.height:,} rows)")
        
        # Check for __NULL__ values (our explicit null strategy)
        null_count = df.filter(pl.col(partition_col) == '__NULL__').height
        null_ratio = null_count / df.height if df.height > 0 else 0
        
        # Set different thresholds for different columns
        null_threshold = 0.2  # Default 20% threshold
        if partition_col == 'stat_area_name':
            null_threshold = 0.5  # 50% threshold for geographical data (common to be missing)
        elif partition_col in ['primary_taxonomy_code']:
            null_threshold = 0.3  # 30% threshold for taxonomy data
        
        if null_ratio > null_threshold:
            issues.append(f"Column {partition_col} has {null_ratio:.1%} __NULL__ values ({null_count:,}/{df.height:,} rows, threshold: {null_threshold:.1%})")
        
        # Check for actual null values (should be handled by __NULL__ strategy)
        actual_null_count = df.filter(pl.col(partition_col).is_null()).height
        actual_null_ratio = actual_null_count / df.height if df.height > 0 else 0
        
        if actual_null_ratio > 0.05:  # More than 5% actual nulls
            issues.append(f"Column {partition_col} has {actual_null_ratio:.1%} actual null values ({actual_null_count:,}/{df.height:,} rows)")
        
        # Log validation details for each column
        logger.debug(f"Partition validation for {partition_col}: "
                    f"Unknown={unknown_ratio:.1%}, __NULL__={null_ratio:.1%}, "
                    f"Actual nulls={actual_null_ratio:.1%}")
    
    # Check for partition key uniqueness and distribution
    if len(config.PARTITION_COLUMNS) > 1:
        # Check partition distribution
        partition_combinations = (
            df
            .select(config.PARTITION_COLUMNS)
            .unique()
            .height
        )
        
        # Warn if too few unique partition combinations (but don't fail for small datasets)
        if partition_combinations < 3 and df.height > 10:
            issues.append(f"Only {partition_combinations} unique partition combinations found (may indicate data quality issues)")
        elif partition_combinations < 2:
            issues.append(f"Only {partition_combinations} unique partition combinations found (insufficient for partitioning)")
        
        logger.info(f"Found {partition_combinations} unique partition combinations")
    
    if issues:
        error_message = f"Partition validation failed:\n" + "\n".join(f"  - {issue}" for issue in issues)
        logger.error(error_message)
        raise ValueError(error_message)
    
    logger.info("Partition validation passed successfully")
    return True


def extract_partition_keys(enriched_df: pl.DataFrame) -> pl.DataFrame:
    """
    Extract partition keys from enriched data.
    No defaults - only use actual values or handle nulls explicitly.
    
    Args:
        enriched_df: Input DataFrame (enriched with all dimension data)
        
    Returns:
        DataFrame with correct partition columns added
        
    Partition mapping:
    - payer_slug: from fact table
    - state: from dim_npi_address_geo (state, LOCATION addresses only)
    - billing_class: from fact table
    - procedure_set: from dim_code_cat (proc_set)
    - procedure_class: from dim_code_cat (proc_class)
    - primary_taxonomy_code: from dim_npi
    - stat_area_name: from dim_npi_address_geo (stat_area_name, LOCATION addresses only)
    - year: extracted from year_month
    - month: extracted from year_month
    """
    
    logger.info("Extracting partition keys from enriched data...")
    
    try:
        # Define the partition column mapping
        partition_mapping = {
            'payer_slug': 'payer_slug',  # from fact
            'state': 'state_geo',  # from dim_npi_address_geo (LOCATION addresses only) - gets _geo suffix due to conflict
            'billing_class': 'billing_class',  # from fact
            'procedure_set': 'proc_set',  # from dim_code_cat
            'procedure_class': 'proc_class',  # from dim_code_cat
            'primary_taxonomy_code': 'primary_taxonomy_code',  # from dim_npi
            'stat_area_name': 'stat_area_name',  # from dim_npi_address_geo (LOCATION addresses only) - no suffix needed
            'year': 'year_month',  # will be extracted
            'month': 'year_month'  # will be extracted
        }
        
        # Validate that all source columns exist
        missing_columns = []
        for partition_key, source_column in partition_mapping.items():
            if source_column not in enriched_df.columns:
                missing_columns.append(f"{partition_key} -> {source_column}")
        
        if missing_columns:
            logger.warning(f"Missing source columns for partition keys: {missing_columns}")
            logger.warning("Using __NULL__ for missing columns")
        
        # Start with the enriched dataframe
        result_df = enriched_df
        
        # Extract year and month from year_month column
        if 'year_month' in enriched_df.columns:
            result_df = result_df.with_columns([
                pl.when(pl.col('year_month').is_null())
                .then(pl.lit('__NULL__'))
                .otherwise(pl.col('year_month').str.slice(0, 4))
                .alias('year'),
                pl.when(pl.col('year_month').is_null())
                .then(pl.lit('__NULL__'))
                .otherwise(pl.col('year_month').str.slice(5, 2))
                .alias('month')
            ])
            logger.info("Extracted year and month from year_month")
        else:
            result_df = result_df.with_columns([
                pl.lit('__NULL__').alias('year'),
                pl.lit('__NULL__').alias('month')
            ])
            logger.warning("year_month column not found, using __NULL__")
        
        # Handle payer_slug (already in fact table)
        if 'payer_slug' in enriched_df.columns:
            result_df = result_df.with_columns(
                pl.when(pl.col('payer_slug').is_null())
                .then(pl.lit('__NULL__'))
                .otherwise(pl.col('payer_slug'))
                .alias('payer_slug')
            )
            logger.info("Added payer_slug partition key")
        else:
            result_df = result_df.with_columns(pl.lit('__NULL__').alias('payer_slug'))
            logger.warning("payer_slug not found, using __NULL__")
        
        # Handle state (from dim_npi_address_geo, LOCATION addresses only)
        if 'state_geo' in enriched_df.columns:
            result_df = result_df.with_columns(
                pl.when(pl.col('state_geo').is_null())
                .then(pl.lit('__NULL__'))
                .otherwise(pl.col('state_geo'))
                .alias('state')
            )
            logger.info("Added state partition key from dim_npi_address_geo.state (LOCATION addresses)")
        elif 'state' in enriched_df.columns:
            # Fallback to fact table state if geo state not available
            result_df = result_df.with_columns(
                pl.when(pl.col('state').is_null())
                .then(pl.lit('__NULL__'))
                .otherwise(pl.col('state'))
                .alias('state')
            )
            logger.info("Added state partition key from fact table state (fallback)")
        else:
            result_df = result_df.with_columns(pl.lit('__NULL__').alias('state'))
            logger.warning("No state column found, using __NULL__")
        
        # Handle billing_class (already in fact table)
        if 'billing_class' in enriched_df.columns:
            result_df = result_df.with_columns(
                pl.when(pl.col('billing_class').is_null())
                .then(pl.lit('__NULL__'))
                .otherwise(pl.col('billing_class'))
                .alias('billing_class')
            )
            logger.info("Added billing_class partition key")
        else:
            result_df = result_df.with_columns(pl.lit('__NULL__').alias('billing_class'))
            logger.warning("billing_class not found, using __NULL__")
        
        # Handle procedure_set (from dim_code_cat)
        if 'proc_set' in enriched_df.columns:
            result_df = result_df.with_columns(
                pl.when(pl.col('proc_set').is_null())
                .then(pl.lit('__NULL__'))
                .otherwise(pl.col('proc_set'))
                .alias('procedure_set')
            )
            logger.info("Added procedure_set partition key from dim_code_cat.proc_set")
        else:
            result_df = result_df.with_columns(pl.lit('__NULL__').alias('procedure_set'))
            logger.warning("proc_set not found in dim_code_cat, using __NULL__")
        
        # Handle procedure_class (from dim_code_cat)
        if 'proc_class' in enriched_df.columns:
            result_df = result_df.with_columns(
                pl.when(pl.col('proc_class').is_null())
                .then(pl.lit('__NULL__'))
                .otherwise(pl.col('proc_class'))
                .alias('procedure_class')
            )
            logger.info("Added procedure_class partition key from dim_code_cat.proc_class")
        else:
            result_df = result_df.with_columns(pl.lit('__NULL__').alias('procedure_class'))
            logger.warning("proc_class not found in dim_code_cat, using __NULL__")
        
        # Handle primary_taxonomy_code (from dim_npi)
        if 'primary_taxonomy_code' in enriched_df.columns:
            result_df = result_df.with_columns(
                pl.when(pl.col('primary_taxonomy_code').is_null())
                .then(pl.lit('__NULL__'))
                .otherwise(pl.col('primary_taxonomy_code'))
                .alias('primary_taxonomy_code')
            )
            logger.info("Added primary_taxonomy_code partition key from dim_npi")
        else:
            result_df = result_df.with_columns(pl.lit('__NULL__').alias('primary_taxonomy_code'))
            logger.warning("primary_taxonomy_code not found, using __NULL__")
        
        # Handle stat_area_name (from dim_npi_address_geo, LOCATION addresses only)
        if 'stat_area_name' in enriched_df.columns:
            result_df = result_df.with_columns(
                pl.when(pl.col('stat_area_name').is_null())
                .then(pl.lit('__NULL__'))
                .otherwise(pl.col('stat_area_name'))
                .alias('stat_area_name')
            )
            logger.info("Added stat_area_name partition key from dim_npi_address_geo (LOCATION addresses)")
        else:
            result_df = result_df.with_columns(pl.lit('__NULL__').alias('stat_area_name'))
            logger.warning("stat_area_name not found, using __NULL__")
        
        logger.info("Partition keys extracted successfully")
        return result_df
        
    except Exception as e:
        logger.error(f"Error extracting partition keys: {e}")
        # Return dataframe with __NULL__ partition keys to avoid complete failure
        logger.warning("Falling back to __NULL__ partition keys")
        return enriched_df.with_columns([
            pl.lit("__NULL__").alias('year'),
            pl.lit("__NULL__").alias('month'),
            pl.lit('__NULL__').alias('payer_slug'),
            pl.lit('__NULL__').alias('state'),
            pl.lit('__NULL__').alias('billing_class'),
            pl.lit('__NULL__').alias('procedure_set'),
            pl.lit('__NULL__').alias('procedure_class'),
            pl.lit('__NULL__').alias('primary_taxonomy_code'),
            pl.lit('__NULL__').alias('stat_area_name')
        ])


def _add_partitioning_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add partitioning columns to the DataFrame using actual dimension data.
    
    Args:
        df: Input DataFrame (enriched with all dimension data)
        
    Returns:
        DataFrame with partitioning columns added from actual dimension data
        
    Partitioning columns mapping:
    - payer_slug: already in fact table
    - state: from dim_npi_address (state column)
    - billing_class: already in fact table
    - procedure_set/procedure_class: from dim_code
    - primary_taxonomy_code: from dim_npi
    - stat_area_name: from dim_npi_address_geo (cbsa_name or similar)
    """
    
    logger.info("Adding partitioning columns from actual dimension data...")
    
    try:
        # Add year and month from year_month column (already in fact table)
        if 'year_month' in df.columns:
            df = df.with_columns([
                pl.col('year_month').str.slice(0, 4).alias('year'),
                pl.col('year_month').str.slice(5, 2).alias('month')
            ])
        else:
            logger.warning("year_month column not found, using default values")
            df = df.with_columns([
                pl.lit("2025").alias('year'),
                pl.lit("01").alias('month')
            ])
        
        # payer_slug: already in fact table, no changes needed
        
        # state: use from dim_npi_address (state column)
        if 'state_addr' in df.columns:
            df = df.with_columns(
                pl.col('state_addr').alias('state')
            )
            logger.info("Added state from dim_npi_address")
        elif 'state' in df.columns:
            # Keep existing state column if it exists
            logger.info("Using existing state column from fact table")
        else:
            logger.warning("No state column found, adding Unknown")
            df = df.with_columns(
                pl.lit('Unknown').alias('state')
            )
        
        # billing_class: already in fact table, no changes needed
        
        # procedure_set: create from code_description using actual categorization
        if 'code_description' in df.columns:
            df = df.with_columns(
                pl.when(pl.col('code_description').is_null())
                .then(pl.lit('Unknown'))
                .when(pl.col('code_description').str.to_lowercase().str.contains('evaluation|management|office visit|consultation'))
                .then(pl.lit('Evaluation and Management'))
                .when(pl.col('code_description').str.to_lowercase().str.contains('surgery|surgical|operation|procedure'))
                .then(pl.lit('Surgery'))
                .when(pl.col('code_description').str.to_lowercase().str.contains('imaging|radiology|mri|ct|ultrasound|x-ray'))
                .then(pl.lit('Radiology'))
                .when(pl.col('code_description').str.to_lowercase().str.contains('laboratory|lab|blood|urine|test'))
                .then(pl.lit('Laboratory'))
                .when(pl.col('code_description').str.to_lowercase().str.contains('therapy|rehabilitation|physical|occupational'))
                .then(pl.lit('Therapy'))
                .when(pl.col('code_description').str.to_lowercase().str.contains('emergency|urgent|trauma'))
                .then(pl.lit('Emergency'))
                .when(pl.col('code_description').str.to_lowercase().str.contains('anesthesia|anesthetic'))
                .then(pl.lit('Anesthesia'))
                .otherwise(pl.lit('Other'))
                .alias('procedure_set')
            )
            logger.info("Added procedure_set from code_description categorization")
        else:
            logger.warning("code_description not found, adding Unknown for procedure_set")
            df = df.with_columns(
                pl.lit('Unknown').alias('procedure_set')
            )
        
        # procedure_class: use code_type from dim_code (already joined)
        if 'code_type' in df.columns:
            df = df.with_columns(
                pl.when(pl.col('code_type').is_null())
                .then(pl.lit('Unknown'))
                .otherwise(pl.col('code_type'))
                .alias('procedure_class')
            )
            logger.info("Added procedure_class from code_type")
        else:
            logger.warning("code_type not found, adding Unknown for procedure_class")
            df = df.with_columns(
                pl.lit('Unknown').alias('procedure_class')
            )
        
        # primary_taxonomy_code: use from dim_npi (already joined)
        if 'primary_taxonomy_code' in df.columns:
            df = df.with_columns(
                pl.when(pl.col('primary_taxonomy_code').is_null())
                .then(pl.lit('Unknown'))
                .otherwise(pl.col('primary_taxonomy_code'))
                .alias('primary_taxonomy_code')
            )
            logger.info("Added primary_taxonomy_code from dim_npi")
        else:
            logger.warning("primary_taxonomy_code not found, adding Unknown")
            df = df.with_columns(
                pl.lit('Unknown').alias('primary_taxonomy_code')
            )
        
        # stat_area_name: use from dim_npi_address_geo (already joined)
        if 'stat_area_name_geo' in df.columns:
            df = df.with_columns(
                pl.when(pl.col('stat_area_name_geo').is_null())
                .then(pl.lit('Unknown'))
                .otherwise(pl.col('stat_area_name_geo'))
                .alias('stat_area_name')
            )
            logger.info("Added stat_area_name from dim_npi_address_geo")
        elif 'stat_area_name' in df.columns:
            # Use existing stat_area_name if it exists
            df = df.with_columns(
                pl.when(pl.col('stat_area_name').is_null())
                .then(pl.lit('Unknown'))
                .otherwise(pl.col('stat_area_name'))
                .alias('stat_area_name')
            )
            logger.info("Using existing stat_area_name column")
        else:
            logger.warning("stat_area_name not found, adding Unknown")
            df = df.with_columns(
                pl.lit('Unknown').alias('stat_area_name')
            )
        
        logger.info("Partitioning columns added successfully from actual dimension data")
        return df
        
    except Exception as e:
        logger.error(f"Error adding partitioning columns: {e}")
        # Return minimal partitioning columns to avoid complete failure
        logger.warning("Falling back to default partitioning columns")
        return df.with_columns([
            pl.lit("2025").alias('year'),
            pl.lit("01").alias('month'),
            pl.lit('Unknown').alias('state'),
            pl.lit('Unknown').alias('procedure_set'),
            pl.lit('Unknown').alias('procedure_class'),
            pl.lit('Unknown').alias('primary_taxonomy_code'),
            pl.lit('Unknown').alias('stat_area_name')
        ])


if __name__ == "__main__":
    # Run pipeline if called directly
    config = ETL3Config()
    summary = run_etl3_pipeline(config)
    print(f"Pipeline completed: {summary}")
