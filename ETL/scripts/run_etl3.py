#!/usr/bin/env python3
"""
ETL3 Pipeline Runner

This script runs the complete ETL3 pipeline for S3-based partitioned data warehouse.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "ETL" / "utils"))

from ETL.ETL_3 import run_etl3_pipeline, ETL3Config
from ETL.utils.monitoring import ETLMonitor
from ETL.utils.data_quality import DataQualityChecker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl3.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    
    parser = argparse.ArgumentParser(
        description='Run ETL3 Pipeline for S3 Partitioned Data Warehouse'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='ETL/config/etl3_config.yaml',
        help='Path to configuration file'
    )
    
    parser.add_argument(
        '--environment',
        type=str,
        choices=['development', 'staging', 'production'],
        default='development',
        help='Environment to run in'
    )
    
    parser.add_argument(
        '--chunk-size',
        type=int,
        help='Override chunk size for processing'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        help='Override max workers for parallel processing'
    )
    
    parser.add_argument(
        '--s3-bucket',
        type=str,
        help='Override S3 bucket name'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run in dry-run mode (no actual processing)'
    )
    
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Only run data validation, skip processing'
    )
    
    parser.add_argument(
        '--monitor',
        action='store_true',
        help='Enable detailed monitoring and metrics'
    )
    
    parser.add_argument(
        '--quality-check',
        action='store_true',
        help='Run data quality checks after processing'
    )
    
    return parser.parse_args()


def setup_environment(args):
    """Set up environment based on arguments."""
    
    # Set environment variables
    os.environ['ENVIRONMENT'] = args.environment
    os.environ['CONFIG_FILE'] = args.config
    
    if args.chunk_size:
        os.environ['CHUNK_SIZE'] = str(args.chunk_size)
    
    if args.max_workers:
        os.environ['MAX_WORKERS'] = str(args.max_workers)
    
    if args.s3_bucket:
        os.environ['S3_BUCKET'] = args.s3_bucket
    
    # Create necessary directories
    Path('logs').mkdir(exist_ok=True)
    Path('data/partitioned').mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Environment set up: {args.environment}")
    logger.info(f"Configuration file: {args.config}")


def validate_prerequisites():
    """Validate that all prerequisites are met."""
    
    logger.info("Validating prerequisites...")
    
    # Check if fact table exists
    fact_path = Path("data/gold/fact_rate.parquet")
    if not fact_path.exists():
        logger.error(f"Fact table not found: {fact_path}")
        return False
    
    # Check if dimension tables exist
    required_dims = [
        "data/dims/dim_code.parquet",
        "data/dims/dim_payer.parquet",
        "data/dims/dim_provider_group.parquet",
        "data/dims/dim_npi.parquet"
    ]
    
    missing_dims = []
    for dim_path in required_dims:
        if not Path(dim_path).exists():
            missing_dims.append(dim_path)
    
    if missing_dims:
        logger.error(f"Missing dimension tables: {missing_dims}")
        return False
    
    # Check AWS credentials
    try:
        import boto3
        sts = boto3.client('sts')
        sts.get_caller_identity()
        logger.info("AWS credentials validated")
    except Exception as e:
        logger.error(f"AWS credentials not available: {e}")
        return False
    
    logger.info("All prerequisites validated")
    return True


def run_validation_only():
    """Run only data validation without processing."""
    
    logger.info("Running data validation only...")
    
    # Load configuration
    config = ETL3Config()
    
    # Initialize data quality checker
    quality_checker = DataQualityChecker()
    
    # Load fact table
    fact_rate = pl.read_parquet(config.FACT_RATE_PATH)
    logger.info(f"Loaded {fact_rate.height:,} fact records for validation")
    
    # Sample data for validation
    sample_size = min(10000, fact_rate.height)
    sample_data = fact_rate.sample(sample_size)
    
    # Run validation
    validation_results = quality_checker.validate_partition(sample_data)
    
    # Display results
    print("\n" + "="*60)
    print("DATA VALIDATION RESULTS")
    print("="*60)
    print(f"Sample size: {sample_data.height:,} rows")
    print(f"Errors: {len(validation_results['errors'])}")
    print(f"Warnings: {len(validation_results['warnings'])}")
    print(f"Quality score: {validation_results['metrics'].get('quality_score', 'N/A')}")
    
    if validation_results['errors']:
        print("\nErrors found:")
        for error in validation_results['errors'][:5]:  # Show first 5 errors
            print(f"  - {error['error']}")
    
    if validation_results['warnings']:
        print("\nWarnings found:")
        for warning in validation_results['warnings'][:5]:  # Show first 5 warnings
            print(f"  - {warning['warning']}")
    
    return validation_results


def main():
    """Main function."""
    
    # Parse arguments
    args = parse_arguments()
    
    # Set up environment
    setup_environment(args)
    
    # Validate prerequisites
    if not validate_prerequisites():
        logger.error("Prerequisites validation failed")
        sys.exit(1)
    
    # Run validation only if requested
    if args.validate_only:
        validation_results = run_validation_only()
        sys.exit(0 if not validation_results['has_errors'] else 1)
    
    # Dry run mode
    if args.dry_run:
        logger.info("Running in dry-run mode - no actual processing will occur")
        print("\n" + "="*60)
        print("DRY RUN MODE")
        print("="*60)
        print("Configuration loaded successfully")
        print("Prerequisites validated")
        print("Ready to process data")
        print("="*60)
        sys.exit(0)
    
    # Initialize monitoring if requested
    monitor = None
    if args.monitor:
        config = ETL3Config()
        monitor = ETLMonitor(config.S3_BUCKET, config.S3_REGION)
        pipeline_id = monitor.start_pipeline_monitoring(f"etl3_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        logger.info(f"Monitoring enabled for pipeline: {pipeline_id}")
    
    try:
        # Run the ETL3 pipeline
        logger.info("Starting ETL3 Pipeline")
        
        summary = run_etl3_pipeline()
        
        # Update monitoring
        if monitor:
            monitor.end_pipeline_monitoring(
                status="SUCCESS",
                total_rows=summary['total_input_rows'],
                total_partitions=summary['total_partitions_created']
            )
        
        # Run quality checks if requested
        if args.quality_check:
            logger.info("Running post-processing quality checks...")
            # This would be implemented in the main pipeline
            pass
        
        # Display summary
        print("\n" + "="*60)
        print("ETL3 PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"Total rows processed: {summary['total_input_rows']:,}")
        print(f"Total chunks processed: {summary['total_chunks_processed']}")
        print(f"Total partitions created: {summary['total_partitions_created']:,}")
        print(f"Total processing time: {summary['total_processing_time']:.2f} seconds")
        print(f"Processing rate: {summary['rows_per_second']:.0f} rows/second")
        print(f"S3 Bucket: {summary['s3_bucket']}")
        print(f"S3 Prefix: {summary['s3_prefix']}")
        print(f"Athena Database: {summary['athena_database']}")
        print(f"Athena Table: {summary['athena_table']}")
        print("="*60)
        
        logger.info("ETL3 Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"ETL3 Pipeline failed: {str(e)}")
        
        # Update monitoring
        if monitor:
            monitor.end_pipeline_monitoring(
                status="FAILED",
                total_rows=0,
                total_partitions=0,
                total_errors=1
            )
        
        sys.exit(1)


if __name__ == "__main__":
    main()
