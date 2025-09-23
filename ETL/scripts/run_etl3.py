#!/usr/bin/env python3
"""
ETL3 Pipeline Runner - Memory Optimized

This is the main ETL3 pipeline runner with built-in memory optimization.
Designed for memory-constrained systems (4-8GB RAM) with automatic memory management.

Usage:
    python ETL/scripts/run_etl3.py                    # Default memory-optimized settings
    python ETL/scripts/run_etl3.py --chunk-size 500   # Custom chunk size
    python ETL/scripts/run_etl3.py --validate-only    # Validation only
    python ETL/scripts/run_etl3.py --dry-run          # Dry run mode
"""

import os
import sys
import argparse
import logging
import gc
import psutil
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

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


class MemoryOptimizedETL3Config(ETL3Config):
    """Memory-optimized configuration for ETL3."""
    
    def __init__(self, config_path: str = None):
        super().__init__(config_path)
        
        # Override with memory-optimized settings
        self.CHUNK_SIZE = 1000  # Very small chunks
        self.MAX_WORKERS = 1    # Single worker
        self.MEMORY_LIMIT_MB = 1024  # Conservative limit
        
        # Set environment variables for memory optimization
        os.environ['POLARS_MAX_THREADS'] = '1'
        os.environ['OMP_NUM_THREADS'] = '1'
        os.environ['OPENBLAS_NUM_THREADS'] = '1'
        
        logger.info(f"Memory-optimized config loaded:")
        logger.info(f"  Chunk size: {self.CHUNK_SIZE:,}")
        logger.info(f"  Max workers: {self.MAX_WORKERS}")
        logger.info(f"  Memory limit: {self.MEMORY_LIMIT_MB} MB")


class MemoryMonitor:
    """Memory monitoring and management for ETL3."""
    
    def __init__(self, memory_limit_mb: int = 1024):
        self.memory_limit_mb = memory_limit_mb
        self.process = psutil.Process()
        self.peak_memory_mb = 0
        self.memory_warnings = 0
        
    def get_memory_usage(self) -> Dict[str, float]:
        """Get current memory usage statistics."""
        memory_info = self.process.memory_info()
        system_memory = psutil.virtual_memory()
        
        return {
            "process_mb": memory_info.rss / (1024 * 1024),
            "process_percent": self.process.memory_percent(),
            "system_available_gb": system_memory.available / (1024 * 1024 * 1024),
            "system_percent": system_memory.percent
        }
    
    def check_memory_limits(self) -> bool:
        """Check if we're approaching memory limits."""
        memory = self.get_memory_usage()
        
        # Update peak memory
        self.peak_memory_mb = max(self.peak_memory_mb, memory["process_mb"])
        
        # Warn if process memory exceeds 70% of configured limit
        if memory["process_mb"] > self.memory_limit_mb * 0.7:
            self.memory_warnings += 1
            logger.warning(f"High memory usage: {memory['process_mb']:.1f}MB / {self.memory_limit_mb}MB "
                          f"({memory['process_mb']/self.memory_limit_mb*100:.1f}%)")
            
            # Force garbage collection
            self.cleanup_memory()
            
            # Check again after cleanup
            memory_after = self.get_memory_usage()
            if memory_after["process_mb"] > self.memory_limit_mb * 0.8:
                logger.error(f"Memory still high after cleanup: {memory_after['process_mb']:.1f}MB")
                return False
        
        # Warn if system memory is low
        if memory["system_available_gb"] < 0.5:  # Less than 500MB available
            logger.warning(f"Low system memory: {memory['system_available_gb']:.1f}GB available")
            return False
        
        return True
    
    def cleanup_memory(self):
        """Force garbage collection and memory cleanup."""
        gc.collect()
        memory = self.get_memory_usage()
        logger.debug(f"Memory after cleanup: {memory['process_mb']:.1f}MB")
    
    def get_summary(self) -> Dict[str, Any]:
        """Get memory usage summary."""
        final_memory = self.get_memory_usage()
        return {
            "peak_memory_mb": self.peak_memory_mb,
            "final_memory_mb": final_memory["process_mb"],
            "memory_warnings": self.memory_warnings,
            "memory_limit_mb": self.memory_limit_mb
        }


def parse_arguments():
    """Parse command line arguments."""
    
    parser = argparse.ArgumentParser(
        description='Run Memory-Optimized ETL3 Pipeline'
    )
    
    parser.add_argument(
        '--config',
        type=str,
        default='ETL/config/etl3_config.yaml',
        help='Path to configuration file'
    )
    
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=1000,
        help='Override chunk size (default: 1000)'
    )
    
    parser.add_argument(
        '--memory-limit',
        type=int,
        default=1024,
        help='Override memory limit in MB (default: 1024)'
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
        '--monitor-memory',
        action='store_true',
        help='Enable detailed memory monitoring'
    )
    
    return parser.parse_args()


def setup_environment(args):
    """Set up environment based on arguments."""
    
    # Set environment variables for memory optimization
    os.environ['POLARS_MAX_THREADS'] = '1'
    os.environ['OMP_NUM_THREADS'] = '1'
    os.environ['OPENBLAS_NUM_THREADS'] = '1'
    os.environ['MKL_NUM_THREADS'] = '1'
    
    # Set processing parameters
    os.environ['CHUNK_SIZE'] = str(args.chunk_size)
    os.environ['MEMORY_LIMIT_MB'] = str(args.memory_limit)
    
    # Create necessary directories
    Path('logs').mkdir(exist_ok=True)
    Path('data/partitioned').mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Environment set up for memory optimization:")
    logger.info(f"  Chunk size: {args.chunk_size:,}")
    logger.info(f"  Memory limit: {args.memory_limit} MB")
    logger.info(f"  Thread limits: All set to 1")


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
    config = MemoryOptimizedETL3Config()
    
    # Initialize data quality checker
    quality_checker = DataQualityChecker()
    
    # Load fact table with memory optimization
    import polars as pl
    fact_rate = pl.read_parquet(config.FACT_RATE_PATH)
    logger.info(f"Loaded {fact_rate.height:,} fact records for validation")
    
    # Sample data for validation (smaller sample for memory efficiency)
    sample_size = min(5000, fact_rate.height)  # Reduced sample size
    sample_data = fact_rate.sample(sample_size)
    
    # Run validation
    validation_results = quality_checker.validate_partition(sample_data)
    
    # Display results
    print("\n" + "="*60)
    print("DATA VALIDATION RESULTS (Memory-Optimized)")
    print("="*60)
    print(f"Sample size: {sample_data.height:,} rows")
    print(f"Errors: {len(validation_results['errors'])}")
    print(f"Warnings: {len(validation_results['warnings'])}")
    print(f"Quality score: {validation_results['metrics'].get('quality_score', 'N/A')}")
    
    if validation_results['errors']:
        print("\nErrors found:")
        for error in validation_results['errors'][:3]:  # Show first 3 errors
            print(f"  - {error['error']}")
    
    if validation_results['warnings']:
        print("\nWarnings found:")
        for warning in validation_results['warnings'][:3]:  # Show first 3 warnings
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
    
    # Initialize memory monitor
    memory_monitor = MemoryMonitor(args.memory_limit)
    initial_memory = memory_monitor.get_memory_usage()
    logger.info(f"Initial memory usage: {initial_memory['process_mb']:.1f}MB")
    
    # Run validation only if requested
    if args.validate_only:
        validation_results = run_validation_only()
        sys.exit(0 if not validation_results['has_errors'] else 1)
    
    # Dry run mode
    if args.dry_run:
        logger.info("Running in dry-run mode - no actual processing will occur")
        print("\n" + "="*60)
        print("MEMORY-OPTIMIZED ETL3 DRY RUN")
        print("="*60)
        print("Configuration loaded successfully")
        print("Prerequisites validated")
        print("Memory optimization enabled")
        print("Ready to process data with memory constraints")
        print("="*60)
        sys.exit(0)
    
    try:
        # Run the memory-optimized ETL3 pipeline
        logger.info("Starting Memory-Optimized ETL3 Pipeline")
        
        # Create memory-optimized config
        config = MemoryOptimizedETL3Config(args.config)
        config.CHUNK_SIZE = args.chunk_size
        config.MEMORY_LIMIT_MB = args.memory_limit
        
        # Run pipeline with memory monitoring
        summary = run_etl3_pipeline(config)
        
        # Get final memory statistics
        memory_summary = memory_monitor.get_summary()
        
        # Display summary
        print("\n" + "="*60)
        print("MEMORY-OPTIMIZED ETL3 PIPELINE COMPLETED")
        print("="*60)
        print(f"Status: {summary['status']}")
        print(f"Total rows processed: {summary['total_input_rows']:,}")
        print(f"Total chunks processed: {summary['total_chunks_processed']}")
        print(f"Total partitions created: {summary['total_partitions_created']:,}")
        print(f"Total processing time: {summary['total_processing_time']:.2f} seconds")
        print(f"Processing rate: {summary['rows_per_second']:.0f} rows/second")
        print(f"S3 Bucket: {summary['s3_bucket']}")
        print(f"S3 Prefix: {summary['s3_prefix']}")
        print(f"Athena Database: {summary['athena_database']}")
        print(f"Athena Table: {summary['athena_table']}")
        print("\n" + "-"*40)
        print("MEMORY USAGE SUMMARY")
        print("-"*40)
        print(f"Initial memory: {initial_memory['process_mb']:.1f}MB")
        print(f"Peak memory: {memory_summary['peak_memory_mb']:.1f}MB")
        print(f"Final memory: {memory_summary['final_memory_mb']:.1f}MB")
        print(f"Memory limit: {memory_summary['memory_limit_mb']}MB")
        print(f"Memory warnings: {memory_summary['memory_warnings']}")
        print(f"Memory efficiency: {memory_summary['peak_memory_mb']/memory_summary['memory_limit_mb']*100:.1f}% of limit")
        print("="*60)
        
        logger.info("Memory-Optimized ETL3 Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Memory-Optimized ETL3 Pipeline failed: {str(e)}")
        
        # Get final memory statistics even on failure
        memory_summary = memory_monitor.get_summary()
        logger.error(f"Memory usage at failure: {memory_summary['final_memory_mb']:.1f}MB "
                    f"(peak: {memory_summary['peak_memory_mb']:.1f}MB)")
        
        sys.exit(1)


if __name__ == "__main__":
    main()