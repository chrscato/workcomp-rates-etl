#!/usr/bin/env python3
"""
Test script for memory-efficient ETL3 pipeline.
This script demonstrates the streaming approach with smaller chunks.
"""

import os
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "ETL" / "utils"))

from ETL.ETL_3 import run_etl3_pipeline, ETL3Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def test_memory_efficient_etl():
    """Test the memory-efficient ETL3 pipeline with small chunks."""
    
    logger.info("Testing Memory-Efficient ETL3 Pipeline")
    
    # Create configuration with small chunks for testing
    config = ETL3Config()
    config.CHUNK_SIZE = 5000  # Very small chunks for testing
    config.MAX_WORKERS = 1    # Single worker to avoid complexity
    
    logger.info(f"Configuration:")
    logger.info(f"  Chunk size: {config.CHUNK_SIZE:,}")
    logger.info(f"  Max workers: {config.MAX_WORKERS}")
    logger.info(f"  Memory limit: {config.MEMORY_LIMIT_MB} MB")
    
    try:
        # Run the pipeline
        summary = run_etl3_pipeline(config)
        
        # Display results
        print("\n" + "="*60)
        print("MEMORY-EFFICIENT ETL3 TEST RESULTS")
        print("="*60)
        print(f"Status: {summary['status']}")
        print(f"Total rows processed: {summary['total_input_rows']:,}")
        print(f"Total chunks processed: {summary['total_chunks_processed']}")
        print(f"Total partitions created: {summary['total_partitions_created']:,}")
        print(f"Total processing time: {summary['total_processing_time']:.2f} seconds")
        print(f"Processing rate: {summary['rows_per_second']:.0f} rows/second")
        print(f"Memory efficiency: {summary['total_input_rows'] / summary['total_processing_time'] / 1024:.1f} KB/s")
        print("="*60)
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return False


if __name__ == "__main__":
    success = test_memory_efficient_etl()
    sys.exit(0 if success else 1)
