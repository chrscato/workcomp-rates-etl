#!/usr/bin/env python3
"""
Runner script for the scalable ETL1 pipeline.
This script provides a convenient interface for running the pipeline with different configurations.
"""

import os
import sys
import argparse
import json
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from ETL.etl1_scalable import ScalableETL1, ETL1Config


def main():
    """Main entry point for the runner script"""
    parser = argparse.ArgumentParser(
        description="Run the scalable ETL1 pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default settings
  python ETL/scripts/run_etl1_scalable.py --payer aetna
  
  # Run with custom chunk size and memory limit
  python ETL/scripts/run_etl1_scalable.py --payer aetna --chunk-size 100000 --memory-limit 16384
  
  # Run with configuration file
  python ETL/scripts/run_etl1_scalable.py --payer aetna --config ETL/config/etl1_config.yaml
  
  # Run for different state
  python ETL/scripts/run_etl1_scalable.py --payer uhc --state VA --payer-slug unitedhealthcare-virginia
        """
    )
    
    # Required arguments
    parser.add_argument("--payer", required=True, help="Payer name (e.g., aetna, uhc)")
    
    # Optional arguments
    parser.add_argument("--state", default="GA", help="State code (default: GA)")
    parser.add_argument("--payer-slug", help="Override payer slug generation")
    parser.add_argument("--chunk-size", type=int, help="Chunk size for processing")
    parser.add_argument("--memory-limit", type=int, help="Memory limit in MB")
    parser.add_argument("--config", help="Path to YAML config file")
    parser.add_argument("--output-format", choices=["json", "summary"], default="summary", 
                       help="Output format (default: summary)")
    parser.add_argument("--dry-run", action="store_true", help="Show configuration without running")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Load configuration
    if args.config:
        config = ETL1Config.from_yaml(Path(args.config))
    else:
        config = ETL1Config()
    
    # Override with command line arguments
    if args.state:
        config.state = args.state
    if args.payer_slug:
        config.payer_slug_override = args.payer_slug
    if args.chunk_size:
        config.chunk_size = args.chunk_size
    if args.memory_limit:
        config.memory_limit_mb = args.memory_limit
    if args.verbose:
        config.log_level = "DEBUG"
    
    # Show configuration
    print("🔧 ETL1 Scalable Pipeline Configuration")
    print("=" * 50)
    print(f"Payer: {args.payer}")
    print(f"State: {config.state}")
    print(f"Payer Slug Override: {config.payer_slug_override or 'Auto-generated'}")
    print(f"Chunk Size: {config.chunk_size:,} rows")
    print(f"Memory Limit: {config.memory_limit_mb:,} MB")
    print(f"Data Root: {config.data_root}")
    print(f"Log Level: {config.log_level}")
    
    # Check input files
    rates_file, providers_file = config.get_input_files(args.payer)
    print(f"\n📁 Input Files:")
    print(f"Rates: {rates_file} {'✅' if rates_file.exists() else '❌'}")
    print(f"Providers: {providers_file} {'✅' if providers_file.exists() else '❌'}")
    
    if not rates_file.exists() or not providers_file.exists():
        print("\n❌ Input files not found. Please check file paths and payer name.")
        return 1
    
    if args.dry_run:
        print("\n🔍 Dry run complete. Use --dry-run=false to execute the pipeline.")
        return 0
    
    # Run pipeline
    print(f"\n🚀 Starting ETL1 pipeline for {args.payer}...")
    
    try:
        with ScalableETL1(config) as etl:
            summary = etl.run_pipeline(args.payer)
        
        # Output results
        if args.output_format == "json":
            print(json.dumps(summary, indent=2))
        else:
            print("\n📊 Pipeline Summary")
            print("=" * 30)
            print(f"Duration: {summary['duration_seconds']:.2f} seconds")
            print(f"Rates Processed: {summary['processed_rates']:,}")
            print(f"Providers Processed: {summary['processed_providers']:,}")
            print(f"Chunk Size: {summary['chunk_size']:,}")
            print(f"\n📁 Output Files:")
            for name, path in summary['output_files'].items():
                print(f"  {name}: {path}")
        
        print("\n✅ Pipeline completed successfully!")
        return 0
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
