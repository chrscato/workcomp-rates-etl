#!/usr/bin/env python3
"""
Unified ETL Pipeline Runner - Memory Optimized

This script orchestrates the complete ETL pipeline with intelligent memory management:
1. ETL1: Process raw rates/providers into dimensions and fact tables
2. NPI Fetch: Enrich provider data with NPPES information  
3. Geocoding: Add geographic coordinates and census data
4. ETL3: Partition and upload to S3 for analytics

Usage:
    python ETL/scripts/run_unified_etl_pipeline.py --payer aetna --all-stages
    python ETL/scripts/run_unified_etl_pipeline.py --payer uhc --stages etl1,npi,geo
    python ETL/scripts/run_unified_etl_pipeline.py --resume-from geocoding
"""

import os
import sys
import gc
import time
import logging
import argparse
import subprocess
import psutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

# Add project paths
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
sys.path.append(str(project_root / "ETL" / "utils"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/unified_etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for the unified ETL pipeline."""
    
    # Input parameters
    payer: str
    state: str = "GA"
    
    # Memory management
    memory_limit_mb: int = 1024
    chunk_size: int = 1000
    
    # Stage control
    stages: List[str] = None  # ["etl1", "npi", "geocoding", "etl3"]
    resume_from: Optional[str] = None
    skip_stages: List[str] = None
    
    # Processing options
    force_cleanup: bool = False
    dry_run: bool = False
    threads_npi: int = 3
    npi_sleep: float = 0.1
    
    # File paths
    project_root: Path = project_root
    data_dir: Path = project_root / "data"
    logs_dir: Path = project_root / "logs"
    
    def __post_init__(self):
        """Initialize default values and create directories."""
        if self.stages is None:
            self.stages = ["etl1", "npi", "geocoding", "etl3"]
        
        if self.skip_stages is None:
            self.skip_stages = []
        
        # Create necessary directories
        self.logs_dir.mkdir(exist_ok=True)
        (self.data_dir / "dims").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "gold").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "partitioned").mkdir(parents=True, exist_ok=True)


class MemoryManager:
    """Comprehensive memory management for the ETL pipeline."""
    
    def __init__(self, memory_limit_mb: int = 1024):
        self.memory_limit_mb = memory_limit_mb
        self.process = psutil.Process()
        self.peak_memory_mb = 0
        self.memory_warnings = 0
        self.stage_memory_history = {}
        
    def setup_memory_optimization(self):
        """Set up environment for memory optimization."""
        env_vars = {
            'POLARS_MAX_THREADS': '1',
            'OMP_NUM_THREADS': '1', 
            'OPENBLAS_NUM_THREADS': '1',
            'MKL_NUM_THREADS': '1',
            'NUMBA_NUM_THREADS': '1'
        }
        
        for var, value in env_vars.items():
            os.environ[var] = value
            
        logger.info("Memory optimization environment configured")
        
    def get_memory_usage(self) -> Dict[str, float]:
        """Get comprehensive memory statistics."""
        memory_info = self.process.memory_info()
        system_memory = psutil.virtual_memory()
        
        return {
            "process_mb": memory_info.rss / (1024 * 1024),
            "process_percent": self.process.memory_percent(),
            "system_available_gb": system_memory.available / (1024 * 1024 * 1024),
            "system_percent": system_memory.percent,
            "system_total_gb": system_memory.total / (1024 * 1024 * 1024)
        }
    
    def check_memory_health(self, stage: str) -> bool:
        """Check memory health and trigger cleanup if needed."""
        memory = self.get_memory_usage()
        
        # Update peak memory
        self.peak_memory_mb = max(self.peak_memory_mb, memory["process_mb"])
        
        # Store stage-specific memory usage
        if stage not in self.stage_memory_history:
            self.stage_memory_history[stage] = []
        self.stage_memory_history[stage].append(memory["process_mb"])
        
        # Check for memory pressure
        if memory["process_mb"] > self.memory_limit_mb * 0.7:
            self.memory_warnings += 1
            logger.warning(f"High memory usage in {stage}: {memory['process_mb']:.1f}MB / {self.memory_limit_mb}MB")
            
            # Force cleanup
            self.force_cleanup()
            
            # Check again after cleanup
            memory_after = self.get_memory_usage()
            if memory_after["process_mb"] > self.memory_limit_mb * 0.8:
                logger.error(f"Memory still high after cleanup: {memory_after['process_mb']:.1f}MB")
                return False
        
        # Check system memory
        if memory["system_available_gb"] < 0.5:
            logger.warning(f"Low system memory: {memory['system_available_gb']:.1f}GB available")
            return False
        
        return True
    
    def force_cleanup(self):
        """Force garbage collection and memory cleanup."""
        logger.debug("Forcing memory cleanup...")
        gc.collect()
        # Additional cleanup for specific libraries
        try:
            import polars as pl
            # Polars doesn't have explicit cleanup, but this ensures imports are fresh
        except ImportError:
            pass
    
    def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive memory usage summary."""
        final_memory = self.get_memory_usage()
        
        summary = {
            "peak_memory_mb": self.peak_memory_mb,
            "final_memory_mb": final_memory["process_mb"],
            "memory_warnings": self.memory_warnings,
            "memory_limit_mb": self.memory_limit_mb,
            "memory_efficiency": self.peak_memory_mb / self.memory_limit_mb * 100,
            "stage_memory_history": self.stage_memory_history
        }
        
        return summary


class PipelineStage:
    """Base class for pipeline stages."""
    
    def __init__(self, name: str, config: PipelineConfig, memory_manager: MemoryManager):
        self.name = name
        self.config = config
        self.memory_manager = memory_manager
        self.start_time = None
        self.end_time = None
        self.success = False
        self.error_message = None
        
    def should_run(self) -> bool:
        """Check if this stage should run."""
        if self.name in self.config.skip_stages:
            return False
        
        if self.config.resume_from:
            # Find the position of resume_from stage
            try:
                resume_index = self.config.stages.index(self.config.resume_from)
                current_index = self.config.stages.index(self.name)
                return current_index >= resume_index
            except ValueError:
                return True
        
        return self.name in self.config.stages
    
    def run(self) -> bool:
        """Run the stage with memory monitoring."""
        if not self.should_run():
            logger.info(f"Skipping stage: {self.name}")
            return True
        
        logger.info(f"Starting stage: {self.name}")
        self.start_time = datetime.now()
        
        # Check memory before starting
        if not self.memory_manager.check_memory_health(self.name):
            logger.error(f"Memory check failed before {self.name}")
            return False
        
        try:
            if self.config.dry_run:
                logger.info(f"DRY RUN: Would execute {self.name}")
                self.success = True
            else:
                self.success = self.execute()
            
            if self.success:
                logger.info(f"Stage {self.name} completed successfully")
            else:
                logger.error(f"Stage {self.name} failed")
                
        except Exception as e:
            self.error_message = str(e)
            logger.error(f"Stage {self.name} failed with exception: {e}")
            self.success = False
        
        finally:
            self.end_time = datetime.now()
            
            # Force cleanup after each stage
            self.memory_manager.force_cleanup()
            
            # Check memory after stage
            self.memory_manager.check_memory_health(f"{self.name}_post")
        
        return self.success
    
    def execute(self) -> bool:
        """Execute the actual stage logic - to be implemented by subclasses."""
        raise NotImplementedError
    
    def get_duration(self) -> float:
        """Get stage duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0


class ETL1Stage(PipelineStage):
    """ETL1 stage - Process raw data into dimensions and fact tables."""
    
    def execute(self) -> bool:
        cmd = [
            "python", "ETL/scripts/01_run_etl1_main.py",
            "--payer", self.config.payer,
            "--state", self.config.state,
            "--chunk-size", str(self.config.chunk_size),
            "--memory-limit", str(self.config.memory_limit_mb)
        ]
        
        if self.config.force_cleanup:
            cmd.append("--force-cleanup")
        
        logger.info(f"Running ETL1: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.info("ETL1 completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"ETL1 failed with exit code {e.returncode}")
            logger.error(f"ETL1 stderr: {e.stderr}")
            return False


class NPIFetchStage(PipelineStage):
    """NPI fetch stage - Enrich provider data with NPPES information."""
    
    def execute(self) -> bool:
        cmd = [
            "python", "ETL/utils/fetch_npi_data.py",
            "--threads", str(self.config.threads_npi),
            "--sleep", str(self.config.npi_sleep),
            "--batch-size", str(self.config.chunk_size // 2),  # Smaller batches
            "--yes"  # Skip confirmation
        ]
        
        logger.info(f"Running NPI fetch: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.info("NPI fetch completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"NPI fetch failed with exit code {e.returncode}")
            logger.error(f"NPI fetch stderr: {e.stderr}")
            return False


class GeocodingStage(PipelineStage):
    """Geocoding stage - Add geographic coordinates and census data."""
    
    def execute(self) -> bool:
        cmd = ["python", "ETL/utils/geo.py"]
        
        logger.info(f"Running geocoding: {' '.join(cmd)}")
        
        try:
            # Execute the geocoding script directly
            import subprocess
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.info("Geocoding completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Geocoding failed with exit code {e.returncode}")
            logger.error(f"Geocoding stderr: {e.stderr}")
            return False


class ETL3Stage(PipelineStage):
    """ETL3 stage - Partition and upload to S3 for analytics."""
    
    def execute(self) -> bool:
        cmd = [
            "python", "ETL/scripts/run_etl3.py",
            "--chunk-size", str(self.config.chunk_size),
            "--memory-limit", str(self.config.memory_limit_mb)
        ]
        
        logger.info(f"Running ETL3: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.info("ETL3 completed successfully")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"ETL3 failed with exit code {e.returncode}")
            logger.error(f"ETL3 stderr: {e.stderr}")
            return False


class UnifiedETLPipeline:
    """Main pipeline orchestrator."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.memory_manager = MemoryManager(config.memory_limit_mb)
        self.stages = []
        self.pipeline_start_time = None
        self.pipeline_end_time = None
        
        # Initialize stages
        self._initialize_stages()
        
    def _initialize_stages(self):
        """Initialize all pipeline stages."""
        stage_classes = {
            "etl1": ETL1Stage,
            "npi": NPIFetchStage, 
            "geocoding": GeocodingStage,
            "etl3": ETL3Stage
        }
        
        for stage_name in self.config.stages:
            if stage_name in stage_classes:
                stage = stage_classes[stage_name](stage_name, self.config, self.memory_manager)
                self.stages.append(stage)
            else:
                logger.warning(f"Unknown stage: {stage_name}")
    
    def validate_prerequisites(self) -> bool:
        """Validate prerequisites for the pipeline."""
        logger.info("Validating prerequisites...")
        
        # Check input files exist for ETL1
        if "etl1" in self.config.stages:
            rates_file = self.config.data_dir / "input" / f"202508_{self.config.payer}_ga_rates.parquet"
            providers_file = self.config.data_dir / "input" / f"202508_{self.config.payer}_ga_providers.parquet"
            
            if not rates_file.exists():
                logger.error(f"Missing rates file: {rates_file}")
                return False
            
            if not providers_file.exists():
                logger.error(f"Missing providers file: {providers_file}")
                return False
        
        # Check ETL1 outputs exist for subsequent stages
        required_for_npi = ["etl1"]
        required_for_geo = ["etl1", "npi"]
        required_for_etl3 = ["etl1", "npi", "geocoding"]
        
        stages_to_run = [s for s in self.config.stages if s not in self.config.skip_stages]
        
        if "npi" in stages_to_run and not any(r in stages_to_run for r in required_for_npi):
            dim_npi_file = self.config.data_dir / "dims" / "dim_npi.parquet"
            if not dim_npi_file.exists():
                logger.error(f"Missing file for NPI stage: {dim_npi_file}")
                return False
        
        if "geocoding" in stages_to_run and not any(r in stages_to_run for r in required_for_geo):
            dim_address_file = self.config.data_dir / "dims" / "dim_npi_address.parquet"
            if not dim_address_file.exists():
                logger.error(f"Missing file for geocoding stage: {dim_address_file}")
                return False
        
        if "etl3" in stages_to_run and not any(r in stages_to_run for r in required_for_etl3):
            fact_file = self.config.data_dir / "gold" / "fact_rate.parquet"
            geo_file = self.config.data_dir / "dims" / "dim_npi_address_geolocation.parquet"
            
            if not fact_file.exists():
                logger.error(f"Missing file for ETL3 stage: {fact_file}")
                return False
            
            if not geo_file.exists():
                logger.error(f"Missing file for ETL3 stage: {geo_file}")
                return False
        
        # Check AWS credentials for ETL3
        if "etl3" in stages_to_run:
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
    
    def run(self) -> bool:
        """Run the complete pipeline."""
        logger.info("Starting Unified ETL Pipeline")
        self.pipeline_start_time = datetime.now()
        
        # Setup memory optimization
        self.memory_manager.setup_memory_optimization()
        
        # Log initial configuration
        self._log_configuration()
        
        # Validate prerequisites
        if not self.validate_prerequisites():
            logger.error("Prerequisites validation failed")
            return False
        
        # Run each stage
        all_success = True
        for stage in self.stages:
            success = stage.run()
            if not success:
                all_success = False
                logger.error(f"Pipeline failed at stage: {stage.name}")
                break
            
            # Check memory health between stages
            if not self.memory_manager.check_memory_health(f"between_stages"):
                logger.error("Memory health check failed between stages")
                all_success = False
                break
        
        self.pipeline_end_time = datetime.now()
        
        # Generate summary
        self._generate_summary(all_success)
        
        return all_success
    
    def _log_configuration(self):
        """Log the current configuration."""
        logger.info("Pipeline Configuration:")
        logger.info(f"  Payer: {self.config.payer}")
        logger.info(f"  State: {self.config.state}")
        logger.info(f"  Stages: {', '.join(self.config.stages)}")
        logger.info(f"  Memory limit: {self.config.memory_limit_mb}MB")
        logger.info(f"  Chunk size: {self.config.chunk_size:,}")
        logger.info(f"  Dry run: {self.config.dry_run}")
        if self.config.skip_stages:
            logger.info(f"  Skipping stages: {', '.join(self.config.skip_stages)}")
        if self.config.resume_from:
            logger.info(f"  Resuming from: {self.config.resume_from}")
    
    def _generate_summary(self, success: bool):
        """Generate and display pipeline summary."""
        total_time = (self.pipeline_end_time - self.pipeline_start_time).total_seconds()
        memory_summary = self.memory_manager.get_summary()
        
        print("\n" + "="*70)
        print("UNIFIED ETL PIPELINE SUMMARY")
        print("="*70)
        print(f"Status: {'SUCCESS' if success else 'FAILED'}")
        print(f"Total runtime: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
        print(f"Payer: {self.config.payer}")
        print(f"State: {self.config.state}")
        print()
        
        print("STAGE SUMMARY:")
        print("-" * 40)
        for stage in self.stages:
            status = "✅ SUCCESS" if stage.success else "❌ FAILED"
            duration = stage.get_duration()
            if stage.should_run():
                print(f"  {stage.name:12} {status:10} {duration:6.1f}s")
                if not stage.success and stage.error_message:
                    print(f"               Error: {stage.error_message}")
            else:
                print(f"  {stage.name:12} {'⏭️  SKIPPED':10}")
        
        print()
        print("MEMORY SUMMARY:")
        print("-" * 40)
        print(f"  Peak memory: {memory_summary['peak_memory_mb']:.1f}MB")
        print(f"  Final memory: {memory_summary['final_memory_mb']:.1f}MB")
        print(f"  Memory limit: {memory_summary['memory_limit_mb']}MB")
        print(f"  Memory efficiency: {memory_summary['memory_efficiency']:.1f}% of limit")
        print(f"  Memory warnings: {memory_summary['memory_warnings']}")
        
        print()
        print("NEXT STEPS:")
        print("-" * 40)
        if success:
            if "etl3" in [s.name for s in self.stages if s.success]:
                print("  ✅ Data is available in S3 for analytics")
                print("  ✅ Athena table is ready for queries")
            else:
                print("  📊 Consider running ETL3 to upload to S3")
        else:
            failed_stage = next((s.name for s in self.stages if not s.success), "unknown")
            print(f"  🔧 Fix issues with {failed_stage} stage")
            print(f"  🔄 Resume pipeline with: --resume-from {failed_stage}")
        
        print("="*70)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Unified ETL Pipeline Runner with Memory Optimization',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run complete pipeline for Aetna
  python ETL/scripts/run_unified_etl_pipeline.py --payer aetna --all-stages
  
  # Run only ETL1 and NPI fetch stages  
  python ETL/scripts/run_unified_etl_pipeline.py --payer uhc --stages etl1,npi
  
  # Resume from geocoding stage
  python ETL/scripts/run_unified_etl_pipeline.py --payer aetna --resume-from geocoding
  
  # Skip geocoding stage
  python ETL/scripts/run_unified_etl_pipeline.py --payer uhc --all-stages --skip geocoding
  
  # Dry run to see what would be executed
  python ETL/scripts/run_unified_etl_pipeline.py --payer aetna --all-stages --dry-run
        """
    )
    
    # Required arguments
    parser.add_argument(
        '--payer', 
        required=True,
        help='Payer name (e.g., aetna, uhc)'
    )
    
    # Stage control
    stage_group = parser.add_mutually_exclusive_group()
    stage_group.add_argument(
        '--all-stages',
        action='store_true',
        help='Run all stages: etl1, npi, geocoding, etl3'
    )
    stage_group.add_argument(
        '--stages',
        type=str,
        help='Comma-separated list of stages to run (etl1,npi,geocoding,etl3)'
    )
    
    parser.add_argument(
        '--resume-from',
        type=str,
        choices=['etl1', 'npi', 'geocoding', 'etl3'],
        help='Resume pipeline from specified stage'
    )
    
    parser.add_argument(
        '--skip',
        type=str,
        help='Comma-separated list of stages to skip'
    )
    
    # Processing parameters
    parser.add_argument(
        '--state',
        type=str,
        default='GA',
        help='State code (default: GA)'
    )
    
    parser.add_argument(
        '--memory-limit',
        type=int,
        default=1024,
        help='Memory limit in MB (default: 1024)'
    )
    
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=1000,
        help='Chunk size for processing (default: 1000)'
    )
    
    # NPI-specific options
    parser.add_argument(
        '--npi-threads',
        type=int,
        default=3,
        help='Number of threads for NPI fetching (default: 3)'
    )
    
    parser.add_argument(
        '--npi-sleep',
        type=float,
        default=0.1,
        help='Sleep time between NPI requests in seconds (default: 0.1)'
    )
    
    # Control options
    parser.add_argument(
        '--force-cleanup',
        action='store_true',
        help='Force cleanup of existing files'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be executed without running'
    )
    
    return parser.parse_args()


def main():
    """Main function."""
    args = parse_arguments()
    
    # Determine stages to run
    if args.all_stages:
        stages = ["etl1", "npi", "geocoding", "etl3"]
    elif args.stages:
        stages = [s.strip() for s in args.stages.split(',')]
    elif args.resume_from:
        all_stages = ["etl1", "npi", "geocoding", "etl3"]
        try:
            start_index = all_stages.index(args.resume_from)
            stages = all_stages[start_index:]
        except ValueError:
            logger.error(f"Invalid resume stage: {args.resume_from}")
            sys.exit(1)
    else:
        logger.error("Must specify --all-stages, --stages, or --resume-from")
        sys.exit(1)
    
    # Parse skip stages
    skip_stages = []
    if args.skip:
        skip_stages = [s.strip() for s in args.skip.split(',')]
    
    # Create configuration
    config = PipelineConfig(
        payer=args.payer,
        state=args.state,
        memory_limit_mb=args.memory_limit,
        chunk_size=args.chunk_size,
        stages=stages,
        resume_from=args.resume_from,
        skip_stages=skip_stages,
        force_cleanup=args.force_cleanup,
        dry_run=args.dry_run,
        threads_npi=args.npi_threads,
        npi_sleep=args.npi_sleep
    )
    
    # Create and run pipeline
    pipeline = UnifiedETLPipeline(config)
    success = pipeline.run()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()