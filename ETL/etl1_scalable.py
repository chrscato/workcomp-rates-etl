#!/usr/bin/env python3
"""
Scalable ETL1 Pipeline - Memory-Efficient Processing for Large Files

This script converts the ETL_1.ipynb notebook into a production-ready pipeline
that can handle large files through:
- Streaming/batched processing
- Memory-efficient upserts using DuckDB
- Progress tracking and logging
- Error handling and recovery
- Configurable chunk sizes

Usage:
    python ETL/etl1_scalable.py --state GA --payer-slug aetna --chunk-size 50000
    python ETL/etl1_scalable.py --config ETL/config/etl1_config.yaml
"""

import os
import sys
import json
import time
import logging
import argparse
import hashlib
import re
import gc
import psutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field

import polars as pl
import duckdb
from tqdm import tqdm


@dataclass
class ETL1Config:
    """Configuration for ETL1 scalable pipeline"""
    
    # Data paths
    data_root: Path = Path("../data")
    input_dir: Path = field(default_factory=lambda: Path("../data/input"))
    dims_dir: Path = field(default_factory=lambda: Path("../data/dims"))
    xrefs_dir: Path = field(default_factory=lambda: Path("../data/xrefs"))
    gold_dir: Path = field(default_factory=lambda: Path("../data/gold"))
    
    # Processing parameters
    state: str = "GA"
    payer_slug_override: Optional[str] = None
    chunk_size: int = 1000  # Very conservative chunk size for memory-constrained systems
    memory_limit_mb: int = 1024  # Very conservative memory limit
    
    # File patterns
    rates_file_pattern: str = "202508_{payer}_ga_rates.parquet"
    providers_file_pattern: str = "202508_{payer}_ga_providers.parquet"
    
    # Output files
    dim_code_file: str = "dim_code.parquet"
    dim_payer_file: str = "dim_payer.parquet"
    dim_provider_group_file: str = "dim_provider_group.parquet"
    dim_pos_set_file: str = "dim_pos_set.parquet"
    xref_pg_npi_file: str = "xref_pg_member_npi.parquet"
    xref_pg_tin_file: str = "xref_pg_member_tin.parquet"
    fact_rate_file: str = "fact_rate.parquet"
    
    # Logging
    log_level: str = "INFO"
    log_file: Optional[str] = "logs/etl1_scalable.log"
    
    # Cleanup options
    force_cleanup: bool = False
    
    def __post_init__(self):
        """Initialize paths and create directories"""
        # Convert string paths to Path objects and resolve relative to script location
        script_dir = Path(__file__).parent
        self.data_root = Path(self.data_root)
        
        # If data_root is relative, resolve it relative to the script location
        if not self.data_root.is_absolute():
            # Try both ../data (if running from ETL/) and data (if running from root/)
            if (script_dir / self.data_root).exists():
                self.data_root = script_dir / self.data_root
            elif (script_dir.parent / "data").exists():
                self.data_root = script_dir.parent / "data"
            else:
                # Fallback to current working directory
                self.data_root = Path.cwd() / "data"
        
        self.input_dir = self.data_root / "input"
        self.dims_dir = self.data_root / "dims"
        self.xrefs_dir = self.data_root / "xrefs"
        self.gold_dir = self.data_root / "gold"
        
        # Create directories
        for dir_path in [self.input_dir, self.dims_dir, self.xrefs_dir, self.gold_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Set up logging
        self._setup_logging()
    
    def _setup_logging(self):
        """Configure logging"""
        log_level = getattr(logging, self.log_level.upper())
        
        # Create logs directory
        if self.log_file:
            log_path = Path(self.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            handlers = [
                logging.FileHandler(log_path),
                logging.StreamHandler()
            ]
        else:
            handlers = [logging.StreamHandler()]
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=handlers
        )
    
    def get_input_files(self, payer: str) -> Tuple[Path, Path]:
        """Get input file paths for given payer"""
        rates_file = self.input_dir / self.rates_file_pattern.format(payer=payer)
        providers_file = self.input_dir / self.providers_file_pattern.format(payer=payer)
        return rates_file, providers_file
    
    def get_output_files(self) -> Dict[str, Path]:
        """Get output file paths"""
        return {
            "dim_code": self.dims_dir / self.dim_code_file,
            "dim_payer": self.dims_dir / self.dim_payer_file,
            "dim_provider_group": self.dims_dir / self.dim_provider_group_file,
            "dim_pos_set": self.dims_dir / self.dim_pos_set_file,
            "xref_pg_npi": self.xrefs_dir / self.xref_pg_npi_file,
            "xref_pg_tin": self.xrefs_dir / self.xref_pg_tin_file,
            "fact_rate": self.gold_dir / self.fact_rate_file,
        }
    
    @classmethod
    def from_yaml(cls, config_path: Path) -> "ETL1Config":
        """Load configuration from YAML file"""
        import yaml
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        return cls(**config_data)
    
    @classmethod
    def from_args(cls, args) -> "ETL1Config":
        """Create configuration from command line arguments"""
        config = cls()
        
        if hasattr(args, 'state') and args.state:
            config.state = args.state
        if hasattr(args, 'payer_slug') and args.payer_slug:
            config.payer_slug_override = args.payer_slug
        if hasattr(args, 'chunk_size') and args.chunk_size:
            config.chunk_size = args.chunk_size
        if hasattr(args, 'memory_limit') and args.memory_limit:
            config.memory_limit_mb = args.memory_limit
        if hasattr(args, 'force_cleanup'):
            config.force_cleanup = args.force_cleanup
        
        return config


class ScalableETL1:
    """Memory-efficient ETL1 pipeline for large files"""
    
    def __init__(self, config: ETL1Config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Column definitions (matching actual file schemas)
        self.rates_cols = [
            "last_updated_on", "reporting_entity_name", "version",
            "billing_class", "billing_code_type", "billing_code",
            "service_codes", "negotiated_type", "negotiation_arrangement",
            "negotiated_rate", "expiration_date", "description", "name",
            "provider_reference_id", "reporting_entity_type"
        ]
        
        self.prov_cols = [
            "last_updated_on", "reporting_entity_name", "version",
            "provider_group_id", "npi", "tin_type", "tin_value", "reporting_entity_type"
        ]
        
        # Initialize DuckDB connection for upserts
        self.db_conn = None
    
    def get_memory_usage(self) -> Dict[str, float]:
        """Get current memory usage statistics"""
        process = psutil.Process()
        memory_info = process.memory_info()
        system_memory = psutil.virtual_memory()
        
        return {
            "process_mb": memory_info.rss / (1024 * 1024),
            "process_percent": process.memory_percent(),
            "system_available_gb": system_memory.available / (1024 * 1024 * 1024),
            "system_percent": system_memory.percent
        }
    
    def check_memory_limits(self) -> bool:
        """Check if we're approaching memory limits"""
        memory = self.get_memory_usage()
        
        # Warn if process memory exceeds 70% of configured limit (more aggressive)
        process_limit_mb = self.config.memory_limit_mb
        if memory["process_mb"] > process_limit_mb * 0.7:
            self.logger.warning(f"High memory usage: {memory['process_mb']:.1f}MB / {process_limit_mb}MB")
            # Force garbage collection
            gc.collect()
            return False
        
        # Warn if system memory is low
        if memory["system_available_gb"] < 0.5:  # Less than 500MB available (more aggressive)
            self.logger.warning(f"Low system memory: {memory['system_available_gb']:.1f}GB available")
            # Force garbage collection
            gc.collect()
            return False
        
        return True
    
    def cleanup_memory(self):
        """Force garbage collection and memory cleanup"""
        gc.collect()
        memory = self.get_memory_usage()
        self.logger.debug(f"Memory after cleanup: {memory['process_mb']:.1f}MB")
    
    def __enter__(self):
        """Context manager entry"""
        self.db_conn = duckdb.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.db_conn:
            self.db_conn.close()
    
    def cleanup_temp_files(self, base_path: Path) -> None:
        """Clean up temporary files that might be left behind"""
        temp_patterns = [
            base_path.with_suffix(".temp.parquet"),
            base_path.with_suffix(".backup.parquet"),
            base_path.with_suffix(".final.parquet"),
        ]
        
        for temp_path in temp_patterns:
            if temp_path.exists():
                try:
                    os.remove(temp_path)
                    self.logger.info(f"Cleaned up temp file: {temp_path}")
                except (OSError, PermissionError) as e:
                    self.logger.warning(f"Could not clean up temp file {temp_path}: {e}")
    
    def force_cleanup_existing_file(self, file_path: Path, max_retries: int = 3) -> bool:
        """Force cleanup of existing file with retries"""
        for attempt in range(max_retries):
            try:
                if file_path.exists():
                    os.remove(file_path)
                    self.logger.info(f"Successfully removed existing file: {file_path}")
                    return True
            except (OSError, PermissionError) as e:
                self.logger.warning(f"Attempt {attempt + 1} to remove {file_path} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)  # Wait 1 second before retry
                else:
                    self.logger.error(f"Failed to remove {file_path} after {max_retries} attempts")
                    return False
        return True
    
    def md5(self, s: str) -> str:
        """Generate MD5 hash"""
        return hashlib.md5(s.encode("utf-8")).hexdigest()
    
    def slugify(self, s: str) -> str:
        """Convert string to URL-friendly slug"""
        if s is None:
            return ""
        s = s.lower()
        s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
        s = re.sub(r"-+", "-", s)
        return s
    
    def _co(self, x):
        """Convert to string, handling None"""
        return "" if x is None else str(x)
    
    def payer_slug_from_name(self, name: str) -> str:
        """Generate payer slug from name"""
        if self.config.payer_slug_override:
            return self.config.payer_slug_override
        return self.slugify(name or "")
    
    def normalize_yymm(self, date_str: str | None) -> str:
        """Normalize date string to YYYY-MM format"""
        if not date_str:
            return ""
        
        # Try common formats
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m", "%Y/%m", "%Y%m%d", "%Y%m"):
            try:
                dt = datetime.strptime(date_str[:len(fmt.replace("%", "").replace("-", "").replace("/", ""))], fmt)
                return dt.strftime("%Y-%m")
            except Exception:
                continue
        
        # Fallback: extract yyyy-mm
        m = re.search(r"(20\d{2})[-/](0[1-9]|1[0-2])", date_str)
        return f"{m.group(1)}-{m.group(2)}" if m else ""
    
    def normalize_service_codes(self, svc) -> List[str]:
        """Normalize service_codes into sorted unique list"""
        if svc is None:
            return []
        
        # Handle different input types
        if isinstance(svc, (list, tuple)):
            vals = ["" if v is None else str(v) for v in svc]
        else:
            s = str(svc)
            
            # Try JSON parsing
            if s.startswith("[") and s.endswith("]"):
                try:
                    parsed = json.loads(s)
                    if isinstance(parsed, list):
                        vals = ["" if v is None else str(v) for v in parsed]
                    else:
                        vals = re.split(r"[;,|\s]+", s)
                except Exception:
                    vals = re.split(r"[;,|\s]+", s)
            else:
                vals = re.split(r"[;,|\s]+", s)
        
        # Clean and dedupe
        cleaned = []
        for v in vals:
            sv = str(v).strip()
            if len(sv) > 0:
                cleaned.append(sv)
        
        return sorted(set(cleaned))
    
    def pos_set_id_from_members(self, members) -> str:
        """Generate stable ID from POS members list"""
        if members is None:
            return self.md5("none")
        
        try:
            n = len(members)
        except Exception:
            members = [str(members)]
            n = 1
        
        if n == 0:
            return self.md5("none")
        
        parts = ["" if m is None else str(m) for m in members]
        return self.md5("|".join(parts))
    
    def pg_uid_from_parts(self, payer_slug: str, version: str | None, pgid: str | None, pref: str | None) -> str:
        """Generate provider group UID from components"""
        key = f"{self._co(payer_slug)}|{self._co(version)}|{self._co(pgid)}|{self._co(pref)}"
        return self.md5(key)
    
    def fact_uid_from_struct(self, s: dict) -> str:
        """Generate deterministic fact UID"""
        rate_val = s.get("negotiated_rate")
        try:
            rate_str = f"{float(rate_val):.4f}" if rate_val is not None else ""
        except Exception:
            rate_str = ""
        
        parts = [
            self._co(s.get("state")),
            self._co(s.get("year_month")),
            self._co(s.get("payer_slug")),
            self._co(s.get("billing_class")),
            self._co(s.get("code_type")),
            self._co(s.get("code")),
            self._co(s.get("pg_uid")),
            self._co(s.get("pos_set_id")),
            self._co(s.get("negotiated_type")),
            self._co(s.get("negotiation_arrangement")),
            self._co(s.get("expiration_date")),
            rate_str,
            self._co(s.get("provider_group_id_raw")),
        ]
        return self.md5("|".join(parts))
    
    def read_parquet_safely(self, path: Path, desired_cols: List[str]) -> pl.DataFrame:
        """Safely read parquet file, handling missing columns"""
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        
        lf = pl.scan_parquet(str(path))
        avail = set(lf.columns)
        use_cols = [c for c in desired_cols if c in avail]
        df = lf.select(use_cols).collect()
        
        # Add missing columns as nulls
        missing = [c for c in desired_cols if c not in df.columns]
        if missing:
            df = df.with_columns([pl.lit(None).alias(c) for c in missing])
        
        return df
    
    def process_rates_chunk(self, chunk: pl.DataFrame) -> pl.DataFrame:
        """Process a chunk of rates data"""
        # Add derived columns
        chunk = chunk.with_columns([
            pl.col("reporting_entity_name").map_elements(
                self.payer_slug_from_name, return_dtype=pl.Utf8
            ).alias("payer_slug"),
            
            pl.col("last_updated_on").map_elements(
                self.normalize_yymm, return_dtype=pl.Utf8
            ).alias("year_month"),
            
            pl.col("service_codes").map_elements(
                self.normalize_service_codes, return_dtype=pl.List(pl.Utf8)
            ).alias("pos_members"),
        ])
        
        # Generate POS set ID
        chunk = chunk.with_columns([
            pl.col("pos_members").map_elements(
                self.pos_set_id_from_members, return_dtype=pl.Utf8
            ).alias("pos_set_id")
        ])
        
        # Generate provider group UID (use provider_reference_id for rates)
        chunk = chunk.with_columns([
            pl.concat_str([
                pl.col("payer_slug"),
                pl.col("version").fill_null(""),
                pl.col("provider_reference_id").fill_null(""),
                pl.lit(""),  # provider_group_id doesn't exist in rates
            ], separator="|").map_elements(
                self.md5, return_dtype=pl.Utf8
            ).alias("pg_uid")
        ])
        
        return chunk
    
    def process_providers_chunk(self, chunk: pl.DataFrame) -> pl.DataFrame:
        """Process a chunk of providers data"""
        # Add payer slug
        chunk = chunk.with_columns([
            pl.col("reporting_entity_name").map_elements(
                self.payer_slug_from_name, return_dtype=pl.Utf8
            ).alias("payer_slug")
        ])
        
        # Generate provider group UID (use provider_group_id for providers)
        chunk = chunk.with_columns([
            pl.concat_str([
                pl.col("payer_slug"),
                pl.col("version").fill_null(""),
                pl.col("provider_group_id").fill_null(""),
                pl.lit(""),  # provider_reference_id doesn't exist in providers
            ], separator="|").map_elements(
                self.md5, return_dtype=pl.Utf8
            ).alias("pg_uid")
        ])
        
        return chunk
    
    def write_chunk_to_temp(self, df_new: pl.DataFrame, output_path: Path, 
                           table_name: str, chunk_idx: int) -> Path:
        """Write chunk to temporary file - no reading of existing files"""
        if df_new.is_empty():
            return None
        
        # Create chunk-specific temp file
        temp_dir = output_path.parent / "temp_chunks"
        temp_dir.mkdir(exist_ok=True)
        temp_path = temp_dir / f"{table_name}_chunk_{chunk_idx:06d}.parquet"
        
        self.logger.info(f"Writing {df_new.height:,} rows to temp file: {temp_path.name}")
        df_new.write_parquet(temp_path, compression="zstd")
        
        return temp_path
    
    def merge_temp_files(self, output_path: Path, table_name: str, keys: List[str]) -> None:
        """Merge all temporary files into final output file using batched approach"""
        temp_dir = output_path.parent / "temp_chunks"
        temp_pattern = f"{table_name}_chunk_*.parquet"
        
        # Find all temp files for this table
        temp_files = list(temp_dir.glob(temp_pattern))
        
        if not temp_files:
            self.logger.warning(f"No temp files found for {table_name}")
            return
        
        self.logger.info(f"Merging {len(temp_files)} temp files for {table_name}")
        
        # Set memory limit for DuckDB operations
        memory_limit_gb = max(1, self.config.memory_limit_mb // 1024)
        self.db_conn.execute(f"SET memory_limit='{memory_limit_gb}GB'")
        
        try:
            if len(temp_files) == 1:
                # Single file - just rename it
                os.rename(temp_files[0], output_path)
                self.logger.info(f"Renamed single temp file to {output_path}")
            else:
                # Multiple files - merge them in batches to avoid memory issues
                max_files_per_batch = 10  # Process max 10 files at a time
                
                if len(temp_files) <= max_files_per_batch:
                    # Small number of files - merge all at once
                    temp_paths_str = "', '".join(str(f) for f in temp_files)
                    
                    if table_name == "fact_rate":
                        # For fact table, just concatenate all files
                        self.db_conn.execute(f"""
                            COPY (
                                SELECT * FROM read_parquet(['{temp_paths_str}'])
                            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
                        """)
                    else:
                        # For dimensions, deduplicate using simple DISTINCT
                        self.db_conn.execute(f"""
                            COPY (
                                SELECT DISTINCT * FROM read_parquet(['{temp_paths_str}'])
                            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
                        """)
                else:
                    # Large number of files - merge in batches
                    self.logger.info(f"Large number of files ({len(temp_files)}), merging in batches of {max_files_per_batch}")
                    
                    # Create intermediate merge files
                    intermediate_files = []
                    batch_num = 0
                    
                    for i in range(0, len(temp_files), max_files_per_batch):
                        batch_files = temp_files[i:i + max_files_per_batch]
                        batch_paths_str = "', '".join(str(f) for f in batch_files)
                        
                        # Create intermediate file for this batch
                        intermediate_file = temp_dir / f"{table_name}_batch_{batch_num:03d}.parquet"
                        intermediate_files.append(intermediate_file)
                        
                        if table_name == "fact_rate":
                            # For fact table, just concatenate
                            self.db_conn.execute(f"""
                                COPY (
                                    SELECT * FROM read_parquet(['{batch_paths_str}'])
                                ) TO '{intermediate_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
                            """)
                        else:
                            # For dimensions, deduplicate
                            self.db_conn.execute(f"""
                                COPY (
                                    SELECT DISTINCT * FROM read_parquet(['{batch_paths_str}'])
                                ) TO '{intermediate_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
                            """)
                        
                        batch_num += 1
                        self.logger.info(f"Created intermediate batch file {batch_num}: {intermediate_file.name}")
                        
                        # Clean up original temp files in this batch
                        for temp_file in batch_files:
                            try:
                                os.remove(temp_file)
                            except (OSError, PermissionError):
                                self.logger.warning(f"Could not remove temp file {temp_file}")
                    
                    # Now merge all intermediate files into final output
                    if len(intermediate_files) == 1:
                        os.rename(intermediate_files[0], output_path)
                    else:
                        intermediate_paths_str = "', '".join(str(f) for f in intermediate_files)
                        
                        if table_name == "fact_rate":
                            self.db_conn.execute(f"""
                                COPY (
                                    SELECT * FROM read_parquet(['{intermediate_paths_str}'])
                                ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
                            """)
                        else:
                            self.db_conn.execute(f"""
                                COPY (
                                    SELECT DISTINCT * FROM read_parquet(['{intermediate_paths_str}'])
                                ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
                            """)
                        
                        # Clean up intermediate files
                        for intermediate_file in intermediate_files:
                            try:
                                os.remove(intermediate_file)
                            except (OSError, PermissionError):
                                self.logger.warning(f"Could not remove intermediate file {intermediate_file}")
                
                self.logger.info(f"Merged {len(temp_files)} temp files into {output_path}")
            
            # Clean up any remaining temp files
            for temp_file in temp_files:
                try:
                    if temp_file.exists():
                        os.remove(temp_file)
                except (OSError, PermissionError):
                    self.logger.warning(f"Could not remove temp file {temp_file}")
            
            # Remove temp directory if empty
            try:
                temp_dir.rmdir()
            except OSError:
                pass  # Directory not empty or other error
                
        except Exception as e:
            self.logger.error(f"Failed to merge temp files for {table_name}: {e}")
            raise
    
    def process_dimensions(self, rates_chunk: pl.DataFrame, providers_chunk: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        """Process dimension tables from chunks"""
        dims = {}
        
        # Dim Code
        dims["dim_code"] = (
            rates_chunk.select([
                pl.col("billing_code_type").alias("code_type"),
                pl.col("billing_code").cast(pl.Utf8).alias("code"),
                pl.col("description").alias("code_description"),
                pl.col("name").alias("code_name"),
            ])
            .drop_nulls(subset=["code_type", "code"])
            .unique()
        )
        
        # Dim Payer
        dims["dim_payer"] = (
            rates_chunk.select([
                pl.col("payer_slug"),
                pl.col("reporting_entity_name"),
                pl.col("version"),
            ])
            .drop_nulls(subset=["payer_slug"])
            .unique()
        )
        
        # Dim Provider Group (from rates data - use provider_reference_id)
        dims["dim_provider_group"] = (
            rates_chunk.select([
                pl.col("pg_uid"),
                pl.col("payer_slug"),
                pl.col("provider_reference_id").alias("provider_group_id_raw"),
                pl.col("version"),
            ])
            .drop_nulls(subset=["pg_uid"])
            .unique()
        )
        
        # Dim POS Set
        dims["dim_pos_set"] = (
            rates_chunk.select(["pos_set_id", "pos_members"])
            .drop_nulls(subset=["pos_set_id"])
            .unique()
        )
        
        # Xref PG NPI (only if providers_chunk is not empty)
        if not providers_chunk.is_empty():
            dims["xref_pg_npi"] = (
                providers_chunk.select(["pg_uid", "npi"])
                .drop_nulls(subset=["pg_uid", "npi"])
                .unique()
            )
        else:
            dims["xref_pg_npi"] = pl.DataFrame({"pg_uid": [], "npi": []})
        
        # Xref PG TIN (only if providers_chunk is not empty)
        if not providers_chunk.is_empty():
            dims["xref_pg_tin"] = (
                providers_chunk.select(["pg_uid", "tin_type", "tin_value"])
                .drop_nulls(subset=["pg_uid", "tin_value"])
                .unique()
            )
        else:
            dims["xref_pg_tin"] = pl.DataFrame({"pg_uid": [], "tin_type": [], "tin_value": []})
        
        return dims
    
    def process_provider_dimensions(self, providers_chunk: pl.DataFrame) -> Dict[str, pl.DataFrame]:
        """Process provider-specific dimension tables from providers chunk"""
        dims = {}
        
        # Dim Provider Group (from providers data - use provider_group_id)
        dims["dim_provider_group"] = (
            providers_chunk.select([
                pl.col("pg_uid"),
                pl.col("payer_slug"),
                pl.col("provider_group_id").alias("provider_group_id_raw"),
                pl.col("version"),
            ])
            .drop_nulls(subset=["pg_uid"])
            .unique()
        )
        
        # Xref PG NPI
        dims["xref_pg_npi"] = (
            providers_chunk.select(["pg_uid", "npi"])
            .drop_nulls(subset=["pg_uid", "npi"])
            .unique()
        )
        
        # Xref PG TIN
        dims["xref_pg_tin"] = (
            providers_chunk.select(["pg_uid", "tin_type", "tin_value"])
            .drop_nulls(subset=["pg_uid", "tin_value"])
            .unique()
        )
        
        return dims
    
    def process_fact_table(self, rates_chunk: pl.DataFrame) -> pl.DataFrame:
        """Process fact table from rates chunk"""
        fact = (
            rates_chunk
            .with_columns(pl.lit(self.config.state).alias("state"))
            .select([
                "state", "year_month", "payer_slug", "billing_class",
                pl.col("billing_code_type").alias("code_type"),
                pl.col("billing_code").cast(pl.Utf8).alias("code"),
                "pg_uid", "pos_set_id", "negotiated_type", "negotiation_arrangement",
                pl.col("negotiated_rate").cast(pl.Float64).alias("negotiated_rate"),
                "expiration_date",
                pl.col("provider_reference_id").alias("provider_group_id_raw"),  # Use provider_reference_id since provider_group_id doesn't exist in rates
                "reporting_entity_name",
            ])
            .with_columns([
                pl.struct([
                    "state", "year_month", "payer_slug", "billing_class", "code_type", "code",
                    "pg_uid", "pos_set_id", "negotiated_type", "negotiation_arrangement",
                    "expiration_date", "negotiated_rate", "provider_group_id_raw"
                ]).map_elements(self.fact_uid_from_struct, return_dtype=pl.Utf8).alias("fact_uid")
            ])
            .select([
                "fact_uid", "state", "year_month", "payer_slug", "billing_class", "code_type", "code",
                "pg_uid", "pos_set_id", "negotiated_type", "negotiation_arrangement",
                "negotiated_rate", "expiration_date", "provider_group_id_raw", "reporting_entity_name"
            ])
            .unique()
        )
        
        return fact
    
    def run_pipeline(self, payer: str) -> Dict[str, Any]:
        """Run the complete ETL1 pipeline"""
        start_time = time.time()
        self.logger.info(f"Starting ETL1 pipeline for payer: {payer}")
        
        # Get input files
        rates_file, providers_file = self.config.get_input_files(payer)
        output_files = self.config.get_output_files()
        
        # Clean up any existing temp files and directories
        self.logger.info("Cleaning up any existing temporary files...")
        for output_path in output_files.values():
            self.cleanup_temp_files(output_path)
            # Clean up temp chunks directory
            temp_dir = output_path.parent / "temp_chunks"
            if temp_dir.exists():
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
                self.logger.info(f"Cleaned up temp directory: {temp_dir}")
        
        # Force cleanup of existing files if requested
        if hasattr(self.config, 'force_cleanup') and self.config.force_cleanup:
            self.logger.info("Force cleanup requested - removing existing output files...")
            for output_path in output_files.values():
                self.force_cleanup_existing_file(output_path)
        
        # Verify input files exist
        if not rates_file.exists():
            raise FileNotFoundError(f"Rates file not found: {rates_file}")
        if not providers_file.exists():
            raise FileNotFoundError(f"Providers file not found: {providers_file}")
        
        # Get total row counts for progress tracking
        rates_total = pl.scan_parquet(rates_file).select(pl.len()).collect().item()
        providers_total = pl.scan_parquet(providers_file).select(pl.len()).collect().item()
        
        self.logger.info(f"Processing {rates_total:,} rates records and {providers_total:,} provider records")
        
        # Process in chunks
        chunk_size = self.config.chunk_size
        rates_chunks = (rates_total + chunk_size - 1) // chunk_size
        providers_chunks = (providers_total + chunk_size - 1) // chunk_size
        
        self.logger.info(f"Processing in {rates_chunks} rates chunks and {providers_chunks} provider chunks of {chunk_size:,} rows each")
        
        # Initialize progress tracking
        processed_rates = 0
        processed_providers = 0
        
        # Process rates data
        self.logger.info("Processing rates data...")
        for chunk_idx in range(rates_chunks):
            chunk_start = chunk_idx * chunk_size
            chunk_end = min(chunk_start + chunk_size, rates_total)
            
            self.logger.info(f"Processing rates chunk {chunk_idx + 1}/{rates_chunks} (rows {chunk_start:,}-{chunk_end:,})")
            
            # Load chunk
            rates_chunk = pl.scan_parquet(rates_file).slice(chunk_start, chunk_size).collect()
            if rates_chunk.is_empty():
                break
            
            # Process chunk
            rates_chunk = self.process_rates_chunk(rates_chunk)
            
            # Process dimensions and fact table
            dims = self.process_dimensions(rates_chunk, pl.DataFrame())  # Empty providers for rates-only dims
            fact_chunk = self.process_fact_table(rates_chunk)
            
            # Write dimensions to temp files
            for dim_name, dim_data in dims.items():
                if not dim_data.is_empty():
                    output_path = output_files[dim_name]
                    self.write_chunk_to_temp(dim_data, output_path, dim_name, chunk_idx)
            
            # Write fact table to temp file
            if not fact_chunk.is_empty():
                self.write_chunk_to_temp(fact_chunk, output_files["fact_rate"], "fact_rate", chunk_idx)
            
            processed_rates += rates_chunk.height
            
            # Memory cleanup
            del rates_chunk, dims, fact_chunk
            self.cleanup_memory()
            
            # Check memory limits every 5 chunks (more frequent)
            if chunk_idx % 5 == 0:
                if not self.check_memory_limits():
                    self.logger.warning(f"Memory pressure detected at chunk {chunk_idx + 1}")
                    # Force more aggressive cleanup
                    gc.collect()
                    # If memory is still high, reduce chunk size for next iteration
                    if self.get_memory_usage()["process_mb"] > self.config.memory_limit_mb * 0.8:
                        self.logger.warning("Memory still high after cleanup - consider reducing chunk size")
        
        # Process providers data
        self.logger.info("Processing providers data...")
        for chunk_idx in range(providers_chunks):
            chunk_start = chunk_idx * chunk_size
            chunk_end = min(chunk_start + chunk_size, providers_total)
            
            self.logger.info(f"Processing providers chunk {chunk_idx + 1}/{providers_chunks} (rows {chunk_start:,}-{chunk_end:,})")
            
            # Load chunk
            providers_chunk = pl.scan_parquet(providers_file).slice(chunk_start, chunk_size).collect()
            if providers_chunk.is_empty():
                break
            
            # Process chunk
            providers_chunk = self.process_providers_chunk(providers_chunk)
            
            # Process provider-specific dimensions
            prov_dims = self.process_provider_dimensions(providers_chunk)
            
            # Write provider dimensions to temp files
            for dim_name in ["dim_provider_group", "xref_pg_npi", "xref_pg_tin"]:
                if dim_name in prov_dims and not prov_dims[dim_name].is_empty():
                    output_path = output_files[dim_name]
                    self.write_chunk_to_temp(prov_dims[dim_name], output_path, dim_name, chunk_idx)
            
            processed_providers += providers_chunk.height
            
            # Memory cleanup
            del providers_chunk, prov_dims
            self.cleanup_memory()
            
            # Check memory limits every 5 chunks (more frequent)
            if chunk_idx % 5 == 0:
                if not self.check_memory_limits():
                    self.logger.warning(f"Memory pressure detected at provider chunk {chunk_idx + 1}")
                    gc.collect()
                    # If memory is still high, reduce chunk size for next iteration
                    if self.get_memory_usage()["process_mb"] > self.config.memory_limit_mb * 0.8:
                        self.logger.warning("Memory still high after cleanup - consider reducing chunk size")
        
        # Final merge step - combine all temp files into final outputs
        self.logger.info("Merging all temporary files into final outputs...")
        for table_name, output_path in output_files.items():
            keys = self._get_dimension_keys(table_name)
            self.merge_temp_files(output_path, table_name, keys)
        
        # Final summary
        end_time = time.time()
        duration = end_time - start_time
        
        summary = {
            "payer": payer,
            "state": self.config.state,
            "duration_seconds": duration,
            "processed_rates": processed_rates,
            "processed_providers": processed_providers,
            "chunk_size": chunk_size,
            "output_files": {k: str(v) for k, v in output_files.items()}
        }
        
        self.logger.info(f"ETL1 pipeline completed in {duration:.2f} seconds")
        self.logger.info(f"Processed {processed_rates:,} rates and {processed_providers:,} provider records")
        
        return summary
    
    def _get_dimension_keys(self, dim_name: str) -> List[str]:
        """Get primary key columns for dimension table"""
        key_mapping = {
            "dim_code": ["code_type", "code"],
            "dim_payer": ["payer_slug"],
            "dim_provider_group": ["pg_uid"],
            "dim_pos_set": ["pos_set_id"],
            "xref_pg_npi": ["pg_uid", "npi"],
            "xref_pg_tin": ["pg_uid", "tin_value"],
        }
        return key_mapping.get(dim_name, [])


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Scalable ETL1 Pipeline")
    parser.add_argument("--state", default="GA", help="State code (default: GA)")
    parser.add_argument("--payer-slug", help="Payer slug override")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Chunk size for processing (default: 1000)")
    parser.add_argument("--memory-limit", type=int, default=1024, help="Memory limit in MB (default: 1024)")
    parser.add_argument("--config", help="Path to YAML config file")
    parser.add_argument("--payer", required=True, help="Payer name (e.g., aetna, uhc)")
    parser.add_argument("--force-cleanup", action="store_true", help="Force cleanup of existing output files before processing")
    
    args = parser.parse_args()
    
    # Load configuration
    if args.config:
        config = ETL1Config.from_yaml(Path(args.config))
    else:
        config = ETL1Config.from_args(args)
    
    # Run pipeline
    with ScalableETL1(config) as etl:
        try:
            summary = etl.run_pipeline(args.payer)
            print(json.dumps(summary, indent=2))
        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
