#!/usr/bin/env python3
"""
S3 Partition Inventory Script with SQLite Database
Efficiently discovers and catalogs partitioned healthcare data in S3
Creates a SQLite database with navigation tables and taxonomy descriptions
"""

import boto3
import json
import time
import re
import sqlite3
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict
import argparse
import csv
from pathlib import Path

# Configure boto3 for optimal performance
from botocore.config import Config

@dataclass
class PartitionInfo:
    """Structured partition information"""
    partition_path: str
    payer_slug: str
    state: str
    billing_class: str
    procedure_set: str
    procedure_class: str
    taxonomy_code: str
    stat_area_name: str
    year: int
    month: int
    file_size_bytes: int
    last_modified: datetime
    record_count_estimate: Optional[int] = None
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        data = asdict(self)
        data['last_modified'] = self.last_modified.isoformat()
        return data

class S3PartitionInventory:
    """Efficient S3 partition discovery and cataloging"""
    
    def __init__(self, bucket_name: str, region: str = 'us-east-1', prefix: str = 'partitioned-data'):
        self.bucket_name = bucket_name
        self.region = region
        self.prefix = prefix
        
        # Optimized S3 client configuration
        self.s3_config = Config(
            region_name=region,
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            max_pool_connections=50
        )
        
        self.s3_client = boto3.client('s3', config=self.s3_config)
        
        # Partition parsing regex
        self.partition_pattern = re.compile(
            r'payer_slug=([^/]+)/'
            r'state=([^/]+)/'
            r'billing_class=([^/]+)/'
            r'procedure_set=([^/]+)/'
            r'procedure_class=([^/]+)/'
            r'primary_taxonomy_code=([^/]+)/'
            r'stat_area_name=([^/]+)/'
            r'year=(\d{4})/'
            r'month=(\d{2})/'
            r'fact_rate_enriched\.parquet'
        )
        
        # Statistics tracking
        self.stats = {
            'api_calls': 0,
            'partitions_found': 0,
            'total_size_bytes': 0,
            'scan_duration': 0,
            'errors': []
        }
    
    def parse_partition_path(self, s3_key: str) -> Optional[PartitionInfo]:
        """Extract partition information from S3 key"""
        match = self.partition_pattern.search(s3_key)
        if not match:
            return None
        
        try:
            return PartitionInfo(
                partition_path=s3_key,
                payer_slug=self._decode_partition_value(match.group(1)),
                state=self._decode_partition_value(match.group(2)),
                billing_class=self._decode_partition_value(match.group(3)),
                procedure_set=self._decode_partition_value(match.group(4)),
                procedure_class=self._decode_partition_value(match.group(5)),
                taxonomy_code=self._decode_partition_value(match.group(6)),
                stat_area_name=self._decode_partition_value(match.group(7)),
                year=int(match.group(8)),
                month=int(match.group(9)),
                file_size_bytes=0,  # Will be populated by discovery
                last_modified=datetime.now(timezone.utc)  # Will be populated by discovery
            )
        except (ValueError, IndexError) as e:
            self.stats['errors'].append(f"Error parsing partition {s3_key}: {e}")
            return None
    
    def _decode_partition_value(self, value: str) -> str:
        """Decode S3-encoded partition values"""
        if value == '__NULL__':
            return None
        return value.replace('_', ' ').replace('__NULL__', '')
    
    def discover_partitions(self, 
                          max_keys_per_request: int = 1000,
                          include_empty: bool = False) -> List[PartitionInfo]:
        """
        Efficiently discover all partitions using pagination
        
        Args:
            max_keys_per_request: Number of keys to fetch per API call (max 1000)
            include_empty: Whether to include empty partitions
            
        Returns:
            List of PartitionInfo objects
        """
        partitions = []
        start_time = time.time()
        
        print(f"🔍 Scanning S3 bucket: s3://{self.bucket_name}/{self.prefix}")
        print(f"📊 Using {max_keys_per_request} keys per request to minimize API calls")
        
        # Use paginator for efficient scanning
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=self.prefix,
            PaginationConfig={
                'MaxItems': None,
                'PageSize': max_keys_per_request
            }
        )
        
        page_count = 0
        for page in page_iterator:
            page_count += 1
            self.stats['api_calls'] += 1
            
            objects = page.get('Contents', [])
            print(f"📄 Processing page {page_count}: {len(objects)} objects")
            
            for obj in objects:
                # Only process parquet files
                if not obj['Key'].endswith('.parquet'):
                    continue
                
                # Skip empty files unless requested
                if obj['Size'] == 0 and not include_empty:
                    continue
                
                # Parse partition information
                partition_info = self.parse_partition_path(obj['Key'])
                if partition_info:
                    # Populate metadata from S3 object
                    partition_info.file_size_bytes = obj['Size']
                    partition_info.last_modified = obj['LastModified']
                    
                    # Estimate record count (rough approximation)
                    partition_info.record_count_estimate = self._estimate_record_count(obj['Size'])
                    
                    partitions.append(partition_info)
                    self.stats['partitions_found'] += 1
                    self.stats['total_size_bytes'] += obj['Size']
        
        self.stats['scan_duration'] = time.time() - start_time
        
        print(f"✅ Discovery complete: {len(partitions)} partitions found")
        print(f"⏱️  Duration: {self.stats['scan_duration']:.2f} seconds")
        print(f"🔌 API calls made: {self.stats['api_calls']}")
        
        return partitions
    
    def _estimate_record_count(self, file_size_bytes: int) -> int:
        """Rough estimate of record count based on file size"""
        # Assumes approximately 200-500 bytes per record after compression
        bytes_per_record = 350  # Conservative estimate
        return max(1, file_size_bytes // bytes_per_record)
    
    def analyze_partitions(self, partitions: List[PartitionInfo]) -> Dict:
        """Generate comprehensive partition analytics"""
        if not partitions:
            return {'error': 'No partitions to analyze'}
        
        analysis = {
            'summary': {
                'total_partitions': len(partitions),
                'total_size_gb': self.stats['total_size_bytes'] / (1024**3),
                'estimated_total_records': sum(p.record_count_estimate or 0 for p in partitions),
                'date_range': {
                    'earliest': min(p.last_modified for p in partitions).isoformat(),
                    'latest': max(p.last_modified for p in partitions).isoformat()
                }
            },
            'dimensions': {
                'payers': len(set(p.payer_slug for p in partitions if p.payer_slug)),
                'states': len(set(p.state for p in partitions if p.state)),
                'billing_classes': len(set(p.billing_class for p in partitions if p.billing_class)),
                'procedure_sets': len(set(p.procedure_set for p in partitions if p.procedure_set)),
                'taxonomy_codes': len(set(p.taxonomy_code for p in partitions if p.taxonomy_code)),
                'time_periods': len(set((p.year, p.month) for p in partitions))
            },
            'size_distribution': self._analyze_size_distribution(partitions),
            'temporal_distribution': self._analyze_temporal_distribution(partitions),
            'top_dimensions': self._analyze_top_dimensions(partitions)
        }
        
        return analysis
    
    def _analyze_size_distribution(self, partitions: List[PartitionInfo]) -> Dict:
        """Analyze partition size distribution"""
        sizes = [p.file_size_bytes for p in partitions]
        sizes.sort()
        
        return {
            'min_size_mb': min(sizes) / (1024**2),
            'max_size_mb': max(sizes) / (1024**2),
            'median_size_mb': sizes[len(sizes)//2] / (1024**2),
            'avg_size_mb': sum(sizes) / len(sizes) / (1024**2),
            'percentiles': {
                'p10': sizes[int(len(sizes) * 0.1)] / (1024**2),
                'p25': sizes[int(len(sizes) * 0.25)] / (1024**2),
                'p75': sizes[int(len(sizes) * 0.75)] / (1024**2),
                'p90': sizes[int(len(sizes) * 0.9)] / (1024**2),
            }
        }
    
    def _analyze_temporal_distribution(self, partitions: List[PartitionInfo]) -> Dict:
        """Analyze temporal distribution of partitions"""
        monthly_counts = defaultdict(int)
        monthly_sizes = defaultdict(int)
        
        for p in partitions:
            key = f"{p.year}-{p.month:02d}"
            monthly_counts[key] += 1
            monthly_sizes[key] += p.file_size_bytes
        
        return {
            'months_covered': len(monthly_counts),
            'monthly_breakdown': {
                month: {
                    'partition_count': count,
                    'total_size_mb': monthly_sizes[month] / (1024**2)
                }
                for month, count in sorted(monthly_counts.items())
            }
        }
    
    def _analyze_top_dimensions(self, partitions: List[PartitionInfo]) -> Dict:
        """Analyze top values in each dimension"""
        dimensions = {
            'payers': defaultdict(int),
            'states': defaultdict(int),
            'procedure_sets': defaultdict(int),
            'billing_classes': defaultdict(int)
        }
        
        for p in partitions:
            if p.payer_slug:
                dimensions['payers'][p.payer_slug] += 1
            if p.state:
                dimensions['states'][p.state] += 1
            if p.procedure_set:
                dimensions['procedure_sets'][p.procedure_set] += 1
            if p.billing_class:
                dimensions['billing_classes'][p.billing_class] += 1
        
        return {
            dimension: dict(sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10])
            for dimension, counts in dimensions.items()
        }
    
    def export_inventory(self, partitions: List[PartitionInfo], 
                        output_format: str = 'json',
                        output_file: Optional[str] = None) -> str:
        """Export partition inventory to file"""
        
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'partition_inventory_{timestamp}.{output_format}'
        
        if output_format == 'json':
            with open(output_file, 'w') as f:
                json.dump({
                    'metadata': {
                        'bucket': self.bucket_name,
                        'prefix': self.prefix,
                        'scan_timestamp': datetime.now().isoformat(),
                        'stats': self.stats
                    },
                    'partitions': [p.to_dict() for p in partitions],
                    'analysis': self.analyze_partitions(partitions)
                }, f, indent=2, default=str)
        
        elif output_format == 'csv':
            with open(output_file, 'w', newline='') as f:
                writer = csv.writer(f)
                # Header
                writer.writerow([
                    'partition_path', 'payer_slug', 'state', 'billing_class',
                    'procedure_set', 'procedure_class', 'taxonomy_code',
                    'stat_area_name', 'year', 'month', 'file_size_mb',
                    'last_modified', 'estimated_records'
                ])
                # Data
                for p in partitions:
                    writer.writerow([
                        p.partition_path, p.payer_slug, p.state, p.billing_class,
                        p.procedure_set, p.procedure_class, p.taxonomy_code,
                        p.stat_area_name, p.year, p.month,
                        p.file_size_bytes / (1024**2),  # Convert to MB
                        p.last_modified.isoformat(),
                        p.record_count_estimate
                    ])
        
        print(f"📁 Inventory exported to: {output_file}")
        return output_file
    
    def get_cost_estimate(self) -> Dict:
        """Estimate AWS costs for this scan"""
        # S3 pricing (approximate)
        list_cost_per_1000 = 0.0004  # USD
        
        list_requests = self.stats['api_calls']
        estimated_cost = (list_requests / 1000) * list_cost_per_1000
        
        return {
            'api_calls_made': list_requests,
            'estimated_cost_usd': round(estimated_cost, 6),
            'note': 'Actual costs may vary by region and usage tier'
        }
    
    def create_navigation_database(self, partitions: List[PartitionInfo], 
                                 dim_npi_path: str = None,
                                 output_db: str = None) -> str:
        """
        Create SQLite database with navigation tables for partition discovery
        
        Args:
            partitions: List of discovered partitions
            dim_npi_path: Path to dim_npi.parquet file for taxonomy descriptions
            output_db: Output database filename (auto-generated if not specified)
            
        Returns:
            Path to created database file
        """
        if not output_db:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_db = f'partition_navigation_{timestamp}.db'
        
        # Auto-detect dim_npi.parquet if not provided
        if not dim_npi_path:
            # Try common locations relative to current directory
            possible_paths = [
                '../../data/dims/dim_npi.parquet',  # From ETL/utils/
                '../data/dims/dim_npi.parquet',     # From ETL/
                'data/dims/dim_npi.parquet',        # From project root
                './data/dims/dim_npi.parquet'       # Current directory
            ]
            
            for path in possible_paths:
                if Path(path).exists():
                    dim_npi_path = path
                    print(f"🔍 Auto-detected dim_npi.parquet: {dim_npi_path}")
                    break
        
        print(f"🗄️  Creating navigation database: {output_db}")
        
        # Create database connection
        conn = sqlite3.connect(output_db)
        cursor = conn.cursor()
        
        try:
            # Create main partitions table
            self._create_partitions_table(cursor)
            
            # Create dimension tables
            self._create_dimension_tables(cursor)
            
            # Create taxonomy lookup table
            self._create_taxonomy_table(cursor, dim_npi_path)
            
            # Insert partition data
            self._insert_partition_data(cursor, partitions)
            
            # Create indexes for performance
            self._create_indexes(cursor)
            
            # Create views for easy navigation
            self._create_navigation_views(cursor)
            
            conn.commit()
            print(f"✅ Database created successfully with {len(partitions)} partitions")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error creating database: {e}")
            raise
        finally:
            conn.close()
        
        return output_db
    
    def _create_partitions_table(self, cursor):
        """Create main partitions table"""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS partitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                partition_path TEXT UNIQUE NOT NULL,
                payer_slug TEXT,
                state TEXT,
                billing_class TEXT,
                procedure_set TEXT,
                procedure_class TEXT,
                taxonomy_code TEXT,
                taxonomy_desc TEXT,
                stat_area_name TEXT,
                year INTEGER,
                month INTEGER,
                file_size_bytes INTEGER,
                file_size_mb REAL,
                last_modified TEXT,
                estimated_records INTEGER,
                s3_bucket TEXT,
                s3_key TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    def _create_dimension_tables(self, cursor):
        """Create dimension lookup tables"""
        # Payers dimension
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_payers (
                payer_slug TEXT PRIMARY KEY,
                payer_display_name TEXT,
                partition_count INTEGER,
                total_size_mb REAL
            )
        """)
        
        # States dimension
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_states (
                state_code TEXT PRIMARY KEY,
                state_name TEXT,
                partition_count INTEGER,
                total_size_mb REAL
            )
        """)
        
        # Billing classes dimension
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_billing_classes (
                billing_class TEXT PRIMARY KEY,
                partition_count INTEGER,
                total_size_mb REAL
            )
        """)
        
        # Procedure sets dimension
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_procedure_sets (
                procedure_set TEXT PRIMARY KEY,
                partition_count INTEGER,
                total_size_mb REAL
            )
        """)
        
        # Statistical areas dimension
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_stat_areas (
                stat_area_name TEXT PRIMARY KEY,
                partition_count INTEGER,
                total_size_mb REAL
            )
        """)
        
        # Time periods dimension
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_time_periods (
                year INTEGER,
                month INTEGER,
                year_month TEXT,
                partition_count INTEGER,
                total_size_mb REAL,
                PRIMARY KEY (year, month)
            )
        """)
    
    def _create_taxonomy_table(self, cursor, dim_npi_path: str = None):
        """Create taxonomy lookup table with descriptions"""
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_taxonomies (
                taxonomy_code TEXT PRIMARY KEY,
                taxonomy_desc TEXT,
                partition_count INTEGER,
                total_size_mb REAL
            )
        """)
        
        # Load taxonomy descriptions from dim_npi if available
        if dim_npi_path and Path(dim_npi_path).exists():
            try:
                print(f"📋 Loading taxonomy descriptions from {dim_npi_path}")
                df_npi = pd.read_parquet(dim_npi_path)
                
                # Extract unique taxonomy codes and descriptions
                taxonomy_data = df_npi[['primary_taxonomy_code', 'primary_taxonomy_desc']].drop_duplicates()
                taxonomy_data = taxonomy_data.dropna(subset=['primary_taxonomy_code'])
                
                # Insert taxonomy data
                for _, row in taxonomy_data.iterrows():
                    cursor.execute("""
                        INSERT OR IGNORE INTO dim_taxonomies (taxonomy_code, taxonomy_desc)
                        VALUES (?, ?)
                    """, (row['primary_taxonomy_code'], row['primary_taxonomy_desc']))
                
                print(f"✅ Loaded {len(taxonomy_data)} taxonomy descriptions")
                
            except Exception as e:
                print(f"⚠️  Warning: Could not load taxonomy descriptions: {e}")
    
    def _insert_partition_data(self, cursor, partitions: List[PartitionInfo]):
        """Insert partition data into database"""
        print(f"📊 Inserting {len(partitions)} partitions into database...")
        
        for partition in partitions:
            # Extract S3 bucket and key from partition path
            s3_parts = partition.partition_path.split('/', 1)
            s3_bucket = s3_parts[0] if len(s3_parts) > 0 else ''
            s3_key = s3_parts[1] if len(s3_parts) > 1 else partition.partition_path
            
            # Get taxonomy description
            taxonomy_desc = None
            if partition.taxonomy_code:
                cursor.execute("""
                    SELECT taxonomy_desc FROM dim_taxonomies 
                    WHERE taxonomy_code = ?
                """, (partition.taxonomy_code,))
                result = cursor.fetchone()
                taxonomy_desc = result[0] if result else None
            
            # Insert partition
            cursor.execute("""
                INSERT OR REPLACE INTO partitions (
                    partition_path, payer_slug, state, billing_class, procedure_set,
                    procedure_class, taxonomy_code, taxonomy_desc, stat_area_name,
                    year, month, file_size_bytes, file_size_mb, last_modified,
                    estimated_records, s3_bucket, s3_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                partition.partition_path,
                partition.payer_slug,
                partition.state,
                partition.billing_class,
                partition.procedure_set,
                partition.procedure_class,
                partition.taxonomy_code,
                taxonomy_desc,
                partition.stat_area_name,
                partition.year,
                partition.month,
                partition.file_size_bytes,
                partition.file_size_bytes / (1024**2),  # Convert to MB
                partition.last_modified.isoformat(),
                partition.record_count_estimate,
                s3_bucket,
                s3_key
            ))
        
        # Populate dimension tables
        self._populate_dimension_tables(cursor)
    
    def _populate_dimension_tables(self, cursor):
        """Populate dimension tables with aggregated data"""
        print("📈 Populating dimension tables...")
        
        # Payers
        cursor.execute("""
            INSERT OR REPLACE INTO dim_payers (payer_slug, payer_display_name, partition_count, total_size_mb)
            SELECT 
                payer_slug,
                REPLACE(REPLACE(payer_slug, '-', ' '), '_', ' ') as payer_display_name,
                COUNT(*) as partition_count,
                SUM(file_size_mb) as total_size_mb
            FROM partitions 
            WHERE payer_slug IS NOT NULL
            GROUP BY payer_slug
        """)
        
        # States
        cursor.execute("""
            INSERT OR REPLACE INTO dim_states (state_code, state_name, partition_count, total_size_mb)
            SELECT 
                state,
                state as state_name,
                COUNT(*) as partition_count,
                SUM(file_size_mb) as total_size_mb
            FROM partitions 
            WHERE state IS NOT NULL
            GROUP BY state
        """)
        
        # Billing classes
        cursor.execute("""
            INSERT OR REPLACE INTO dim_billing_classes (billing_class, partition_count, total_size_mb)
            SELECT 
                billing_class,
                COUNT(*) as partition_count,
                SUM(file_size_mb) as total_size_mb
            FROM partitions 
            WHERE billing_class IS NOT NULL
            GROUP BY billing_class
        """)
        
        # Procedure sets
        cursor.execute("""
            INSERT OR REPLACE INTO dim_procedure_sets (procedure_set, partition_count, total_size_mb)
            SELECT 
                procedure_set,
                COUNT(*) as partition_count,
                SUM(file_size_mb) as total_size_mb
            FROM partitions 
            WHERE procedure_set IS NOT NULL
            GROUP BY procedure_set
        """)
        
        # Statistical areas
        cursor.execute("""
            INSERT OR REPLACE INTO dim_stat_areas (stat_area_name, partition_count, total_size_mb)
            SELECT 
                stat_area_name,
                COUNT(*) as partition_count,
                SUM(file_size_mb) as total_size_mb
            FROM partitions 
            WHERE stat_area_name IS NOT NULL
            GROUP BY stat_area_name
        """)
        
        # Time periods
        cursor.execute("""
            INSERT OR REPLACE INTO dim_time_periods (year, month, year_month, partition_count, total_size_mb)
            SELECT 
                year,
                month,
                printf('%04d-%02d', year, month) as year_month,
                COUNT(*) as partition_count,
                SUM(file_size_mb) as total_size_mb
            FROM partitions 
            WHERE year IS NOT NULL AND month IS NOT NULL
            GROUP BY year, month
        """)
        
        # Update taxonomy counts
        cursor.execute("""
            UPDATE dim_taxonomies 
            SET partition_count = (
                SELECT COUNT(*) FROM partitions 
                WHERE partitions.taxonomy_code = dim_taxonomies.taxonomy_code
            ),
            total_size_mb = (
                SELECT SUM(file_size_mb) FROM partitions 
                WHERE partitions.taxonomy_code = dim_taxonomies.taxonomy_code
            )
        """)
    
    def _create_indexes(self, cursor):
        """Create indexes for performance"""
        print("🔍 Creating database indexes...")
        
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_partitions_payer ON partitions(payer_slug)",
            "CREATE INDEX IF NOT EXISTS idx_partitions_state ON partitions(state)",
            "CREATE INDEX IF NOT EXISTS idx_partitions_taxonomy ON partitions(taxonomy_code)",
            "CREATE INDEX IF NOT EXISTS idx_partitions_time ON partitions(year, month)",
            "CREATE INDEX IF NOT EXISTS idx_partitions_billing_class ON partitions(billing_class)",
            "CREATE INDEX IF NOT EXISTS idx_partitions_procedure_set ON partitions(procedure_set)",
            "CREATE INDEX IF NOT EXISTS idx_partitions_stat_area ON partitions(stat_area_name)"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
    
    def _create_navigation_views(self, cursor):
        """Create views for easy navigation"""
        print("👁️  Creating navigation views...")
        
        # Main navigation view with all dimensions
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS v_partition_navigation AS
            SELECT 
                p.id,
                p.partition_path,
                p.s3_bucket,
                p.s3_key,
                p.payer_slug,
                dp.payer_display_name,
                p.state,
                ds.state_name,
                p.billing_class,
                p.procedure_set,
                p.procedure_class,
                p.taxonomy_code,
                p.taxonomy_desc,
                p.stat_area_name,
                p.year,
                p.month,
                printf('%04d-%02d', p.year, p.month) as year_month,
                p.file_size_mb,
                p.estimated_records,
                p.last_modified
            FROM partitions p
            LEFT JOIN dim_payers dp ON p.payer_slug = dp.payer_slug
            LEFT JOIN dim_states ds ON p.state = ds.state_code
        """)
        
        # Summary view for dashboard
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS v_partition_summary AS
            SELECT 
                payer_slug,
                state,
                billing_class,
                procedure_set,
                COUNT(*) as partition_count,
                SUM(file_size_mb) as total_size_mb,
                SUM(estimated_records) as total_estimated_records,
                MIN(last_modified) as earliest_partition,
                MAX(last_modified) as latest_partition
            FROM partitions
            GROUP BY payer_slug, state, billing_class, procedure_set
            ORDER BY total_size_mb DESC
        """)
        
        # Taxonomy summary view
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS v_taxonomy_summary AS
            SELECT 
                t.taxonomy_code,
                t.taxonomy_desc,
                COUNT(p.id) as partition_count,
                SUM(p.file_size_mb) as total_size_mb,
                SUM(p.estimated_records) as total_estimated_records
            FROM dim_taxonomies t
            LEFT JOIN partitions p ON t.taxonomy_code = p.taxonomy_code
            GROUP BY t.taxonomy_code, t.taxonomy_desc
            ORDER BY partition_count DESC
        """)
    
    def query_partitions(self, db_path: str, 
                        payer_slug: str = None,
                        state: str = None,
                        taxonomy_code: str = None,
                        year: int = None,
                        month: int = None) -> List[Dict]:
        """
        Query partitions from the database
        
        Args:
            db_path: Path to SQLite database
            payer_slug: Filter by payer
            state: Filter by state
            taxonomy_code: Filter by taxonomy code
            year: Filter by year
            month: Filter by month
            
        Returns:
            List of matching partitions
        """
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            # Build query
            where_conditions = []
            params = []
            
            if payer_slug:
                where_conditions.append("payer_slug = ?")
                params.append(payer_slug)
            
            if state:
                where_conditions.append("state = ?")
                params.append(state)
            
            if taxonomy_code:
                where_conditions.append("taxonomy_code = ?")
                params.append(taxonomy_code)
            
            if year:
                where_conditions.append("year = ?")
                params.append(year)
            
            if month:
                where_conditions.append("month = ?")
                params.append(month)
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
                SELECT * FROM v_partition_navigation 
                {where_clause}
                ORDER BY file_size_mb DESC
            """
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            return [dict(row) for row in rows]
            
        finally:
            conn.close()

def main():
    """CLI interface for partition inventory"""
    parser = argparse.ArgumentParser(description='S3 Healthcare Partition Inventory Tool with SQLite Database')
    parser.add_argument('bucket', help='S3 bucket name')
    parser.add_argument('--prefix', default='partitioned-data', help='S3 prefix (default: partitioned-data)')
    parser.add_argument('--region', default='us-east-1', help='AWS region (default: us-east-1)')
    parser.add_argument('--output-format', choices=['json', 'csv', 'db'], default='json', 
                       help='Output format (default: json)')
    parser.add_argument('--output-file', help='Output filename (auto-generated if not specified)')
    parser.add_argument('--max-keys', type=int, default=1000, 
                       help='Max keys per API request (default: 1000)')
    parser.add_argument('--include-empty', action='store_true', 
                       help='Include empty partitions')
    parser.add_argument('--quiet', action='store_true', help='Suppress progress output')
    parser.add_argument('--dim-npi-path', help='Path to dim_npi.parquet for taxonomy descriptions (auto-detected if not provided)')
    parser.add_argument('--create-db', action='store_true', 
                       help='Create SQLite database with navigation tables')
    
    args = parser.parse_args()
    
    # Initialize inventory scanner
    inventory = S3PartitionInventory(
        bucket_name=args.bucket,
        region=args.region,
        prefix=args.prefix
    )
    
    try:
        # Discover partitions
        partitions = inventory.discover_partitions(
            max_keys_per_request=args.max_keys,
            include_empty=args.include_empty
        )
        
        if not partitions:
            print("❌ No partitions found")
            return
        
        # Analyze partitions
        analysis = inventory.analyze_partitions(partitions)
        
        if not args.quiet:
            print("\n📊 PARTITION ANALYSIS")
            print("=" * 50)
            print(f"Total partitions: {analysis['summary']['total_partitions']:,}")
            print(f"Total size: {analysis['summary']['total_size_gb']:.2f} GB")
            print(f"Estimated records: {analysis['summary']['estimated_total_records']:,}")
            print(f"Unique payers: {analysis['dimensions']['payers']}")
            print(f"Unique states: {analysis['dimensions']['states']}")
            print(f"Time periods: {analysis['dimensions']['time_periods']}")
            
            # Cost estimate
            cost_info = inventory.get_cost_estimate()
            print(f"\n💰 COST ESTIMATE")
            print("=" * 30)
            print(f"API calls: {cost_info['api_calls_made']}")
            print(f"Estimated cost: ${cost_info['estimated_cost_usd']:.6f}")
        
        # Export results
        if args.create_db or args.output_format == 'db':
            # Create SQLite database
            db_file = inventory.create_navigation_database(
                partitions,
                dim_npi_path=args.dim_npi_path,
                output_db=args.output_file
            )
            print(f"\n✅ Database created: {db_file}")
            
            # Show database summary
            if not args.quiet:
                conn = sqlite3.connect(db_file)
                cursor = conn.cursor()
                
                # Get table counts
                cursor.execute("SELECT COUNT(*) FROM partitions")
                partition_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM dim_taxonomies")
                taxonomy_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM dim_payers")
                payer_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM dim_states")
                state_count = cursor.fetchone()[0]
                
                print(f"\n📊 DATABASE SUMMARY")
                print("=" * 40)
                print(f"Partitions: {partition_count:,}")
                print(f"Taxonomies: {taxonomy_count:,}")
                print(f"Payers: {payer_count:,}")
                print(f"States: {state_count:,}")
                
                # Show sample queries
                print(f"\n🔍 SAMPLE QUERIES")
                print("=" * 40)
                print("-- Find partitions by payer and state:")
                print("SELECT * FROM v_partition_navigation WHERE payer_slug = 'unitedhealthcare-of-georgia-inc' AND state = 'GA';")
                print("\n-- Find partitions by taxonomy:")
                print("SELECT * FROM v_partition_navigation WHERE taxonomy_code = '101YP2500X';")
                print("\n-- Get taxonomy summary:")
                print("SELECT * FROM v_taxonomy_summary ORDER BY partition_count DESC LIMIT 10;")
                print("\n-- Get partition summary by payer/state:")
                print("SELECT * FROM v_partition_summary ORDER BY total_size_mb DESC LIMIT 10;")
                
                conn.close()
        else:
            # Export to JSON/CSV
            output_file = inventory.export_inventory(
                partitions, 
                args.output_format, 
                args.output_file
            )
            print(f"\n✅ Inventory complete! Results saved to {output_file}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())