#!/usr/bin/env python3
"""
Partition Navigator Template
A reusable template for implementing partition navigation systems
"""

import sqlite3
import pandas as pd
import io
from pathlib import Path
from typing import Dict, List, Optional, Any
import os

class PartitionNavigatorTemplate:
    """
    Template class for partition navigation systems
    Adapt this for your specific use case
    """
    
    def __init__(self, db_path: str, storage_config: Dict[str, Any]):
        self.db_path = db_path
        self.storage_config = storage_config
        self.conn = None
        self.storage_client = None
        
        # Configuration
        self.required_filters = storage_config.get('required_filters', [])
        self.optional_filters = storage_config.get('optional_filters', [])
        self.temporal_filters = storage_config.get('temporal_filters', [])
        self.max_rows = storage_config.get('max_rows', 10000)
    
    def connect_db(self):
        """Connect to the partition navigation database"""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
        return self.conn
    
    def connect_storage(self):
        """Connect to storage system (implement for your storage type)"""
        # Example implementations:
        
        # For S3:
        # import boto3
        # if self.storage_client is None:
        #     self.storage_client = boto3.client('s3', region_name=self.storage_config['region'])
        
        # For Google Cloud Storage:
        # from google.cloud import storage
        # if self.storage_client is None:
        #     self.storage_client = storage.Client()
        
        # For Azure Blob Storage:
        # from azure.storage.blob import BlobServiceClient
        # if self.storage_client is None:
        #     self.storage_client = BlobServiceClient.from_connection_string(self.storage_config['connection_string'])
        
        # For local filesystem:
        # self.storage_client = None  # Use direct file access
        
        raise NotImplementedError("Implement connect_storage for your storage system")
    
    def get_filter_options(self) -> Dict[str, List]:
        """Get all available filter options from dimension tables"""
        conn = self.connect_db()
        cursor = conn.cursor()
        
        options = {}
        
        # Define your dimension tables and their queries
        dimension_queries = {
            'payers': 'SELECT DISTINCT payer_slug, payer_display_name FROM dim_payers ORDER BY payer_display_name',
            'states': 'SELECT DISTINCT state_code, state_name FROM dim_states ORDER BY state_name',
            'billing_classes': 'SELECT DISTINCT billing_class FROM dim_billing_classes ORDER BY billing_class',
            'procedure_sets': 'SELECT DISTINCT procedure_set FROM dim_procedure_sets ORDER BY procedure_set',
            'taxonomies': 'SELECT DISTINCT taxonomy_code, taxonomy_desc FROM dim_taxonomies WHERE taxonomy_desc IS NOT NULL ORDER BY taxonomy_desc',
            'stat_areas': 'SELECT DISTINCT stat_area_name FROM dim_stat_areas ORDER BY stat_area_name',
            'years': 'SELECT DISTINCT year FROM dim_time_periods ORDER BY year',
            'months': 'SELECT DISTINCT month FROM dim_time_periods ORDER BY month'
        }
        
        for key, query in dimension_queries.items():
            try:
                cursor.execute(query)
                options[key] = cursor.fetchall()
            except Exception as e:
                print(f"Warning: Could not load {key} options: {e}")
                options[key] = []
        
        return options
    
    def search_partitions(self, filters: Dict[str, Any], require_top_levels: bool = True) -> pd.DataFrame:
        """
        Search partitions based on filters with hierarchical requirements
        
        Args:
            filters: Dictionary of filter criteria
            require_top_levels: Whether to enforce required filter validation
            
        Returns:
            DataFrame of matching partitions
        """
        conn = self.connect_db()
        
        # Build WHERE clause
        where_conditions = []
        params = []
        
        # Apply required filters
        for filter_key in self.required_filters:
            if filters.get(filter_key):
                where_conditions.append(f"p.{filter_key} = ?")
                params.append(filters[filter_key])
            elif require_top_levels:
                # Return empty if required filters missing
                return pd.DataFrame()
        
        # Apply optional filters
        for filter_key in self.optional_filters + self.temporal_filters:
            if filters.get(filter_key):
                where_conditions.append(f"p.{filter_key} = ?")
                params.append(filters[filter_key])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Build query (customize based on your schema)
        query = f"""
            SELECT 
                p.id,
                p.partition_path,
                p.s3_bucket,
                p.s3_key,
                p.payer_slug,
                dp.payer_display_name,
                p.state,
                p.billing_class,
                p.procedure_set,
                p.taxonomy_code,
                p.taxonomy_desc,
                p.file_size_mb,
                p.estimated_records,
                p.last_modified
            FROM partitions p
            LEFT JOIN dim_payers dp ON p.payer_slug = dp.payer_slug
            {where_clause}
            ORDER BY p.file_size_mb DESC
            LIMIT 1000
        """
        
        return pd.read_sql_query(query, conn, params=params)
    
    def load_partition_data(self, partition_path: str) -> Optional[pd.DataFrame]:
        """
        Load data from a single partition
        
        Args:
            partition_path: Path to the partition (S3, GCS, local, etc.)
            
        Returns:
            DataFrame with partition data or None if error
        """
        try:
            # Parse partition path based on your storage system
            if partition_path.startswith('s3://'):
                return self._load_from_s3(partition_path)
            elif partition_path.startswith('gs://'):
                return self._load_from_gcs(partition_path)
            elif partition_path.startswith('az://'):
                return self._load_from_azure(partition_path)
            else:
                return self._load_from_local(partition_path)
        except Exception as e:
            print(f"Error loading partition {partition_path}: {e}")
            return None
    
    def _load_from_s3(self, s3_path: str) -> pd.DataFrame:
        """Load data from S3 (implement based on your needs)"""
        # Example implementation:
        # s3_client = self.connect_storage()
        # if s3_path.startswith('s3://'):
        #     s3_path = s3_path[5:]
        # bucket, key = s3_path.split('/', 1)
        # response = s3_client.get_object(Bucket=bucket, Key=key)
        # return pd.read_parquet(io.BytesIO(response['Body'].read()))
        raise NotImplementedError("Implement S3 loading")
    
    def _load_from_gcs(self, gcs_path: str) -> pd.DataFrame:
        """Load data from Google Cloud Storage"""
        raise NotImplementedError("Implement GCS loading")
    
    def _load_from_azure(self, azure_path: str) -> pd.DataFrame:
        """Load data from Azure Blob Storage"""
        raise NotImplementedError("Implement Azure loading")
    
    def _load_from_local(self, local_path: str) -> pd.DataFrame:
        """Load data from local filesystem"""
        return pd.read_parquet(local_path)
    
    def combine_partitions_for_analysis(self, partition_paths: List[str], max_rows: int = None) -> Optional[pd.DataFrame]:
        """
        Combine multiple partitions in memory for analysis
        
        Args:
            partition_paths: List of partition paths to combine
            max_rows: Maximum rows to load (uses config default if None)
            
        Returns:
            Combined DataFrame or None if error
        """
        if not partition_paths:
            return None
        
        max_rows = max_rows or self.max_rows
        
        try:
            combined_dfs = []
            total_rows = 0
            
            print(f"🔄 Combining {len(partition_paths)} partitions...")
            
            for i, partition_path in enumerate(partition_paths):
                if total_rows >= max_rows:
                    print(f"⚠️  Reached max rows limit ({max_rows}), stopping at partition {i+1}")
                    break
                
                df = self.load_partition_data(partition_path)
                if df is not None:
                    # Add partition metadata
                    df['_partition_source'] = partition_path
                    df['_partition_index'] = i
                    
                    combined_dfs.append(df)
                    total_rows += len(df)
                    
                    print(f"   ✅ Loaded partition {i+1}/{len(partition_paths)}: {len(df)} rows")
                else:
                    print(f"   ❌ Failed to load partition {i+1}: {partition_path}")
            
            if combined_dfs:
                combined_df = pd.concat(combined_dfs, ignore_index=True)
                print(f"🎉 Successfully combined {len(combined_dfs)} partitions: {len(combined_df)} total rows")
                return combined_df
            else:
                print("❌ No partitions could be loaded")
                return None
                
        except Exception as e:
            print(f"❌ Error combining partitions: {e}")
            return None
    
    def get_partition_preview(self, partition_path: str, max_rows: int = 10) -> Optional[pd.DataFrame]:
        """Get a preview of partition data"""
        df = self.load_partition_data(partition_path)
        if df is not None:
            return df.head(max_rows)
        return None
    
    def analyze_combined_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze combined partition data
        
        Args:
            df: Combined DataFrame
            
        Returns:
            Dictionary with analysis results
        """
        analysis = {
            'shape': df.shape,
            'columns': list(df.columns),
            'dtypes': df.dtypes.to_dict(),
            'memory_usage': df.memory_usage(deep=True).sum(),
            'null_counts': df.isnull().sum().to_dict(),
            'numeric_summary': None,
            'categorical_summary': None
        }
        
        # Numeric summary
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            analysis['numeric_summary'] = df[numeric_cols].describe().to_dict()
        
        # Categorical summary
        categorical_cols = df.select_dtypes(include=['object']).columns
        if len(categorical_cols) > 0:
            analysis['categorical_summary'] = {}
            for col in categorical_cols:
                if df[col].nunique() < 50:  # Only for reasonable number of categories
                    analysis['categorical_summary'][col] = df[col].value_counts().head(10).to_dict()
        
        return analysis
    
    def export_data(self, df: pd.DataFrame, format: str = 'csv', filename: str = None) -> bytes:
        """
        Export DataFrame in specified format
        
        Args:
            df: DataFrame to export
            format: Export format ('csv', 'parquet', 'json')
            filename: Optional filename for the export
            
        Returns:
            Bytes of exported data
        """
        if format.lower() == 'csv':
            return df.to_csv(index=False).encode('utf-8')
        elif format.lower() == 'parquet':
            return df.to_parquet(index=False)
        elif format.lower() == 'json':
            return df.to_json(orient='records', indent=2).encode('utf-8')
        else:
            raise ValueError(f"Unsupported format: {format}")


# Example usage and configuration
def create_example_config():
    """Example configuration for different use cases"""
    
    # Healthcare example
    healthcare_config = {
        'required_filters': ['payer_slug', 'state', 'billing_class'],
        'optional_filters': ['procedure_set', 'taxonomy_code', 'stat_area_name'],
        'temporal_filters': ['year', 'month'],
        'max_rows': 10000,
        'storage_type': 's3',
        'bucket': 'healthcare-data-lake',
        'region': 'us-east-1'
    }
    
    # E-commerce example
    ecommerce_config = {
        'required_filters': ['category', 'region', 'channel'],
        'optional_filters': ['brand', 'product_type', 'price_tier'],
        'temporal_filters': ['year', 'quarter', 'month'],
        'max_rows': 50000,
        'storage_type': 'gcs',
        'bucket': 'ecommerce-analytics',
        'project': 'my-gcp-project'
    }
    
    # IoT example
    iot_config = {
        'required_filters': ['device_type', 'location', 'sensor_type'],
        'optional_filters': ['manufacturer', 'model', 'firmware_version'],
        'temporal_filters': ['year', 'month', 'day'],
        'max_rows': 100000,
        'storage_type': 'azure',
        'container': 'iot-data',
        'connection_string': 'your-connection-string'
    }
    
    return {
        'healthcare': healthcare_config,
        'ecommerce': ecommerce_config,
        'iot': iot_config
    }


# Example implementation
if __name__ == "__main__":
    # Example usage
    config = create_example_config()['healthcare']
    
    navigator = PartitionNavigatorTemplate(
        db_path='partition_navigation.db',
        storage_config=config
    )
    
    # Search for partitions
    filters = {
        'payer_slug': 'unitedhealthcare',
        'state': 'GA',
        'billing_class': 'institutional',
        'taxonomy_code': '101YP2500X'
    }
    
    results = navigator.search_partitions(filters)
    print(f"Found {len(results)} partitions")
    
    # Combine partitions for analysis
    if len(results) > 0:
        partition_paths = [f"s3://{row['s3_bucket']}/{row['s3_key']}" for _, row in results.iterrows()]
        combined_df = navigator.combine_partitions_for_analysis(partition_paths)
        
        if combined_df is not None:
            analysis = navigator.analyze_combined_data(combined_df)
            print(f"Combined data shape: {analysis['shape']}")
            
            # Export data
            csv_data = navigator.export_data(combined_df, 'csv')
            print(f"Exported {len(csv_data)} bytes of CSV data")
