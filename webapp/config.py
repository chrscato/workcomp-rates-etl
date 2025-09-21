#!/usr/bin/env python3
"""
Configuration file for the Healthcare Partition Navigator webapp
"""

import os
from pathlib import Path

# App configuration
APP_CONFIG = {
    'title': 'Healthcare Partition Navigator',
    'icon': '🏥',
    'layout': 'wide',
    'initial_sidebar_state': 'expanded'
}

# Database configuration
DATABASE_CONFIG = {
    'search_paths': [
        '../../ETL/utils',  # From webapp/
        '../ETL/utils',     # From project root
        'ETL/utils',        # From current directory
        '.',                # Current directory
        '..'                # Parent directory
    ],
    'file_patterns': ['*partition*.db', '*navigation*.db']
}

# S3 configuration
S3_CONFIG = {
    'default_region': 'us-east-1',
    'max_preview_rows': 10,
    'timeout_seconds': 30
}

# UI configuration
UI_CONFIG = {
    'max_search_results': 1000,
    'default_chart_height': 400,
    'enable_data_export': True,
    'enable_s3_preview': True
}

# Chart colors
CHART_COLORS = {
    'primary': '#1f77b4',
    'secondary': '#ff7f0e',
    'success': '#2ca02c',
    'warning': '#d62728',
    'info': '#9467bd'
}

def get_database_paths():
    """Get list of potential database file paths"""
    paths = []
    
    for search_path in DATABASE_CONFIG['search_paths']:
        search_dir = Path(search_path)
        if search_dir.exists():
            for pattern in DATABASE_CONFIG['file_patterns']:
                paths.extend(search_dir.glob(pattern))
    
    return [str(p) for p in paths if p.is_file()]

def get_aws_region():
    """Get AWS region from environment or config"""
    return os.getenv('AWS_DEFAULT_REGION', S3_CONFIG['default_region'])
