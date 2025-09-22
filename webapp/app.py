#!/usr/bin/env python3
"""
Healthcare Partition Navigator Webapp
A Streamlit app for navigating S3 partitioned healthcare data using the partition navigation database
"""

import streamlit as st
import sqlite3
import pandas as pd
import boto3
import io
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from typing import List, Dict, Optional
import os

# Page configuration
st.set_page_config(
    page_title="Healthcare Partition Navigator",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .partition-card {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #e0e0e0;
        margin-bottom: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .s3-path {
        font-family: monospace;
        background-color: #f8f9fa;
        padding: 0.25rem 0.5rem;
        border-radius: 0.25rem;
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)

class PartitionNavigator:
    """Main class for partition navigation functionality"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.s3_client = None
    
    def connect_db(self):
        """Connect to the partition navigation database"""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
        return self.conn
    
    def connect_s3(self):
        """Connect to S3 for file access"""
        if self.s3_client is None:
            self.s3_client = boto3.client('s3')
        return self.s3_client
    
    def get_database_stats(self) -> Dict:
        """Get overall database statistics"""
        conn = self.connect_db()
        cursor = conn.cursor()
        
        stats = {}
        
        # Get counts for each table
        tables = ['partitions', 'dim_payers', 'dim_states', 'dim_taxonomies', 
                 'dim_billing_classes', 'dim_procedure_sets', 'dim_stat_areas']
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = cursor.fetchone()[0]
        
        # Get total size
        cursor.execute("SELECT SUM(file_size_mb) FROM partitions")
        stats['total_size_mb'] = cursor.fetchone()[0] or 0
        
        # Get date range
        cursor.execute("SELECT MIN(last_modified), MAX(last_modified) FROM partitions")
        date_range = cursor.fetchone()
        stats['date_range'] = {
            'earliest': date_range[0],
            'latest': date_range[1]
        }
        
        return stats
    
    def get_filter_options(self) -> Dict:
        """Get all available filter options"""
        conn = self.connect_db()
        cursor = conn.cursor()
        
        options = {}
        
        # Get unique values for each dimension
        dimensions = {
            'payers': 'SELECT DISTINCT payer_slug, payer_display_name FROM dim_payers ORDER BY payer_display_name',
            'states': 'SELECT DISTINCT state_code, state_name FROM dim_states ORDER BY state_name',
            'billing_classes': 'SELECT DISTINCT billing_class FROM dim_billing_classes ORDER BY billing_class',
            'procedure_sets': 'SELECT DISTINCT procedure_set FROM dim_procedure_sets ORDER BY procedure_set',
            'taxonomies': 'SELECT DISTINCT taxonomy_code, taxonomy_desc FROM dim_taxonomies WHERE taxonomy_desc IS NOT NULL ORDER BY taxonomy_desc',
            'stat_areas': 'SELECT DISTINCT stat_area_name FROM dim_stat_areas ORDER BY stat_area_name',
            'years': 'SELECT DISTINCT year FROM dim_time_periods ORDER BY year',
            'months': 'SELECT DISTINCT month FROM dim_time_periods ORDER BY month'
        }
        
        for key, query in dimensions.items():
            cursor.execute(query)
            options[key] = cursor.fetchall()
        
        return options
    
    def search_partitions(self, filters: Dict, require_top_levels: bool = True) -> pd.DataFrame:
        """Search partitions based on filters with hierarchical requirements"""
        conn = self.connect_db()
        
        # Define required top-level filters
        required_filters = ['payer_slug', 'state', 'billing_class']
        
        # Build WHERE clause
        where_conditions = []
        params = []
        
        # Always apply required filters if specified
        for filter_key in required_filters:
            if filters.get(filter_key):
                where_conditions.append(f"p.{filter_key} = ?")
                params.append(filters[filter_key])
            elif require_top_levels:
                # If requiring top levels but not provided, return empty
                return pd.DataFrame()
        
        # Apply optional filters
        optional_filters = ['procedure_set', 'taxonomy_code', 'stat_area_name', 'year', 'month']
        for filter_key in optional_filters:
            if filters.get(filter_key):
                where_conditions.append(f"p.{filter_key} = ?")
                params.append(filters[filter_key])
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
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
                p.procedure_class,
                p.taxonomy_code,
                p.taxonomy_desc,
                p.stat_area_name,
                p.year,
                p.month,
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
    
    def combine_partitions_for_analysis(self, partition_paths: List[str], max_rows: int = 10000) -> Optional[pd.DataFrame]:
        """
        Combine multiple partitions in memory for analysis
        
        Args:
            partition_paths: List of S3 paths to combine
            max_rows: Maximum rows to load (for memory management)
            
        Returns:
            Combined DataFrame or None if error
        """
        if not partition_paths:
            return None
        
        try:
            s3_client = self.connect_s3()
            combined_dfs = []
            total_rows = 0
            
            print(f"🔄 Combining {len(partition_paths)} partitions...")
            
            for i, s3_path in enumerate(partition_paths):
                if total_rows >= max_rows:
                    print(f"⚠️  Reached max rows limit ({max_rows}), stopping at partition {i+1}")
                    break
                
                try:
                    # Parse S3 path
                    if s3_path.startswith('s3://'):
                        s3_path = s3_path[5:]  # Remove s3:// prefix
                    
                    bucket, key = s3_path.split('/', 1)
                    
                    # Read parquet file from S3
                    response = s3_client.get_object(Bucket=bucket, Key=key)
                    parquet_data = response['Body'].read()
                    
                    # Convert to DataFrame
                    df = pd.read_parquet(io.BytesIO(parquet_data))
                    
                    # Add partition metadata
                    df['_partition_source'] = s3_path
                    df['_partition_index'] = i
                    
                    combined_dfs.append(df)
                    total_rows += len(df)
                    
                    print(f"   ✅ Loaded partition {i+1}/{len(partition_paths)}: {len(df)} rows")
                    
                except Exception as e:
                    print(f"   ❌ Error loading partition {s3_path}: {e}")
                    continue
            
            if combined_dfs:
                # Combine all DataFrames
                combined_df = pd.concat(combined_dfs, ignore_index=True)
                print(f"🎉 Successfully combined {len(combined_dfs)} partitions: {len(combined_df)} total rows")
                return combined_df
            else:
                print("❌ No partitions could be loaded")
                return None
                
        except Exception as e:
            print(f"❌ Error combining partitions: {e}")
            return None
    
    def get_partition_preview(self, s3_bucket: str, s3_key: str, max_rows: int = 10) -> Optional[pd.DataFrame]:
        """Get a preview of the partition data from S3"""
        try:
            s3_client = self.connect_s3()
            
            # Read parquet file from S3
            response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
            parquet_data = response['Body'].read()
            
            # Convert to DataFrame
            df = pd.read_parquet(io.BytesIO(parquet_data))
            
            return df.head(max_rows)
        
        except Exception as e:
            st.error(f"Error reading partition preview: {e}")
            return None

def main():
    """Main Streamlit application"""
    
    # Header
    st.markdown('<h1 class="main-header">🏥 Healthcare Partition Navigator</h1>', unsafe_allow_html=True)
    st.markdown("Navigate and explore your S3 partitioned healthcare data with ease")
    
    # Sidebar for database selection
    st.sidebar.header("📁 Database Configuration")
    
    # Look for database files
    db_files = []
    for root, dirs, files in os.walk('.'):
        for file in files:
            if file.endswith('.db') and 'partition' in file.lower():
                db_files.append(os.path.join(root, file))
    
    if not db_files:
        st.error("No partition navigation database found. Please run the s3_partition_inventory.py script first.")
        st.stop()
    
    selected_db = st.sidebar.selectbox(
        "Select Database:",
        db_files,
        index=0
    )
    
    # Initialize navigator
    try:
        navigator = PartitionNavigator(selected_db)
        st.sidebar.success(f"✅ Connected to {os.path.basename(selected_db)}")
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        st.stop()
    
    # Get database stats
    stats = navigator.get_database_stats()
    
    # Display overview metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Partitions", f"{stats['partitions']:,}")
    
    with col2:
        st.metric("Total Size", f"{stats['total_size_mb']:.1f} MB")
    
    with col3:
        st.metric("Unique States", f"{stats['dim_states']:,}")
    
    with col4:
        st.metric("Taxonomies", f"{stats['dim_taxonomies']:,}")
    
    # Main content tabs
    tab1, tab2, tab3, tab4 = st.tabs(["🔍 Search Partitions", "📊 Analytics", "📈 Visualizations", "ℹ️ About"])
    
    with tab1:
        st.header("Search and Filter Partitions")
        
        # Hierarchical filtering explanation
        st.info("""
        **🎯 Hierarchical Filtering**: The top 3 levels (Payer, State, Billing Class) are required for analysis. 
        Additional filters are optional and will combine matching partitions for comprehensive analysis.
        """)
        
        # Get filter options
        filter_options = navigator.get_filter_options()
        
        # Create filter form
        with st.form("partition_filters"):
            # Required filters section
            st.subheader("🔴 Required Filters (Top 3 Levels)")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                payer = st.selectbox(
                    "Payer *:",
                    [None] + [f"{row[0]} ({row[1]})" for row in filter_options['payers']],
                    format_func=lambda x: x.split(' (')[0] if x else "Select Payer",
                    help="Required: Choose the insurance payer"
                )
            
            with col2:
                state = st.selectbox(
                    "State *:",
                    [None] + [f"{row[0]} ({row[1]})" for row in filter_options['states']],
                    format_func=lambda x: x.split(' (')[0] if x else "Select State",
                    help="Required: Choose the state"
                )
            
            with col3:
                billing_class = st.selectbox(
                    "Billing Class *:",
                    [None] + [row[0] for row in filter_options['billing_classes']],
                    format_func=lambda x: x or "Select Billing Class",
                    help="Required: Choose institutional or professional"
                )
            
            # Optional filters section
            st.subheader("🟡 Optional Filters (For Refinement)")
            col1, col2 = st.columns(2)
            
            with col1:
                procedure_set = st.selectbox(
                    "Procedure Set:",
                    [None] + [row[0] for row in filter_options['procedure_sets']],
                    format_func=lambda x: x or "All Procedure Sets",
                    help="Optional: Filter by procedure category"
                )
                
                taxonomy = st.selectbox(
                    "Medical Specialty:",
                    [None] + [f"{row[0]} - {row[1]}" for row in filter_options['taxonomies']],
                    format_func=lambda x: x.split(' - ')[1] if x and ' - ' in x else (x or "All Specialties"),
                    help="Optional: Filter by medical specialty"
                )
            
            with col2:
                stat_area = st.selectbox(
                    "Statistical Area:",
                    [None] + [row[0] for row in filter_options['stat_areas']],
                    format_func=lambda x: x or "All Areas",
                    help="Optional: Filter by geographic area"
                )
                
                year = st.selectbox(
                    "Year:",
                    [None] + [row[0] for row in filter_options['years']],
                    format_func=lambda x: str(x) if x else "All Years",
                    help="Optional: Filter by year"
                )
                
                month = st.selectbox(
                    "Month:",
                    [None] + [row[0] for row in filter_options['months']],
                    format_func=lambda x: str(x) if x else "All Months",
                    help="Optional: Filter by month"
                )
            
            # Analysis options
            st.subheader("📊 Analysis Options")
            col1, col2 = st.columns(2)
            
            with col1:
                combine_partitions = st.checkbox(
                    "Combine Multiple Partitions for Analysis",
                    value=True,
                    help="When multiple partitions match, combine them for comprehensive analysis"
                )
            
            with col2:
                max_rows = st.number_input(
                    "Max Rows to Load",
                    min_value=1000,
                    max_value=100000,
                    value=10000,
                    step=1000,
                    help="Maximum number of rows to load for memory management"
                )
            
            submitted = st.form_submit_button("🔍 Search & Analyze Partitions", use_container_width=True)
        
        # Process filters
        if submitted:
            # Validate required filters
            if not all([payer, state, billing_class]):
                st.error("❌ Please select all required filters: Payer, State, and Billing Class")
                st.stop()
            
            filters = {}
            
            # Required filters
            filters['payer_slug'] = payer.split(' (')[0]
            filters['state'] = state.split(' (')[0]
            filters['billing_class'] = billing_class
            
            # Optional filters
            if procedure_set:
                filters['procedure_set'] = procedure_set
            
            if taxonomy:
                filters['taxonomy_code'] = taxonomy.split(' - ')[0]
            
            if stat_area:
                filters['stat_area_name'] = stat_area
            
            if year:
                filters['year'] = int(year)
            
            if month:
                filters['month'] = int(month)
            
            # Search partitions
            results_df = navigator.search_partitions(filters, require_top_levels=True)
            
            if not results_df.empty:
                st.success(f"Found {len(results_df)} partitions matching your criteria")
                
                # Display partition summary
                total_size = results_df['file_size_mb'].sum()
                total_records = results_df['estimated_records'].sum()
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Partitions", len(results_df))
                with col2:
                    st.metric("Total Size", f"{total_size:.1f} MB")
                with col3:
                    st.metric("Estimated Records", f"{total_records:,}")
                
                # Show partition details
                st.subheader("📁 Partition Details")
                for idx, row in results_df.iterrows():
                    with st.container():
                        st.markdown(f"""
                        <div class="partition-card">
                            <h4>Partition {row['id']}</h4>
                            <p><strong>Payer:</strong> {row['payer_display_name'] or row['payer_slug']}</p>
                            <p><strong>Location:</strong> {row['state']} - {row['stat_area_name']}</p>
                            <p><strong>Specialty:</strong> {row['taxonomy_desc'] or row['taxonomy_code']}</p>
                            <p><strong>Billing:</strong> {row['billing_class']} | {row['procedure_set']}</p>
                            <p><strong>Size:</strong> {row['file_size_mb']:.2f} MB | <strong>Records:</strong> {row['estimated_records']:,}</p>
                            <p><strong>S3 Path:</strong> <span class="s3-path">s3://{row['s3_bucket']}/{row['s3_key']}</span></p>
                        </div>
                        """, unsafe_allow_html=True)
                
                # Combined analysis section
                if combine_partitions and len(results_df) > 1:
                    st.subheader("🔄 Combined Analysis")
                    
                    if st.button("🚀 Load & Combine All Partitions for Analysis", type="primary"):
                        with st.spinner("Loading and combining partitions..."):
                            # Get S3 paths
                            s3_paths = [f"s3://{row['s3_bucket']}/{row['s3_key']}" for _, row in results_df.iterrows()]
                            
                            # Combine partitions
                            combined_df = navigator.combine_partitions_for_analysis(s3_paths, max_rows)
                            
                            if combined_df is not None:
                                st.success(f"✅ Successfully combined {len(results_df)} partitions into {len(combined_df)} rows")
                                
                                # Show combined data summary
                                st.subheader("📊 Combined Data Summary")
                                
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    st.write("**Data Shape:**")
                                    st.write(f"- Rows: {len(combined_df):,}")
                                    st.write(f"- Columns: {len(combined_df.columns)}")
                                    
                                    st.write("**Column Names:**")
                                    st.write(list(combined_df.columns))
                                
                                with col2:
                                    st.write("**Data Types:**")
                                    st.write(combined_df.dtypes)
                                
                                # Show sample data
                                st.subheader("👁️ Sample Data")
                                st.dataframe(combined_df.head(20))
                                
                                # Download options
                                st.subheader("💾 Download Options")
                                
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    # Download as CSV
                                    csv = combined_df.to_csv(index=False)
                                    st.download_button(
                                        label="📄 Download as CSV",
                                        data=csv,
                                        file_name=f"combined_analysis_{filters['payer_slug']}_{filters['state']}_{filters['billing_class']}.csv",
                                        mime="text/csv"
                                    )
                                
                                with col2:
                                    # Download as Parquet
                                    parquet_bytes = combined_df.to_parquet(index=False)
                                    st.download_button(
                                        label="📦 Download as Parquet",
                                        data=parquet_bytes,
                                        file_name=f"combined_analysis_{filters['payer_slug']}_{filters['state']}_{filters['billing_class']}.parquet",
                                        mime="application/octet-stream"
                                    )
                                
                                # Quick analysis
                                st.subheader("📈 Quick Analysis")
                                
                                # Show value counts for categorical columns
                                categorical_cols = combined_df.select_dtypes(include=['object']).columns
                                if len(categorical_cols) > 0:
                                    for col in categorical_cols[:5]:  # Show first 5 categorical columns
                                        if combined_df[col].nunique() < 20:  # Only show if reasonable number of unique values
                                            st.write(f"**{col} Distribution:**")
                                            value_counts = combined_df[col].value_counts().head(10)
                                            st.bar_chart(value_counts)
                                
                                # Show numeric summary
                                numeric_cols = combined_df.select_dtypes(include=['number']).columns
                                if len(numeric_cols) > 0:
                                    st.write("**Numeric Summary:**")
                                    st.dataframe(combined_df[numeric_cols].describe())
                            
                            else:
                                st.error("❌ Failed to combine partitions. Check AWS credentials and S3 access.")
                
                # Individual partition preview
                st.subheader("👁️ Individual Partition Preview")
                for idx, row in results_df.iterrows():
                    if st.button(f"Preview Partition {row['id']}", key=f"preview_{row['id']}"):
                        with st.spinner("Loading partition preview..."):
                            preview_df = navigator.get_partition_preview(row['s3_bucket'], row['s3_key'])
                            if preview_df is not None:
                                st.subheader(f"Partition {row['id']} Preview")
                                st.dataframe(preview_df)
                                
                                # Download button
                                csv = preview_df.to_csv(index=False)
                                st.download_button(
                                    label="Download Preview as CSV",
                                    data=csv,
                                    file_name=f"partition_{row['id']}_preview.csv",
                                    mime="text/csv"
                                )
            else:
                st.warning("No partitions found matching your criteria. Please adjust your filters.")
    
    with tab2:
        st.header("Analytics Dashboard")
        
        # Get summary data
        conn = navigator.connect_db()
        
        # Top states by partition count
        st.subheader("📊 Partitions by State")
        state_data = pd.read_sql_query("""
            SELECT state_code, state_name, partition_count, total_size_mb
            FROM dim_states 
            ORDER BY partition_count DESC
        """, conn)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.dataframe(state_data.head(10))
        
        with col2:
            fig = px.bar(state_data.head(10), x='state_code', y='partition_count', 
                        title="Top 10 States by Partition Count")
            st.plotly_chart(fig, use_container_width=True)
        
        # Top taxonomies
        st.subheader("🏥 Top Medical Specialties")
        taxonomy_data = pd.read_sql_query("""
            SELECT taxonomy_code, taxonomy_desc, partition_count, total_size_mb
            FROM v_taxonomy_summary 
            ORDER BY partition_count DESC
            LIMIT 15
        """, conn)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.dataframe(taxonomy_data)
        
        with col2:
            fig = px.pie(taxonomy_data.head(10), values='partition_count', 
                        names='taxonomy_desc', title="Distribution by Medical Specialty")
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.header("Data Visualizations")
        
        # Size distribution
        st.subheader("📈 File Size Distribution")
        size_data = pd.read_sql_query("""
            SELECT file_size_mb, COUNT(*) as count
            FROM partitions 
            GROUP BY ROUND(file_size_mb, 1)
            ORDER BY file_size_mb
        """, conn)
        
        fig = px.scatter(size_data, x='file_size_mb', y='count', 
                        title="Partition Size Distribution",
                        labels={'file_size_mb': 'File Size (MB)', 'count': 'Number of Partitions'})
        st.plotly_chart(fig, use_container_width=True)
        
        # Temporal distribution
        st.subheader("📅 Temporal Distribution")
        temporal_data = pd.read_sql_query("""
            SELECT year, month, partition_count, total_size_mb
            FROM dim_time_periods
            ORDER BY year, month
        """, conn)
        
        if not temporal_data.empty:
            temporal_data['date'] = pd.to_datetime(temporal_data[['year', 'month']].assign(day=1))
            
            fig = px.line(temporal_data, x='date', y='partition_count',
                         title="Partitions Over Time")
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.header("About This Application")
        
        st.markdown("""
        ### 🏥 Healthcare Partition Navigator
        
        This application provides an intuitive interface for navigating and exploring 
        your S3-partitioned healthcare data. It uses a SQLite database created by the 
        `s3_partition_inventory.py` script to enable fast searching and filtering.
        
        #### Features:
        - **🔍 Advanced Search**: Filter partitions by payer, state, medical specialty, billing class, and more
        - **📊 Analytics Dashboard**: View summary statistics and distributions
        - **📈 Visualizations**: Interactive charts for data exploration
        - **👁️ Data Preview**: Preview partition contents directly from S3
        - **💾 Export**: Download preview data as CSV
        
        #### How to Use:
        1. Use the filters in the Search tab to narrow down your criteria
        2. Click "Search Partitions" to find matching data files
        3. Click "Preview Data" to see a sample of the partition contents
        4. Use the Analytics tab to explore overall data patterns
        5. Use the Visualizations tab for interactive charts
        
        #### Database Information:
        """)
        
        st.json({
            "Database File": selected_db,
            "Total Partitions": stats['partitions'],
            "Total Size (MB)": round(stats['total_size_mb'], 2),
            "Unique States": stats['dim_states'],
            "Medical Specialties": stats['dim_taxonomies'],
            "Date Range": stats['date_range']
        })

if __name__ == "__main__":
    main()
