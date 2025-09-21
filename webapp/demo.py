#!/usr/bin/env python3
"""
Demo script to show the Healthcare Partition Navigator webapp functionality
"""

import sqlite3
import pandas as pd
from pathlib import Path

def demo_database_queries():
    """Demonstrate key database queries used by the webapp"""
    
    # Find database files
    db_files = []
    for root, dirs, files in os.walk('.'):
        for file in files:
            if file.endswith('.db') and 'partition' in file.lower():
                db_files.append(os.path.join(root, file))
    
    if not db_files:
        print("❌ No partition navigation database found.")
        print("Please run: python ETL/utils/s3_partition_inventory.py healthcare-data-lake-prod --create-db")
        return
    
    db_path = db_files[0]
    print(f"📊 Using database: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    print("\n🔍 DEMO: Healthcare Partition Navigator Queries")
    print("=" * 60)
    
    # 1. Database overview
    print("\n1️⃣ Database Overview:")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM partitions")
    partition_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT SUM(file_size_mb) FROM partitions")
    total_size = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM dim_taxonomies")
    taxonomy_count = cursor.fetchone()[0]
    
    print(f"   📁 Total Partitions: {partition_count:,}")
    print(f"   💾 Total Size: {total_size:.1f} MB")
    print(f"   🏥 Medical Specialties: {taxonomy_count:,}")
    
    # 2. Top states
    print("\n2️⃣ Top States by Partition Count:")
    state_data = pd.read_sql_query("""
        SELECT state, partition_count, total_size_mb
        FROM dim_states 
        ORDER BY partition_count DESC
        LIMIT 5
    """, conn)
    
    for _, row in state_data.iterrows():
        print(f"   {row['state']}: {row['partition_count']} partitions ({row['total_size_mb']:.1f} MB)")
    
    # 3. Top medical specialties
    print("\n3️⃣ Top Medical Specialties:")
    taxonomy_data = pd.read_sql_query("""
        SELECT taxonomy_desc, partition_count, total_size_mb
        FROM v_taxonomy_summary 
        ORDER BY partition_count DESC
        LIMIT 5
    """, conn)
    
    for _, row in taxonomy_data.iterrows():
        print(f"   {row['taxonomy_desc']}: {row['partition_count']} partitions ({row['total_size_mb']:.1f} MB)")
    
    # 4. Sample partition search
    print("\n4️⃣ Sample Partition Search (GA state, Pediatrics):")
    search_results = pd.read_sql_query("""
        SELECT 
            p.payer_slug,
            p.state,
            p.billing_class,
            p.procedure_set,
            p.taxonomy_desc,
            p.file_size_mb,
            p.estimated_records
        FROM partitions p
        WHERE p.state = 'GA' 
        AND p.taxonomy_code = '208000000X'
        ORDER BY p.file_size_mb DESC
        LIMIT 3
    """, conn)
    
    if not search_results.empty:
        for _, row in search_results.iterrows():
            print(f"   Payer: {row['payer_slug']}")
            print(f"   Billing: {row['billing_class']} | {row['procedure_set']}")
            print(f"   Size: {row['file_size_mb']:.2f} MB | Records: {row['estimated_records']:,}")
            print()
    else:
        print("   No results found for this search")
    
    # 5. File size distribution
    print("\n5️⃣ File Size Distribution:")
    size_data = pd.read_sql_query("""
        SELECT 
            MIN(file_size_mb) as min_size,
            MAX(file_size_mb) as max_size,
            AVG(file_size_mb) as avg_size,
            COUNT(*) as count
        FROM partitions
    """, conn)
    
    row = size_data.iloc[0]
    print(f"   Min: {row['min_size']:.2f} MB")
    print(f"   Max: {row['max_size']:.2f} MB")
    print(f"   Avg: {row['avg_size']:.2f} MB")
    print(f"   Count: {row['count']:,}")
    
    conn.close()
    
    print("\n✅ Demo completed!")
    print("\n🚀 To run the full webapp:")
    print("   python run_app.py")
    print("   or")
    print("   streamlit run app.py")

if __name__ == "__main__":
    import os
    demo_database_queries()
