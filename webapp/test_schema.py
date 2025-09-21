#!/usr/bin/env python3
"""Test database schema to identify column name issues"""

import sqlite3
import os
from pathlib import Path

def test_database_schema():
    """Test the database schema to identify column name issues"""
    
    # Find database files
    db_files = []
    for root, dirs, files in os.walk('..'):
        for file in files:
            if file.endswith('.db') and 'partition' in file.lower():
                db_files.append(os.path.join(root, file))
    
    if not db_files:
        print("❌ No database files found")
        return
    
    db_path = db_files[0]
    print(f"📊 Testing database: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Test each table schema
    tables_to_test = [
        'dim_states',
        'dim_payers', 
        'dim_taxonomies',
        'dim_billing_classes',
        'dim_procedure_sets',
        'dim_stat_areas',
        'dim_time_periods',
        'partitions'
    ]
    
    print("\n🔍 Database Schema Analysis:")
    print("=" * 50)
    
    for table in tables_to_test:
        try:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            
            if columns:
                print(f"\n📋 Table: {table}")
                for col in columns:
                    print(f"   {col[1]} ({col[2]})")
            else:
                print(f"\n❌ Table {table} not found")
                
        except Exception as e:
            print(f"\n❌ Error reading table {table}: {e}")
    
    # Test specific queries that were failing
    print("\n🧪 Testing Problematic Queries:")
    print("=" * 40)
    
    # Test dim_states query
    try:
        cursor.execute("SELECT state_code, state_name, partition_count, total_size_mb FROM dim_states LIMIT 1")
        result = cursor.fetchone()
        print("✅ dim_states query works")
        if result:
            print(f"   Sample: {result}")
    except Exception as e:
        print(f"❌ dim_states query failed: {e}")
    
    # Test partitions query
    try:
        cursor.execute("SELECT COUNT(*) FROM partitions")
        count = cursor.fetchone()[0]
        print(f"✅ partitions table has {count} rows")
    except Exception as e:
        print(f"❌ partitions query failed: {e}")
    
    conn.close()
    print("\n✅ Schema test completed!")

if __name__ == "__main__":
    test_database_schema()
