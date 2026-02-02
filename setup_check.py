#!/usr/bin/env python3
"""
Complete setup fix for Campaign Analytics Pipeline
This script:
1. Creates missing __init__.py files
2. Checks all imports work
3. Verifies database connection
4. Tests PySpark installation
"""

import os
import sys

def create_init_files():
    """Create __init__.py files for all packages"""
    
    print("📁 Creating __init__.py files...")
    
    # Bronze __init__.py
    bronze_init = """\"\"\"
Bronze Layer - Raw Data Ingestion
\"\"\"

from .ingest_campaigns import ingest_campaigns_to_bronze
from .ingest_performance import ingest_performance_to_bronze
from .ingest_advertisers import ingest_advertisers_to_bronze

__all__ = [
    'ingest_campaigns_to_bronze',
    'ingest_performance_to_bronze',
    'ingest_advertisers_to_bronze'
]
"""
    
    # Silver __init__.py
    silver_init = """\"\"\"
Silver Layer - Data Transformation & Quality
\"\"\"

from .clean_campaigns import clean_campaigns_to_silver
from .clean_performance import clean_performance_to_silver
from .clean_advertisers import clean_advertisers_to_silver

__all__ = [
    'clean_campaigns_to_silver',
    'clean_performance_to_silver',
    'clean_advertisers_to_silver'
]
"""
    
    # Create files
    os.makedirs('pyspark/bronze', exist_ok=True)
    os.makedirs('pyspark/silver', exist_ok=True)
    os.makedirs('pyspark/utils', exist_ok=True)
    
    with open('pyspark/bronze/__init__.py', 'w') as f:
        f.write(bronze_init)
    print("✅ Created pyspark/bronze/__init__.py")
    
    with open('pyspark/silver/__init__.py', 'w') as f:
        f.write(silver_init)
    print("✅ Created pyspark/silver/__init__.py")
    
    if not os.path.exists('pyspark/utils/__init__.py'):
        with open('pyspark/utils/__init__.py', 'w') as f:
            f.write('"""Utilities - Spark, PostgreSQL, Logging"""\n')
        print("✅ Created pyspark/utils/__init__.py")


def test_pyspark():
    """Test PySpark installation"""
    print("\n🔍 Testing PySpark...")
    
    try:
        from pyspark.sql import SparkSession
        print("✅ PySpark package imported successfully")
        return True
    except ImportError as e:
        print(f"❌ PySpark import failed: {e}")
        print("\nFix: pip install pyspark")
        return False


def test_project_imports():
    """Test project imports"""
    print("\n🔍 Testing project imports...")
    
    # Add project to path
    project_root = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, os.path.join(project_root, 'pyspark'))
    
    try:
        from bronze.ingest_campaigns import ingest_campaigns_to_bronze
        print("✅ bronze.ingest_campaigns")
    except ImportError as e:
        print(f"❌ bronze.ingest_campaigns: {e}")
        return False
    
    try:
        from bronze.ingest_performance import ingest_performance_to_bronze
        print("✅ bronze.ingest_performance")
    except ImportError as e:
        print(f"❌ bronze.ingest_performance: {e}")
        return False
    
    try:
        from bronze.ingest_advertisers import ingest_advertisers_to_bronze
        print("✅ bronze.ingest_advertisers")
    except ImportError as e:
        print(f"❌ bronze.ingest_advertisers: {e}")
        return False
    
    try:
        from silver.clean_campaigns import clean_campaigns_to_silver
        print("✅ silver.clean_campaigns")
    except ImportError as e:
        print(f"❌ silver.clean_campaigns: {e}")
        return False
    
    try:
        from silver.clean_performance import clean_performance_to_silver
        print("✅ silver.clean_performance")
    except ImportError as e:
        print(f"❌ silver.clean_performance: {e}")
        return False
    
    try:
        from silver.clean_advertisers import clean_advertisers_to_silver
        print("✅ silver.clean_advertisers")
    except ImportError as e:
        print(f"❌ silver.clean_advertisers: {e}")
        return False
    
    return True


def test_database():
    """Test database connection"""
    print("\n🔍 Testing database connection...")
    
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='campaign_analytics',
            user='postgres',
            password='postgres'
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ Database connected: {version.split(',')[0]}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("\nFix: Make sure PostgreSQL is running")
        print("  Docker: docker ps | grep postgres")
        print("  Start: docker start campaign-analytics-db")
        return False


def test_java():
    """Test Java installation"""
    print("\n🔍 Testing Java...")
    
    import subprocess
    
    try:
        result = subprocess.run(
            ['java', '-version'],
            capture_output=True,
            text=True,
            check=True
        )
        
        version_output = result.stderr.split('\n')[0]
        print(f"✅ Java installed: {version_output}")
        return True
        
    except FileNotFoundError:
        print("❌ Java not found")
        print("\nFix: Install Java 11")
        print("  macOS: brew install openjdk@11")
        print("  Ubuntu: sudo apt install openjdk-11-jdk")
        return False
    except Exception as e:
        print(f"❌ Java check failed: {e}")
        return False


def check_files():
    """Check all required files exist"""
    print("\n🔍 Checking required files...")
    
    required_files = [
        'data/raw/campaigns.csv',
        'data/raw/performance.csv',
        'data/raw/advertisers.csv',
        'pipeline/bronze/ingest_campaigns.py',
        'pipeline/bronze/ingest_performance.py',
        'pipeline/bronze/ingest_advertisers.py',
        'pipeline/silver/clean_campaigns.py',
        'pipeline/silver/clean_performance.py',
        'pipeline/silver/clean_advertisers.py',
        'pipeline/utils/spark_postgres.py',
    ]
    
    all_exist = True
    
    for file_path in required_files:
        if os.path.exists(file_path):
            print(f"✅ {file_path}")
        else:
            print(f"❌ {file_path} - MISSING")
            all_exist = False
    
    return all_exist


def main():
    """Run all checks"""
    print("="*70)
    print("🔧 Campaign Analytics Pipeline - Setup Check")
    print("="*70)
    
    # Create missing files
    create_init_files()
    
    # Run tests
    results = {
        'PySpark': test_pyspark(),
        'Project Imports': test_project_imports(),
        'Database': test_database(),
        'Java': test_java(),
        'Files': check_files()
    }
    
    # Summary
    print("\n" + "="*70)
    print("📊 SUMMARY")
    print("="*70)
    
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {test_name}")
    
    all_passed = all(results.values())
    
    if all_passed:
        print("\n🎉 All checks passed! Ready to run pipeline:")
        print("   python run_pipeline.py")
    else:
        print("\n⚠️  Some checks failed. Fix the issues above before running pipeline.")
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)