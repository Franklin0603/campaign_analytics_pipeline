#!/usr/bin/env python3
"""
Campaign Analytics Pipeline - Master Runner
Orchestrates Bronze → Silver → Gold pipeline
"""

import sys
import os
from datetime import datetime

# Add pipeline to path 
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'pipeline')))

# Bronze imports
from bronze.ingest_campaigns import ingest_campaigns_to_bronze
from bronze.ingest_performance import ingest_performance_to_bronze
from bronze.ingest_advertisers import ingest_advertisers_to_bronze

# Silver imports
from silver.clean_campaigns import clean_campaigns_to_silver
from silver.clean_performance import clean_performance_to_silver
from silver.clean_advertisers import clean_advertisers_to_silver

def print_header(message):
    """Print formatted header"""
    print("\n" + "=" * 70)
    print(f"🚀 {message}")
    print("=" * 70)


def run_pipeline():
    """Run the complete data pipeline"""
    
    start_time = datetime.now()
    
    print_header("CAMPAIGN ANALYTICS PIPELINE - START")
    print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # ============ BRONZE LAYER ============
        print_header("BRONZE LAYER: Raw Data Ingestion")
        
        print("\n1/6: Ingesting Campaigns...")
        ingest_campaigns_to_bronze()
        
        print("2/6: Ingesting Performance...")
        ingest_performance_to_bronze()
        
        print("3/6: Ingesting Advertisers...")
        ingest_advertisers_to_bronze()
        
        # ============ SILVER LAYER ============
        print_header("SILVER LAYER: Data Transformation & Quality")
        
        print("4/6: Transforming Campaigns...")
        clean_campaigns_to_silver()
        
        print("5/6: Transforming Performance...")
        clean_performance_to_silver()
        
        print("6/6: Transforming Advertisers...")
        clean_advertisers_to_silver()
        
        # ============ GOLD LAYER (dbt) ============
        print_header("GOLD LAYER: Analytics Models (dbt)")
        print("Run: cd dbt_project && dbt run && dbt test")
        
        # Calculate duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print_header("PIPELINE COMPLETE ✅")
        print(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {duration:.2f} seconds")
        print("\nNext Steps:")
        print("  1. cd dbt_project")
        print("  2. dbt run")
        print("  3. dbt test")
        print("  4. psql campaign_analytics -c 'SELECT * FROM analytics.dim_campaigns LIMIT 5;'")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)
