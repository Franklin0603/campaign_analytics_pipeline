"""
Bronze Layer: Ingest Campaigns CSV to PostgreSQL
Preserves raw data exactly as received
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime
from utils.spark_postgres import create_spark_session, write_to_postgres


def ingest_campaigns_to_bronze(csv_path="data/raw/campaigns.csv"):
    """
    Ingest campaigns CSV to Bronze layer
    
    Bronze layer principles:
    - Preserve all raw data
    - All columns as STRING type
    - Add metadata columns for lineage
    - No transformations or validations
    """
    
    print("=" * 70)
    print("🔵 BRONZE INGESTION: Campaigns")
    print("=" * 70)
    
    # Create Spark session
    spark = create_spark_session("Bronze - Campaigns")
    
    # Read CSV (all columns as STRING to preserve raw data)
    print(f"\n📂 Reading CSV: {csv_path}")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(csv_path)
    
    print(f"✅ Read {df.count()} rows")
    print(f"   Columns: {len(df.columns)}")
    
    # Add Bronze metadata columns
    df_bronze = df \
        .withColumn("_source_file", lit(csv_path)) \
        .withColumn("_source_system", lit("csv_upload")) \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    
    # Show sample
    print("\n📊 Sample Bronze data:")
    df_bronze.show(3, truncate=False)
    
    # Write to PostgreSQL bronze schema
    write_to_postgres(
        df=df_bronze,
        table_name="raw_campaigns",
        schema="bronze",
        mode="append"
    )
    
    spark.stop()
    print("✅ Bronze ingestion complete!\n")
    
    return True


if __name__ == "__main__":
    try:
        ingest_campaigns_to_bronze()
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)