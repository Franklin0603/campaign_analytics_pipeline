"""
Bronze Layer: Ingest Advertisers CSV to PostgreSQL
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime
from utils.spark_postgres import create_spark_session, write_to_postgres


def ingest_advertisers_to_bronze(csv_path="data/raw/advertisers.csv"):
    """Ingest advertisers CSV to Bronze layer"""
    
    print("=" * 70)
    print("🔵 BRONZE INGESTION: Advertisers")
    print("=" * 70)
    
    spark = create_spark_session("Bronze - Advertisers")
    
    print(f"\n📂 Reading CSV: {csv_path}")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(csv_path)
    
    print(f"✅ Read {df.count()} rows")
    
    # Add Bronze metadata
    df_bronze = df \
        .withColumn("_source_file", lit(csv_path)) \
        .withColumn("_source_system", lit("csv_upload")) \
        .withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    
    print("\n📊 Sample Bronze data:")
    df_bronze.show(3, truncate=False)
    
    write_to_postgres(
        df=df_bronze,
        table_name="raw_advertisers",
        schema="bronze",
        mode="append"
    )
    
    spark.stop()
    print("✅ Bronze ingestion complete!\n")
    
    return True


if __name__ == "__main__":
    try:
        ingest_advertisers_to_bronze()
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
