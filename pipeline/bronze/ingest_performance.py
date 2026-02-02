"""
Bronze Layer: Ingest Performance CSV to PostgreSQL
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime
from utils.spark_postgres import create_spark_session, write_to_postgres


def ingest_performance_to_bronze(csv_path="data/raw/performance.csv"):
    """Ingest performance CSV to Bronze layer"""
    
    print("=" * 70)
    print("🔵 BRONZE INGESTION: Performance")
    print("=" * 70)
    
    spark = create_spark_session("Bronze - Performance")
    
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
        table_name="raw_performance",
        schema="bronze",
        mode="append"
    )
    
    spark.stop()
    print("✅ Bronze ingestion complete!\n")
    
    return True


if __name__ == "__main__":
    try:
        ingest_performance_to_bronze()
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)