"""
Bronze Layer - Campaign Ingestion
"""
import sys
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, current_date, lit
from config.minio_config import minio_config
from pipeline.utils.spark_postgres import get_postgres_properties

# Delta Lake imports
from delta import configure_spark_with_delta_pip


def create_spark_session():
    """
    Create Spark session with Delta Lake 2.4 support.
    
    Returns:
        SparkSession: Configured Spark session with Delta Lake
    """
    # Compatible Delta Lake configuration for Spark 3.4
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.6.0",
    ]

    builder = SparkSession.builder \
        .appName("BronzeLayer-Campaigns") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    # Add MinIO S3 configurations
    for key, value in minio_config.get_spark_config().items():
        builder = builder.config(key, value)
    
    # Configure with Delta Lake and extra packages
    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    
    return spark


def ingest_campaigns():
    """
    Ingest campaigns from CSV to Bronze layer with Delta Lake format.
    
    Architecture:
        CSV → PySpark → [Delta Lake (MinIO) + Postgres (JDBC)]
    
    Strategy:
        1. Read raw CSV (all columns as STRING)
        2. Add metadata columns for lineage
        3. Write to MinIO as Delta Lake format (ACID, versioning)
        4. Write to Postgres for querying (silver layer input)
    """
    spark = create_spark_session()
    
    try:
        print("=" * 80)
        print("BRONZE LAYER INGESTION - CAMPAIGNS (DELTA LAKE)")
        print("=" * 80)
        
        # ===== READ RAW DATA =====
        csv_path = f"{project_root}/data/raw/campaigns.csv"
        print(f"\n📂 Reading CSV: {csv_path}")
        
        raw_df = spark.read.csv(
            csv_path,
            header=True,
            inferSchema=False
        )
        
        record_count = raw_df.count()
        print(f"✅ Loaded {record_count} records")
        print(f"   Columns: {', '.join(raw_df.columns)}")
        
        # ===== ADD METADATA =====
        # Add ingest_date as a column, not path
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        bronze_df = raw_df \
            .withColumn("_ingestion_timestamp", current_timestamp()) \
            .withColumn("_source_file", lit("campaigns.csv")) \
            .withColumn("_batch_id", lit(batch_id)) \
            .withColumn("ingest_date", current_date())
        
        print(f"\n📝 Added metadata:")
        print(f"   Batch ID: {batch_id}")
        
        # ===== WRITE TO DELTA LAKE (MINIO) =====
        delta_path = minio_config.get_s3_path("bronze", "campaigns") 
                
        print(f"\n💾 Writing to Delta Lake (MinIO):")
        print(f"   Path: {delta_path}")
        print(f"   Format: Delta Lake 2.4 (ACID + Versioning)")
        
        # Partition BY ingest_date column
        bronze_df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("ingest_date") \
            .save(delta_path)
        
        print(f"✅ Delta Lake write successful")
        
        # ===== WRITE TO POSTGRES (QUERYABLE MIRROR) =====
        postgres_props = get_postgres_properties()
        
        print(f"\n💾 Writing to Postgres:")
        print(f"   Table: bronze.raw_campaigns")
        
        bronze_df.write \
            .jdbc(
                url=postgres_props["url"],
                table="bronze.raw_campaigns",
                mode="append",
                properties=postgres_props
            )
        
        print(f"✅ Postgres write successful")
        
        # ===== SUMMARY =====
        print("\n" + "=" * 80)
        print("📊 INGESTION SUMMARY")
        print("=" * 80)
        print(f"Total Records:     {record_count}")
        print(f"Total Columns:     {len(bronze_df.columns)}")
        print(f"Delta Lake Path:   {delta_path}")
        print(f"Postgres Table:    bronze.raw_campaigns")
        print(f"Format:            Delta Lake (Parquet + Transaction Log)")
        print(f"Status:            ✅ SUCCESS")
        print("=" * 80 + "\n")
        
    except Exception as e:
        print(f"\n❌ ERROR during ingestion: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    ingest_campaigns()