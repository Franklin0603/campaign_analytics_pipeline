"""
Bronze Layer - Performance Ingestion
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


def ingest_performance():
    """Ingest performance metrics to Bronze layer."""
    spark = create_spark_session()
    
    try:
        print("=" * 80)
        print("BRONZE LAYER INGESTION - PERFORMANCE")
        print("=" * 80)
        
        # Read CSV
        csv_path = f"{project_root}/data/raw/performance.csv"
        print(f"\n📂 Reading CSV: {csv_path}")
        
        raw_df = spark.read.csv(csv_path, header=True, inferSchema=False)
        record_count = raw_df.count()
        print(f"✅ Loaded {record_count} records")
        
        # Add metadata
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        bronze_df = raw_df \
            .withColumn("_ingestion_timestamp", current_timestamp()) \
            .withColumn("_source_file", lit("campaigns.csv")) \
            .withColumn("_batch_id", lit(batch_id)) \
            .withColumn("ingest_date", current_date()) 
        
        # Write to MinIO
        delta_path = minio_config.get_s3_path("bronze", "performance")
        print(f"\n💾 Writing to MinIO: {delta_path}")
        
        bronze_df.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("ingest_date") \
            .save(delta_path)

        print(f"✅ Delta Lake write successful")
        
        # Write to Postgres
        postgres_props = get_postgres_properties()
        print(f"\n💾 Writing to Postgres: bronze.raw_performance")
        
        bronze_df.write.jdbc(
            url=postgres_props["url"],
            table="bronze.raw_performance",
            mode="append",
            properties=postgres_props
        )
        print(f"✅ Postgres write successful")
        
        # Summary
        print(f"\n📊 Ingested {record_count} performance records")
        print(f"   MinIO: {delta_path}")
        print(f"   Status: ✅ SUCCESS\n")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    ingest_performance()