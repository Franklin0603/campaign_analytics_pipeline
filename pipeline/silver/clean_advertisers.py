"""
Silver Layer - Advertiser Transformation
NOW READS FROM MINIO BRONZE
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, current_timestamp, trim, upper
from pyspark.sql.types import IntegerType, DecimalType, DateType
from config.minio_config import minio_config
from pipeline.utils.spark_postgres import truncate_and_write

# Delta Lake imports
from delta import configure_spark_with_delta_pip


def create_spark_session():
    """Create Spark session with Delta Lake support."""
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.6.0",
    ]
    
    builder = SparkSession.builder \
        .appName("SilverLayer-Campaigns") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    for key, value in minio_config.get_spark_config().items():
        builder = builder.config(key, value)
    
    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    
    return spark


def clean_advertisers():
    """
    Transform Bronze advertisers to Silver.
    
    Pipeline:
        MinIO Bronze → Transformations → MinIO Silver + Postgres Silver
    
    Transformations:
        - Type casting
        - Deduplication
        - Data standardization (uppercase, trim)
        - Preserve lineage metadata
    """
    spark = create_spark_session()
    
    try:
        print("=" * 80)
        print("SILVER LAYER TRANSFORMATION - ADVERTISERS (DELTA LAKE)")
        print("=" * 80)
        
        # ===== READ FROM MINIO BRONZE =====
        bronze_path = minio_config.get_s3_path("bronze", "advertisers/")
        print(f"\n📂 Reading from Delta Lake Bronze: {bronze_path}")
        
        raw_df = spark.read.format("delta").load(bronze_path)
        raw_count = raw_df.count()
        print(f"✅ Loaded {raw_count} records from Bronze Delta Lake")
        
        # ===== TYPE CASTING =====
        print("\n🔧 Applying transformations...")
        print("   Step 1: Type casting")
        
        typed_df = raw_df \
            .withColumn("advertiser_id", col("advertiser_id").cast("int"))
        
        # ===== DEDUPLICATION =====
        print("   Step 2: Deduplication")
        
        window_spec = Window.partitionBy("advertiser_id") \
            .orderBy(col("_ingestion_timestamp").desc())
        
        deduped_df = typed_df \
            .withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .drop("rn")
        
        deduped_count = deduped_df.count()
        duplicates_removed = raw_count - deduped_count
        print(f"      Removed {duplicates_removed} duplicate records")
        
        # ===== DATA STANDARDIZATION =====
        print("   Step 3: Data standardization")
        
        silver_df = deduped_df \
            .withColumn("advertiser_name", trim(col("advertiser_name"))) \
            .withColumn("industry", upper(trim(col("industry")))) \
            .withColumn("country", upper(trim(col("country")))) \
            .withColumn("_silver_processed_at", current_timestamp())
        
        print(f"      Standardized: advertiser_name, industry, country")

        # Show final columns
        print(f"\n   Silver columns: {', '.join(silver_df.columns)}")
        
        # Select only columns matching Postgres table schema
        postgres_columns = [
            "advertiser_id",
            "advertiser_name",
            "industry",
            "country",
            "account_manager",
            "_silver_processed_at"
        ]
        postgres_df = silver_df.select(*postgres_columns)
        
        # ===== WRITE TO MINIO SILVER =====
        silver_path = minio_config.get_s3_path("silver", "advertisers/")
        print(f"\n💾 Writing to Delta Lake Silver: {silver_path}")
        print(f"   Partitioning by: status")
        print(f"   Format: Delta Lake (ACID + Versioning)")
        
        silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("industry") \
            .save(silver_path)
        
        print(f"✅ MinIO Silver write successful")
        
        # ===== WRITE TO POSTGRES SILVER =====
        print(f"\n💾 Writing to Postgres: silver.advertisers")
        truncate_and_write(postgres_df, "advertisers", "silver")
        
        # ===== SUMMARY =====
        print("\n" + "=" * 80)
        print("📊 TRANSFORMATION SUMMARY")
        print("=" * 80)
        print(f"Input Records:      {raw_count}")
        print(f"Output Records:     {deduped_count}")
        print(f"Duplicates Removed: {duplicates_removed}")
        print(f"MinIO Path:         {silver_path}")
        print(f"Partitioning:       industry")
        print(f"Format:             Delta Lake (Parquet + Transaction Log)")
        print(f"Status:             ✅ SUCCESS")
        print("=" * 80 + "\n")
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    clean_advertisers()