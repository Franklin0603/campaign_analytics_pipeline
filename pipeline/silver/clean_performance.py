"""
Silver Layer - Performance Transformation
NOW READS FROM MINIO BRONZE
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, row_number, current_timestamp, to_date
)
from pyspark.sql.types import IntegerType, DecimalType, LongType
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



def clean_performance():
    """
    Transform Bronze campaigns to Silver with Delta Lake.
    
    Pipeline:
        Delta Lake Bronze → Transformations → Delta Lake Silver + Postgres Silver
    
    Transformations:
        - Type casting
        - Deduplication
        - Business rule validation (clicks <= impressions, conversions <= clicks)
    """
    spark = create_spark_session()
    
    try:
        print("=" * 80)
        print("SILVER LAYER TRANSFORMATION - PERFORMANCE (DELTA LAKE)")
        print("=" * 80)
        
        # ===== READ FROM MINIO BRONZE =====
        bronze_path = minio_config.get_s3_path("bronze", "performance/")
        print(f"\n📂 Reading from Delta Lake Bronze: {bronze_path}")
        
        raw_df = spark.read.format("delta").load(bronze_path)
        raw_count = raw_df.count()
        print(f"✅ Loaded {raw_count} records from Bronze Delta Lake")
        
        # ===== TYPE CASTING =====
        print("\n🔧 Applying transformations...")
        print("   Step 1: Type casting")
        
        typed_df = raw_df \
            .withColumn("performance_id", col("performance_id").cast(IntegerType())) \
            .withColumn("campaign_id", col("campaign_id").cast(IntegerType())) \
            .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
            .withColumn("impressions", col("impressions").cast(LongType())) \
            .withColumn("clicks", col("clicks").cast(LongType())) \
            .withColumn("conversions", col("conversions").cast(LongType())) \
            .withColumn("cost", col("cost").cast(DecimalType(10, 2))) \
            .withColumn("revenue", col("revenue").cast(DecimalType(10, 2)))
        
        # ===== DEDUPLICATION =====
        print("   Step 2: Deduplication")
        
        window_spec = Window.partitionBy("performance_id") \
            .orderBy(col("_ingestion_timestamp").desc())
        
        deduped_df = typed_df \
            .withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .drop("rn")
        
        deduped_count = deduped_df.count()
        duplicates_removed = raw_count - deduped_count
        print(f"      Removed {duplicates_removed} duplicate records")
        
        # ===== BUSINESS RULE VALIDATION =====
        print("   Step 3: Business rule validation")
        
        validated_df = deduped_df.filter(
            # Required fields
            col("performance_id").isNotNull() &
            col("campaign_id").isNotNull() &
            col("date").isNotNull() &
            # Non-negative values
            (col("impressions") >= 0) &
            (col("clicks") >= 0) &
            (col("conversions") >= 0) &
            (col("cost") >= 0) &
            (col("revenue") >= 0) &
            # Business logic: clicks <= impressions
            (col("clicks") <= col("impressions")) &
            # Business logic: conversions <= clicks
            (col("conversions") <= col("clicks"))
        )
        
        validated_count = validated_df.count()
        invalid_count = deduped_count - validated_count
        
        if invalid_count > 0:
            print(f"      ⚠️  Filtered out {invalid_count} invalid records")
        else:
            print(f"      ✅ All records passed business rules")
        
        # ===== ADD SILVER METADATA & SELECT FINAL COLUMNS =====
        print("   Step 4: Add metadata and select final columns")
        
        silver_df = validated_df \
            .withColumn("_silver_processed_at", current_timestamp()) \
            .select(
                "performance_id",
                "campaign_id",
                "date",
                "impressions",
                "clicks",
                "conversions",
                "cost",
                "revenue",
                "_silver_processed_at"
            )

        print(f"      Added: _silver_processed_at")
        print(f"      Dropped: Bronze metadata columns")
        
        # ===== WRITE TO DELTA LAKE SILVER =====
        silver_path = minio_config.get_s3_path("silver", "performance/")
        print(f"\n💾 Writing to Delta Lake Silver: {silver_path}")
        print(f"   Partitioning by: date")
        print(f"   Format: Delta Lake (ACID + Versioning)")
        
        silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("date") \
            .save(silver_path)
        
        print(f"✅ Delta Lake Silver write successful")
        
        # ===== WRITE TO POSTGRES SILVER =====
        print(f"\n💾 Writing to Postgres: silver.performance")
        truncate_and_write(silver_df, "performance", "silver")
        
        # ===== SUMMARY =====
        print("\n" + "=" * 80)
        print("📊 TRANSFORMATION SUMMARY")
        print("=" * 80)
        print(f"Input Records:      {raw_count}")
        print(f"After Dedup:        {deduped_count}")
        print(f"After Validation:   {validated_count}")
        print(f"Invalid Filtered:   {invalid_count}")
        print(f"MinIO Path:         {silver_path}")
        print(f"Partitioning:       date")
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
    clean_performance()