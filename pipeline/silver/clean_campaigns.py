"""
Silver Layer - Campaign Transformation
NOW WRITES DELTA LAKE FORMAT
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, current_timestamp, trim
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


def clean_campaigns():
    """
    Transform Bronze campaigns to Silver with Delta Lake.
    
    Pipeline:
        Delta Lake Bronze → Transformations → Delta Lake Silver + Postgres Silver
    
    Transformations:
        - Type casting
        - Deduplication
        - Data cleaning
    """
    spark = create_spark_session()
    
    try:
        print("=" * 80)
        print("SILVER LAYER TRANSFORMATION - CAMPAIGNS (DELTA LAKE)")
        print("=" * 80)
        
        # ===== READ FROM DELTA LAKE BRONZE =====
        bronze_path = minio_config.get_s3_path("bronze", "campaigns/")
        print(f"\n📂 Reading from Delta Lake Bronze: {bronze_path}")
        
        # Read as Delta format
        raw_df = spark.read.format("delta").load(bronze_path)
        raw_count = raw_df.count()
        print(f"✅ Loaded {raw_count} records from Bronze Delta Lake")
        
        # ===== TYPE CASTING =====
        print("\n🔧 Applying transformations...")
        print("   Step 1: Type casting")
        
        typed_df = raw_df \
            .withColumn("campaign_id", col("campaign_id").cast(IntegerType())) \
            .withColumn("advertiser_id", col("advertiser_id").cast(IntegerType())) \
            .withColumn("start_date", col("start_date").cast(DateType())) \
            .withColumn("end_date", col("end_date").cast(DateType())) \
            .withColumn("budget_daily", col("budget_daily").cast(DecimalType(10, 2))) \
            .withColumn("budget_total", col("budget_total").cast(DecimalType(10, 2)))
        
        # ===== DEDUPLICATION =====
        print("   Step 2: Deduplication")
        
        window_spec = Window.partitionBy("campaign_id") \
            .orderBy(col("_ingestion_timestamp").desc())
        
        deduped_df = typed_df \
            .withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") == 1) \
            .drop("rn")
        
        deduped_count = deduped_df.count()
        duplicates_removed = raw_count - deduped_count
        print(f"      Removed {duplicates_removed} duplicate records")
        
        # ===== DATA CLEANING =====
        print("   Step 3: Data cleaning")
        
        cleaned_df = deduped_df \
            .withColumn("campaign_name", trim(col("campaign_name"))) \
            .withColumn("campaign_type", trim(col("campaign_type"))) \
            .withColumn("status", trim(col("status"))) \
            .withColumn("objective", trim(col("objective")))
        
        print(f"      Trimmed whitespace from text columns")
        
        # ===== ADD SILVER METADATA & SELECT FINAL COLUMNS =====
        print("   Step 4: Add metadata and select final columns")
        
        silver_df = cleaned_df \
            .withColumn("_silver_processed_at", current_timestamp()) \
            .select(
                "campaign_id",
                "campaign_name",
                "advertiser_id",
                "campaign_type",
                "start_date",
                "end_date",
                "budget_daily",
                "budget_total",
                "status",
                "objective",
                "_silver_processed_at"
            )
        
        print(f"      Added: _silver_processed_at")
        print(f"      Dropped: Bronze metadata columns")
        
        # ===== WRITE TO DELTA LAKE SILVER =====
        silver_path = minio_config.get_s3_path("silver", "campaigns/")
        print(f"\n💾 Writing to Delta Lake Silver: {silver_path}")
        print(f"   Partitioning by: status")
        print(f"   Format: Delta Lake (ACID + Versioning)")
        
        silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("status") \
            .save(silver_path)
        
        print(f"✅ Delta Lake Silver write successful")
        
        # ===== WRITE TO POSTGRES SILVER =====
        print(f"\n💾 Writing to Postgres: silver.campaigns")
        truncate_and_write(silver_df, "campaigns", "silver")
        
        # ===== SUMMARY =====
        print("\n" + "=" * 80)
        print("📊 TRANSFORMATION SUMMARY")
        print("=" * 80)
        print(f"Input Records:      {raw_count}")
        print(f"Output Records:     {deduped_count}")
        print(f"Duplicates Removed: {duplicates_removed}")
        print(f"Delta Lake Path:    {silver_path}")
        print(f"Partitioning:       status")
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
    clean_campaigns()