"""
Silver Layer: Transform and Validate Performance
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, trim, row_number, current_timestamp, to_date
)
from pyspark.sql.types import IntegerType, DecimalType, DateType, LongType
from great_expectations.dataset import SparkDFDataset
from utils.spark_postgres import (
    create_spark_session, read_from_postgres, write_to_postgres
)


def clean_performance_to_silver():
    """Transform Bronze performance to Silver with quality checks"""
    
    print("=" * 70)
    print("🔷 SILVER TRANSFORMATION: Performance")
    print("=" * 70)
    
    spark = create_spark_session("Silver - Performance")
    
    # Read from Bronze
    bronze_df = read_from_postgres(spark, "raw_performance", "bronze")
    
    print("\n🔧 Step 1: Deduplication")
    # Deduplicate based on performance_id
    window_spec = Window.partitionBy("performance_id") \
        .orderBy(col("_ingestion_timestamp").desc())
    
    deduped_df = bronze_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print(f"   Removed {bronze_df.count() - deduped_df.count()} duplicates")
    
    print("\n🔧 Step 2: Type Casting & Cleaning")
    # Type casting and standardization
    typed_df = deduped_df \
        .withColumn("performance_id", col("performance_id").cast(IntegerType())) \
        .withColumn("campaign_id", col("campaign_id").cast(IntegerType())) \
        .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
        .withColumn("impressions", col("impressions").cast(LongType())) \
        .withColumn("clicks", col("clicks").cast(LongType())) \
        .withColumn("conversions", col("conversions").cast(LongType())) \
        .withColumn("cost", col("cost").cast(DecimalType(10, 2))) \
        .withColumn("revenue", col("revenue").cast(DecimalType(10, 2)))
    
    print("\n🔧 Step 3: Business Rule Validation")
    # Apply business rules
    validated_df = typed_df.filter(
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
    
    invalid_count = typed_df.count() - validated_df.count()
    if invalid_count > 0:
        print(f"   ⚠️  Filtered out {invalid_count} invalid records")
    else:
        print(f"   ✅ All records passed business rules")
    
    print("\n🔍 Step 4: Data Quality Checks")
    # Great Expectations validation
    ge_df = SparkDFDataset(validated_df)
    
    expectations = [
        ("Table has rows", 
         ge_df.expect_table_row_count_to_be_between(min_value=1)),
        ("performance_id unique", 
         ge_df.expect_column_values_to_be_unique("performance_id")),
        ("performance_id not null", 
         ge_df.expect_column_values_to_not_be_null("performance_id")),
        ("campaign_id not null", 
         ge_df.expect_column_values_to_not_be_null("campaign_id")),
        ("impressions >= 0", 
         ge_df.expect_column_values_to_be_between("impressions", min_value=0)),
        ("clicks >= 0", 
         ge_df.expect_column_values_to_be_between("clicks", min_value=0)),
        ("conversions >= 0", 
         ge_df.expect_column_values_to_be_between("conversions", min_value=0)),
        ("cost >= 0", 
         ge_df.expect_column_values_to_be_between("cost", min_value=0)),
    ]
    
    all_passed = True
    for check_name, result in expectations:
        if result.success:
            print(f"   ✅ {check_name}")
        else:
            print(f"   ❌ {check_name}")
            all_passed = False
    
    if not all_passed:
        print("\n❌ Data quality checks FAILED!")
        spark.stop()
        raise ValueError("Data quality validation failed")
    
    print("\n   ✅ All quality checks passed!")
    
    # Add Silver metadata
    silver_df = validated_df \
        .withColumn("_silver_processed_at", current_timestamp())
    
    # Select final columns
    final_df = silver_df.select(
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
    
    print("\n📊 Sample Silver data:")
    final_df.show(5, truncate=False)
    
    # Write to Silver
    write_to_postgres(final_df, "performance", "silver", mode="overwrite")
    
    spark.stop()
    print("✅ Silver transformation complete!\n")
    return True


if __name__ == "__main__":
    try:
        clean_performance_to_silver()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)