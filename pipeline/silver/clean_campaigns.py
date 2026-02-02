"""
Silver Layer: Transform and Validate Campaigns
Applies business rules, type casting, deduplication, and quality checks
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, trim, upper, row_number, current_timestamp, to_date
)
from pyspark.sql.types import IntegerType, DecimalType, DateType
from great_expectations.dataset import SparkDFDataset
from utils.spark_postgres import (
    create_spark_session, read_from_postgres, write_to_postgres
)


def clean_campaigns_to_silver():
    """Transform Bronze campaigns to Silver with quality checks"""
    
    print("=" * 70)
    print("🔷 SILVER TRANSFORMATION: Campaigns")
    print("=" * 70)
    
    spark = create_spark_session("Silver - Campaigns")
    
    # Read from Bronze
    bronze_df = read_from_postgres(spark, "raw_campaigns", "bronze")
    
    print("\n🔧 Step 1: Deduplication")
    # Deduplicate based on campaign_id
    window_spec = Window.partitionBy("campaign_id") \
        .orderBy(col("_ingestion_timestamp").desc())
    
    deduped_df = bronze_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print(f"   Removed {bronze_df.count() - deduped_df.count()} duplicates")
    
    print("\n🔧 Step 2: Type Casting & Cleaning")
    # Type casting and standardization
    typed_df = deduped_df \
        .withColumn("campaign_id", col("campaign_id").cast(IntegerType())) \
        .withColumn("advertiser_id", col("advertiser_id").cast(IntegerType())) \
        .withColumn("campaign_name", trim(col("campaign_name"))) \
        .withColumn("campaign_type", trim(col("campaign_type"))) \
        .withColumn("status", trim(col("status"))) \
        .withColumn("objective", trim(col("objective"))) \
        .withColumn("start_date", to_date(col("start_date"), "yyyy-MM-dd")) \
        .withColumn("end_date", to_date(col("end_date"), "yyyy-MM-dd")) \
        .withColumn("budget_daily", col("budget_daily").cast(DecimalType(10, 2))) \
        .withColumn("budget_total", col("budget_total").cast(DecimalType(10, 2)))
    
    print("\n🔧 Step 3: Business Rule Validation")
    # Apply business rules
    validated_df = typed_df.filter(
        # Required fields
        col("campaign_id").isNotNull() &
        col("advertiser_id").isNotNull() &
        col("campaign_name").isNotNull() &
        col("campaign_type").isNotNull() &
        col("start_date").isNotNull() &
        col("end_date").isNotNull() &
        # Business logic
        (col("end_date") >= col("start_date")) &
        (col("budget_daily") > 0) &
        (col("budget_total") > 0)
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
        ("campaign_id unique", 
         ge_df.expect_column_values_to_be_unique("campaign_id")),
        ("campaign_id not null", 
         ge_df.expect_column_values_to_not_be_null("campaign_id")),
        ("advertiser_id not null", 
         ge_df.expect_column_values_to_not_be_null("advertiser_id")),
        ("campaign_type valid", 
         ge_df.expect_column_values_to_be_in_set(
             "campaign_type", 
             ["Search", "Display", "Video", "Social", "Shopping"]
         )),
        ("status valid", 
         ge_df.expect_column_values_to_be_in_set(
             "status", 
             ["Active", "Paused", "Completed", "Draft"]
         )),
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
    
    print("\n📊 Sample Silver data:")
    final_df.show(5, truncate=False)
    
    # Write to Silver
    write_to_postgres(final_df, "campaigns", "silver", mode="overwrite")
    
    spark.stop()
    print("✅ Silver transformation complete!\n")
    return True


if __name__ == "__main__":
    try:
        clean_campaigns_to_silver()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)