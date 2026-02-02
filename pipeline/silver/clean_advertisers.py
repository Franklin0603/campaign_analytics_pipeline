"""
Silver Layer: Transform and Validate Advertisers
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, trim, upper, row_number, current_timestamp
)
from pyspark.sql.types import IntegerType
from great_expectations.dataset import SparkDFDataset
from utils.spark_postgres import (
    create_spark_session, read_from_postgres, write_to_postgres
)


def clean_advertisers_to_silver():
    """Transform Bronze advertisers to Silver with quality checks"""
    
    print("=" * 70)
    print("🔷 SILVER TRANSFORMATION: Advertisers")
    print("=" * 70)
    
    spark = create_spark_session("Silver - Advertisers")
    
    # Read from Bronze
    bronze_df = read_from_postgres(spark, "raw_advertisers", "bronze")
    
    print("\n🔧 Step 1: Deduplication")
    # Deduplicate
    window_spec = Window.partitionBy("advertiser_id") \
        .orderBy(col("_ingestion_timestamp").desc())
    
    deduped_df = bronze_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    print(f"   Removed {bronze_df.count() - deduped_df.count()} duplicates")
    
    print("\n🔧 Step 2: Type Casting & Cleaning")
    # Type casting and standardization
    typed_df = deduped_df \
        .withColumn("advertiser_id", col("advertiser_id").cast(IntegerType())) \
        .withColumn("advertiser_name", trim(col("advertiser_name"))) \
        .withColumn("industry", trim(col("industry"))) \
        .withColumn("country", upper(trim(col("country")))) \
        .withColumn("account_manager", trim(col("account_manager")))
    
    print("\n🔧 Step 3: Business Rule Validation")
    # Apply business rules
    validated_df = typed_df.filter(
        col("advertiser_id").isNotNull() &
        col("advertiser_name").isNotNull() &
        col("industry").isNotNull()
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
        
        ("advertiser_id unique", 
         ge_df.expect_column_values_to_be_unique("advertiser_id")),
        
        ("advertiser_id not null", 
         ge_df.expect_column_values_to_not_be_null("advertiser_id")),
        
        ("advertiser_name not null", 
         ge_df.expect_column_values_to_not_be_null("advertiser_name")),
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
        "advertiser_id",
        "advertiser_name",
        "industry",
        "country",
        "account_manager",
        "_silver_processed_at"
    )
    
    print("\n📊 Sample Silver data:")
    final_df.show(5, truncate=False)
    
    # Write to Silver
    write_to_postgres(final_df, "advertisers", "silver", mode="overwrite")
    
    spark.stop()
    print("✅ Silver transformation complete!\n")
    
    return True


if __name__ == "__main__":
    try:
        clean_advertisers_to_silver()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)