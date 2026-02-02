#!/bin/bash

echo "🚀 Running Campaign Analytics Pipeline"
echo "======================================"

echo ""
echo "Step 1: Bronze Ingestion"
python pyspark/bronze/ingest_campaigns.py

echo ""
echo "Step 2: Silver Transformation"
python pyspark/silver/clean_campaigns.py

echo ""
echo "Step 3: dbt Models"
cd dbt_project
dbt run
dbt test

echo ""
echo "✅ Pipeline Complete!"
echo ""
echo "View results:"
echo "  psql campaign_analytics -c 'SELECT * FROM analytics.dim_campaigns;'"