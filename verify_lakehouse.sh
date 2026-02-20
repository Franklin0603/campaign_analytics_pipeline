#!/bin/bash

echo "🔍 Lakehouse Verification Checklist"
echo "===================================="

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Test 1: Docker containers
echo -e "\n1. Docker Containers"
if docker ps | grep -q campaign_analytics_db && docker ps | grep -q campaign_minio; then
    echo -e "${GREEN}✅ Both containers running${NC}"
else
    echo -e "${RED}❌ Containers not running${NC}"
    exit 1
fi

# Test 2: MinIO buckets
echo -e "\n2. MinIO Buckets"
if docker exec campaign_minio mc ls local/ | grep -q bronze && \
   docker exec campaign_minio mc ls local/ | grep -q silver; then
    echo -e "${GREEN}✅ Bronze and Silver buckets exist${NC}"
else
    echo -e "${RED}❌ Buckets missing${NC}"
    exit 1
fi

# Test 3: MinIO data
echo -e "\n3. MinIO Data"
if docker exec campaign_minio mc ls local/bronze/campaigns/ | grep -q "date=" && \
   docker exec campaign_minio mc ls local/silver/campaigns/ | grep -q "/"; then
    echo -e "${GREEN}✅ Data exists in MinIO${NC}"
else
    echo -e "${RED}❌ No data in MinIO${NC}"
    exit 1
fi

# Test 4: Postgres schemas
echo -e "\n4. Postgres Schemas"
SCHEMA_COUNT=$(docker exec campaign_analytics_db psql -U dbt_user -d campaign_analytics -t -c "\dn" | grep -c "bronze\|silver\|core\|analytics")
if [ "$SCHEMA_COUNT" -ge 4 ]; then
    echo -e "${GREEN}✅ All schemas exist${NC}"
else
    echo -e "${RED}❌ Missing schemas${NC}"
    exit 1
fi

# Test 5: Postgres data
echo -e "\n5. Postgres Data"
BRONZE_COUNT=$(docker exec campaign_analytics_db psql -U dbt_user -d campaign_analytics -t -c "SELECT COUNT(*) FROM bronze.raw_campaigns")
SILVER_COUNT=$(docker exec campaign_analytics_db psql -U dbt_user -d campaign_analytics -t -c "SELECT COUNT(*) FROM silver.campaigns")

if [ "$BRONZE_COUNT" -gt 0 ] && [ "$SILVER_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✅ Data exists in Postgres${NC}"
    echo "   Bronze: $BRONZE_COUNT rows"
    echo "   Silver: $SILVER_COUNT rows"
else
    echo -e "${RED}❌ No data in Postgres${NC}"
    exit 1
fi

# Test 6: dbt models
echo -e "\n6. dbt Models"
GOLD_COUNT=$(docker exec campaign_analytics_db psql -U dbt_user -d campaign_analytics -t -c "SELECT COUNT(*) FROM core.dim_campaigns")
if [ "$GOLD_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✅ dbt models built${NC}"
    echo "   Gold layer: $GOLD_COUNT rows"
else
    echo -e "${RED}❌ dbt models not built${NC}"
    exit 1
fi

echo -e "\n${GREEN}🎉 All checks passed! Lakehouse is operational.${NC}\n"