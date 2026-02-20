# Lakehouse Architecture Guide

## Overview

This project implements a **lakehouse architecture** combining:
- **Data Lake** (MinIO) - Scalable, cheap object storage
- **Data Warehouse** (PostgreSQL) - Fast SQL queries for analytics

## Architecture
```
CSV → Bronze (MinIO + Postgres) → Silver (MinIO + Postgres) → Gold (Postgres)
```

### Bronze Layer
- **Purpose**: Raw data preservation (immutable)
- **Storage**: MinIO (Parquet) + Postgres (tables)
- **Format**: All columns as STRING
- **Partitioning**: By ingestion date
- **Location**: 
  - MinIO: `s3a://bronze/{table}/date=YYYY-MM-DD/`
  - Postgres: `bronze.raw_{table}`

### Silver Layer
- **Purpose**: Cleaned, validated, business-ready data
- **Storage**: MinIO (Parquet) + Postgres (tables)
- **Transformations**: Type casting, deduplication, validation
- **Partitioning**: By business attributes (status, date, industry)
- **Location**:
  - MinIO: `s3a://silver/{table}/`
  - Postgres: `silver.{table}`

### Gold Layer
- **Purpose**: Analytics-ready models (star schema)
- **Storage**: Postgres only
- **Tool**: dbt
- **Models**: Staging → Intermediate → Marts
- **Location**: `core.*`, `analytics.*`

## Running the Pipeline

### Full Pipeline
```bash
python run_full_pipeline.py
```

### Individual Layers
```bash
# Bronze only
python pipeline/bronze/ingest_campaigns.py
python pipeline/bronze/ingest_performance.py
python pipeline/bronze/ingest_advertisers.py

# Silver only
python pipeline/silver/clean_campaigns.py
python pipeline/silver/clean_performance.py
python pipeline/silver/clean_advertisers.py

# Gold only
cd dbt_project
dbt run
dbt test
```

## MinIO Operations

### Access Console
- URL: http://localhost:9001
- User: minioadmin
- Pass: minioadmin123

### CLI Operations
```bash
# List buckets
docker exec campaign_minio mc ls local/

# List bronze data
docker exec campaign_minio mc ls local/bronze/campaigns/

# Get file size
docker exec campaign_minio mc du local/bronze/

# Download file
docker exec campaign_minio mc cp local/bronze/campaigns/date=2024-02-09/part-00000.parquet ./
```

## Postgres Operations

### Connect
```bash
docker exec -it campaign_analytics_db psql -U dbt_user -d campaign_analytics
```

### Useful Queries
```sql
-- Check record counts
SELECT 'bronze.raw_campaigns' as table, COUNT(*) FROM bronze.raw_campaigns
UNION ALL
SELECT 'silver.campaigns', COUNT(*) FROM silver.campaigns
UNION ALL
SELECT 'core.dim_campaigns', COUNT(*) FROM core.dim_campaigns;

-- Check data lineage
SELECT 
    campaign_id,
    _silver_processed_at,
    _silver_processed_at - _ingestion_timestamp as processing_time
FROM silver.campaigns
LIMIT 5;

-- Verify partitioning worked
SELECT status, COUNT(*) 
FROM silver.campaigns 
GROUP BY status;
```

## Data Quality

### Silver Layer Validations
- Type casting (STRING → proper types)
- Deduplication (by primary key)
- Business rules:
  - clicks ≤ impressions
  - conversions ≤ clicks
  - Non-negative values
  - Required fields not null

### Gold Layer Tests (dbt)
- 110+ automated tests
- Primary key uniqueness
- Foreign key relationships
- Custom business logic tests

## Storage Comparison

| Format | Size | Savings |
|--------|------|---------|
| CSV (raw) | 15 MB | - |
| Parquet (MinIO) | 4.5 MB | 70% |

## Troubleshooting

### Pipeline Fails
```bash
# Check Docker containers
docker ps

# Check logs
docker logs campaign_analytics_db
docker logs campaign_minio

# Restart infrastructure
docker-compose down
docker-compose up -d
```

### MinIO Connection Issues
```bash
# Test MinIO connection
python test_minio_connection.py

# Check MinIO health
docker exec campaign_minio mc admin info local
```

### Postgres Connection Issues
```bash
# Test connection
docker exec campaign_analytics_db pg_isready -U dbt_user

# Reset database (WARNING: deletes data)
docker-compose down -v
docker-compose up -d
```

## Interview Talking Points

**Lakehouse Architecture:**
> "I built a lakehouse combining MinIO object storage with PostgreSQL, achieving 70% storage reduction through Parquet columnar format while maintaining SQL query performance for analytics."

**Separation of Concerns:**
> "The architecture separates compute from storage—PySpark handles transformations reading from S3-compatible MinIO, while PostgreSQL serves as the semantic layer for dbt and BI tools."

**Data Quality:**
> "Implemented comprehensive validation with 110+ dbt tests plus Great Expectations checks in the Silver layer, ensuring data quality gates before analytics consumption."

**Scalability:**
> "The medallion pattern with Bronze/Silver/Gold layers enables independent scaling—can add Spark workers for compute without moving data, and MinIO scales horizontally for storage."