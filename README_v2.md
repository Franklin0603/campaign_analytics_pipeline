# Campaign Analytics Lakehouse

Production-grade data lakehouse implementing Delta Lake, medallion architecture, and dbt transformations.

## 🏗️ Architecture
```
CSV Files → Bronze (Delta) → Silver (Delta) → Gold (dbt) → Postgres
              ↓                  ↓               ↓
           MinIO              MinIO         Postgres
```

### Key Features
- ✅ **Delta Lake**: ACID transactions, time travel, MERGE operations
- ✅ **Medallion Architecture**: Bronze → Silver → Gold layers
- ✅ **Data Quality**: 110+ automated dbt tests
- ✅ **Incremental Processing**: Partition-based incremental loads
- ✅ **Containerized**: Full Docker Compose setup

## 📊 Layer Details

### Bronze Layer (Raw Archive)
- **Purpose**: Immutable raw data archive
- **Format**: Delta Lake (Parquet + transaction log)
- **Partitioning**: `ingest_date` for incremental processing
- **Location**: MinIO `s3a://bronze/{table}/`
- **Schema**: All columns as STRING (raw format)

**Example:**
```
bronze/campaigns/
├── ingest_date=2026-02-19/
│   └── part-00000.snappy.parquet
└── _delta_log/
    └── 00000000000.json
```

### Silver Layer (Curated Data)
- **Purpose**: Cleaned, validated, business-ready data
- **Format**: Delta Lake
- **Partitioning**: Business attributes (status, date, industry)
- **Location**: MinIO `s3a://silver/{table}/`
- **Transformations**: Type casting, deduplication, validation

**Example:**
```
silver/campaigns/
├── status=Active/
│   └── part-00000.snappy.parquet
├── status=Paused/
└── _delta_log/
```

### Gold Layer (Analytics)
- **Purpose**: Star schema for BI/analytics
- **Tool**: dbt
- **Location**: Postgres `core.*`, `analytics.*`
- **Models**: 9 models (staging → intermediate → marts)
- **Tests**: 110+ data quality tests

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.12+
- 8GB RAM minimum

### Setup
```bash
# 1. Clone repository
git clone https://github.com/Franklin0603/campaign_analytics_pipeline.git
cd campaign_analytics_pipeline

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Start infrastructure
docker-compose up -d

# 5. Run full pipeline
python run_full_pipeline.py
```

### Verify
```bash
# Check MinIO Console
open http://localhost:9001  # Login: minioadmin/minioadmin123

# Check Postgres data
docker exec -it campaign_analytics_db psql -U dbt_user -d campaign_analytics -c "SELECT COUNT(*) FROM core.dim_campaigns;"

# View dbt docs
cd dbt_project
dbt docs generate
dbt docs serve
```

## 📁 Project Structure
```
campaign_analytics_pipeline/
├── config/
│   ├── database.yaml          # Database connection configs
│   └── minio_config.py        # MinIO S3 configuration
├── data/
│   └── raw/                   # Source CSV files
│       ├── campaigns.csv
│       ├── performance.csv
│       └── advertisers.csv
├── pipeline/
│   ├── bronze/                # Bronze layer ingestion
│   │   ├── ingest_campaigns.py
│   │   ├── ingest_performance.py
│   │   └── ingest_advertisers.py
│   ├── silver/                # Silver layer transformations
│   │   ├── clean_campaigns.py
│   │   ├── clean_performance.py
│   │   └── clean_advertisers.py
│   └── utils/
│       └── spark_postgres.py  # Shared utilities
├── dbt_project/               # Gold layer (dbt)
│   ├── models/
│   │   ├── staging/           # Source data staging
│   │   ├── intermediate/      # Business logic
│   │   └── marts/             # Final analytics tables
│   └── tests/                 # Data quality tests
├── scripts/
│   └── init_db.sql            # Database initialization
├── docker-compose.yml         # Infrastructure setup
├── run_full_pipeline.py       # Master pipeline runner
├── demo_delta_features.py     # Delta Lake capabilities demo
└── requirements.txt
```

## 💾 Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Object Storage | MinIO | S3-compatible data lake |
| Table Format | Delta Lake 2.4 | ACID transactions, versioning |
| Processing | Apache Spark 3.4 | Distributed data processing |
| Warehouse | PostgreSQL 15 | Analytics database |
| Transformation | dbt 1.9 | SQL-based modeling |
| Data Quality | Great Expectations | Validation framework |
| Orchestration | Python scripts | Pipeline automation |
| Infrastructure | Docker Compose | Containerization |

## 📈 Data Flow

### Full Pipeline
```bash
python run_full_pipeline.py
```

**Execution:**
1. **Bronze**: Ingest CSV → Delta Lake (MinIO + Postgres)
2. **Silver**: Transform → Delta Lake (MinIO + Postgres)
3. **Gold**: dbt run → Star schema (Postgres)

**Duration:** ~60-90 seconds

### Individual Layers
```bash
# Bronze only
python pipeline/bronze/ingest_campaigns.py

# Silver only
python pipeline/silver/clean_campaigns.py

# Gold only (dbt)
cd dbt_project
dbt run
dbt test
```

## 🎯 Delta Lake Features

### Time Travel
```python
# Query historical versions
df = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("s3a://silver/campaigns/")

# Query by timestamp
df = spark.read.format("delta") \
    .option("timestampAsOf", "2026-02-19") \
    .load("s3a://silver/campaigns/")
```

### MERGE Operations
```python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "s3a://silver/campaigns/")

# Update existing, insert new
deltaTable.alias("target").merge(
    updates.alias("source"),
    "target.campaign_id = source.campaign_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

### Version History
```python
# Run demo script
python demo_delta_features.py
```

## 🧪 Data Quality

### dbt Tests (110+ tests)
```bash
cd dbt_project
dbt test

# Run specific test
dbt test --select dim_campaigns
```

**Test categories:**
- ✅ Uniqueness constraints
- ✅ Not null checks
- ✅ Referential integrity
- ✅ Accepted values
- ✅ Custom business rules

### Great Expectations
```bash
# Run validation (integrated in Silver layer)
python pipeline/silver/clean_campaigns.py
```

## 📊 Performance Metrics

| Metric | Value |
|--------|-------|
| Storage Reduction | 70% (Parquet vs CSV) |
| Bronze Ingestion | ~15 seconds |
| Silver Transform | ~20 seconds |
| Gold dbt Run | ~25 seconds |
| **Total Pipeline** | **~60 seconds** |
| Data Quality Tests | 110+ automated |

## 🔧 Configuration

### Environment Variables

Create `.env` file:
```bash
# Postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=campaign_analytics
POSTGRES_USER=dbt_user
POSTGRES_PASSWORD=dbt_password

# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_SECURE=false

# JDBC
JDBC_DRIVER_PATH=lib/postgresql-42.6.0.jar
```

### MinIO Buckets

Created automatically on startup:
- `bronze` - Raw data archive
- `silver` - Curated data
- `logs` - Pipeline logs
- `data-quality` - Quality reports

## 📖 Documentation

- [Architecture Guide](docs/ARCHITECTURE.md) - Detailed system design
- [Lakehouse Guide](docs/LAKEHOUSE_GUIDE.md) - Delta Lake operations
- [dbt Documentation](http://localhost:8080) - Run `dbt docs serve`

## 🐛 Troubleshooting

### Docker containers not starting
```bash
# Check what's using port 5432
lsof -i :5432

# Stop local PostgreSQL
brew services stop postgresql

# Restart containers
docker-compose down
docker-compose up -d
```

### Pipeline fails
```bash
# Check container logs
docker logs campaign_analytics_db
docker logs campaign_minio

# Verify MinIO is accessible
docker exec campaign_minio mc ls local/

# Check Postgres connection
docker exec -it campaign_analytics_db psql -U dbt_user -d campaign_analytics
```

### Delta Lake "table doesn't exist"
```bash
# Ensure Bronze ran first
python pipeline/bronze/ingest_campaigns.py

# Then run Silver
python pipeline/silver/clean_campaigns.py
```

## 🤝 Contributing

This is a portfolio project. Feedback and suggestions welcome via issues.

## 📝 License

MIT License - see LICENSE file

## 👤 Author

**Franklin Ajisogun**
- LinkedIn: [franklin-ajisogun](https://linkedin.com/in/franklin-ajisogun)
- GitHub: [@Franklin0603](https://github.com/Franklin0603)
- Portfolio: [Your Portfolio URL]

## 🙏 Acknowledgments

- Built with guidance from industry best practices
- Inspired by production lakehouse implementations at Uber, Netflix, Airbnb
- Delta Lake technology from Linux Foundations