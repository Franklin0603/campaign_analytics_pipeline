# Campaign Analytics Pipeline

> **End-to-end data pipeline** implementing Bronze/Silver/Gold (Medallion) architecture with PySpark, dbt, and Great Expectations

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/PySpark-3.4.0-orange.svg)](https://spark.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.8+-red.svg)](https://www.getdbt.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-blue.svg)](https://www.postgresql.org/)

## 📋 Overview

This project demonstrates a **production-ready data pipeline** for campaign analytics, showcasing:

- ✅ **Medallion Architecture** (Bronze → Silver → Gold)
- ✅ **PySpark** for distributed data processing
- ✅ **dbt** for analytics engineering
- ✅ **Great Expectations** for data quality validation
- ✅ **PostgreSQL** as the data warehouse
- ✅ **Modular, testable, and maintainable** code structure

### Architecture

```
CSV Files (Raw Data)
     ↓
┌─────────────────┐
│  BRONZE LAYER   │  ← Raw data preservation
│  (PySpark)      │     • Campaigns
│                 │     • Performance
│  PostgreSQL     │     • Advertisers
└────────┬────────┘
         ↓
┌─────────────────┐
│  SILVER LAYER   │  ← Data transformation & quality
│  (PySpark +     │     • Type casting
│   Great Exp.)   │     • Deduplication
│                 │     • Business rules
│  PostgreSQL     │     • Quality checks
└────────┬────────┘
         ↓
┌─────────────────┐
│   GOLD LAYER    │  ← Analytics-ready models
│   (dbt)         │     • Dimension tables
│                 │     • Fact tables
│  PostgreSQL     │     • Aggregations
└─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- PostgreSQL 15+
- Java 8+ (for PySpark)
- 2GB RAM minimum

### Installation

```bash
# 1. Clone the repository
git clone <your-repo-url>
cd campaign_analytics_pipeline

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Download PostgreSQL JDBC driver
mkdir lib
cd lib
curl -O https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
cd ..

# 5. Setup PostgreSQL database
createdb campaign_analytics
psql campaign_analytics < sql/create_schemas.sql
```

### Configuration

Edit `config/database.yaml` with your PostgreSQL credentials:

```yaml
postgres:
  host: localhost
  port: 5432
  database: campaign_analytics
  user: postgres
  password: "your_password"  # Add your password
```

### Run the Pipeline

```bash
# Run Bronze → Silver layers
python run_pipeline.py

# Initialize dbt (first time only)
dbt init dbt_project
# Follow prompts with your PostgreSQL credentials

# Run dbt models (Gold layer)
cd dbt_project
dbt run
dbt test
dbt docs generate
dbt docs serve
```

## 📁 Project Structure

```
campaign_analytics_pipeline/
├── data/
│   └── raw/                    # Sample CSV data
│       ├── campaigns.csv
│       ├── performance.csv
│       └── advertisers.csv
├── pyspark/
│   ├── bronze/                 # Bronze layer ingestion
│   │   ├── ingest_campaigns.py
│   │   ├── ingest_performance.py
│   │   └── ingest_advertisers.py
│   ├── silver/                 # Silver layer transformations
│   │   ├── clean_campaigns.py
│   │   ├── clean_performance.py
│   │   └── clean_advertisers.py
│   └── utils/                  # Shared utilities
│       └── spark_postgres.py
├── dbt_project/                # dbt analytics models
│   ├── models/
│   │   ├── staging/           # Staging views
│   │   ├── intermediate/      # Business logic
│   │   └── marts/             # Final dimensions & facts
│   └── tests/                 # dbt tests
├── config/
│   └── database.yaml          # Database configuration
├── sql/
│   └── create_schemas.sql     # Schema setup
├── quality_reports/           # Data quality reports
├── run_pipeline.py            # Master pipeline runner
├── requirements.txt           # Python dependencies
└── README.md
```

## 🔄 Pipeline Stages

### Bronze Layer (Raw Data)
- **Purpose**: Preserve raw data exactly as received
- **Technology**: PySpark
- **Schema**: `bronze.raw_*`
- **Features**:
  - No transformations
  - All columns as STRING type
  - Metadata columns for lineage
  - Append-only writes

### Silver Layer (Cleansed Data)
- **Purpose**: Transform and validate data
- **Technology**: PySpark + Great Expectations
- **Schema**: `silver.*`
- **Features**:
  - Type casting and validation
  - Deduplication
  - Business rule application
  - Data quality checks
  - Calculated fields (CTR, CPC, etc.)

### Gold Layer (Analytics)
- **Purpose**: Business-ready analytics models
- **Technology**: dbt
- **Schema**: `analytics.*`
- **Features**:
  - Dimension tables
  - Fact tables with joins
  - Aggregations
  - Tests and documentation

## 📊 Data Model

### Source Tables (Bronze)
- `bronze.raw_campaigns` - Campaign master data
- `bronze.raw_performance` - Daily performance metrics
- `bronze.raw_advertisers` - Advertiser information

### Cleansed Tables (Silver)
- `silver.campaigns` - Validated campaigns with derived fields
- `silver.performance` - Metrics with CTR, CPC, conversion rates
- `silver.advertisers` - Standardized advertiser data

### Analytics Tables (Gold)
- `analytics.dim_campaigns` - Campaign dimension
- `analytics.dim_advertisers` - Advertiser dimension
- `analytics.fact_performance` - Performance fact table

## 🔍 Data Quality

The pipeline includes comprehensive data quality checks using **Great Expectations**:

- ✅ Schema validation
- ✅ Null checks
- ✅ Uniqueness constraints
- ✅ Range validation
- ✅ Business rule enforcement
- ✅ Referential integrity

Quality check results are logged and can fail the pipeline if critical issues are detected.

## 🧪 Testing

```bash
# Test Bronze ingestion
python pyspark/bronze/ingest_campaigns.py

# Test Silver transformation
python pyspark/silver/clean_campaigns.py

# Test dbt models
cd dbt_project
dbt test

# Verify data in PostgreSQL
psql campaign_analytics -c "SELECT COUNT(*) FROM silver.campaigns;"
psql campaign_analytics -c "SELECT * FROM analytics.dim_campaigns LIMIT 5;"
```

## 📈 Key Metrics Calculated

The pipeline automatically calculates:

- **Click-Through Rate (CTR)**: `(clicks / impressions) × 100`
- **Conversion Rate**: `(conversions / clicks) × 100`
- **Cost Per Click (CPC)**: `spend / clicks`
- **Cost Per Conversion**: `spend / conversions`
- **Campaign Duration**: `end_date - start_date`
- **Budget Tier**: Small (<$10K), Medium (<$50K), Large (>$50K)

## 🛠️ Technologies Used

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Processing** | PySpark 3.4 | Distributed data transformation |
| **Analytics** | dbt 1.8 | SQL-based modeling |
| **Data Quality** | Great Expectations | Validation framework |
| **Database** | PostgreSQL 15 | Data warehouse |
| **Language** | Python 3.8+ | Pipeline orchestration |

## 🎯 Use Cases

This pipeline architecture is suitable for:

- Marketing analytics platforms
- Campaign performance tracking
- Multi-source data integration
- Data quality monitoring
- Analytics engineering workflows

## 📝 Development Notes

### Adding New Data Sources

1. Create CSV in `data/raw/`
2. Add ingestion script in `pyspark/bronze/`
3. Add transformation in `pyspark/silver/`
4. Create dbt model in `dbt_project/models/`

### Extending Transformations

Silver layer transformations can be extended with:
- Additional calculated fields
- More complex business rules
- Advanced data quality checks
- ML feature engineering

## 🤝 Contributing

Contributions welcome! Areas for enhancement:
- Airflow/Prefect orchestration
- Delta Lake integration
- Advanced aggregations
- Real-time streaming ingestion
- CI/CD pipeline

## 📧 Contact

**Franklin Ajisogun**
- LinkedIn: [your-linkedin]
- GitHub: [your-github]
- Email: [your-email]

## 📄 License

MIT License - feel free to use this project for learning and portfolio purposes.

---

**Built with** ❤️ **by Franklin Ajisogun** | Demonstrating modern data engineering practices