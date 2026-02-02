# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2026-02-02

### Added
- **Campaign Analytics Pipeline** - End-to-end data pipeline from CSV to analytics
- **PySpark Layer**: Bronze and Silver data processing
- **dbt Transformation Layer**:
  - Staging: 3 models (stg_advertisers, stg_campaigns, stg_performance)
  - Intermediate: 2 models (int_campaigns_enriched, int_performance_metrics)
  - Marts: 4 models (dim_advertisers, dim_campaigns, fact_performance, campaign_performance_summary)
- **Star Schema**: 2 dimensions + 1 fact table with incremental materialization
- **Custom Macros**: 6 reusable macros (safe_divide, calculate_campaign_kpis, get_performance_tier, etc.)
- **Custom Tests**: 10 custom tests (6 generic, 4 singular)
- **SCD Type 2 Snapshots**: campaigns_snapshot, advertisers_snapshot
- **Comprehensive Testing**: 110 total tests ensuring data quality
- **Complete Documentation**: Model and column-level documentation

### Technical Details
- dbt version: 1.9.0
- Database: PostgreSQL
- Python: 3.12.2
- PySpark: 3.5
