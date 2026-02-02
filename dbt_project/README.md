# Campaign Analytics dbt Project

## Overview

This dbt project transforms campaign performance data through a **staging → intermediate → marts** architecture, producing analytics-ready dimensional models.

## Data Flow

```
Silver Layer (PySpark)
    ↓
Staging Models (stg_*)
    ↓
Intermediate Models (int_*)
    ↓
Marts Layer (dim_*, fact_*, analytics)
```

## Project Structure

### Staging Layer
**Purpose**: Light transformations, 1:1 with source tables

- `stg_advertisers` - Advertiser dimension from silver
- `stg_campaigns` - Campaign dimension from silver  
- `stg_performance` - Daily performance metrics from silver

**Characteristics**:
- Materialized as views
- Standardized column naming
- No business logic
- Schema: `staging`

### Intermediate Layer
**Purpose**: Business logic, joins, calculated metrics

- `int_campaigns_enriched` - Campaigns joined with advertiser data
- `int_performance_metrics` - Performance with all KPIs calculated

**Characteristics**:
- Materialized as views
- Contains joins and business rules
- Uses reusable macros for calculations
- Schema: `intermediate`

### Marts Layer
**Purpose**: Final business models ready for BI tools

#### Core Marts (Dimensional Model)
- `dim_advertisers` - Advertiser dimension
- `dim_campaigns` - Campaign dimension  
- `fact_performance` - Daily performance fact (incremental)

**Characteristics**:
- Star schema design
- Materialized as tables
- Incremental processing on facts
- Schema: `core`

#### Analytics Marts (Aggregated Views)
- `campaign_performance_summary` - Campaign-level aggregations

**Characteristics**:
- Materialized as views
- Pre-aggregated for performance
- BI tool ready
- Schema: `analytics`

## Custom Macros

- `calculate_campaign_kpis` - Standardized KPI calculations
- `safe_divide` - Prevent division by zero errors
- `get_performance_tier` - Categorize ROI performance
- `date_spine` - Generate date ranges
- `generate_schema_name` - Multi-environment schema control

## Data Quality

### Built-in Tests
- `unique` - Primary key validation
- `not_null` - Required field validation
- `relationships` - Referential integrity

### Custom Tests
- `is_percentage` - Validate 0-100% range
- `is_non_negative` - No negative values
- `clicks_lte_impressions` - Funnel logic
- `conversions_lte_clicks` - Funnel logic
- `end_date_after_start_date` - Date validation

## Key Metrics Calculated

- **CTR** (Click-Through Rate) = (Clicks / Impressions) × 100
- **CVR** (Conversion Rate) = (Conversions / Clicks) × 100
- **CPC** (Cost Per Click) = Cost / Clicks
- **CPM** (Cost Per Mille) = (Cost / Impressions) × 1000
- **CPA** (Cost Per Acquisition) = Cost / Conversions
- **ROI** (Return on Investment) = ((Revenue - Cost) / Cost) × 100

## Running the Project

```bash
# Run all models
dbt run

# Run specific layer
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Test all models
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Environments

- **dev**: Development environment (local PostgreSQL)
- **prod**: Production environment (configure separately)