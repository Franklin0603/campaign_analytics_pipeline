# Campaign Analytics dbt Project

## 📊 Project Overview

This dbt project implements the **Gold layer** of our campaign analytics pipeline, transforming cleaned data from the Silver layer into analytics-ready dimensional models.

### Architecture Pattern
```
Silver Layer (PySpark) → Staging → Intermediate → Marts (Star Schema)
```

**Data Flow:**
1. **Silver → Staging**: 1:1 lightweight transformations, column renaming
2. **Staging → Intermediate**: Business logic, KPI calculations, joins
3. **Intermediate → Marts**: Final dimensional models for BI tools

### Key Features

- ✅ **9 production models** (3 staging, 2 intermediate, 4 marts)
- ✅ **102 comprehensive tests** ensuring data quality
- ✅ **6 custom macros** for code reusability
- ✅ **2 SCD Type 2 snapshots** for historical tracking
- ✅ **Incremental materialization** for performance optimization

### Project Statistics

| Metric | Value |
|--------|-------|
| Total Models | 9 |
| Total Tests | 102 |
| Test Coverage | 100% |
| Custom Macros | 6 |
| Snapshots | 2 |
| Daily Refresh Time | ~2 minutes |

### Data Refresh Schedule

| Layer | Frequency | Time (UTC) | Runtime |
|-------|-----------|------------|---------|
| Staging | Daily | 2:00 AM | ~10s |
| Intermediate | Daily | 2:01 AM | ~30s |
| Marts | Daily | 2:02 AM | ~1m |

### Ownership

**Team**: Data Engineering  
**Contact**: data-engineering@company.com  
**On-Call**: #data-eng-oncall

---

## 📚 Layer Documentation

### Staging Layer
[View Staging Documentation →](#!/model/model.campaign_analytics.stg_campaigns)

1:1 transformations from Silver layer with standardized naming.

### Intermediate Layer
[View Intermediate Documentation →](#!/model/model.campaign_analytics.int_campaigns_enriched)

Business logic, joins, and KPI calculations.

### Marts Layer
[View Marts Documentation →](#!/model/model.campaign_analytics.dim_campaigns)

Final dimensional models for analytics and BI tools.

{% enddocs %}

---

## 🎯 STAGING LAYER DOCUMENTATION

{% docs stg_advertisers %}

## Advertiser Staging Model

**Purpose**: 1:1 transformation from `silver.advertisers` with standardized column naming.

**Grain**: One row per advertiser

**Materialization**: View (always fresh)

**Refresh**: Daily at 2:00 AM UTC

### Source
- **Layer**: Silver (PySpark)
- **Table**: `silver.advertisers`
- **Owner**: Data Platform Team

### Transformations
- Column renaming to match naming standards
- Country code standardization (uppercase)
- No business logic (pure staging)

### Usage
- Input for `int_campaigns_enriched`
- Input for `dim_advertisers`

### Data Quality
- 100% test coverage on primary key
- Uniqueness enforced
- Not-null constraints on all required fields

{% enddocs %}

{% docs stg_campaigns %}

## Campaign Staging Model

**Purpose**: 1:1 transformation from `silver.campaigns` with standardized column naming.

**Grain**: One row per campaign

**Materialization**: View

**Refresh**: Daily at 2:00 AM UTC

### Source
- **Layer**: Silver (PySpark)
- **Table**: `silver.campaigns`

### Transformations
- Column renaming (e.g., `start_date` → `campaign_start_date`)
- Budget fields renamed with `_usd` suffix for clarity
- Status field standardization

### Downstream Models
- `int_campaigns_enriched` - Joins with advertiser data
- `dim_campaigns` - Final dimension table

### Business Rules
- **Status Values**: Active, Paused, Completed, Draft
- **Date Logic**: `campaign_end_date >= campaign_start_date`
- **Budget Validation**: Both daily and total budgets must be > 0

{% enddocs %}

{% docs stg_performance %}

## Performance Staging Model

**Purpose**: Daily campaign performance metrics from Silver layer.

**Grain**: One row per campaign per day

**Materialization**: View

**Refresh**: Daily at 2:00 AM UTC

### Source
- **Layer**: Silver (PySpark)
- **Table**: `silver.performance`

### Transformations
- Column renaming for consistency
- Currency fields suffixed with `_usd`
- Date column renamed to `performance_date`

### Downstream Models
- `int_performance_metrics` - KPI calculations
- `fact_performance` - Incremental fact table

### Data Volume
- **Daily Records**: ~50-100 per day
- **Historical Records**: ~5,000 total
- **Growth Rate**: ~50 rows/day

### Critical Fields
- `performance_date`: Date of metrics (NOT the processing date)
- `cost_usd`: Total ad spend for the day
- `revenue_usd`: Total revenue generated

{% enddocs %}

---

## 🔧 INTERMEDIATE LAYER DOCUMENTATION

{% docs int_campaigns_enriched %}

## Campaigns Enriched (Intermediate)

**Purpose**: Campaigns joined with advertiser data + derived business fields.

**Grain**: One row per campaign

**Materialization**: View

**Refresh**: Daily at 2:01 AM UTC

### Business Logic

This model enriches campaign data with:

1. **Advertiser Context**: Joins advertiser name, industry from `dim_advertisers`
2. **Derived Fields**:
   - `campaign_duration_days`: Calculates campaign length
   - `campaign_current_state`: Determines if campaign is Running, Scheduled, Ended, etc.
   - `is_currently_active`: Boolean flag for today's active campaigns
   - `expected_daily_spend`: Budget / duration for pacing analysis

### Calculated Fields

**`campaign_duration_days`**:
```sql
DATEDIFF(day, campaign_start_date, campaign_end_date) + 1
```

**`campaign_current_state`**:
- **Running**: Today between start/end dates AND status = 'Active'
- **Scheduled**: Start date in future
- **Ended**: End date in past
- **Paused**: Status = 'Paused'
- **Completed**: Status = 'Completed'

**`expected_daily_spend`**:
```sql
total_budget_usd / NULLIF(campaign_duration_days, 0)
```

### Use Cases
- Campaign pacing analysis (actual vs. expected spend)
- Active campaign monitoring
- Campaign lifecycle reporting

### Dependencies
- `stg_campaigns`
- `stg_advertisers`

### Data Quality
- Primary key: `campaign_id` (unique, not null)
- All derived fields validated with custom tests

{% enddocs %}

{% docs int_performance_metrics %}

## Performance Metrics (Intermediate)

**Purpose**: Daily performance data with ALL calculated KPIs and business metrics.

**Grain**: One row per campaign per day

**Materialization**: View

**Refresh**: Daily at 2:01 AM UTC

### KPI Calculations

All industry-standard marketing KPIs are calculated using custom macros for consistency:

#### Efficiency Metrics

**Click-Through Rate (CTR)**:
```sql
{{ safe_divide('clicks', 'impressions') }} * 100
```
- **Range**: 0-100%
- **Industry Benchmark**: 2-5%

**Conversion Rate (CVR)**:
```sql
{{ safe_divide('conversions', 'clicks') }} * 100
```
- **Range**: 0-100%
- **Industry Benchmark**: 1-3%

#### Cost Metrics

**Cost Per Click (CPC)**:
```sql
{{ safe_divide('cost_usd', 'clicks') }}
```
- **Unit**: USD
- **Lower is better**

**Cost Per Mille (CPM)** - Cost per 1,000 impressions:
```sql
{{ safe_divide('cost_usd', 'impressions') }} * 1000
```

**Cost Per Acquisition (CPA)**:
```sql
{{ safe_divide('cost_usd', 'conversions') }}
```

#### Profitability Metrics

**ROI (Return on Investment)**:
```sql
{{ safe_divide('revenue_usd - cost_usd', 'cost_usd') }} * 100
```
- **Positive ROI**: > 0%
- **Break-even**: 0%
- **Loss**: < 0%

**Profit**:
```sql
revenue_usd - cost_usd
```

### Performance Tiers

Campaigns are classified based on ROI:
- **Excellent**: ROI > 100%
- **Good**: ROI 50-100%
- **Fair**: ROI 0-50%
- **Poor**: ROI < 0%
- **Unknown**: No revenue data

### Data Quality Flags

**`has_invalid_clicks`**: TRUE if clicks > impressions (data quality issue)

**`has_invalid_conversions`**: TRUE if conversions > clicks (data quality issue)

### Macro Usage
- `{{ safe_divide() }}` - Prevents division by zero errors
- `{{ calculate_campaign_kpis() }}` - Centralized KPI logic
- `{{ get_performance_tier() }}` - Classifies performance

### Use Cases
- Campaign performance dashboards
- ROI analysis and reporting
- Budget optimization
- A/B testing analysis

{% enddocs %}

---

## 🏛️ MARTS LAYER DOCUMENTATION

{% docs dim_advertisers %}

## Advertiser Dimension

**Purpose**: Master advertiser data for analytics.

**Grain**: One row per advertiser

**Materialization**: Table (refreshed daily)

**Refresh**: Daily at 2:02 AM UTC (~3s runtime)

### Type
Dimension table (slowly changing - but not snapshotted in this version)

### Keys
- **Primary Key**: `advertiser_id`
- **Natural Key**: `advertiser_name`

### Attributes
- `advertiser_name`: Company name
- `industry`: Business vertical
- `country_code`: ISO country code
- `account_manager`: Assigned account manager

### Relationships
- **One-to-Many** with `dim_campaigns`
- **One-to-Many** with `fact_performance` (through campaigns)

### Use Cases
- Advertiser performance reports
- Industry analysis
- Account manager performance

### SLA
- **Freshness**: < 24 hours
- **Quality**: 100% unique, not null on PK

{% enddocs %}

{% docs dim_campaigns %}

## Campaign Dimension

**Purpose**: Master campaign data with advertiser context.

**Grain**: One row per campaign

**Materialization**: Table (refreshed daily)

**Refresh**: Daily at 2:02 AM UTC (~10s runtime)

### Type
Dimension table (Type 1 SCD - always current state)

### Keys
- **Primary Key**: `campaign_id`
- **Foreign Key**: `advertiser_id` → `dim_advertisers`

### Attributes

**Core Fields**:
- `campaign_name`: Campaign name
- `campaign_type`: Search, Display, Video, Social, Shopping
- `campaign_status`: Active, Paused, Completed, Draft

**Date Fields**:
- `campaign_start_date`: When campaign starts
- `campaign_end_date`: When campaign ends

**Budget Fields**:
- `daily_budget_usd`: Daily spending limit
- `total_budget_usd`: Lifetime budget

**Derived Fields** (from intermediate layer):
- `campaign_duration_days`: Total campaign length
- `campaign_current_state`: Running, Scheduled, Ended, etc.

### Relationships
- **Many-to-One** with `dim_advertisers`
- **One-to-Many** with `fact_performance`

### Historical Tracking

For historical changes (budget adjustments, status changes), see:
- `snapshots.campaigns_snapshot` (SCD Type 2)

### Use Cases
- Campaign performance dashboards
- Budget pacing analysis
- Campaign lifecycle reporting

### Business Rules
- `campaign_end_date` must be >= `campaign_start_date`
- Both budgets must be > 0
- Status must be one of: Active, Paused, Completed, Draft

{% enddocs %}

{% docs fact_performance %}

## Performance Fact Table

**Purpose**: Daily campaign performance metrics with all KPIs.

**Grain**: One row per campaign per day

**Materialization**: **Incremental Table** (for performance)

**Refresh**: Daily at 2:02 AM UTC (~20s runtime)

### Type
Fact table (transaction grain - daily metrics)

### Keys
- **Primary Key**: `performance_id`
- **Foreign Keys**:
  - `campaign_id` → `dim_campaigns`
  - `performance_date` (time dimension)

### Fact Measures

**Raw Metrics** (additive):
- `impressions`: Ad views
- `clicks`: Ad clicks
- `conversions`: Goal completions
- `cost_usd`: Total spend
- `revenue_usd`: Total revenue

**Calculated Metrics** (semi-additive/non-additive):
- `ctr_percent`: Click-through rate
- `cvr_percent`: Conversion rate
- `cpc_usd`: Cost per click
- `cpm_usd`: Cost per thousand impressions
- `cpa_usd`: Cost per acquisition
- `roi_percent`: Return on investment

### Incremental Strategy

**Unique Key**: `performance_id`

**Incremental Logic**:
```sql
WHERE performance_date > (SELECT MAX(performance_date) FROM {{ this }})
```

**Performance Impact**:
- Initial load: ~45s for 5,000 rows
- Daily incremental: ~5s for ~50 new rows
- Efficiency: 80% reduction in runtime

### Full Refresh

Run with `--full-refresh` for:
- Schema changes
- Logic updates
- Historical recalculation
```bash
dbt run --select fact_performance --full-refresh
```

### Use Cases
- Daily performance dashboards (Tableau, Looker)
- Trend analysis over time
- ROI calculation and reporting
- Campaign optimization

### Data Volume
- **Daily Inserts**: ~50 rows/day
- **Total Records**: ~5,000
- **Growth Rate**: ~18,000 rows/year

### SLA
- **Freshness**: < 24 hours
- **Completeness**: 100% of campaigns with activity
- **Quality**: All KPIs validated with tests

{% enddocs %}

{% docs campaign_performance_summary %}

## Campaign Performance Summary

**Purpose**: Pre-aggregated campaign-level metrics for fast BI queries.

**Grain**: One row per campaign (lifetime aggregation)

**Materialization**: View (materializes on query)

**Refresh**: Real-time (view always queries latest data)

### Aggregation Logic

Aggregates `fact_performance` to campaign level:
- `SUM(cost_usd)` → `total_cost_usd`
- `SUM(revenue_usd)` → `total_revenue_usd`
- `SUM(revenue - cost)` → `total_profit_usd`
- `(SUM(revenue) - SUM(cost)) / SUM(cost) * 100` → `total_roi_percent`

### Performance Tier

Classifies campaigns based on total ROI:
- **Excellent**: > 100%
- **Good**: 50-100%
- **Fair**: 0-50%
- **Poor**: < 0%
- **Unknown**: No revenue

### Use Cases
- Executive dashboards
- Campaign comparison reports
- Top/bottom performer analysis
- ROI leaderboards

### BI Tool Usage
- **Tableau**: "Campaign Performance Dashboard"
- **Looker**: "Campaign ROI Analysis"
- **Excel**: Ad-hoc executive reports

### Optimization Note
This is a **view** for simplicity. For large datasets (>10K campaigns), consider:
- Materializing as table
- Adding incremental logic
- Partitioning by advertiser

{% enddocs %}

---

## 📊 COLUMN DOCUMENTATION

{% docs col_campaign_id %}
**Unique identifier for each campaign.**

**Type**: Integer  
**Source**: Advertising platform auto-generated ID  
**Example Values**: 1, 2, 3, 4, 5

**Usage**:
- Primary key in dimension tables
- Foreign key in fact tables
- Join key across all models
{% enddocs %}

{% docs col_performance_date %}
**Date when the metrics were recorded.**

**Type**: Date  
**Format**: YYYY-MM-DD  
**Example**: 2024-01-15

**Important**: This is the **metrics date**, not the processing date.

**Business Rule**: Should never be in the future.
{% enddocs %}

{% docs col_ctr_percent %}
**Click-Through Rate: Percentage of impressions that resulted in clicks.**

**Formula**: `(clicks / impressions) × 100`

**Range**: 0-100%  
**Industry Benchmark**: 2-5%  
**Higher is better**

**Example**: If 1,000 impressions generated 50 clicks, CTR = 5%

**Use**: Measures ad effectiveness and relevance.
{% enddocs %}

{% docs col_roi_percent %}
**Return on Investment: Profit as percentage of cost.**

**Formula**: `((revenue - cost) / cost) × 100`

**Interpretation**:
- **ROI = 100%**: Doubled your money
- **ROI = 0%**: Break-even
- **ROI < 0%**: Loss

**Example**: Spend $100, earn $150 → ROI = 50%

**Critical Metric**: Primary profitability indicator.
{% enddocs %}