{% docs stg_advertisers %}
## Staging: Advertisers

**Source**: `silver.advertisers` (cleaned by PySpark)

**Purpose**: Standardize advertiser data with consistent naming conventions

**Transformations**:
- Rename `country` → `country_code`
- Rename `_silver_processed_at` → `silver_loaded_at`
- No filtering or business logic

**Grain**: One row per advertiser

**Usage**: Base table for joins in intermediate layer

{% enddocs %}

{% docs stg_campaigns %}
## Staging: Campaigns

**Source**: `silver.campaigns` (cleaned by PySpark)

**Purpose**: Standardize campaign data with consistent naming

**Transformations**:
- Rename `status` → `campaign_status`
- Rename `objective` → `campaign_objective`
- Rename date columns for clarity
- Rename budget columns with USD suffix

**Grain**: One row per campaign

**Usage**: Joined with advertisers in `int_campaigns_enriched`

{% enddocs %}

{% docs stg_performance %}
## Staging: Performance

**Source**: `silver.performance` (cleaned by PySpark)

**Purpose**: Standardize daily performance metrics

**Transformations**:
- Rename `date` → `performance_date`
- Add USD suffix to monetary columns
- No calculations (raw metrics only)

**Grain**: One row per campaign per day

**Usage**: All KPIs calculated in `int_performance_metrics`

{% enddocs %}

{% docs int_campaigns_enriched %}
## Intermediate: Campaigns Enriched

**Purpose**: Combine campaigns with advertiser context and add derived fields

**Business Logic**:
- Join campaigns with advertisers
- Calculate campaign duration
- Determine current campaign state (Running/Scheduled/Ended)
- Calculate expected daily spend

**Grain**: One row per campaign

**Downstream**: Used in `dim_campaigns` and analytics

{% enddocs %}

{% docs int_performance_metrics %}
## Intermediate: Performance Metrics

**Purpose**: Calculate all campaign KPIs using standardized macro

**Calculations** (via `calculate_campaign_kpis` macro):
- CTR, CVR, CPC, CPM, CPA
- ROI, Profit, Cost/Revenue Ratio
- Revenue per conversion, Revenue per click

**Additional Logic**:
- Performance tier categorization (Excellent/Good/Fair/Poor)
- Data quality flags (invalid clicks/conversions)
- Profitability flag

**Grain**: One row per campaign per day

**Downstream**: Used in `fact_performance` and aggregations

{% enddocs %}

{% docs dim_advertisers %}
## Core: Advertiser Dimension

**Type**: SCD Type 1 (overwrite)

**Purpose**: Clean advertiser master data for analytics

**Grain**: One row per advertiser

**Keys**: `advertiser_id` (Primary Key)

{% enddocs %}

{% docs dim_campaigns %}
## Core: Campaign Dimension

**Type**: SCD Type 1 (overwrite)

**Purpose**: Denormalized campaign dimension with advertiser context

**Grain**: One row per campaign

**Keys**: 
- `campaign_id` (Primary Key)
- `advertiser_id` (Foreign Key)

**Denormalization**: Includes advertiser attributes for query performance

{% enddocs %}

{% docs fact_performance %}
## Core: Performance Fact Table

**Type**: Incremental fact table

**Purpose**: Daily campaign performance with all KPIs

**Grain**: One row per campaign per day

**Keys**:
- `performance_id` (Primary Key)
- `campaign_id` (Foreign Key to dim_campaigns)
- `performance_date` (Date dimension)

**Incremental Strategy**: Only process records newer than max `last_updated_at`

**Metrics**:
- Raw: impressions, clicks, conversions, cost, revenue
- Calculated: CTR, CVR, CPC, CPM, CPA, ROI, profit

{% enddocs %}

{% docs campaign_performance_summary %}
## Analytics: Campaign Performance Summary

**Purpose**: Pre-aggregated campaign metrics for dashboards

**Grain**: One row per campaign (lifetime aggregation)

**Contains**:
- Total metrics (impressions, clicks, conversions, cost, revenue)
- Average KPIs (CTR, CVR, CPC, CPA)
- Performance tier classification
- Budget utilization percentage

**Usage**: Power BI, Tableau, Looker dashboards

{% enddocs %}