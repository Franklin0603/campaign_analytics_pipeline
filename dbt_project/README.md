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

## 📊 Project Overview

This dbt project transforms cleaned data from the Silver layer (PySpark) into analytics-ready dimensional models following the **Kimball methodology**.

### Quick Stats

| Metric | Count |
|--------|-------|
| **Models** | 9 |
| **Tests** | 102 |
| **Macros** | 6 |
| **Snapshots** | 2 |
| **Sources** | 3 |
| **Test Coverage** | 100% |

---


### Layer Breakdown

**Staging** (3 models)
- 1:1 with source tables
- Column renaming and light transformations
- Materialized as **views**

**Intermediate** (2 models)
- Business logic and joins
- KPI calculations
- Materialized as **views**

**Marts** (4 models)
- Star schema design
- Dimensions and facts
- Materialized as **tables** (incremental for facts)

---

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

## 📁 Project Structure
```
dbt_project/
├── models/
│   ├── staging/              # 3 models - Source data staging
│   │   ├── stg_campaigns.sql
│   │   ├── stg_performance.sql
│   │   └── stg_advertisers.sql
│   │
│   ├── intermediate/         # 2 models - Business logic
│   │   ├── int_campaigns_enriched.sql
│   │   └── int_performance_metrics.sql
│   │
│   └── marts/
│       ├── core/             # 3 models - Star schema
│       │   ├── dim_campaigns.sql
│       │   ├── dim_advertisers.sql
│       │   └── fact_performance.sql
│       │
│       └── analytics/        # 1 model - Aggregates
│           └── campaign_performance_summary.sql
│
├── macros/                   # 6 custom macros
│   ├── safe_divide.sql
│   ├── calculate_campaign_kpis.sql
│   ├── get_performance_tier.sql
│   ├── cents_to_dollars.sql
│   ├── date_spine.sql
│   └── generate_schema_name.sql
│
├── tests/
│   ├── generic/              # 6 custom generic tests
│   └── singular/             # 4 singular tests
│
├── snapshots/                # 2 SCD Type 2 snapshots
│   ├── campaigns_snapshot.sql
│   └── advertisers_snapshot.sql
│
└── docs/                     # Documentation
    ├── macros.md
    ├── tests.md
    └── dag.md
```
---

## 🎯 Key Features

### 1. Dimensional Modeling
- **Star Schema** with 2 dimensions and 1 fact table
- **Conformed Dimensions** for consistent reporting
- **Incremental Facts** for performance optimization

### 2. Data Quality
- **102 comprehensive tests** across all layers
- **Custom generic tests** for reusable validation
- **Singular tests** for business-specific rules

### 3. Code Reusability
- **6 custom macros** reducing code duplication by 40%
- **DRY principles** applied throughout
- **Centralized business logic**

### 4. Historical Tracking
- **SCD Type 2 snapshots** for campaigns and advertisers
- **Point-in-time analysis** capability
- **Audit trail** for compliance

---

## 📊 Data Model

### Star Schema
```
       dim_advertisers
              │
              │ (1:N)
              ↓
       dim_campaigns ←─── (1:N) ──→ fact_performance
```

**Grain:**
- `dim_advertisers`: One row per advertiser
- `dim_campaigns`: One row per campaign
- `fact_performance`: One row per campaign per day

---

## 🧪 Testing Strategy

### Test Categories

| Category | Count | Purpose |
|----------|-------|---------|
| Source Tests | 30 | Validate silver layer data |
| Primary Keys | 9 | Ensure uniqueness |
| Foreign Keys | 2 | Referential integrity |
| Custom Generic | 30 | Reusable business rules |
| Singular Tests | 4 | Specific validations |
| Accepted Values | 27 | Enum validation |

**Total: 102 tests** with 100% coverage

---

## 🔄 Refresh Schedule

| Layer | Frequency | Time (UTC) | Runtime |
|-------|-----------|------------|---------|
| Staging | Daily | 2:00 AM | ~10s |
| Intermediate | Daily | 2:01 AM | ~30s |
| Marts | Daily | 2:02 AM | ~60s |
| Snapshots | Daily | 2:03 AM | ~15s |

**Total Pipeline Runtime:** ~2 minutes

---

## 📚 Documentation

### In-Code Documentation
- **models/docs.md** - Detailed model descriptions
- **YAML files** - Column-level documentation
- **Overview docs** - Layer explanations

### Generated Documentation
```bash
dbt docs generate
dbt docs serve
```

Visit `http://localhost:8080` for interactive documentation with:
- DAG visualization
- Column lineage
- Test results
- Model descriptions

---

## 🛠️ Development

### Adding a New Model

1. **Create SQL file** in appropriate folder
2. **Add to YAML** with description and tests
3. **Write tests** for data quality
4. **Run and validate**:
```bash
   dbt run --select my_new_model
   dbt test --select my_new_model
```

### Modifying Existing Models

1. **Update SQL** file
2. **Update tests** if logic changed
3. **Run with full-refresh** if schema changed:
```bash
   dbt run --select my_model --full-refresh
```

### Best Practices

✅ **DO:**
- Use `ref()` for model dependencies
- Add tests to every model
- Document all columns
- Use macros for repeated logic
- Follow naming conventions

❌ **DON'T:**
- Hard-code table names
- Skip testing
- Use SELECT *
- Duplicate logic across models

---

## 🐛 Troubleshooting

### Common Issues

**Issue:** `Database Error: relation does not exist`
```bash
# Solution: Ensure silver layer is populated
dbt run --select staging.*
```

**Issue:** `Compilation Error`
```bash
# Solution: Clean and recompile
dbt clean
dbt deps
dbt compile
```

**Issue:** `Tests failing`
```bash
# Solution: Check specific test
dbt test --select test_name
```

---

## 📞 Support

**Owner:** Data Engineering Team  
**Contact:** data-eng@company.com  
**Slack:** #data-engineering

---

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

