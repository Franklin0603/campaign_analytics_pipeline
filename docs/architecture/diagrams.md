# Campaign Analytics Pipeline - Architecture Diagrams

> **Visual documentation of the end-to-end data pipeline**

---

## 📊 Table of Contents

1. [End-to-End Pipeline Architecture](#1-end-to-end-pipeline-architecture)
2. [dbt DAG (Lineage Graph)](#2-dbt-dag-lineage-graph)
3. [Star Schema - Dimensional Model](#3-star-schema---dimensional-model)
4. [Data Flow by Layer](#4-data-flow-by-layer)
5. [CI/CD Pipeline](#5-cicd-pipeline)
6. [Test Coverage Architecture](#6-test-coverage-architecture)
7. [Data Refresh Schedule](#7-data-refresh-schedule)
8. [Macro Usage Architecture](#8-macro-usage-architecture)
9. [Staging Layer Lineage](#9-staging-layer-lineage)
10. [Intermediate Layer Lineage](#10-intermediate-layer-lineage)
11. [Marts Layer Lineage](#11-marts-layer-lineage)
12. [Test Distribution](#12-test-distribution)

---

## 1. End-to-End Pipeline Architecture
```mermaid
graph TB
    subgraph "Data Sources"
        CSV1[📄 campaigns.csv]
        CSV2[📄 performance.csv]
        CSV3[📄 advertisers.csv]
    end
    
    subgraph "Bronze Layer - PySpark"
        B1[🔶 raw_campaigns]
        B2[🔶 raw_performance]
        B3[🔶 raw_advertisers]
    end
    
    subgraph "Silver Layer - PySpark + Great Expectations"
        S1[⚪ campaigns]
        S2[⚪ performance]
        S3[⚪ advertisers]
        GE[✓ Data Quality Validation]
    end
    
    subgraph "Gold Layer - dbt"
        subgraph "Staging"
            ST1[📋 stg_campaigns]
            ST2[📋 stg_performance]
            ST3[📋 stg_advertisers]
        end
        
        subgraph "Intermediate"
            INT1[⚙️ int_campaigns_enriched]
            INT2[⚙️ int_performance_metrics]
        end
        
        subgraph "Marts - Core"
            DIM1[🔷 dim_campaigns]
            DIM2[🔷 dim_advertisers]
            FACT[📊 fact_performance]
        end
        
        subgraph "Marts - Analytics"
            AGG[📈 campaign_performance_summary]
        end
        
        subgraph "Snapshots"
            SNAP1[📸 campaigns_snapshot]
            SNAP2[📸 advertisers_snapshot]
        end
    end
    
    subgraph "Consumption Layer"
        BI1[📊 Tableau]
        BI2[📊 Looker]
        BI3[📊 Excel]
    end
    
    CSV1 --> B1
    CSV2 --> B2
    CSV3 --> B3
    
    B1 --> S1
    B2 --> S2
    B3 --> S3
    
    S1 --> GE
    S2 --> GE
    S3 --> GE
    
    S1 --> ST1
    S2 --> ST2
    S3 --> ST3
    
    ST1 --> INT1
    ST3 --> INT1
    ST2 --> INT2
    
    INT1 --> DIM1
    ST3 --> DIM2
    INT2 --> FACT
    
    FACT --> AGG
    DIM1 --> AGG
    
    ST1 --> SNAP1
    ST3 --> SNAP2
    
    DIM1 --> BI1
    DIM2 --> BI1
    FACT --> BI1
    AGG --> BI2
    AGG --> BI3
    
    style B1 fill:#ff9800
    style B2 fill:#ff9800
    style B3 fill:#ff9800
    style S1 fill:#9e9e9e
    style S2 fill:#9e9e9e
    style S3 fill:#9e9e9e
    style ST1 fill:#4caf50
    style ST2 fill:#4caf50
    style ST3 fill:#4caf50
    style INT1 fill:#2196f3
    style INT2 fill:#2196f3
    style DIM1 fill:#9c27b0
    style DIM2 fill:#9c27b0
    style FACT fill:#f44336
    style AGG fill:#00bcd4
```

---

## 2. dbt DAG (Lineage Graph)
```mermaid
graph LR
    subgraph Sources
        src1[(silver.campaigns)]
        src2[(silver.performance)]
        src3[(silver.advertisers)]
    end
    
    subgraph Staging
        stg1[stg_campaigns]
        stg2[stg_performance]
        stg3[stg_advertisers]
    end
    
    subgraph Intermediate
        int1[int_campaigns_enriched]
        int2[int_performance_metrics]
    end
    
    subgraph Marts
        dim1[dim_campaigns]
        dim2[dim_advertisers]
        fact1[fact_performance]
        agg1[campaign_performance_summary]
    end
    
    subgraph Snapshots
        snap1[campaigns_snapshot]
        snap2[advertisers_snapshot]
    end
    
    src1 --> stg1
    src2 --> stg2
    src3 --> stg3
    
    stg1 --> int1
    stg3 --> int1
    stg2 --> int2
    
    int1 --> dim1
    stg3 --> dim2
    int2 --> fact1
    
    fact1 --> agg1
    dim1 --> agg1
    
    stg1 --> snap1
    stg3 --> snap2
    
    style stg1 fill:#4caf50
    style stg2 fill:#4caf50
    style stg3 fill:#4caf50
    style int1 fill:#2196f3
    style int2 fill:#2196f3
    style dim1 fill:#9c27b0
    style dim2 fill:#9c27b0
    style fact1 fill:#f44336
    style agg1 fill:#00bcd4
```

---

## 3. Star Schema - Dimensional Model
```mermaid
erDiagram
    dim_campaigns ||--o{ fact_performance : "campaign_id"
    dim_advertisers ||--o{ dim_campaigns : "advertiser_id"
    
    dim_advertisers {
        int advertiser_id PK
        varchar advertiser_name
        varchar industry
        varchar country_code
        varchar account_manager
    }
    
    dim_campaigns {
        int campaign_id PK
        int advertiser_id FK
        varchar campaign_name
        varchar campaign_type
        varchar campaign_status
        date campaign_start_date
        date campaign_end_date
        decimal daily_budget_usd
        decimal total_budget_usd
    }
    
    fact_performance {
        int performance_id PK
        int campaign_id FK
        date performance_date
        bigint impressions
        bigint clicks
        bigint conversions
        decimal cost_usd
        decimal revenue_usd
        decimal ctr_percent
        decimal cvr_percent
        decimal cpc_usd
        decimal roi_percent
    }
```

---

## 4. Data Flow by Layer
```mermaid
flowchart TD
    subgraph Bronze["🔶 Bronze Layer - Raw Data Preservation"]
        B1[All columns as STRING]
        B2[No transformations]
        B3[Append-only writes]
        B4[Metadata tracking]
    end
    
    subgraph Silver["⚪ Silver Layer - Cleaned & Validated"]
        S1[Type casting]
        S2[Deduplication]
        S3[Business rules]
        S4[Quality checks]
        S5[Derived fields]
    end
    
    subgraph Gold["🟡 Gold Layer - Analytics Ready"]
        G1[Staging: 1:1 transformation]
        G2[Intermediate: Business logic]
        G3[Marts: Star schema]
        G4[Tests: 102 validations]
    end
    
    Bronze --> Silver
    Silver --> Gold
    
    style Bronze fill:#ff9800,color:#000
    style Silver fill:#9e9e9e,color:#000
    style Gold fill:#ffd700,color:#000
```

---

## 5. CI/CD Pipeline
```mermaid
flowchart LR
    A[💻 Git Push] --> B{GitHub Actions}
    
    B --> C[🐍 Python Lint]
    B --> D[📝 SQL Lint]
    B --> E[🔧 dbt CI]
    
    E --> E1[🐘 Start PostgreSQL]
    E1 --> E2[📊 Generate Sample Data]
    E2 --> E3[⬆️ Load to Silver]
    E3 --> E4[🔨 dbt compile]
    E4 --> E5[▶️ dbt run<br/>9 models]
    E5 --> E6[✅ dbt test<br/>102 tests]
    E6 --> E7[📚 dbt docs generate]
    
    C --> F{All Pass?}
    D --> F
    E7 --> F
    
    F -->|✅ Yes| G[🟢 Merge to Main]
    F -->|❌ No| H[🔴 Block Merge]
    
    style A fill:#4caf50
    style G fill:#4caf50
    style H fill:#f44336
    style F fill:#ff9800
```

---

## 6. Test Coverage Architecture
```mermaid
mindmap
    root((102 Tests))
        Source Tests
            Uniqueness 3
            Not Null 27
            Freshness Checks
        Model Tests
            Primary Keys 9
            Foreign Keys 2
            Relationships 2
        Custom Generic Tests
            is_percentage
            is_non_negative
            clicks_lte_impressions
            conversions_lte_clicks
            end_date_after_start_date
            recency
        Custom Singular Tests
            assert_no_future_performance_dates
            assert_campaigns_have_performance
            assert_budget_consistency
            assert_performance_metrics_consistent
        Accepted Values
            campaign_status
            campaign_type
            performance_tier
```

---

## 7. Data Refresh Schedule
```mermaid
gantt
    title Daily Data Refresh Schedule (UTC)
    dateFormat HH:mm
    axisFormat %H:%M
    
    section Bronze Layer
    PySpark Ingestion           :active, bronze, 01:00, 20m
    
    section Silver Layer
    PySpark Transformation      :active, silver, 01:20, 30m
    Great Expectations          :active, ge, 01:50, 10m
    
    section Gold - Staging
    stg_campaigns              :active, stg1, 02:00, 3s
    stg_performance            :active, stg2, 02:00, 3s
    stg_advertisers            :active, stg3, 02:00, 3s
    
    section Gold - Intermediate
    int_campaigns_enriched     :active, int1, 02:01, 5s
    int_performance_metrics    :active, int2, 02:01, 10s
    
    section Gold - Marts
    dim_advertisers            :active, dim1, 02:02, 3s
    dim_campaigns              :active, dim2, 02:02, 5s
    fact_performance           :active, fact1, 02:02, 20s
    campaign_performance_summary :active, agg1, 02:02, 5s
    
    section Snapshots
    campaigns_snapshot         :active, snap1, 02:03, 10s
    advertisers_snapshot       :active, snap2, 02:03, 5s
```

---

## 8. Macro Usage Architecture
```mermaid
graph TB
    subgraph "Custom Macros - 6 Total"
        M1[safe_divide]
        M2[calculate_campaign_kpis]
        M3[get_performance_tier]
        M4[cents_to_dollars]
        M5[date_spine]
        M6[generate_schema_name]
    end
    
    subgraph "Models Using Macros"
        INT2[int_performance_metrics]
        FACT[fact_performance]
        AGG[campaign_performance_summary]
    end
    
    M1 --> INT2
    M1 --> FACT
    M2 --> INT2
    M3 --> INT2
    M3 --> FACT
    
    INT2 -.40% code reduction.-> CodeReuse[📉 DRY Principles Applied]
    
    style M1 fill:#2196f3
    style M2 fill:#2196f3
    style M3 fill:#2196f3
    style CodeReuse fill:#4caf50
```

---

## 9. Staging Layer Lineage
```mermaid
graph LR
    S1[(silver.campaigns<br/>PySpark Output)] --> STG1[stg_campaigns<br/>View]
    S2[(silver.performance<br/>PySpark Output)] --> STG2[stg_performance<br/>View]
    S3[(silver.advertisers<br/>PySpark Output)] --> STG3[stg_advertisers<br/>View]
    
    STG1 --> |Column Rename|C1[campaign_start_date<br/>campaign_end_date<br/>daily_budget_usd]
    STG2 --> |Column Rename|C2[performance_date<br/>cost_usd<br/>revenue_usd]
    STG3 --> |Column Rename|C3[country_code<br/>UPPER transformation]
    
    style S1 fill:#9e9e9e
    style S2 fill:#9e9e9e
    style S3 fill:#9e9e9e
    style STG1 fill:#4caf50
    style STG2 fill:#4caf50
    style STG3 fill:#4caf50
```

---

## 10. Intermediate Layer Lineage
```mermaid
graph TB
    STG1[stg_campaigns] --> INT1[int_campaigns_enriched]
    STG3[stg_advertisers] --> INT1
    
    INT1 --> |Derived Fields|D1[campaign_duration_days<br/>campaign_current_state<br/>is_currently_active<br/>expected_daily_spend]
    
    STG2[stg_performance] --> INT2[int_performance_metrics]
    
    INT2 --> |KPI Calculations|D2[ctr_percent = clicks/impressions<br/>cvr_percent = conversions/clicks<br/>cpc_usd = cost/clicks<br/>roi_percent = revenue-cost/cost<br/>performance_tier = CASE WHEN...]
    
    M1[Macro: safe_divide] -.Used by.-> INT2
    M2[Macro: calculate_campaign_kpis] -.Used by.-> INT2
    M3[Macro: get_performance_tier] -.Used by.-> INT2
    
    style STG1 fill:#4caf50
    style STG2 fill:#4caf50
    style STG3 fill:#4caf50
    style INT1 fill:#2196f3
    style INT2 fill:#2196f3
    style M1 fill:#ff9800
    style M2 fill:#ff9800
    style M3 fill:#ff9800
```

---

## 11. Marts Layer Lineage
```mermaid
graph TB
    subgraph Dimensions
        STG3[stg_advertisers] --> DIM2[dim_advertisers<br/>Table]
        INT1[int_campaigns_enriched] --> DIM1[dim_campaigns<br/>Table]
    end
    
    subgraph Facts
        INT2[int_performance_metrics] --> FACT[fact_performance<br/>Incremental Table]
    end
    
    subgraph Analytics
        FACT --> AGG[campaign_performance_summary<br/>View]
        DIM1 --> AGG
        DIM2 -.Join via DIM1.-> AGG
    end
    
    AGG --> |Aggregations|A1[SUM cost_usd<br/>SUM revenue_usd<br/>Total ROI calculation<br/>Performance tier]
    
    style DIM1 fill:#9c27b0
    style DIM2 fill:#9c27b0
    style FACT fill:#f44336
    style AGG fill:#00bcd4
```

---

## 12. Test Distribution
```mermaid
pie title "102 Total Tests by Category"
    "Source Not Null" : 27
    "Accepted Values" : 27
    "Custom Generic" : 30
    "Primary Keys" : 9
    "Relationships" : 2
    "Singular Tests" : 4
    "Source Uniqueness" : 3
```

---

## 📝 Notes

- All diagrams render automatically on GitHub using Mermaid
- Color coding: Bronze (🔶), Silver (⚪), Staging (🟢), Intermediate (🔵), Marts (🟣/🔴), Analytics (🔵)
- For interactive lineage, run `dbt docs serve` and view the DAG