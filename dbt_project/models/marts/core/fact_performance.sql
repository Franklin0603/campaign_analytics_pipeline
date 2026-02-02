{{
    config(
        materialized='incremental',
        unique_key='performance_id',
        schema='core',
        tags=['marts', 'core', 'fact'],
        on_schema_change='append_new_columns'
    )
}}

/*
    Fact: Daily Campaign Performance
    
    Purpose: Daily performance metrics with all KPIs
    Source: int_performance_metrics
    Grain: One row per campaign per day
    Incremental: Yes (based on performance_date)
*/

with performance as (
    
    select * from {{ ref('int_performance_metrics') }}

    {% if is_incremental() %}
        -- only process new or updated records
        where silver_loaded_at > (select max(last_updated_at) from {{ this }})
    {% endif %}
),


final as (

    select
        -- Primary key
        performance_id,
        
        -- Foreign keys
        campaign_id,
        
        -- Date dimension
        performance_date,
        
        -- Raw metrics
        impressions,
        clicks,
        conversions,
        cost_usd,
        revenue_usd,
        
        -- Calculated KPIs
        ctr_percent,
        cvr_percent,
        cpc_usd,
        cpm_usd,
        cpa_usd,
        profit_usd,
        roi_percent,
        cost_revenue_ratio_percent,
        revenue_per_conversion_usd,
        revenue_per_click_usd,
        
        -- Business categorization
        performance_tier,
        is_profitable,
        
        -- Data quality flags
        has_invalid_clicks,
        has_invalid_conversions,
        
        -- Metadata
        silver_loaded_at as last_updated_at,
        current_timestamp as dbt_updated_at

    from performance

)

select * from final
