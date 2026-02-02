{{
    config(
        materialized='table',
        schema='core',
        tags=['marts', 'core', 'dimension']
    )
}}

/*
    Dimension: Campaigns
    
    Purpose: Campaign master dimension with enriched attributes
    Source: int_campaigns_enriched
    Type: SCD Type 1 (overwrite)
    Grain: One row per campaign
*/

with campaigns as (

    select * from {{ ref('int_campaigns_enriched') }}

),

final as (

    select
        -- Primary key
        campaign_id,
        
        -- Foreign keys
        advertiser_id,
        
        -- Campaign attributes
        campaign_name,
        campaign_type,
        campaign_status,
        campaign_objective,
        
        -- Advertiser attributes (denormalized for query performance)
        advertiser_name,
        industry,
        country_code,
        account_manager,
        
        -- Dates
        campaign_start_date,
        campaign_end_date,
        campaign_duration_days,
        
        -- Budget
        daily_budget_usd,
        total_budget_usd,
        expected_daily_spend,
        
        -- Derived fields
        campaign_current_state,
        is_currently_active,
        has_ended,
        
        -- Metadata
        silver_loaded_at as last_updated_at,
        current_timestamp as dbt_updated_at

    from campaigns

)

select * from final