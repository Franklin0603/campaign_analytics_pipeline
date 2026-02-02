{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging']
    )
}}

/*
    Staging model for campaigns
    
    Purpose: Light transformation - standardize column names
    Source: silver.campaigns
    Grain: One row per campaign
*/

with source as (

    select * from {{ source('silver', 'campaigns') }}

),

renamed as (

    select
        -- Primary key
        campaign_id,
        
        -- Foreign keys
        advertiser_id,
        
        -- Attributes
        campaign_name,
        campaign_type,
        status as campaign_status,
        objective as campaign_objective,
        
        -- Dates
        start_date as campaign_start_date,
        end_date as campaign_end_date,
        
        -- Financial
        budget_daily as daily_budget_usd,
        budget_total as total_budget_usd,
        
        -- Metadata
        _silver_processed_at as silver_loaded_at

    from source

)

select * from renamed