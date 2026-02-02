{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging']
    )
}}

/*
    Staging model for performance
    
    Purpose: Standardize column names, no calculations
    Source: silver.performance
    Grain: One row per campaign per day
*/

with source as (

    select * from {{ source('silver', 'performance') }}

),

renamed as (

    select
        -- Primary key
        performance_id,
        
        -- Foreign keys
        campaign_id,
        
        -- Dimensions
        date as performance_date,
        
        -- Metrics (raw, no calculations)
        impressions,
        clicks,
        conversions,
        cost as cost_usd,
        revenue as revenue_usd,
        
        -- Metadata
        _silver_processed_at as silver_loaded_at

    from source

)

select * from renamed