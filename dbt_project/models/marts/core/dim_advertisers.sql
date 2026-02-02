{{
    config(
        materialized='table',
        schema='core',
        tags=['marts', 'core', 'dimension']
    )
}}

/*
    Dimension: Advertisers
    
    Purpose: Clean advertiser master dimension for analytics
    Source: stg_advertisers
    Type: SCD Type 1 (overwrite)
    Grain: One row per advertiser
*/

with advertisers as (
    select * from {{ ref('stg_advertisers') }}
),


final as (

    select
        -- Primary key
        advertiser_id,
        
        -- Attributes
        advertiser_name,
        industry,
        country_code,
        account_manager,
        
        -- Metadata
        silver_loaded_at as last_updated_at,
        current_timestamp as dbt_updated_at

    from advertisers

)

select * from final
