{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging']
    )
}}

/*
    Staging model for advertisers
    
    Purpose: Light transformation - rename columns, standardize naming
    Source: silver.advertisers
    Grain: One row per advertiser
*/

with source as (

    select * from {{ source('silver', 'advertisers') }}

),

renamed as (

    select
        -- Primary key
        advertiser_id,
        
        -- Attributes
        advertiser_name,
        industry,
        country as country_code,
        account_manager,
        
        -- Metadata
        _silver_processed_at as silver_loaded_at

    from source

)

select * from renamed