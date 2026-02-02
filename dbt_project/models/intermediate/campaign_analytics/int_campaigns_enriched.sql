{{
    config(
        materialized='view',
        schema='intermediate',
        tags=['intermediate']
    )
}}

/*
    Intermediate model: Campaigns enriched with advertiser data
    
    Purpose: Join campaigns with advertisers, add derived fields
    Source: stg_campaigns + stg_advertisers
    Grain: One row per campaign
*/

with campaigns as (

    select * from {{ ref('stg_campaigns') }}

),

advertisers as (

    select * from {{ ref('stg_advertisers') }}

),

joined as (

    select
        -- Campaign identifiers
        c.campaign_id,
        c.campaign_name,
        c.campaign_type,
        c.campaign_status,
        c.campaign_objective,
        
        -- Advertiser info
        c.advertiser_id,
        a.advertiser_name,
        a.industry,
        a.country_code,
        a.account_manager,
        
        -- Campaign dates
        c.campaign_start_date,
        c.campaign_end_date,
        
        -- Derived date fields
        (c.campaign_end_date - c.campaign_start_date) as campaign_duration_days,
        
        -- Budget info
        c.daily_budget_usd,
        c.total_budget_usd,
        
        -- Campaign state logic
        case
            when c.campaign_status = 'Active' 
                and current_date between c.campaign_start_date and c.campaign_end_date 
                then 'Running'
            when c.campaign_status = 'Active' 
                and current_date < c.campaign_start_date 
                then 'Scheduled'
            when c.campaign_status = 'Active' 
                and current_date > c.campaign_end_date 
                then 'Ended'
            when c.campaign_status = 'Paused' then 'Paused'
            when c.campaign_status = 'Completed' then 'Completed'
            when c.campaign_status = 'Draft' then 'Draft'
            else 'Unknown'
        end as campaign_current_state,
        
        -- Time-based flags
        case 
            when current_date between c.campaign_start_date and c.campaign_end_date 
            then true 
            else false 
        end as is_currently_active,
        
        case 
            when current_date > c.campaign_end_date 
            then true 
            else false 
        end as has_ended,
        
        -- Budget velocity
        case 
            when (c.campaign_end_date - c.campaign_start_date) > 0
            then c.total_budget_usd / (c.campaign_end_date - c.campaign_start_date)
            else c.daily_budget_usd
        end as expected_daily_spend,
        
        -- Metadata
        c.silver_loaded_at

    from campaigns c
    inner join advertisers a 
        on c.advertiser_id = a.advertiser_id

)

select * from joined