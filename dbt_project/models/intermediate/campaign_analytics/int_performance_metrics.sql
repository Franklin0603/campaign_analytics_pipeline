{{
    config(
        materialized='view',
        schema='intermediate',
        tags=['intermediate']
    )
}}

/*
    Intermediate model: Performance with calculated metrics
    
    Purpose: Add all KPI calculations (CTR, CVR, ROI, etc.)
    Source: stg_performance
    Grain: One row per campaign per day
*/

with performance as (

    select * from {{ ref('stg_performance') }}

),

calculated_metrics as (

    select
        -- Identifiers
        performance_id,
        campaign_id,
        performance_date,
        
        -- Raw metrics
        impressions,
        clicks,
        conversions,
        cost_usd,
        revenue_usd,
        
        -- Click metrics
        case 
            when impressions > 0 
            then round((clicks::numeric / impressions::numeric) * 100, 4)
            else 0 
        end as ctr_percent,
        
        -- Conversion metrics
        case 
            when clicks > 0 
            then round((conversions::numeric / clicks::numeric) * 100, 4)
            else 0 
        end as cvr_percent,
        
        -- Cost metrics
        case 
            when clicks > 0 
            then round(cost_usd::numeric / clicks::numeric, 2)
            else 0 
        end as cpc_usd,
        
        case 
            when impressions > 0 
            then round((cost_usd::numeric / impressions::numeric) * 1000, 2)
            else 0 
        end as cpm_usd,
        
        case 
            when conversions > 0 
            then round(cost_usd::numeric / conversions::numeric, 2)
            else 0 
        end as cpa_usd,
        
        -- Revenue metrics
        round((revenue_usd - cost_usd)::numeric, 2) as profit_usd,
        
        case 
            when cost_usd > 0 
            then round(((revenue_usd - cost_usd)::numeric / cost_usd::numeric) * 100, 2)
            else 0 
        end as roi_percent,
        
        case 
            when revenue_usd > 0 
            then round((cost_usd::numeric / revenue_usd::numeric) * 100, 2)
            else 0 
        end as cost_revenue_ratio_percent,
        
        -- Value metrics
        case 
            when conversions > 0 
            then round(revenue_usd::numeric / conversions::numeric, 2)
            else 0 
        end as revenue_per_conversion_usd,
        
        case 
            when clicks > 0 
            then round(revenue_usd::numeric / clicks::numeric, 2)
            else 0 
        end as revenue_per_click_usd,
        
        -- Efficiency flags
        case 
            when clicks > impressions then true 
            else false 
        end as has_invalid_clicks,
        
        case 
            when conversions > clicks then true 
            else false 
        end as has_invalid_conversions,
        
        case 
            when revenue_usd > cost_usd then true 
            else false 
        end as is_profitable,
        
        -- Performance tier
        case 
            when cost_usd > 0 and ((revenue_usd - cost_usd)::numeric / cost_usd::numeric) * 100 >= 100 then 'Excellent'
            when cost_usd > 0 and ((revenue_usd - cost_usd)::numeric / cost_usd::numeric) * 100 >= 50 then 'Good'
            when cost_usd > 0 and ((revenue_usd - cost_usd)::numeric / cost_usd::numeric) * 100 >= 0 then 'Fair'
            when cost_usd > 0 then 'Poor'
            else 'Unknown'
        end as performance_tier,
        
        -- Metadata
        silver_loaded_at

    from performance

)

select * from calculated_metrics