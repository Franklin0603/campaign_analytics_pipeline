{{
    config(
        materialized='view',
        schema='analytics',
        tags=['marts', 'analytics', 'reporting']
    )
}}

/*
    Analytics Mart: Campaign Performance Summary
    
    Purpose: Aggregated campaign-level metrics for reporting/BI
    Source: fact_performance + dim_campaigns + dim_advertisers
    Grain: One row per campaign (all-time aggregated)
*/

with performance as (

    select * from {{ ref('fact_performance') }}

),

campaigns as (

    select * from {{ ref('dim_campaigns') }}

),

advertisers as (

    select * from {{ ref('dim_advertisers') }}

),

campaign_aggregates as (

    select
        p.campaign_id,
        
        -- Aggregated metrics
        sum(p.impressions) as total_impressions,
        sum(p.clicks) as total_clicks,
        sum(p.conversions) as total_conversions,
        sum(p.cost_usd) as total_cost_usd,
        sum(p.revenue_usd) as total_revenue_usd,
        sum(p.profit_usd) as total_profit_usd,
        
        -- Calculated aggregate KPIs
        case 
            when sum(p.impressions) > 0 
            then round((sum(p.clicks)::numeric / sum(p.impressions)::numeric) * 100, 2)
            else 0 
        end as avg_ctr_percent,
        
        case 
            when sum(p.clicks) > 0 
            then round((sum(p.conversions)::numeric / sum(p.clicks)::numeric) * 100, 2)
            else 0 
        end as avg_cvr_percent,
        
        case 
            when sum(p.clicks) > 0 
            then round(sum(p.cost_usd)::numeric / sum(p.clicks)::numeric, 2)
            else 0 
        end as avg_cpc_usd,
        
        case 
            when sum(p.conversions) > 0 
            then round(sum(p.cost_usd)::numeric / sum(p.conversions)::numeric, 2)
            else 0 
        end as avg_cpa_usd,
        
        case 
            when sum(p.cost_usd) > 0 
            then round((sum(p.profit_usd)::numeric / sum(p.cost_usd)::numeric) * 100, 2)
            else 0 
        end as total_roi_percent,
        
        -- Activity metrics
        count(distinct p.performance_date) as days_active,
        min(p.performance_date) as first_activity_date,
        max(p.performance_date) as last_activity_date

    from performance p
    group by p.campaign_id

),

final as (

    select
        -- Campaign info
        c.campaign_id,
        c.campaign_name,
        c.campaign_type,
        c.campaign_status,
        c.campaign_objective,
        
        -- Advertiser info
        a.advertiser_id,
        a.advertiser_name,
        a.industry,
        a.country_code,
        a.account_manager,
        
        -- Campaign details
        c.campaign_start_date,
        c.campaign_end_date,
        c.campaign_duration_days,
        c.daily_budget_usd,
        c.total_budget_usd,
        c.campaign_current_state,
        
        -- Aggregated performance
        ca.total_impressions,
        ca.total_clicks,
        ca.total_conversions,
        ca.total_cost_usd,
        ca.total_revenue_usd,
        ca.total_profit_usd,
        
        -- KPIs
        ca.avg_ctr_percent,
        ca.avg_cvr_percent,
        ca.avg_cpc_usd,
        ca.avg_cpa_usd,
        ca.total_roi_percent,
        
        -- Activity
        ca.days_active,
        ca.first_activity_date,
        ca.last_activity_date,
        
        -- Performance tier (based on ROI)
        case 
            when ca.total_roi_percent >= 100 then 'Excellent'
            when ca.total_roi_percent >= 50 then 'Good'
            when ca.total_roi_percent >= 0 then 'Fair'
            when ca.total_roi_percent < 0 then 'Poor'
            else 'Unknown'
        end as performance_tier,
        
        -- Budget efficiency
        case 
            when ca.total_revenue_usd > 0 
            then round((ca.total_cost_usd::numeric / ca.total_revenue_usd::numeric) * 100, 2)
            else null 
        end as cost_revenue_ratio_percent,
        
        -- Budget utilization
        case 
            when c.total_budget_usd > 0 
            then round((ca.total_cost_usd::numeric / c.total_budget_usd::numeric) * 100, 2)
            else 0 
        end as budget_utilization_percent

    from campaign_aggregates ca
    inner join campaigns c on ca.campaign_id = c.campaign_id
    inner join advertisers a on c.advertiser_id = a.advertiser_id

)

select * from final
order by total_revenue_usd desc
