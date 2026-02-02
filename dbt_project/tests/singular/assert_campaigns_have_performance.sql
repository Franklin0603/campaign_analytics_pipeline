/*
    Singular Test: assert_campaigns_have_performance
    
    Purpose: Every active campaign should have at least one performance record
    
    Test fails if campaigns exist without performance data
*/

with campaigns as (
    select campaign_id
    from {{ ref('stg_campaigns') }}
    where campaign_status = 'Active'
),

performance as (
    select distinct campaign_id
    from {{ ref('stg_performance') }}
),

campaigns_without_performance as (
    select 
        c.campaign_id
    from campaigns c
    left join performance p 
        on c.campaign_id = p.campaign_id
    where p.campaign_id is null
)

select * from campaigns_without_performance