/*
    Singular Test: assert_budget_consistency
    
    Purpose: Daily budget should be less than or equal to total budget
    
    Test fails if daily_budget > total_budget
*/

select
    campaign_id,
    campaign_name,
    daily_budget_usd,
    total_budget_usd
from {{ ref('stg_campaigns') }}
where daily_budget_usd > total_budget_usd