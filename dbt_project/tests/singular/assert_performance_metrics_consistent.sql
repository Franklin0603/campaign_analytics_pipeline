/*
    Singular Test: assert_performance_metrics_consistent
    
    Purpose: Validate that calculated CTR matches manual calculation
    
    Test fails if CTR calculation is off by more than 0.01%
*/

with performance_with_manual_ctr as (
    select
        performance_id,
        ctr_percent as calculated_ctr,
        case 
            when impressions > 0 
            then round((clicks::numeric / impressions::numeric) * 100, 4)
            else 0 
        end as manual_ctr
    from {{ ref('int_performance_metrics') }}
    where impressions > 0
)

select
    performance_id,
    calculated_ctr,
    manual_ctr,
    abs(calculated_ctr - manual_ctr) as difference
from performance_with_manual_ctr
where abs(calculated_ctr - manual_ctr) > 0.01